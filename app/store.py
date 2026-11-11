"""有界落盘存储与恢复。

每个分区一个目录::

    runtime/<partition>/
        data.ndjson    只追加的原始记录（每行一条，含换行）
        index.bin      固定宽度索引：uint32 长度 + uint64 offset + 32B 摘要
        state.json     分区状态（RUNNING/STOPPED、确认位置、冲突指纹）

提交顺序严格为：数据追加并 fsync → 索引追加并 fsync → 更新清单并 fsync。
因此清单里的 confirmed_offset 一定指向已经持久化的连续记录；进程在任何
一步崩溃，重启时都以清单为准截断未提交的尾部，不会产生业务重复。
"""

from __future__ import annotations

import json
import os
import struct
import threading
from dataclasses import dataclass
from pathlib import Path

from .codec import Record

# 长度(4) + offset(8) + sha256(32) = 44 字节定长，便于按位置直接寻址
_INDEX_HEADER = struct.Struct("<IQ32s")
_INDEX_ENTRY_SIZE = _INDEX_HEADER.size


@dataclass
class PartitionState:
    partition: str
    status: str = "RUNNING"            # RUNNING / STOPPED
    confirmed_offset: int = -1         # 已持久化连续记录的最大 offset
    stop_reason: str | None = None
    conflict: dict | None = None       # 冲突指纹

    def to_json(self) -> dict:
        return {
            "status": self.status,
            "confirmed_offset": self.confirmed_offset,
            "stop_reason": self.stop_reason,
            "conflict": self.conflict,
        }

    @classmethod
    def from_json(cls, partition: str, value: dict) -> "PartitionState":
        return cls(
            partition=partition,
            status=value.get("status", "RUNNING"),
            confirmed_offset=int(value.get("confirmed_offset", -1)),
            stop_reason=value.get("stop_reason"),
            conflict=value.get("conflict"),
        )


class PartitionStore:
    """单分区的只追加存储，线程内使用；跨线程由调用方加锁。"""

    def __init__(self, root: Path, partition: str) -> None:
        self.partition = partition
        self.dir = root / _safe_name(partition)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.data_path = self.dir / "data.ndjson"
        self.index_path = self.dir / "index.bin"
        self.state_path = self.dir / "state.json"
        self._data = self.data_path.open("ab")
        self._index = self.index_path.open("ab")
        self.state = self._load_state()
        # 已提交记录的 offset -> 摘要，用于识别同内容重传/异内容冲突
        self._digests: dict[int, bytes] = {}
        self._load_index_and_recover()

    # ---------- 恢复 ----------

    def _load_state(self) -> PartitionState:
        if self.state_path.exists():
            value = json.loads(self.state_path.read_text(encoding="utf-8"))
            return PartitionState.from_json(self.partition, value)
        return PartitionState(partition=self.partition)

    def _load_index_and_recover(self) -> None:
        raw = self.index_path.read_bytes() if self.index_path.exists() else b""
        entries: list[tuple[int, int, bytes]] = []
        for pos in range(0, len(raw) - _INDEX_ENTRY_SIZE + 1, _INDEX_ENTRY_SIZE):
            length, offset, digest = _INDEX_HEADER.unpack_from(raw, pos)
            entries.append((length, offset, digest))

        data = self.data_path.read_bytes() if self.data_path.exists() else b""
        committed: list[tuple[int, int, bytes, bytes]] = []
        cursor = 0
        for length, offset, digest in entries:
            end = cursor + length
            if end > len(data):
                # 索引有条目但数据没 fsync 完整：截断到最后一条完整记录
                break
            line = data[cursor:end]
            if line[-1:] != b"\n":
                break
            committed.append((length, offset, digest, line))
            cursor = end

        # 截断任何未完成提交的尾部
        if cursor != len(data):
            with self.data_path.open("r+b") as fix:
                fix.truncate(cursor)
        if len(committed) * _INDEX_ENTRY_SIZE != len(raw):
            with self.index_path.open("r+b") as fix:
                fix.truncate(len(committed) * _INDEX_ENTRY_SIZE)

        for _length, offset, digest, line in committed:
            self._digests[offset] = digest
        if committed:
            max_offset = max(offset for _l, offset, _d, _line in committed)
        else:
            max_offset = -1
        # 清单与索引不一致时以索引（真实持久化数据）为准
        if self.state.confirmed_offset != max_offset:
            self.state.confirmed_offset = max_offset
            self._persist_state_locked()

    # ---------- 写入 ----------

    def lookup_digest(self, offset: int) -> bytes | None:
        return self._digests.get(offset)

    def append_batch(self, records: list[Record]) -> None:
        """原子地持久化一批连续记录，完成后数据与索引都已 fsync。"""
        if not records:
            return
        data_blob = b"".join(r.raw + b"\n" for r in records)
        index_blob = bytearray()
        for record in records:
            index_blob += _INDEX_HEADER.pack(
                len(record.raw) + 1, record.offset, record.digest
            )
        self._data.write(data_blob)
        self._data.flush()
        os.fsync(self._data.fileno())
        self._index.write(index_blob)
        self._index.flush()
        os.fsync(self._index.fileno())
        for record in records:
            self._digests[record.offset] = record.digest

    def mark_confirmed(self, offset: int) -> None:
        self.state.confirmed_offset = offset
        self._persist_state_locked()

    def mark_stopped(self, reason: str, conflict: dict | None = None) -> None:
        self.state.status = "STOPPED"
        self.state.stop_reason = reason
        if conflict is not None:
            self.state.conflict = conflict
        self._persist_state_locked()

    def _persist_state_locked(self) -> None:
        tmp = self.state_path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(self.state.to_json(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(tmp, self.state_path)
        # 目录 fsync 保证 rename 落盘
        dir_fd = os.open(self.dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def persisted_lines(self) -> list[bytes]:
        """测试辅助：读取全部已落盘记录。"""
        if not self.data_path.exists():
            return []
        return self.data_path.read_bytes().splitlines()

    def close(self) -> None:
        self._data.close()
        self._index.close()


def _safe_name(partition: str) -> str:
    safe = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in partition)
    return f"p_{safe}"


class StoreBundle:
    """管理运行目录下全部分区的 :class:`PartitionStore` 与全局清单。"""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.manifest_path = self.root / "manifest.json"
        self._lock = threading.Lock()
        self._stores: dict[str, PartitionStore] = {}
        self._manifest_meta: dict = {}
        if self.manifest_path.exists():
            self._manifest_meta = json.loads(
                self.manifest_path.read_text(encoding="utf-8")
            )

    def open_partition(self, partition: str) -> PartitionStore:
        with self._lock:
            if partition not in self._stores:
                self._stores[partition] = PartitionStore(self.root, partition)
            return self._stores[partition]

    def commit_batch(
        self,
        store: PartitionStore,
        records: list[Record],
        max_offset: int,
        sync_hook=None,
    ) -> None:
        """数据/索引 fsync 后提交清单。

        数据与索引是分区独立文件，慢写入钩子不持全局锁；只有清单更新在
        锁内，因此单个分区的可控慢写不会冻结其他分区的提交进度。
        """
        if not records:
            return
        if sync_hook is not None:
            sync_hook(store.partition, len(records))
        store.append_batch(records)
        with self._lock:
            store.mark_confirmed(max_offset)
            self._write_manifest_locked()

    def mark_stopped(
        self,
        store: PartitionStore,
        reason: str,
        conflict: dict | None = None,
    ) -> None:
        with self._lock:
            store.mark_stopped(reason, conflict)
            self._write_manifest_locked()

    def _write_manifest_locked(self) -> None:
        payload = {
            "version": 1,
            "partitions": {
                name: store.state.to_json()
                for name, store in sorted(self._stores.items())
            },
            "meta": self._manifest_meta,
        }
        tmp = self.manifest_path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        os.replace(tmp, self.manifest_path)
        dir_fd = os.open(self.root, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                name: dict(store.state.to_json())
                for name, store in sorted(self._stores.items())
            }

    def close(self) -> None:
        with self._lock:
            for store in self._stores.values():
                store.close()
