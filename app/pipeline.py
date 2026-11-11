"""有界、可恢复的遥测采集管线。

线程拓扑::

    每个源连接： fetcher 线程 ──raw_queue──▶ decoder 线程
    每个分区：   writer 线程（来自一个或多个 decoder 的记录按分区路由）

背压链路：

* 原始压缩块、解压中的明文、待写记录全部向 :class:`BufferLedger` 申请配额，
  内存上限约束**累计在途字节**；
* 下游 :class:`WriteGate` 暂停 → 写队列积压 → 分区配额耗尽 → decoder 路由
  阻塞 → 源连接配额耗尽 → fetcher 暂停读取，背压一路传回源端；
* 每个分区独立配额，单个分区停住只会冻结它自己的额度，其他分区继续前进。

确认链路：数据+索引 fsync → 清单 fsync → 才调用源 acknowledge。因此源确认
位置只可能推进到已持久化的连续记录；提交后、确认前退出，重启时按摘要识别
同内容重传，不会产生业务重复。
"""

from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from .buffer import BufferLedger
from .codec import (
    CodecError,
    GzipStreamDecoder,
    InvalidRecordError,
    LineAssembler,
    LineTooLongError,
    CompressionError,
    Record,
    parse_record,
)
from .source import StreamSource
from .store import StoreBundle

# 队列只做线程交接，真正的内存约束由账本完成
_QUEUE_MAX = 8
_QUEUE_WAIT = 0.2


class DownstreamPaused(RuntimeError):
    """下游停写超时（只用于指标标注，不中断线程）。"""


@dataclass
class StreamConfig:
    name: str
    source: "ChunkSource"
    partitions: tuple[str, ...]
    account: str = ""

    def __post_init__(self) -> None:
        if not self.account:
            self.account = f"stream:{self.name}"


class ChunkSource:
    """分块源协议：``chunks()`` 产出压缩字节块，``acknowledge`` 记录确认。"""

    def chunks(self):  # pragma: no cover - 协议定义
        raise NotImplementedError

    def acknowledge(self, partition: str, offset: int) -> None:
        raise NotImplementedError


class FixtureSource(StreamSource, ChunkSource):
    """适配现有素材的分块源。

    真实 HTTP 适配由服务端按确认位置续传；本地重放时源可能从头重发，重传
    记录的识别与去重统一由 writer 基于 offset + 摘要完成，因此这里不需要
    在解压前过滤（也避免一次性解压来做过滤）。
    """

    def acknowledge(self, partition: str, offset: int) -> None:
        super().acknowledge(partition, offset)


class WriteGate:
    """下游停写开关：暂停期间所有分区写入线程在配额内等待，不丢数据。"""

    def __init__(self) -> None:
        self._open = threading.Event()
        self._open.set()
        self._paused_total = 0.0
        self._pause_started: float | None = None
        self._lock = threading.Lock()

    def pause(self) -> None:
        with self._lock:
            if self._pause_started is None:
                self._pause_started = time.monotonic()
        self._open.clear()

    def resume(self) -> None:
        with self._lock:
            if self._pause_started is not None:
                self._paused_total += time.monotonic() - self._pause_started
                self._pause_started = None
        self._open.set()

    def wait(self, stopped: threading.Event) -> bool:
        while not self._open.wait(_QUEUE_WAIT):
            if stopped.is_set():
                return False
        return True

    @property
    def is_open(self) -> bool:
        return self._open.is_set()

    def snapshot(self) -> dict:
        with self._lock:
            extra = (
                time.monotonic() - self._pause_started
                if self._pause_started is not None
                else 0.0
            )
            return {
                "open": self._open.is_set(),
                "paused_seconds": round(self._paused_total + extra, 6),
            }


@dataclass
class _StreamStats:
    name: str
    bytes_read: int = 0
    chunks: int = 0
    records_routed: int = 0
    unverified_dropped: int = 0
    finished: bool = False
    error: str | None = None


@dataclass
class _PartitionStats:
    partition: str
    enqueued: int = 0
    committed: int = 0
    duplicate_acks: int = 0
    conflicts: int = 0
    fatal: str | None = None
    last_ack_offset: int = -1
    write_batches: int = 0
    gate_waits: int = 0
    bytes_committed: int = 0


class _Coordinator:
    def __init__(
        self,
        ledger: BufferLedger,
        gate: WriteGate,
        on_crash_after_commit=None,
        sync_hook=None,
    ) -> None:
        self.ledger = ledger
        self.gate = gate
        self.stopped = threading.Event()
        self._lock = threading.Lock()
        self.stop_reasons: dict[str, str] = {}
        self.stopped_partitions: set[str] = set()
        self.stream_stats: dict[str, _StreamStats] = {}
        self.partition_stats: dict[str, _PartitionStats] = {}
        self.active_streams_per_partition: dict[str, int] = {}
        self.on_crash_after_commit = on_crash_after_commit
        self.sync_hook = sync_hook
        # 模拟“提交后、确认前崩溃”：钩子抛出的 BaseException 在此留存，
        # 由主线程 run() 重新抛出，避免被工作线程静默吞掉
        self.crash_exc: BaseException | None = None
        self._crash_lock = threading.Lock()

    def fail_stream(self, name: str, reason: str, partitions: tuple[str, ...]) -> None:
        with self._lock:
            stats = self.stream_stats[name]
            if stats.error is None:
                stats.error = reason
            self.stop_reasons[f"stream:{name}"] = reason
            for partition in partitions:
                self._stop_partition_locked(partition, reason)
        self.stopped.set()

    def fail_stream_partitions(
        self, name: str, reason: str, affected: tuple[str, ...]
    ) -> None:
        """流故障但只停止实际受影响的分区，其余健康分区继续前进。"""
        with self._lock:
            stats = self.stream_stats[name]
            if stats.error is None:
                stats.error = reason
            self.stop_reasons[f"stream:{name}"] = reason
            for partition in affected:
                self._stop_partition_locked(partition, reason)
        if affected:
            self.stopped.set()

    def fail_partition(self, partition: str, reason: str) -> None:
        with self._lock:
            self._stop_partition_locked(partition, reason)
        self.stopped.set()

    def _stop_partition_locked(self, partition: str, reason: str) -> None:
        if partition not in self.stopped_partitions:
            self.stopped_partitions.add(partition)
            self.stop_reasons[f"partition:{partition}"] = reason
            stats = self.partition_stats.get(partition)
            if stats is not None and stats.fatal is None:
                stats.fatal = reason

    def is_partition_stopped(self, partition: str) -> bool:
        with self._lock:
            return partition in self.stopped_partitions

    def stream_done(self, partition: str) -> bool:
        """返回该分区是否所有服务流都已结束，可给 writer 发结束标记。"""
        with self._lock:
            remaining = self.active_streams_per_partition.get(partition, 0) - 1
            self.active_streams_per_partition[partition] = max(remaining, 0)
            return self.active_streams_per_partition[partition] == 0

    def put_queue(self, q, item, account: str, size: int) -> bool:
        """带账本背压与关停感知的入队，失败时回收已申请的配额。"""
        self.ledger.acquire(account, size)
        while True:
            if self.stopped.is_set():
                self.ledger.release(account, size)
                return False
            try:
                q.put(item, timeout=_QUEUE_WAIT)
                return True
            except queue.Full:
                continue

    def route_record(self, q, record: Record, stream_account: str) -> bool:
        """把一条记录放入分区队列。

        所有权模型：记录在队列中期间**继续占用源连接账户**，不提前计入分区
        账户。这样配额只在“字节真正进入 writer 的 pending”时转移，writer 永远
        能从队列取到记录并在落盘后释放，避免“decoder 等分区配额、writer 等
        入队”的握手死锁。流账户的释放发生在 writer 取出（:meth:`transfer_in`）
        或拒绝路径，本方法不做任何 ledger 操作。
        """
        if self.is_partition_stopped(record.partition):
            return False
        while True:
            if self.stopped.is_set():
                return False
            try:
                q.put(record, timeout=_QUEUE_WAIT)
                return True
            except queue.Full:
                continue

    def transfer_in(self, record: Record, stream_account: str) -> None:
        """writer 从队列取出记录：字节从源连接账户转入分区账户。"""
        size = record.size() + 1
        partition_account = f"partition:{record.partition}"
        self.ledger.acquire(partition_account, size)
        self.ledger.release(stream_account, size)


class Collector:
    """采集器：组织 fetcher/decoder/writer 线程并暴露指标。"""

    def __init__(
        self,
        streams: list[StreamConfig],
        store_root: str | Path,
        memory_limit: int = 65536,
        partition_memory_limit: int | None = None,
        max_line_bytes: int = 4096,
        batch_records: int = 32,
        batch_bytes: int = 16384,
        gate: WriteGate | None = None,
        sync_hook=None,
        on_crash_after_commit=None,
    ) -> None:
        if not streams:
            raise ValueError("至少配置一个源连接")
        self.streams = streams
        self.store = StoreBundle(store_root)
        self.ledger = BufferLedger(memory_limit, partition_memory_limit)
        self.gate = gate or WriteGate()
        self.batch_records = batch_records
        self.batch_bytes = batch_bytes
        self.max_line_bytes = max_line_bytes
        self.coord = _Coordinator(
            self.ledger,
            self.gate,
            on_crash_after_commit=on_crash_after_commit,
            sync_hook=sync_hook,
        )
        self._threads: list[threading.Thread] = []
        self._raw_queues: dict[str, "queue.Queue"] = {}
        self._partition_queues: dict[str, "queue.Queue"] = {}
        self._stores: dict[str, object] = {}

        partitions: set[str] = set()
        for stream in streams:
            self.coord.stream_stats[stream.name] = _StreamStats(stream.name)
            for partition in stream.partitions:
                partitions.add(partition)
        for partition in sorted(partitions):
            self.coord.partition_stats[partition] = _PartitionStats(partition)
            self.coord.active_streams_per_partition[partition] = sum(
                1 for s in streams if partition in s.partitions
            )
            self._partition_queues[partition] = queue.Queue(_QUEUE_MAX)
            self._stores[partition] = self.store.open_partition(partition)
        for stream in streams:
            self._raw_queues[stream.name] = queue.Queue(_QUEUE_MAX)

    # ---------- 线程体 ----------

    def _fetcher(self, cfg: StreamConfig) -> None:
        stats = self.coord.stream_stats[cfg.name]
        raw_q = self._raw_queues[cfg.name]
        try:
            for chunk in cfg.source.chunks():
                if self.coord.stopped.is_set():
                    break
                if not self.coord.put_queue(raw_q, chunk, cfg.account, len(chunk)):
                    break
                stats.chunks += 1
                stats.bytes_read += len(chunk)
        except Exception as exc:  # 源读取失败：毒化该流服务的全部分区
            self.coord.fail_stream(cfg.name, f"源读取失败: {exc!r}", cfg.partitions)
        finally:
            raw_q.put(None)  # EOF / 中止标记

    def _decoder(self, cfg: StreamConfig) -> None:
        stats = self.coord.stream_stats[cfg.name]
        raw_q = self._raw_queues[cfg.name]
        gz = GzipStreamDecoder()
        member_id = -1
        # 每个 gzip member 一个装配器；member 校验通过前其明文留在 gz 内部并
        # 已计入流账户（gz_unverified），验证通过后转入装配器半行（member_held）
        assembler: LineAssembler | None = None
        member_held = 0          # 已验证、暂存为装配器半行的字节
        gz_unverified = 0        # gz 内部等待尾部校验的明文字节
        fatal: str | None = None
        fatal_partitions: tuple[str, ...] | None = None  # None 表示全流
        account = cfg.account
        ledger = self.ledger

        def detect_partitions(plain: bytes) -> tuple[str, ...]:
            """从未验证 member 的明文前缀探测它涉及的分区。"""
            found: list[str] = []
            for line in plain.splitlines():
                target = line_partition(line)
                if target and target in cfg.partitions and target not in found:
                    found.append(target)
            return tuple(found)

        def poison_partition(partition: str, reason: str) -> None:
            """记录级致命错误：持久化 STOPPED 并毒化单个分区。"""
            try:
                self.store.mark_stopped(self._stores[partition], reason, None)
            except Exception:  # pragma: no cover - 持久化失败仍需内存毒化
                pass
            self.coord.fail_partition(partition, reason)

        def handle_line(line: bytes) -> None:
            """校验一条完整行并放入分区队列。

            成行记录在队列中期间继续占用流账户，所有权在 writer 取出时才转移，
            因此这里不做任何 ledger 释放。
            """
            try:
                record = parse_record(line, set(cfg.partitions))
            except (InvalidRecordError, LineTooLongError) as exc:
                # 坏 JSON / 超长行：不能跳过，停止对应分区并持久化；该字节不再
                # 进入任何队列，直接从流账户释放
                target = line_partition(line)
                partition = (
                    target if target in cfg.partitions else cfg.partitions[0]
                )
                poison_partition(partition, str(exc))
                ledger.release(account, len(line) + 1)
                return
            self.coord.route_record(
                self._partition_queues[record.partition], record, account
            )
            stats.records_routed += 1

        def consume_verified(mid: int, plain: bytes, flush_last: bool) -> None:
            """消费一段已通过 gzip 校验的明文。

            内存池（均计入流账户）：gz 未验证池 -> 装配器半行池 -> 分区队列。
            队列里的记录仍占流账户，直到 writer 取出才转移所有权，因此本函数
            只做账户内池簿记，除坏行外不释放流账户配额。
            """
            nonlocal assembler, member_held, member_id, gz_unverified
            if mid != member_id:
                assembler = LineAssembler(self.max_line_bytes)
                member_held = 0
                member_id = mid
            assert assembler is not None
            # 同账户内：gz 未验证池 -> 装配器池，总额不变
            gz_unverified -= len(plain)
            member_held += len(plain)
            try:
                lines = list(assembler.feed(plain))
                if flush_last:
                    lines.extend(assembler.finish())
            except LineTooLongError as exc:
                # 超长行是记录级错误：停止受影响分区，不升级为全流压缩故障
                poison_partition(cfg.partitions[0], str(exc))
                ledger.release(account, len(plain))
                member_held -= len(plain)
                return
            for line in lines:
                handle_line(line)
                # 成行字节从装配器半行池移入分区队列（仍在流账户内）
                member_held -= len(line) + 1

        try:
            while True:
                try:
                    item = raw_q.get(timeout=_QUEUE_WAIT)
                except queue.Empty:
                    if self.coord.stopped.is_set():
                        break
                    continue
                if item is None:
                    break
                try:
                    # 内存池模型（全部计入流账户，总额守恒）：
                    #   新解压明文进入流账户 -> acquire，计入 gz 未验证池；
                    #   member 通过尾部校验 -> consume_verified 在账户内部把该段
                    #   从 gz 池转入装配器池，不触碰 ledger；
                    #   成行记录离开流账户 -> release，转入分区账户；
                    #   半行留在装配器池，压缩块消费后释放其原始字节。
                    verified, produced, _drained = gz.feed(item)
                    if produced:
                        ledger.acquire(account, produced)
                        gz_unverified += produced
                    for mid, plain in verified:
                        consume_verified(mid, plain, flush_last=False)
                    # 压缩块已被解码器消费，释放其原始字节占用
                    ledger.release(account, len(item))
                except CodecError as exc:
                    ledger.release(account, len(item))
                    fatal = str(exc)
                    break
                except LineTooLongError as exc:
                    fatal = str(exc)
                    break

            if fatal is None and not self.coord.stopped.is_set():
                # 响应结束：finish 对截断/坏尾部抛 CompressionError
                try:
                    tail_verified = gz.finish()
                except CompressionError as exc:
                    # 残缺 member 的未验证明文拒绝确认，释放其全部占用
                    held = gz_unverified + member_held
                    if held:
                        ledger.release(account, held)
                        stats.unverified_dropped += held
                        gz_unverified = 0
                        member_held = 0
                    fatal = str(exc)
                    # 精确毒化：只停止未验证 member 实际涉及的分区；探测不到
                    # （如完全无法解压）时保守停止全流
                    affected = detect_partitions(gz.unverified_plaintext())
                    fatal_partitions = affected if affected else tuple(cfg.partitions)
                else:
                    for mid, plain in tail_verified:
                        consume_verified(mid, plain, flush_last=True)
                    stats.finished = True
        except Exception as exc:  # 防御性兜底
            fatal = f"解码线程异常: {exc!r}"
        finally:
            # 毒化路径：排空 raw 队列与各内存池，归还全部流账户配额
            drained = 0
            while True:
                rest = raw_q.get_nowait() if not raw_q.empty() else None
                if rest is None:
                    break
                drained += len(rest)
            if drained:
                ledger.release(account, drained)
            leftover = gz_unverified + member_held
            if leftover and fatal is not None:
                ledger.release(account, leftover)
            if fatal is not None:
                if fatal_partitions is not None and len(fatal_partitions) < len(
                    cfg.partitions
                ):
                    self.coord.fail_stream_partitions(
                        cfg.name, fatal, fatal_partitions
                    )
                else:
                    self.coord.fail_stream(cfg.name, fatal, cfg.partitions)
            # 无论是否毒化，都向所有服务分区发 EOF：writer 收到后把已验证、
            # 已入队的连续记录强制刷盘，健康分区不被其他分区的故障拖累
            for partition in cfg.partitions:
                if self.coord.stream_done(partition):
                    self._partition_queues[partition].put(None)

    def _writer(self, partition: str) -> None:
        stats = self.coord.partition_stats[partition]
        store = self._stores[partition]
        q = self._partition_queues[partition]
        account = f"partition:{partition}"
        next_offset = store.state.confirmed_offset + 1
        pending: dict[int, Record] = {}
        pending_bytes = 0
        batch: list[Record] = []
        batch_bytes = 0

        def flush() -> bool:
            """持久化一批连续记录；返回 False 表示分区应停止。"""
            nonlocal pending_bytes, batch_bytes, next_offset
            if not batch:
                return True
            if not self.gate.is_open:
                stats.gate_waits += 1
            if not self.gate.wait(self.coord.stopped):
                return False
            max_offset = batch[-1].offset
            try:
                self.store.commit_batch(
                    store,
                    batch,
                    max_offset,
                    sync_hook=self.coord.sync_hook,
                )
            except Exception as exc:
                self.coord.fail_partition(partition, f"落盘失败: {exc!r}")
                return False
            charged = sum(r.size() + 1 for r in batch)
            self.ledger.release(account, charged)
            pending_bytes -= charged
            for record in batch:
                pending.pop(record.offset, None)
            stats.committed += len(batch)
            stats.bytes_committed += charged
            stats.write_batches += 1
            # 此时数据+索引+清单均已 fsync。崩溃钩子精确模拟“提交完成、源确认
            # 尚未发出”之间的进程退出：先触发钩子，再 acknowledge。
            if self.coord.on_crash_after_commit is not None:
                try:
                    self.coord.on_crash_after_commit(partition, max_offset)
                except BaseException as exc:  # 含 SystemExit
                    with self.coord._crash_lock:
                        if self.coord.crash_exc is None:
                            self.coord.crash_exc = exc
                    self.coord.stopped.set()
                    batch.clear()
                    batch_bytes = 0
                    next_offset = max_offset + 1
                    return False
            # 清单已 fsync 之后才向源确认，保证确认不越过持久化位置
            for record in batch:
                cfg = self._source_for_ack(record.partition)
                cfg.source.acknowledge(record.partition, record.offset)
                stats.last_ack_offset = record.offset
            batch.clear()
            batch_bytes = 0
            next_offset = max_offset + 1
            return True

        def drain_batch() -> bool:
            nonlocal batch_bytes, next_offset
            while next_offset in pending:
                record = pending[next_offset]
                batch.append(record)
                batch_bytes += record.size() + 1
                next_offset += 1
                if len(batch) >= self.batch_records or batch_bytes >= self.batch_bytes:
                    if not flush():
                        return False
            return True

        def release_pending() -> None:
            """分区停止：释放所有未提交在途记录占用的分区配额。"""
            nonlocal pending_bytes
            for record in pending.values():
                ledger.release(account, record.size() + 1)
            pending.clear()
            pending_bytes = 0

        idle_since: float | None = None
        while True:
            try:
                item = q.get(timeout=_QUEUE_WAIT)
            except queue.Empty:
                # 队列暂时为空：把 pending 中连续的记录成批落盘，及时释放分区
                # 配额。否则 decoder 会因等配额阻塞、writer 因等入队空转，形成
                # 循环等待（暂停恢复后尤其容易触发）。
                if not drain_batch() or not flush():
                    return
                if self.coord.stopped.is_set():
                    # 显式关停且 EOF 迟迟未到：兜底收尾后退出
                    now = time.monotonic()
                    if idle_since is None:
                        idle_since = now
                    elif now - idle_since > 2.0:
                        if not drain_batch() or not flush():
                            return
                        # 无论分区是否被停，残留非连续在途配额都要回收
                        release_pending()
                        return
                continue
            idle_since = None
            if item is None:
                # EOF：先把能成批的连续前缀落盘（健康分区即全部连续记录），
                # 再无条件释放任何残留在 pending 中的非连续在途配额，避免泄漏
                if not drain_batch() or not flush():
                    return
                if self.coord.is_partition_stopped(partition):
                    release_pending()
                return
            record: Record = item
            # 所有权转移：记录在队列中占流账户，取出后计入分区账户
            stream_account = self._stream_account_for(record.partition)
            if self.coord.is_partition_stopped(partition):
                # 分区已停：不转入，直接归还流账户
                self.ledger.release(stream_account, record.size() + 1)
                continue
            self.coord.transfer_in(record, stream_account)

            known = store.lookup_digest(record.offset)
            if known is not None:
                if known == record.digest:
                    # 同 offset 同内容：可识别的重传，幂等丢弃
                    stats.duplicate_acks += 1
                    self.ledger.release(account, record.size() + 1)
                    continue
                # 同 offset 不同内容：停止分区并保留冲突指纹
                fingerprint = {
                    "offset": record.offset,
                    "persisted_digest": known.hex(),
                    "incoming_digest": record.digest.hex(),
                    "incoming_preview": record.raw[:128].decode(
                        "utf-8", errors="replace"
                    ),
                }
                self.ledger.release(account, record.size() + 1)
                stats.conflicts += 1
                reason = (
                    f"offset {record.offset} 内容冲突："
                    f"已存 {known.hex()[:12]}… vs 新到 {record.digest.hex()[:12]}…"
                )
                try:
                    self.store.mark_stopped(store, reason, fingerprint)
                except Exception as exc:  # pragma: no cover
                    self.coord.fail_partition(partition, f"状态持久化失败: {exc!r}")
                    return
                self.coord.fail_partition(partition, reason)
                continue

            if record.offset in pending:
                # 同一批在途重复：同内容幂等，异内容冲突
                prior = pending[record.offset]
                self.ledger.release(account, record.size() + 1)
                if prior.digest != record.digest:
                    fingerprint = {
                        "offset": record.offset,
                        "inflight_digest": prior.digest.hex(),
                        "incoming_digest": record.digest.hex(),
                    }
                    stats.conflicts += 1
                    reason = f"offset {record.offset} 在途内容冲突"
                    self.store.mark_stopped(store, reason, fingerprint)
                    self.coord.fail_partition(partition, reason)
                else:
                    stats.duplicate_acks += 1
                continue

            pending[record.offset] = record
            pending_bytes += record.size() + 1
            stats.enqueued += 1

            # 只按连续 offset 成批；缺口期间记录留在 pending，受分区配额约束
            if not drain_batch():
                return
            if self.coord.is_partition_stopped(partition):
                return
        # 收尾 EOF 分支在上方 None 处理

    def _source_for_ack(self, partition: str) -> StreamConfig:
        for cfg in self.streams:
            if partition in cfg.partitions:
                return cfg
        raise KeyError(f"没有源连接负责分区 {partition}")

    def _stream_account_for(self, partition: str) -> str:
        return self._source_for_ack(partition).account

    # ---------- 生命周期 ----------

    def start(self) -> None:
        for cfg in self.streams:
            self._threads.append(
                threading.Thread(
                    target=self._fetcher,
                    args=(cfg,),
                    name=f"fetcher-{cfg.name}",
                    daemon=True,
                )
            )
            self._threads.append(
                threading.Thread(
                    target=self._decoder,
                    args=(cfg,),
                    name=f"decoder-{cfg.name}",
                    daemon=True,
                )
            )
        for partition in sorted(self._partition_queues):
            self._threads.append(
                threading.Thread(
                    target=self._writer,
                    args=(partition,),
                    name=f"writer-{partition}",
                    daemon=True,
                )
            )
        for thread in self._threads:
            thread.start()

    def run(self, join_timeout: float = 30.0) -> dict:
        """启动并等待全部线程退出，返回最终指标快照。

        若崩溃钩子触发（模拟提交后、确认前退出），线程收敛后在主线程重新
        抛出该异常，持久化状态已经落盘。
        """
        self.start()
        deadline = time.monotonic() + join_timeout
        for thread in self._threads:
            remaining = max(0.0, deadline - time.monotonic())
            thread.join(remaining)
        alive = [t.name for t in self._threads if t.is_alive()]
        if alive:
            self.coord.stopped.set()
            raise TimeoutError(f"采集线程未在超时内退出: {alive}")
        self.store.close()
        if self.coord.crash_exc is not None:
            raise self.coord.crash_exc
        return self.metrics()

    def shutdown(self) -> None:
        self.coord.stopped.set()
        for thread in self._threads:
            thread.join(timeout=5.0)
        self.store.close()

    def metrics(self) -> dict:
        ledger = self.ledger.snapshot()
        store_state = self.store.snapshot()
        streams = {
            name: {
                "bytes_read": s.bytes_read,
                "chunks": s.chunks,
                "records_routed": s.records_routed,
                "unverified_dropped_bytes": s.unverified_dropped,
                "finished": s.finished,
                "error": s.error,
            }
            for name, s in self.coord.stream_stats.items()
        }
        partitions = {}
        for name, stats in self.coord.partition_stats.items():
            state = store_state.get(name, {})
            partitions[name] = {
                "status": state.get("status", "RUNNING"),
                "confirmed_offset": state.get("confirmed_offset", -1),
                "last_ack_offset": stats.last_ack_offset,
                "enqueued": stats.enqueued,
                "committed": stats.committed,
                "duplicate_acks": stats.duplicate_acks,
                "conflicts": stats.conflicts,
                "write_batches": stats.write_batches,
                "gate_waits": stats.gate_waits,
                "bytes_committed": stats.bytes_committed,
                "in_flight_bytes": ledger["accounts"].get(f"partition:{name}", 0),
                "peak_bytes": ledger["account_peak"].get(f"partition:{name}", 0),
                "fatal": stats.fatal,
                "stop_reason": state.get("stop_reason"),
                "conflict_fingerprint": state.get("conflict"),
            }
        return {
            "memory": ledger,
            "downstream_gate": self.gate.snapshot(),
            "streams": streams,
            "partitions": partitions,
            "block_reasons": ledger["limit_hits"],
            "stop_reasons": dict(self.coord.stop_reasons),
        }


def line_partition(line: bytes) -> str | None:
    """坏行场景下尽力解析 partition 字段，用于定位应停止的分区。"""
    import json

    try:
        obj = json.loads(line.decode("utf-8", errors="replace"))
        value = obj.get("partition")
        return value if isinstance(value, str) else None
    except Exception:
        return None
