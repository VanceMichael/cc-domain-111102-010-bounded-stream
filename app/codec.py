"""流式解压与行装配。

输入是任意大小的压缩字节块（HTTP 响应可在任意字节边界切块，也可串联多个
gzip member）。本模块：

* :class:`GzipStreamDecoder` 逐块喂给 zlib，按 member 边界自动续接，绝不
  一次性解压整个响应；
* :class:`LineAssembler` 在字节层面按 ``\\n`` 切行，半行与半多字节字符都
  保留到下一块；
* :func:`parse_record` 对完整行做 UTF-8 解码与 JSON 结构校验。

损坏的压缩尾部、坏 JSON、超长行都抛出明确异常，由上层停止对应分区，绝不
跳过继续确认后续 offset。
"""

from __future__ import annotations

import json
import zlib
from dataclasses import dataclass

_GZIP_WBITS = 16 + zlib.MAX_WBITS


class CodecError(RuntimeError):
    """解压或装配阶段的致命错误。"""


class CompressionError(CodecError):
    """压缩流损坏，包括 member 截断、尾部校验失败。"""


class LineTooLongError(CodecError):
    """单行字节数超过配置上限。"""


class InvalidRecordError(CodecError):
    """行不是合法 UTF-8 JSON 或缺少必要字段。"""


class GzipStreamDecoder:
    """多 member gzip 流式解码器。

    用法::

        decoder = GzipStreamDecoder()
        for chunk in source.chunks():
            verified, produced, drained = decoder.feed(chunk)
            for member_id, plain in verified:
                ...
        for member_id, plain in decoder.finish():
            ...

    每个 member 的字节只有在其 gzip 尾部（CRC32 + ISIZE）校验通过后，
    才会随 :meth:`verified_output` 交给调用方；尾部缺失或校验失败时，
    该 member 已解压但未验证的数据一律不输出。
    """

    def __init__(self) -> None:
        self._zlib = zlib.decompressobj(_GZIP_WBITS)
        self._member_id = 0
        # 当前 member 已解压、尚未通过尾部校验的明文
        self._unverified = bytearray()
        self._seen_data = False

    def _drain_verified_members(self) -> list[tuple[int, bytes]]:
        """当前 member 结束（unused_data 非空）则它已完整通过校验。

        返回 ``(member_id, 明文)``；编号是刚结束的 member 序号。尾部字节
        交给下一个 member 的解码器后必须清空，否则会被重复解压。
        """
        output: list[tuple[int, bytes]] = []
        while self._zlib.unused_data:
            tail = self._zlib.unused_data
            finished_id = self._member_id
            block = bytes(self._unverified)
            self._unverified = bytearray()
            self._member_id += 1
            self._zlib = zlib.decompressobj(_GZIP_WBITS)
            self._seen_data = False
            output.append((finished_id, block))
            if not tail:
                break
            try:
                produced = self._zlib.decompress(tail)
                # 新解码器可能在这次调用内就完成（小 member 边界落在同一块）
                if produced:
                    self._unverified.extend(produced)
                    self._seen_data = True
                if self._zlib.unused_data:
                    # 递归处理同一块内串联的更多 member
                    output.extend(self._drain_verified_members())
                    break
            except zlib.error as exc:
                raise CompressionError(f"member {self._member_id} 压缩数据损坏: {exc}") from exc
        return output

    def feed(
        self, chunk: bytes
    ) -> tuple[list[tuple[int, bytes]], int, int]:
        """喂入压缩块，返回 ``(已验证明文列表, 新增未验证明文, 排空已验证明文)``。

        输出的明文均来自已经通过完整尾部校验的 member；新增与排空是两个独立
        量，调用方分别做非负的内存池操作，避免 member 边界落在同一块时净值
        掩盖事实、释放尚未持有的配额。
        """
        if not chunk:
            return [], 0, 0
        try:
            plain = self._zlib.decompress(chunk)
        except zlib.error as exc:
            raise CompressionError(
                f"member {self._member_id} 压缩数据损坏: {exc}"
            ) from exc
        if plain:
            self._unverified.extend(plain)
            self._seen_data = True

        verified = self._drain_verified_members()
        # 分别报告两个量，调用方据此做非负的池操作：新增明文计入未验证池，
        # 排空的已验证明文从该池移出。member 边界落在同一块时两者都可能非零，
        # 净值会掩盖这一事实并导致账本释放未持有的字节。
        verified_bytes = sum(len(block) for _mid, block in verified)
        return verified, len(plain), verified_bytes

    def finish(self) -> list[tuple[int, bytes]]:
        """响应结束。完整 member 的数据可输出，残缺 member 触发致命错误。"""
        verified: list[tuple[int, bytes]] = []
        try:
            tail = self._zlib.flush()
        except zlib.error as exc:
            raise CompressionError(
                f"member {self._member_id} flush 失败: {exc}"
            ) from exc
        if tail:
            self._unverified.extend(tail)

        if self._zlib.unused_data:
            verified.extend(self._drain_verified_members())

        # GzipFile 在流提前结束时抛 EOFError；decompressobj 靠 eof 标志判断
        if not self._zlib.eof:
            if self._seen_data or self._unverified:
                raise CompressionError(
                    f"member {self._member_id} 尾部被截断，缺少完整 gzip 结束标记；"
                    f"{len(self._unverified)} 字节未验证明文已拒绝确认"
                )
            raise CompressionError(
                f"member {self._member_id} 为空或不是合法 gzip 流"
            )
        # 正常结束的最后一个 member，校验通过
        verified.append((self._member_id, bytes(self._unverified)))
        self._unverified = bytearray()
        return verified

    @property
    def unverified_bytes(self) -> int:
        """当前未通过尾部校验、暂存在内存中的明文规模（可观察）。"""
        return len(self._unverified)

    def unverified_plaintext(self) -> bytes:
        """未验证 member 的已解压明文副本。

        仅用于在压缩尾部损坏时探测受影响的分区，调用方不得据此确认任何
        offset——这些字节没有通过 gzip 完整性校验。
        """
        return bytes(self._unverified)


class LineAssembler:
    """字节级 NDJSON 行装配器，半行/半多字节字符都可跨块保留。"""

    def __init__(self, max_line_bytes: int) -> None:
        if max_line_bytes <= 0:
            raise ValueError("行长度上限必须为正")
        self._max_line_bytes = max_line_bytes
        self._buffer = bytearray()

    @property
    def pending_bytes(self) -> int:
        return len(self._buffer)

    def feed(self, data: bytes) -> list[bytes]:
        """返回本次拼出的完整行（不含换行符）。"""
        lines: list[bytes] = []
        self._buffer.extend(data)
        if len(self._buffer) > self._max_line_bytes:
            # 超长行不能悄悄截断：报错并携带定位信息，由上层毒化分区
            head = bytes(self._buffer[: self._max_line_bytes])
            raise LineTooLongError(
                f"行长度超过 {self._max_line_bytes} 字节，前缀: {head!r}"
            )

        start = 0
        while True:
            nl = self._buffer.find(b"\n", start)
            if nl < 0:
                break
            line = bytes(self._buffer[start:nl])
            if len(line) > self._max_line_bytes:
                raise LineTooLongError(
                    f"行长度 {len(line)} 超过 {self._max_line_bytes} 字节"
                )
            lines.append(line)
            start = nl + 1
        del self._buffer[:start]
        return lines

    def finish(self) -> list[bytes]:
        """流结束：允许最后一行没有换行符；残留半个多字节字符则报错。"""
        if not self._buffer:
            return []
        line = bytes(self._buffer)
        self._buffer = bytearray()
        if len(line) > self._max_line_bytes:
            raise LineTooLongError(
                f"末行长度 {len(line)} 超过 {self._max_line_bytes} 字节"
            )
        return [line]


@dataclass(frozen=True)
class Record:
    partition: str
    offset: int
    raw: bytes

    @property
    def digest(self) -> bytes:
        import hashlib

        return hashlib.sha256(self.raw).digest()

    def size(self) -> int:
        return len(self.raw)


def parse_record(line: bytes, allowed_partitions: set[str] | None = None) -> Record:
    """校验一条完整 NDJSON 行，返回 :class:`Record`。"""
    try:
        text = line.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidRecordError(f"非法 UTF-8 行: {exc}") from exc
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        raise InvalidRecordError(f"坏 JSON，不能跳过: {exc}; 行: {line!r}") from exc
    if not isinstance(obj, dict):
        raise InvalidRecordError(f"记录必须是 JSON 对象: {line!r}")
    if "partition" not in obj or "offset" not in obj:
        raise InvalidRecordError(f"记录缺少 partition/offset 字段: {line!r}")
    partition = obj["partition"]
    offset = obj["offset"]
    if not isinstance(partition, str) or not partition:
        raise InvalidRecordError(f"partition 必须是非空字符串: {line!r}")
    # bool 是 int 的子类，显式排除
    if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
        raise InvalidRecordError(f"offset 必须是非负整数: {line!r}")
    if allowed_partitions is not None and partition not in allowed_partitions:
        raise InvalidRecordError(f"未知分区 {partition!r}: {line!r}")
    return Record(partition=partition, offset=offset, raw=line)
