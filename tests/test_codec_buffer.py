"""codec 与 buffer 的单元测试。

覆盖：任意字节边界切块、半个多字节字符跨块、多 gzip member 串联、压缩
member 截断、超长行、坏 JSON，以及内存账本的累计背压与分区配额。
"""

import json
import unittest
from pathlib import Path

from app.buffer import BufferLedger, BufferLimitExceeded
from app.codec import (
    CompressionError,
    GzipStreamDecoder,
    InvalidRecordError,
    LineAssembler,
    LineTooLongError,
    parse_record,
)
from tests.support import gzip_member, multi_member_gzip, record_line


class LineAssemblerTests(unittest.TestCase):
    def test_split_inside_multibyte_char(self) -> None:
        # "合成" 的 UTF-8 字节：合=e5 90 88，成=e6 88 90
        line = json.dumps({"partition": "p", "offset": 0, "v": "合成"},
                          ensure_ascii=False).encode("utf-8")
        asm = LineAssembler(max_line_bytes=4096)
        out: list[bytes] = []
        # 按 1 字节切，必然多次切在多字节字符内部
        for i in range(0, len(line), 1):
            out.extend(asm.feed(line[i:i + 1]))
        out.extend(asm.finish())
        self.assertEqual(out, [line])
        # 能正确解码回中文
        self.assertIn("合成", out[0].decode("utf-8"))

    def test_line_split_across_blocks_with_newline(self) -> None:
        asm = LineAssembler(4096)
        blob = b"alpha\nbeta\ngamma"
        got: list[bytes] = []
        for i in range(0, len(blob), 2):
            got.extend(asm.feed(blob[i:i + 2]))
        got.extend(asm.finish())
        self.assertEqual(got, [b"alpha", b"beta", b"gamma"])

    def test_too_long_line(self) -> None:
        asm = LineAssembler(max_line_bytes=8)
        with self.assertRaises(LineTooLongError):
            asm.feed(b"x" * 9)

    def test_final_line_without_newline(self) -> None:
        asm = LineAssembler(4096)
        self.assertEqual(asm.feed(b"abc"), [])
        self.assertEqual(asm.finish(), [b"abc"])


class GzipDecoderTests(unittest.TestCase):
    def _feed_all(self, blob: bytes, chunk: int):
        gz = GzipStreamDecoder()
        verified: list[tuple[int, bytes]] = []
        for i in range(0, len(blob), chunk):
            blocks, produced, drained = gz.feed(blob[i:i + chunk])
            self.assertGreaterEqual(produced, 0)
            self.assertGreaterEqual(drained, 0)
            verified.extend(blocks)
        tail = gz.finish()
        verified.extend(tail)
        return verified

    def test_multi_member_arbitrary_boundaries(self) -> None:
        lines_a = [record_line("a", i) for i in range(3)]
        lines_b = [record_line("b", i) for i in range(2)]
        blob = multi_member_gzip([lines_a, lines_b])
        for chunk in (1, 3, 7, 13, 64, 10_000):
            verified = self._feed_all(blob, chunk)
            mids = [mid for mid, _ in verified]
            self.assertEqual(mids, [0, 1], f"chunk={chunk}")
            plain = b"".join(p for _, p in verified)
            self.assertEqual(plain.count(b"\n"), 5)

    def test_truncated_member_rejected_but_verified_prefix_kept(self) -> None:
        good = gzip_member([record_line("a", i) for i in range(2)])
        broken = gzip_member([record_line("b", i) for i in range(3)])
        blob = good + broken[:-7]  # 砍掉第二 member 的 gzip 尾部
        gz = GzipStreamDecoder()
        verified: list[tuple[int, bytes]] = []
        for i in range(0, len(blob), 5):
            blocks, _p, _d = gz.feed(blob[i:i + 5])
            verified.extend(blocks)
        with self.assertRaises(CompressionError):
            gz.finish()
        # 已通过校验的第一 member 数据仍可交付
        self.assertEqual([mid for mid, _ in verified], [0])
        self.assertIn(b'"a"', b"".join(p for _, p in verified))
        # 未验证 member 的明文被保留探测、但绝不确认
        self.assertIn(b'"b"', gz.unverified_plaintext())

    def test_corrupt_midstream_raises(self) -> None:
        good = gzip_member([record_line("a", i) for i in range(2)])
        blob = bytearray(good)
        blob[len(blob) // 2] ^= 0xFF
        gz = GzipStreamDecoder()
        with self.assertRaises(CompressionError):
            for i in range(0, len(blob), 4):
                gz.feed(bytes(blob[i:i + 4]))
            gz.finish()

    def test_empty_member_chunked_is_zero_records(self) -> None:
        import gzip as _gzip
        import io as _io

        buf = _io.BytesIO()
        with _gzip.GzipFile(fileobj=buf, mode="wb"):
            pass
        blob = buf.getvalue()
        verified = self._feed_all(blob, 4)
        # 空 member 分块喂入后正常收敛为零行，不报错
        self.assertEqual([p for _m, p in verified], [b""])


class ParseRecordTests(unittest.TestCase):
    def test_valid_record(self) -> None:
        rec = parse_record(record_line("p", 7), {"p"})
        self.assertEqual(rec.partition, "p")
        self.assertEqual(rec.offset, 7)
        self.assertEqual(len(rec.digest), 32)

    def test_bad_json(self) -> None:
        with self.assertRaises(InvalidRecordError):
            parse_record(b"{broken", {"p"})

    def test_bad_utf8(self) -> None:
        with self.assertRaises(InvalidRecordError):
            parse_record(b"\xff\xfe{\"x\":1}", {"p"})

    def test_missing_fields(self) -> None:
        with self.assertRaises(InvalidRecordError):
            parse_record(b'{"offset":1}', {"p"})

    def test_unknown_partition(self) -> None:
        with self.assertRaises(InvalidRecordError):
            parse_record(record_line("other", 0), {"p"})

    def test_negative_offset(self) -> None:
        with self.assertRaises(InvalidRecordError):
            parse_record(b'{"partition":"p","offset":-1}', {"p"})

    def test_digest_stable_for_same_content(self) -> None:
        line = record_line("p", 1)
        self.assertEqual(parse_record(line).digest, parse_record(line).digest)


class BufferLedgerTests(unittest.TestCase):
    def test_cumulative_not_per_read(self) -> None:
        # 上限 100，单次 read 仅 30，但累计四次即触顶阻塞
        led = BufferLedger(global_limit=100)
        led.acquire("s", 30)
        led.acquire("s", 30)
        led.acquire("s", 30)
        self.assertEqual(led.snapshot()["global_in_use"], 90)
        # 再申请 30 会阻塞；用后台线程先释放再申请验证背压
        import threading
        import time

        def releaser() -> None:
            time.sleep(0.05)
            led.release("s", 30)

        t = threading.Thread(target=releaser)
        t.start()
        led.acquire("s", 30)  # 不超时即说明背压等待生效
        t.join()
        self.assertEqual(led.snapshot()["global_in_use"], 90)
        self.assertGreaterEqual(led.snapshot()["global_peak"], 90)
        self.assertIn("global_limit", led.snapshot()["limit_hits"])

    def test_partition_quota_isolation(self) -> None:
        led = BufferLedger(global_limit=1000, account_limit=100)
        led.acquire("partition:a", 100)
        # b 分区仍能拿到自己的配额
        led.acquire("partition:b", 100)
        self.assertEqual(led.snapshot()["accounts"]["partition:a"], 100)
        self.assertEqual(led.snapshot()["accounts"]["partition:b"], 100)
        # a 分区超出自己的配额会阻塞
        import threading

        acquired = threading.Event()

        def try_acquire() -> None:
            led.acquire("partition:a", 10)
            acquired.set()

        t = threading.Thread(target=try_acquire, daemon=True)
        t.start()
        t.join(timeout=0.2)
        self.assertFalse(acquired.is_set())
        led.release("partition:a", 100)
        t.join(timeout=1.0)
        self.assertTrue(acquired.is_set())

    def test_oversized_request_rejected(self) -> None:
        led = BufferLedger(global_limit=10)
        with self.assertRaises(BufferLimitExceeded):
            led.acquire("s", 11)

    def test_release_more_than_held_raises(self) -> None:
        led = BufferLedger(global_limit=100)
        led.acquire("s", 10)
        with self.assertRaises(AssertionError):
            led.release("s", 11)


if __name__ == "__main__":
    unittest.main()
