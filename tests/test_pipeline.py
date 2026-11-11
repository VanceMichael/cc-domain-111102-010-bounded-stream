"""采集管线集成测试。

覆盖端到端关系：
* 多 member / 任意字节边界 / 中文跨块的完整落盘与确认；
* 慢写入下的累计内存背压与缓冲峰值；
* 下游停写时暂停拉取、恢复后继续；
* 单分区停住时其他分区在各自配额内继续前进；
* 确认位置不越过已持久化连续记录；
* 崩溃后从持久化位置恢复，提交后确认前退出不产生业务重复；
* 截断 member、坏 JSON、超长行、内容冲突的毒化与指纹。
"""

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from app.pipeline import (
    Collector,
    FixtureSource,
    StreamConfig,
    WriteGate,
)
from tests.support import (
    FIXTURES,
    gzip_member,
    multi_member_gzip,
    record_line,
    write_bytes,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def make_collector(
    tmp: Path,
    blob_path: Path,
    partitions: tuple[str, ...],
    *,
    chunk_size: int = 5,
    memory_limit: int = 8192,
    partition_memory_limit: int = 4096,
    batch_records: int = 4,
    gate: WriteGate | None = None,
    sync_hook=None,
    on_crash_after_commit=None,
    stream_name: str = "s",
):
    src = FixtureSource(blob_path, chunk_size=chunk_size)
    cfg = StreamConfig(name=stream_name, source=src, partitions=partitions)
    collector = Collector(
        [cfg],
        store_root=tmp,
        memory_limit=memory_limit,
        partition_memory_limit=partition_memory_limit,
        batch_records=batch_records,
        gate=gate,
        sync_hook=sync_hook,
        on_crash_after_commit=on_crash_after_commit,
    )
    return collector, src


def data_lines(root: Path, partition: str) -> list[bytes]:
    path = root / f"p_{partition}" / "data.ndjson"
    if not path.exists():
        return []
    return path.read_bytes().splitlines()


class HappyPathTests(unittest.TestCase):
    def test_members_gz_all_chunksizes(self) -> None:
        for chunk in (1, 7, 13):
            with self.subTest(chunk=chunk):
                with tempfile.TemporaryDirectory() as d:
                    root = Path(d, "rt")
                    collector, src = make_collector(
                        root,
                        FIXTURES / "members.gz",
                        ("line-a", "line-b"),
                        chunk_size=chunk,
                    )
                    metrics = collector.run(join_timeout=20)
                    self.assertIsNone(metrics["streams"]["s"]["error"])
                    for partition in ("line-a", "line-b"):
                        lines = data_lines(root, partition)
                        self.assertEqual(len(lines), 6)
                        # 中文跨块仍可正确解码
                        self.assertIn("合成温度", lines[0].decode("utf-8"))
                        self.assertEqual(
                            metrics["partitions"][partition]["confirmed_offset"], 5
                        )
                    # 确认位置 == 落盘位置，且按 offset 连续
                    acks = [(a["partition"], a["offset"]) for a in src.acks]
                    self.assertEqual(
                        sorted(o for p, o in acks if p == "line-a"),
                        list(range(6)),
                    )
                    # 所有在途内存归还
                    self.assertEqual(metrics["memory"]["global_in_use"], 0)

    def test_ack_never_passes_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d, "rt")
            collector, src = make_collector(
                root, FIXTURES / "members.gz", ("line-a", "line-b"),
                batch_records=2,
            )
            metrics = collector.run(join_timeout=20)
            for partition, state in metrics["partitions"].items():
                self.assertLessEqual(
                    state["last_ack_offset"], state["confirmed_offset"]
                )
            # 最大确认 offset 不超过实际落盘行数 - 1
            for partition in ("line-a", "line-b"):
                persisted = len(data_lines(root, partition))
                max_ack = max(
                    o for p, o in ((a["partition"], a["offset"]) for a in src.acks)
                    if p == partition
                )
                self.assertEqual(max_ack, persisted - 1)


class BackpressureTests(unittest.TestCase):
    def test_slow_write_causes_backpressure_and_bounded_peak(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d, "rt")
            # 分区配额（700）小于慢写入下两个分区想要缓冲的总量，从而稳定触发
            # partition_limit 背压；全局上限仍高于单 member 解压明文，不会死锁
            limit = 1050
            entered = threading.Event()
            release = threading.Event()

            def slow_hook(partition: str, count: int) -> None:
                # 第一批写入时阻塞，制造下游慢写入以堆积在途缓冲
                if not entered.is_set():
                    entered.set()
                    release.wait(timeout=5.0)

            collector, src = make_collector(
                root,
                FIXTURES / "members.gz",
                ("line-a", "line-b"),
                chunk_size=3,
                memory_limit=limit,
                partition_memory_limit=700,
                batch_records=2,
                sync_hook=slow_hook,
            )
            thread = threading.Thread(target=collector.run, kwargs={"join_timeout": 30})
            thread.start()
            self.assertTrue(entered.wait(timeout=5.0))
            # 慢写入期间给 fetcher 一点时间堆积并撞上限额
            time.sleep(0.5)
            mid = collector.ledger.snapshot()
            self.assertLessEqual(mid["global_in_use"], limit)
            release.set()
            thread.join(timeout=30)
            self.assertFalse(thread.is_alive())
            metrics = collector.metrics()
            # 峰值受上限约束，且确实发生过配额阻塞
            self.assertLessEqual(metrics["memory"]["global_peak"], limit)
            self.assertTrue(metrics["memory"]["global_peak"] > 0)
            self.assertTrue(
                metrics["block_reasons"].get("global_limit")
                or metrics["block_reasons"].get("partition_limit")
            )
            for partition in ("line-a", "line-b"):
                self.assertEqual(len(data_lines(root, partition)), 6)

    def test_paused_downstream_pauses_pull_then_resumes(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d, "rt")
            gate = WriteGate()
            # 配额需容纳至少一个完整 gzip member 的解压明文（约 534B），
            # 否则 member 未验证完无法成行，属物理约束而非死锁
            collector, src = make_collector(
                root,
                FIXTURES / "members.gz",
                ("line-a", "line-b"),
                chunk_size=3,
                memory_limit=2048,
                partition_memory_limit=1024,
                gate=gate,
            )
            gate.pause()
            thread = threading.Thread(
                target=collector.run, kwargs={"join_timeout": 30}, daemon=True
            )
            thread.start()
            time.sleep(0.6)
            mid = collector.ledger.snapshot()
            # 停写期间在途缓冲被上限压住，没有无限增长
            self.assertLessEqual(mid["global_in_use"], 2048)
            bytes_read_while_paused = collector.coord.stream_stats["s"].bytes_read
            time.sleep(0.3)
            # 拉取已被背压暂停：字节读数不再增长
            self.assertEqual(
                collector.coord.stream_stats["s"].bytes_read,
                bytes_read_while_paused,
            )
            gate.resume()
            thread.join(timeout=30)
            self.assertFalse(thread.is_alive())
            metrics = collector.metrics()
            self.assertTrue(metrics["downstream_gate"]["paused_seconds"] > 0)
            for partition in ("line-a", "line-b"):
                self.assertEqual(len(data_lines(root, partition)), 6)
            collector.store.close()


class PartitionIsolationTests(unittest.TestCase):
    def test_stopped_partition_does_not_block_others(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d, "rt")
            # badline 只含 line-a 数据并含坏 JSON；line-b 不参与该流，
            # 改用两个独立流验证：a 流毒化，b 流仍能完整前进
            blob_a = write_bytes(
                Path(d, "a.gz"),
                gzip_member([record_line("line-a", 0), b"{broken"]),
            )
            blob_b = write_bytes(
                Path(d, "b.gz"),
                gzip_member([record_line("line-b", i) for i in range(4)]),
            )
            src_a = FixtureSource(blob_a, chunk_size=4)
            src_b = FixtureSource(blob_b, chunk_size=4)
            collector = Collector(
                [
                    StreamConfig("sa", src_a, ("line-a",)),
                    StreamConfig("sb", src_b, ("line-b",)),
                ],
                store_root=root,
                memory_limit=8192,
                partition_memory_limit=2048,
                batch_records=2,
            )
            metrics = collector.run(join_timeout=20)
            self.assertEqual(
                metrics["partitions"]["line-a"]["status"], "STOPPED"
            )
            # line-b 不受 line-a 毒化影响，全部落盘确认
            self.assertEqual(
                metrics["partitions"]["line-b"]["confirmed_offset"], 3
            )
            self.assertEqual(len(data_lines(root, "line-b")), 4)
            self.assertEqual(metrics["memory"]["global_in_use"], 0)


class RecoveryTests(unittest.TestCase):
    def _build_blob(self, d: Path) -> Path:
        lines = [record_line("line-a", i) for i in range(8)]
        blob = multi_member_gzip([lines[:4], lines[4:]])
        return write_bytes(d / "stream.gz", blob)

    def test_restart_resumes_from_persisted_offset_no_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            dpath = Path(d)
            blob = self._build_blob(dpath)
            store_root = dpath / "rt"
            # 第一次运行：在提交第 3 条（offset 2）后模拟进程退出
            crash_after = {"v": 2}

            def hook(partition: str, max_offset: int) -> None:
                if max_offset == crash_after["v"]:
                    raise SystemExit("模拟提交后、确认前崩溃")

            collector, src = make_collector(
                store_root, blob, ("line-a",), batch_records=1,
                on_crash_after_commit=hook,
            )
            with self.assertRaises(SystemExit):
                collector.run(join_timeout=20)

            # 清单已持久化到 offset 2，但源确认可能尚未发出
            manifest = json.loads((store_root / "manifest.json").read_text())
            self.assertEqual(
                manifest["partitions"]["line-a"]["confirmed_offset"], 2
            )
            first_lines = data_lines(store_root, "line-a")
            self.assertEqual(len(first_lines), 3)

            # 第二次运行：源从头重发，采集端按摘要识别重传，最终无重复
            collector2, src2 = make_collector(
                store_root, blob, ("line-a",), batch_records=2,
            )
            metrics = collector2.run(join_timeout=20)
            second_lines = data_lines(store_root, "line-a")
            # 业务记录恰好 8 条，无重复
            offsets = [
                json.loads(line)["offset"] for line in second_lines
            ]
            self.assertEqual(offsets, list(range(8)))
            self.assertEqual(len(second_lines), 8)
            self.assertGreaterEqual(
                metrics["partitions"]["line-a"]["duplicate_acks"], 3
            )
            self.assertEqual(
                metrics["partitions"]["line-a"]["confirmed_offset"], 7
            )

    def test_retransmit_same_content_identified_different_conflicts(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            dpath = Path(d)
            normal = [record_line("line-a", i) for i in range(2)]
            # 同 offset 0 但内容不同
            conflict = record_line("line-a", 0, reading=99)
            blob = write_bytes(
                dpath / "c.gz", gzip_member(normal + [conflict])
            )
            collector, src = make_collector(
                dpath / "rt", blob, ("line-a",), batch_records=8
            )
            metrics = collector.run(join_timeout=20)
            state = metrics["partitions"]["line-a"]
            self.assertEqual(state["status"], "STOPPED")
            self.assertEqual(state["conflicts"], 1)
            self.assertIsNotNone(state["conflict_fingerprint"])
            # 已持久化的连续前缀仍确认到 offset 1
            self.assertEqual(state["confirmed_offset"], 1)
            # 冲突指纹持久化到磁盘，重启后仍在
            manifest = json.loads(
                (dpath / "rt" / "manifest.json").read_text()
            )
            self.assertEqual(
                manifest["partitions"]["line-a"]["status"], "STOPPED"
            )
            self.assertIn("conflict", manifest["partitions"]["line-a"])

    def test_hard_process_crash_between_commit_and_ack(self) -> None:
        """真实子进程 os._exit：验证 fsync 持久性与无业务重复。"""
        env = dict(os.environ)
        env["PROJECT_ROOT"] = str(PROJECT_ROOT)
        worker = PROJECT_ROOT / "scripts" / "crash_worker.py"
        with tempfile.TemporaryDirectory() as d:
            dpath = Path(d)
            blob = self._build_blob(dpath)
            store_root = dpath / "rt"
            # 第一次：硬崩溃在 offset 2 提交后、确认前
            proc = subprocess.run(
                [sys.executable, "-I", str(worker), str(blob),
                 str(store_root), "2", "line-a"],
                env=env, capture_output=True, timeout=30,
            )
            self.assertEqual(proc.returncode, 27, proc.stderr.decode())
            # 清单已持久化，进程被杀也不丢
            manifest = json.loads((store_root / "manifest.json").read_text())
            self.assertEqual(
                manifest["partitions"]["line-a"]["confirmed_offset"], 2
            )
            first = data_lines(store_root, "line-a")
            self.assertEqual(len(first), 3)

            # 第二次：正常运行，源从头重发 -> 摘要去重 -> 恰好 8 条无重复
            collector, _src = make_collector(
                store_root, blob, ("line-a",), batch_records=2
            )
            metrics = collector.run(join_timeout=20)
            offsets = [
                json.loads(line)["offset"]
                for line in data_lines(store_root, "line-a")
            ]
            self.assertEqual(offsets, list(range(8)))
            self.assertGreaterEqual(
                metrics["partitions"]["line-a"]["duplicate_acks"], 3
            )

    def test_repeated_restart_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            dpath = Path(d)
            blob = self._build_blob(dpath)
            store_root = dpath / "rt"
            for _ in range(3):
                collector, _src = make_collector(
                    store_root, blob, ("line-a",), batch_records=3
                )
                collector.run(join_timeout=20)
            lines = data_lines(store_root, "line-a")
            self.assertEqual(
                [json.loads(x)["offset"] for x in lines], list(range(8))
            )


class PoisonTests(unittest.TestCase):
    def test_truncated_member_keeps_verified_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d, "rt")
            collector, src = make_collector(
                root,
                FIXTURES / "truncated.gz",
                ("line-a", "line-b"),
                chunk_size=5,
                batch_records=2,
            )
            metrics = collector.run(join_timeout=20)
            # line-a 来自完整 member，全部确认
            self.assertEqual(
                metrics["partitions"]["line-a"]["confirmed_offset"], 5
            )
            # line-b 来自截断 member，无确认且被毒化
            self.assertEqual(
                metrics["partitions"]["line-b"]["confirmed_offset"], -1
            )
            self.assertEqual(len(data_lines(root, "line-b")), 0)
            self.assertIsNotNone(metrics["streams"]["s"]["error"])
            self.assertGreater(
                metrics["streams"]["s"]["unverified_dropped_bytes"], 0
            )
            # 截断错误不能悄悄跳过：stop_reasons 有记录
            self.assertTrue(
                any("line-b" in k for k in metrics["stop_reasons"])
            )

    def test_bad_json_stops_partition_without_skipping(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d, "rt")
            collector, src = make_collector(
                root, FIXTURES / "badline.gz", ("line-a",), chunk_size=3
            )
            metrics = collector.run(join_timeout=20)
            state = metrics["partitions"]["line-a"]
            self.assertEqual(state["status"], "STOPPED")
            # 第一条是合法记录（offset 0），正常落盘确认
            self.assertEqual(state["confirmed_offset"], 0)
            self.assertEqual(len(data_lines(root, "line-a")), 1)
            # 坏 JSON 不能悄悄跳过：确认位置停在坏行之前，没有越过它确认
            self.assertEqual(
                [a["offset"] for a in src.acks], [0]
            )
            self.assertIn("坏 JSON", state["stop_reason"])

    def test_too_long_line_stops_partition(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            dpath = Path(d)
            long_line = b'{"partition":"line-a","offset":1,"pad":"' + b"z" * 200 + b'"}'
            blob = write_bytes(
                dpath / "long.gz",
                gzip_member([record_line("line-a", 0), long_line]),
            )
            collector, src = make_collector(
                dpath / "rt", blob, ("line-a",), chunk_size=4
            )
            collector.max_line_bytes = 128
            metrics = collector.run(join_timeout=20)
            state = metrics["partitions"]["line-a"]
            self.assertEqual(state["status"], "STOPPED")
            self.assertIn("行长度", state["stop_reason"] or "")


class ObservabilityTests(unittest.TestCase):
    def test_metrics_contain_required_fields(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d, "rt")
            collector, _src = make_collector(
                root, FIXTURES / "members.gz", ("line-a", "line-b")
            )
            metrics = collector.run(join_timeout=20)
            self.assertIn("global_peak", metrics["memory"])
            self.assertIn("accounts", metrics["memory"])
            self.assertIn("recent_blocks", metrics["memory"])
            for partition in ("line-a", "line-b"):
                state = metrics["partitions"][partition]
                for key in (
                    "status",
                    "confirmed_offset",
                    "last_ack_offset",
                    "peak_bytes",
                    "in_flight_bytes",
                    "stop_reason",
                ):
                    self.assertIn(key, state)
            self.assertIn("downstream_gate", metrics)
            self.assertIn("block_reasons", metrics)
            self.assertIn("stop_reasons", metrics)


if __name__ == "__main__":
    unittest.main()
