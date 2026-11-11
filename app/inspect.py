"""采集链路演示：对素材跑一次有界、可恢复采集并打印可观察指标。

从项目根执行（运行产物写入 runtime/，与素材分离）::

    python3 -I -m app.inspect
"""

from __future__ import annotations

import json
from pathlib import Path

from app.pipeline import Collector, FixtureSource, StreamConfig
from app.source import StreamSource, load_manifest

ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    manifest = load_manifest(ROOT / "fixtures" / "manifest.json")
    partitions = tuple(manifest["partitions"])
    runtime = ROOT / "runtime" / "inspect"
    runtime.mkdir(parents=True, exist_ok=True)

    source = FixtureSource(
        ROOT / "fixtures" / "members.gz",
        chunk_size=manifest["chunk_sizes"][1],
    )
    collector = Collector(
        [StreamConfig(name="members", source=source, partitions=partitions)],
        store_root=runtime,
        memory_limit=manifest["memory_limit"],
        max_line_bytes=manifest["max_line_bytes"],
        batch_records=4,
    )
    metrics = collector.run(join_timeout=30)
    print(json.dumps(metrics, ensure_ascii=False, indent=2))

    raw = StreamSource(path=ROOT / "fixtures" / "members.gz")
    print(f"\n源压缩字节总数: {sum(len(b) for b in raw.chunks())}")
    for partition in partitions:
        data = runtime / f"p_{partition}" / "data.ndjson"
        count = len(data.read_bytes().splitlines()) if data.exists() else 0
        print(f"分区 {partition}: 落盘 {count} 条")


if __name__ == "__main__":
    main()
