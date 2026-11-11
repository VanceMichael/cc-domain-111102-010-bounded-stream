"""进程级崩溃恢复演练脚本（由集成测试以子进程方式调用）。

脚本与素材/运行数据目录完全分离，所有路径经命令行参数传入，使用 ``-I``
隔离模式运行。第一次运行在提交指定 offset 后立即 ``os._exit`` 模拟硬崩溃
（不执行任何 Python 清理、不发源确认），用于验证：

* 数据+索引+清单已 fsync，重启后已确认前缀完整；
* 提交后、确认前退出不会产生业务重复。
"""

from __future__ import annotations

import os
import sys

# 仅通过显式路径引入项目，不依赖当前工作目录
sys.path.insert(0, os.environ["PROJECT_ROOT"])

from pathlib import Path  # noqa: E402

from app.pipeline import Collector, FixtureSource, StreamConfig  # noqa: E402


def main() -> None:
    blob = Path(sys.argv[1])
    store_root = Path(sys.argv[2])
    crash_offset = int(sys.argv[3])
    partition = sys.argv[4]

    def crash_after_commit(part: str, max_offset: int) -> None:
        if part == partition and max_offset == crash_offset:
            # 硬退出：刷新操作系统缓冲后立即终止，跳过 acks 与所有 finally
            os.sync()
            os._exit(27)

    source = FixtureSource(blob, chunk_size=3)
    collector = Collector(
        [StreamConfig(name="s", source=source, partitions=(partition,))],
        store_root=store_root,
        memory_limit=4096,
        partition_memory_limit=2048,
        batch_records=1,
        on_crash_after_commit=crash_after_commit,
    )
    collector.run(join_timeout=30)


if __name__ == "__main__":
    main()
