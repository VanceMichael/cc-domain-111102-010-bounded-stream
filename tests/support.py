"""测试夹具：在独立目录生成合成 gzip/NDJSON 字节流。

全部数据为人工生成，不含任何真实生产信息。脚本与素材目录分离，所有路径
通过参数传入，运行解释器使用 ``-I`` 隔离模式。
"""

from __future__ import annotations

import gzip
import io
import json
import os
from pathlib import Path

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"


def record_line(partition: str, offset: int, reading: int | None = None) -> bytes:
    payload = {
        "partition": partition,
        "offset": offset,
        "payload": {
            "reading": offset if reading is None else reading,
            "label": "合成温度",
        },
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def gzip_member(lines: list[bytes]) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        gz.write(b"".join(line + b"\n" for line in lines))
    return buf.getvalue()


def multi_member_gzip(member_lines: list[list[bytes]]) -> bytes:
    return b"".join(gzip_member(lines) for lines in member_lines)


def write_bytes(path: Path, data: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def truncate_member(path: Path, cut: int) -> Path:
    """把文件尾部截掉 ``cut`` 字节，制造 gzip member 截断。"""
    data = path.read_bytes()
    path.write_bytes(data[:-cut])
    return path


def fresh_dir(root: Path, name: str) -> Path:
    target = root / name
    target.mkdir(parents=True, exist_ok=True)
    return target


def fdatasync_supported() -> bool:
    return hasattr(os, "fdatasync")
