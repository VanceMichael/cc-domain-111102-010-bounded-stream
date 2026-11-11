import base64
from copy import deepcopy
from dataclasses import dataclass
import json
from pathlib import Path

@dataclass(frozen=True)
class Reply:
    status: int
    headers: dict
    body: bytes

class Replay:
    def __init__(self, path):
        self.routes = json.loads(Path(path).read_text(encoding="utf-8"))
        self.positions = {}
        self.requests = []

    def get(self, url, headers=None):
        self.requests.append({"url": url, "headers": deepcopy(headers or {})})
        if url not in self.routes:
            return Reply(404, {}, b"")
        entries = self.routes[url]
        index = self.positions.get(url, 0)
        if index >= len(entries):
            raise RuntimeError("响应序列已耗尽：" + url)
        self.positions[url] = index + 1
        value = entries[index]
        data = value.get("body", "")
        body = base64.b64decode(data) if value.get("base64") else data.encode("utf-8")
        return Reply(value["status"], deepcopy(value.get("headers", {})), body)
