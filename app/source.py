import json
from pathlib import Path

class StreamSource:
    def __init__(self,path="fixtures/members.gz",chunk_size=7):
        if chunk_size < 1: raise ValueError("块大小必须为正")
        self.path=Path(path);self.chunk_size=chunk_size;self.acks=[];self.bytes_read=0
    def chunks(self):
        with self.path.open("rb") as stream:
            while True:
                value=stream.read(self.chunk_size)
                if not value: break
                self.bytes_read+=len(value)
                yield value
    def acknowledge(self,partition,offset):
        self.acks.append({"partition":partition,"offset":offset})

def load_manifest(path="fixtures/manifest.json"):
    value=json.loads(Path(path).read_text())
    if any(v<0 for v in value["start_offsets"].values()): raise ValueError("起始位置无效")
    return value
