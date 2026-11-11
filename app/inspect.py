from pathlib import Path
from app.wire import Replay
from app.source import StreamSource
s=StreamSource();print(sum(len(b) for b in s.chunks()),"压缩字节")
