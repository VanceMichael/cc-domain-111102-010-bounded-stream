import unittest
import json
from app.wire import Replay

from app.source import StreamSource, load_manifest
class DomainTests(unittest.TestCase):
    def test_chunk_limit(self):
        s=StreamSource(chunk_size=7);parts=list(s.chunks())
        self.assertTrue(all(len(p)<=7 for p in parts));self.assertEqual(sum(map(len,parts)),s.path.stat().st_size)
    def test_ack(self):
        s=StreamSource();s.acknowledge("line-a",0)
        self.assertEqual(s.acks,[{"partition":"line-a","offset":0}])
    def test_manifest(self):
        self.assertEqual(load_manifest()["memory_limit"],65536)


class WireTests(unittest.TestCase):
    def test_unknown_route(self):
        from tempfile import TemporaryDirectory
        from pathlib import Path
        with TemporaryDirectory() as d:
            p=Path(d)/"routes.json";p.write_text("{}")
            self.assertEqual(Replay(p).get("https://missing.invalid").status,404)
