import asyncio
import os
import tempfile
import unittest
from unittest.mock import patch

import tgdl


class _FakeStream:
    def __init__(self, chunks, fail_once=False):
        self._chunks = iter(chunks)
        self._fail_once = fail_once
        self._failed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._fail_once and not self._failed:
            self._failed = True
            raise RuntimeError("simulated media DC reset")
        try:
            return next(self._chunks)
        except StopIteration as exc:
            raise StopAsyncIteration from exc

    async def aclose(self):
        return None


class _FakeClient:
    def __init__(self, payload, chunk_size, fail_offset=None):
        self.payload = payload
        self.chunk_size = chunk_size
        self.fail_offset = fail_offset
        self.calls = []
        self.failed = False

    def stream_media(self, _media, limit=0, offset=0):
        self.calls.append((limit, offset))
        start = offset * self.chunk_size
        end = len(self.payload) if not limit else min(
            len(self.payload), start + limit * self.chunk_size
        )
        chunks = [
            self.payload[pos : min(pos + self.chunk_size, end)]
            for pos in range(start, end, self.chunk_size)
        ]
        fail = self.fail_offset == offset and not self.failed
        self.failed = self.failed or fail
        return _FakeStream(chunks, fail_once=fail)


class TelegramDownloaderTests(unittest.TestCase):
    def test_parallel_download_resumes_and_atomically_renames(self):
        async def run():
            original_chunk = tgdl.TG_CHUNK_SIZE
            original_segment = tgdl.SEGMENT_CHUNKS
            try:
                tgdl.TG_CHUNK_SIZE = 4
                tgdl.SEGMENT_CHUNKS = 2
                payload = b"abcdefghijklmn"
                client = _FakeClient(payload, chunk_size=4, fail_offset=1)
                progress = []

                async def callback(cur, total, rate):
                    progress.append((cur, total, rate))

                with tempfile.TemporaryDirectory() as directory, patch(
                    "tgdl.asyncio.sleep", new=_no_sleep
                ):
                    path = os.path.join(directory, "video.bin")
                    result = await tgdl.download_media_parallel(
                        client,
                        object(),
                        path,
                        len(payload),
                        callback,
                        workers=2,
                    )
                    self.assertEqual(result, path)
                    with open(path, "rb") as handle:
                        self.assertEqual(handle.read(), payload)
                    self.assertFalse(os.path.exists(path + ".part"))
                    self.assertGreaterEqual(len(client.calls), 3)
                    self.assertEqual(progress[-1][0:2], (len(payload), len(payload)))

            finally:
                tgdl.TG_CHUNK_SIZE = original_chunk
                tgdl.SEGMENT_CHUNKS = original_segment

        asyncio.run(run())

    def test_unknown_size_download_is_resumable(self):
        async def run():
            original_chunk = tgdl.TG_CHUNK_SIZE
            try:
                tgdl.TG_CHUNK_SIZE = 4
                payload = b"abcdefghij"
                client = _FakeClient(payload, chunk_size=4, fail_offset=0)
                with tempfile.TemporaryDirectory() as directory, patch(
                    "tgdl.asyncio.sleep", new=_no_sleep
                ):
                    path = os.path.join(directory, "unknown.bin")
                    await tgdl.download_media_parallel(
                        client, object(), path, 0, _record, workers=1
                    )
                    with open(path, "rb") as handle:
                        self.assertEqual(handle.read(), payload)
                    self.assertFalse(os.path.exists(path + ".part"))
            finally:
                tgdl.TG_CHUNK_SIZE = original_chunk

        asyncio.run(run())


def _no_sleep(_seconds):
    async def done():
        return None

    return done()


async def _record(cur, total, rate):
    return None


if __name__ == "__main__":
    unittest.main()
