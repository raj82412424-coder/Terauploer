"""Resilient parallel Telegram media downloader.

Telegram media is fetched in disjoint 1 MiB ranges. Each range is retried
independently and resumed after a dropped media-DC connection. Data is first
written to a private .part file and atomically renamed only after the complete
file-size check succeeds.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Awaitable, Callable

log = logging.getLogger("tgdl")

TG_CHUNK_SIZE = 1024 * 1024
SEGMENT_CHUNKS = 8                 # queue units are 8 MiB per worker
CHUNK_TIMEOUT = 90                 # seconds without a chunk => stalled
MAX_RETRIES = 8                    # consecutive failures per segment

ProgressCallback = Callable[[int, int, float], Awaitable[None]]


def _flood_seconds(exc: BaseException) -> int:
    """Return FloodWait seconds if exc is a Pyrogram FloodWait, else 0."""
    if exc.__class__.__name__ in ("FloodWait", "SlowmodeWait"):
        try:
            return max(0, int(getattr(exc, "value", 0) or 0))
        except (TypeError, ValueError):
            return 0
    return 0


async def download_media_parallel(
    client,
    media,
    path: str,
    file_size: int,
    progress_callback: ProgressCallback,
    workers: int = 3,
    range_limiter: asyncio.Semaphore | None = None,
):
    """Download Telegram media with bounded parallelism and range resume.

    ``stream_media`` uses chunk indexes for ``offset``/``limit``. A failed
    segment only restarts from its next missing chunk, not from the beginning.
    UI/progress callback failures are intentionally ignored so Telegram edits
    can never abort a valid transfer. ``range_limiter`` optionally bounds the
    number of active Telegram media streams across multiple simultaneous jobs.
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp_path = f"{path}.part"
    try:
        os.unlink(tmp_path)
    except FileNotFoundError:
        pass

    completed = 0
    last_reported = 0
    last_report_at = time.monotonic()
    progress_lock = asyncio.Lock()

    async def record_progress(amount: int, total: int) -> None:
        nonlocal completed, last_reported, last_report_at
        async with progress_lock:
            completed += amount
            now = time.monotonic()
            if now - last_report_at >= 3 or (total and completed >= total):
                rate = (completed - last_reported) / max(now - last_report_at, 0.001)
                last_reported, last_report_at = completed, now
                try:
                    await progress_callback(completed, total, rate)
                except Exception as exc:  # progress must not kill a transfer
                    log.debug("progress callback ignored: %r", exc)

    async def retry_delay(exc: BaseException, failures: int) -> None:
        flood = _flood_seconds(exc)
        if flood:
            delay = flood + 1
        else:
            delay = min(2 ** failures, 30)
        log.warning(
            "Telegram range retry %s/%s in %ss: %r",
            failures, MAX_RETRIES, delay, exc,
        )
        await asyncio.sleep(delay)

    async def fetch_unknown_size() -> None:
        """Sequential resumable mode for media without a reported size."""
        chunk_index = 0
        failures = 0
        with open(tmp_path, "wb") as out:
            while True:
                agen = None
                permit = False
                try:
                    if range_limiter is not None:
                        await range_limiter.acquire()
                        permit = True
                    agen = client.stream_media(media, offset=chunk_index)
                    while True:
                        chunk = await asyncio.wait_for(
                            agen.__anext__(), CHUNK_TIMEOUT
                        )
                        if not chunk:
                            continue
                        out.write(chunk)
                        out.flush()
                        chunk_index += 1
                        failures = 0
                        await record_progress(len(chunk), 0)
                    return
                except StopAsyncIteration:
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    failures += 1
                    if failures > MAX_RETRIES:
                        raise IOError(
                            f"Telegram download failed at chunk {chunk_index} "
                            f"after {MAX_RETRIES} retries: {exc!r}"
                        ) from exc
                    await retry_delay(exc, failures)
                finally:
                    if agen is not None:
                        try:
                            await asyncio.wait_for(agen.aclose(), 10)
                        except BaseException:
                            pass
                    if permit and range_limiter is not None:
                        range_limiter.release()

    try:
        if file_size <= 0:
            await fetch_unknown_size()
            os.replace(tmp_path, path)
            return path

        total_chunks = (file_size + TG_CHUNK_SIZE - 1) // TG_CHUNK_SIZE
        fd = os.open(tmp_path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o600)
        try:
            os.ftruncate(fd, file_size)

            def write_at(offset: int, data: bytes) -> None:
                view = memoryview(data)
                written = 0
                while written < len(view):
                    count = os.pwrite(fd, view[written:], offset + written)
                    if count <= 0:
                        raise OSError("Could not write downloaded chunk to disk")
                    written += count

            def expected_len(idx: int) -> int:
                return min(TG_CHUNK_SIZE, file_size - idx * TG_CHUNK_SIZE)

            async def fetch_segment(start: int, count: int, wid: int) -> None:
                """Download chunks [start, start+count), resuming on failure."""
                next_idx, end, failures = start, start + count, 0
                while next_idx < end:
                    agen = None
                    permit = False
                    try:
                        # Keep stream_media() creation inside the retry block;
                        # media-DC/session creation itself can fail.
                        if range_limiter is not None:
                            await range_limiter.acquire()
                            permit = True
                        agen = client.stream_media(
                            media, limit=end - next_idx, offset=next_idx
                        )
                        while next_idx < end:
                            chunk = await asyncio.wait_for(
                                agen.__anext__(), CHUNK_TIMEOUT
                            )
                            expected = expected_len(next_idx)
                            if len(chunk) != expected:
                                raise IOError(
                                    f"chunk {next_idx}: got {len(chunk)} bytes, "
                                    f"expected {expected}"
                                )
                            write_at(next_idx * TG_CHUNK_SIZE, chunk)
                            next_idx += 1
                            failures = 0
                            await record_progress(len(chunk), file_size)
                        return
                    except StopAsyncIteration:
                        if next_idx >= end:
                            return
                        exc = IOError("Telegram returned fewer chunks than requested")
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:  # timeout, flood, null packet, network
                        exc = error
                    finally:
                        if agen is not None:
                            try:
                                await asyncio.wait_for(agen.aclose(), 10)
                            except BaseException:
                                pass
                        if permit and range_limiter is not None:
                            range_limiter.release()

                    failures += 1
                    if failures > MAX_RETRIES:
                        raise IOError(
                            f"Telegram download failed at chunk {next_idx} "
                            f"after {MAX_RETRIES} retries: {exc!r}"
                        ) from exc
                    log.warning(
                        "[worker %s] chunk %s failed (%r)", wid, next_idx, exc
                    )
                    await retry_delay(exc, failures)

            # Authorise the media DC/session once before starting workers.
            await fetch_segment(0, 1, 0)

            queue: asyncio.Queue[tuple[int, int]] = asyncio.Queue()
            idx = 1
            while idx < total_chunks:
                n = min(SEGMENT_CHUNKS, total_chunks - idx)
                queue.put_nowait((idx, n))
                idx += n

            async def worker(wid: int) -> None:
                while True:
                    try:
                        start, count = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    await fetch_segment(start, count, wid)

            n_workers = (
                max(1, min(max(1, int(workers)), queue.qsize()))
                if queue.qsize() else 0
            )
            tasks = [asyncio.create_task(worker(i + 1)) for i in range(n_workers)]
            try:
                await asyncio.gather(*tasks)
            except BaseException:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise

            if completed != file_size:
                raise IOError(
                    f"Downloaded size mismatch: expected {file_size}, got {completed}"
                )
        finally:
            os.close(fd)

        os.replace(tmp_path, path)
        return path
    except BaseException:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        raise
