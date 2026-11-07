#!/usr/bin/env python3
"""
Standalone test script to test TeraBox video upload & share link generation
without relying on Telegram bot or Telegram download.
Usage:
    python3 test_upload.py "<your_terabox_cookie>"
    OR:
    export TERABOX_COOKIE="ndus=...; browserid=...; ndut_fmt=..."
    python3 test_upload.py
"""
import logging
import os
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("test_upload")

from terabox import TeraBox, TeraboxError

TEST_VIDEO_PATH = os.path.join(os.path.dirname(__file__), "sample_test_2.5mb.mp4")


def human_size(n: float) -> str:
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.2f} {u}"
        n /= 1024
    return f"{n:.2f} PB"


def main():
    token = None
    if len(sys.argv) > 1:
        token = " ".join(sys.argv[1:]).strip()
    elif os.getenv("TERABOX_COOKIE"):
        token = os.getenv("TERABOX_COOKIE").strip()

    # Check for cookies file in current directory or tasks/
    if not token:
        for cpath in (
            os.path.join(os.path.dirname(__file__), "tasks", "terabox_cookies.txt"),
            os.path.join(os.path.dirname(__file__), "terabox_cookies.txt"),
        ):
            if os.path.exists(cpath):
                try:
                    with open(cpath, "r", encoding="utf-8") as f:
                        content = f.read().strip()
                    if content:
                        token = content
                        log.info("Loaded TeraBox cookie from %s", cpath)
                        break
                except Exception:
                    pass

    if not token:
        print("=" * 60)
        print("TeraBox Direct Video Upload Test")
        print("=" * 60)
        print("TeraBox Cookie required to test direct upload.")
        print('Run with: python3 test_upload.py "<paste_your_cookie_here>"')
        print("Or place your cookies in tasks/terabox_cookies.txt")
        print('Or set: export TERABOX_COOKIE="<paste_your_cookie_here>"')
        print("=" * 60)
        token = input("Enter TeraBox Cookie (ndus or full Cookie header): ").strip()

    if not token:
        print("❌ Error: No token provided. Cannot proceed without TeraBox credentials.")
        sys.exit(1)

    if not os.path.exists(TEST_VIDEO_PATH):
        print(f"❌ Test video not found at {TEST_VIDEO_PATH}")
        sys.exit(1)

    video_size = os.path.getsize(TEST_VIDEO_PATH)
    video_name = f"test_video_{int(time.time())}.mp4"
    folder = "TG_Uploads"

    print("\n" + "=" * 60)
    print(f"1. Initializing TeraBox Client...")
    print(f"   Target file : {video_name} ({human_size(video_size)})")
    print(f"   Destination : /{folder}/{video_name}")
    print("=" * 60)

    tb = TeraBox(token)

    print("\n2. Validating Token and Quota via TeraBox API...")
    try:
        quota = tb.validate()
        used = quota.get("used", 0)
        total = quota.get("total", 0)
        print(f"   ✅ Token Valid!")
        print(f"   Used Storage : {human_size(used)} / {human_size(total)}")
    except TeraboxError as e:
        print(f"   ❌ Token validation failed: {e}")
        sys.exit(1)

    print("\n3. Testing Precreate & Upload...")
    last_print = [0.0]

    def on_progress(done, total):
        pct = (done / total * 100) if total else 0
        if time.time() - last_print[0] > 0.5 or done >= total:
            last_print[0] = time.time()
            print(f"   ⬆️ Uploading: {done}/{total} bytes ({pct:.1f}%)")

    t0 = time.time()
    try:
        share_link = tb.upload(
            TEST_VIDEO_PATH,
            video_name,
            folder,
            progress=on_progress,
        )
        elapsed = time.time() - t0
        print("\n" + "=" * 60)
        print("🎉 SUCCESS! Video uploaded and processed successfully!")
        print(f"   Time Taken : {elapsed:.2f} seconds")
        print(f"   File Name  : {video_name}")
        print(f"   Share Link : {share_link}")
        print("=" * 60)
        return share_link
    except TeraboxError as e:
        print(f"\n❌ TeraBox upload error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
