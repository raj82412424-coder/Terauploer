"""Application configuration with safe defaults and explicit runtime validation."""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from cryptography.fernet import Fernet

load_dotenv()


def _env_int(name: str, default: int, minimum: int, maximum: int | None = None) -> int:
    raw = os.getenv(name, str(default)).strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc
    if value < minimum or (maximum is not None and value > maximum):
        limit = f"{minimum}..{maximum}" if maximum is not None else f">={minimum}"
        raise RuntimeError(f"{name} must be in {limit}, got {value}")
    return value


# Keep runtime state together so Docker can mount one persistent directory.
DATA_DIR = Path(os.getenv("DATA_DIR", "./data")).expanduser()
DATA_DIR.mkdir(parents=True, exist_ok=True)
try:
    # Runtime state contains the Fernet key, encrypted cookies and session data.
    os.chmod(DATA_DIR, 0o700)
except OSError:
    pass

API_ID = _env_int("API_ID", 0, 0)
API_HASH = os.getenv("API_HASH", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()


def _load_encryption_key() -> str:
    configured = os.getenv("ENCRYPTION_KEY", "").strip()
    key_path = Path(
        os.getenv("ENCRYPTION_KEY_FILE", str(DATA_DIR / ".encryption_key"))
    ).expanduser()

    if configured:
        key = configured
    elif key_path.exists():
        key = key_path.read_text(encoding="utf-8").strip()
    else:
        key = Fernet.generate_key().decode("ascii")
        key_path.parent.mkdir(parents=True, exist_ok=True)
        # O_EXCL avoids silently replacing a key if two processes start together.
        try:
            fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(key + "\n")
        except FileExistsError:
            key = key_path.read_text(encoding="utf-8").strip()
        try:
            os.chmod(key_path, 0o600)
        except OSError:
            pass

    try:
        Fernet(key.encode("ascii"))
    except Exception as exc:
        raise RuntimeError(
            "ENCRYPTION_KEY invalid hai. Fernet.generate_key() se nayi key banao "
            "ya purani key restore karo; existing encrypted tokens ke liye purani key zaroori hai."
        ) from exc
    return key


ENCRYPTION_KEY = _load_encryption_key()

MAX_FILE_MB = _env_int("MAX_FILE_MB", 4096, 1, 10240)
REMOTE_FOLDER = os.getenv("REMOTE_FOLDER", "TG_Uploads").strip("/") or "TG_Uploads"
MAX_PARALLEL = _env_int("MAX_PARALLEL", 1, 1, 8)
# Telegram bot accounts can trigger FloodWait when too many media ranges are read.
# Per-job workers are capped, while TOTAL_DOWNLOAD_WORKERS bounds all jobs together.
TELEGRAM_DOWNLOAD_WORKERS = _env_int("TELEGRAM_DOWNLOAD_WORKERS", 3, 1, 4)
TELEGRAM_TOTAL_DOWNLOAD_WORKERS = _env_int("TELEGRAM_TOTAL_DOWNLOAD_WORKERS", 6, 1, 8)
TERABOX_UPLOAD_THREADS = _env_int("TERABOX_UPLOAD_THREADS", 6, 1, 8)
TERABOX_TOTAL_UPLOAD_THREADS = _env_int("TERABOX_TOTAL_UPLOAD_THREADS", 8, 1, 16)
TERABOX_MAX_RETRIES = _env_int("TERABOX_MAX_RETRIES", 8, 1, 20)
TERABOX_CONNECT_TIMEOUT = _env_int("TERABOX_CONNECT_TIMEOUT", 15, 3, 120)
TERABOX_READ_TIMEOUT = _env_int("TERABOX_READ_TIMEOUT", 60, 10, 600)
DOWNLOAD_DIR = str(DATA_DIR / "downloads")
MAX_URL_GB = _env_int("MAX_URL_GB", 10, 1, 100)
URL_FRAGMENT_WORKERS = _env_int("URL_FRAGMENT_WORKERS", 8, 1, 16)
STREAM_ABOVE_MB = _env_int("STREAM_ABOVE_MB", 1000, 1, 10240)
# Targeted Telegram ingestion is intentionally bounded so a typo cannot enqueue
# an unbounded channel archive. Use /batch all to scan up to this many messages.
MAX_BATCH_MESSAGES = _env_int("MAX_BATCH_MESSAGES", 10000, 1, 100000)


def validate_runtime_config() -> None:
    """Fail early with an actionable message instead of a Pyrogram traceback."""
    missing = [name for name, value in (
        ("API_ID", API_ID),
        ("API_HASH", API_HASH),
        ("BOT_TOKEN", BOT_TOKEN),
    ) if not value]
    if missing:
        raise RuntimeError(
            "Required environment variables missing: " + ", ".join(missing) +
            ". Copy .env.example to .env and fill them in."
        )


os.makedirs(DOWNLOAD_DIR, exist_ok=True)
