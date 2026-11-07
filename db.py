"""Encrypted SQLite storage for TeraBox cookies and Telegram target ingestion."""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken

import config

_DB_PATH = Path(
    os.getenv("BOT_DB_PATH", str(config.DATA_DIR / "bot.sqlite3"))
).expanduser()
_cipher = Fernet(config.ENCRYPTION_KEY.encode("ascii"))


def _connect() -> sqlite3.Connection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(_DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS terabox_tokens ("
        "user_id INTEGER PRIMARY KEY, token BLOB NOT NULL, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS telegram_sessions ("
        "user_id INTEGER PRIMARY KEY, session BLOB NOT NULL, phone TEXT NOT NULL DEFAULT '', "
        "updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS target_chats ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "user_id INTEGER NOT NULL, "
        "chat_id TEXT NOT NULL, "
        "chat_ref TEXT NOT NULL, "
        "title TEXT NOT NULL DEFAULT '', "
        "username TEXT NOT NULL DEFAULT '', "
        "chat_type TEXT NOT NULL DEFAULT '', "
        "start_message_id INTEGER, "
        "start_url TEXT NOT NULL DEFAULT '', "
        "auto_enabled INTEGER NOT NULL DEFAULT 0, "
        "last_seen_message_id INTEGER NOT NULL DEFAULT 0, "
        "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, "
        "updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, "
        "UNIQUE(user_id, chat_id))"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS target_items ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "target_id INTEGER NOT NULL, "
        "message_id INTEGER NOT NULL, "
        "source_chat_id TEXT NOT NULL, "
        "source_url TEXT NOT NULL DEFAULT '', "
        "file_name TEXT NOT NULL DEFAULT '', "
        "state TEXT NOT NULL DEFAULT 'queued', "
        "terabox_url TEXT NOT NULL DEFAULT '', "
        "error TEXT NOT NULL DEFAULT '', "
        "discovered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, "
        "started_at TEXT, "
        "completed_at TEXT, "
        "UNIQUE(target_id, message_id), "
        "FOREIGN KEY(target_id) REFERENCES target_chats(id) ON DELETE CASCADE)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_target_user ON target_chats(user_id, updated_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_target_auto ON target_chats(chat_id, auto_enabled)")
    try:
        # One source chat must have one owner; otherwise every new post would
        # be uploaded multiple times with different cookies.
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_target_chat_owner ON target_chats(chat_id)")
    except sqlite3.IntegrityError:
        # Keep an older DB usable if a previous experimental build already
        # created duplicates; upsert_target still blocks new cross-owner binds.
        pass
    conn.execute("CREATE INDEX IF NOT EXISTS idx_item_target_state ON target_items(target_id, state, id)")
    conn.commit()
    try:
        os.chmod(_DB_PATH, 0o600)
    except OSError:
        pass
    return conn


@contextmanager
def _connection():
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def get_token(user_id: int) -> str | None:
    with _connection() as conn:
        row = conn.execute(
            "SELECT token FROM terabox_tokens WHERE user_id = ?", (int(user_id),)
        ).fetchone()
    if row is None:
        return None
    try:
        return _cipher.decrypt(bytes(row[0])).decode("utf-8")
    except InvalidToken:
        # A restored DB with the wrong encryption key cannot be used safely.
        # Remove only this unusable row and ask the user to bind again.
        with _connection() as conn:
            conn.execute("DELETE FROM terabox_tokens WHERE user_id = ?", (int(user_id),))
            conn.commit()
        return None


def save_token(user_id: int, token: str) -> None:
    encrypted = _cipher.encrypt(token.encode("utf-8"))
    with _connection() as conn:
        conn.execute(
            "INSERT INTO terabox_tokens (user_id, token, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP) "
            "ON CONFLICT(user_id) DO UPDATE SET token=excluded.token, updated_at=CURRENT_TIMESTAMP",
            (int(user_id), encrypted),
        )
        conn.commit()


def delete_token(user_id: int) -> bool:
    with _connection() as conn:
        cur = conn.execute("DELETE FROM terabox_tokens WHERE user_id = ?", (int(user_id),))
        conn.commit()
        return cur.rowcount > 0


def _dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


def save_telegram_session(user_id: int, session: str, phone: str = "") -> None:
    encrypted = _cipher.encrypt(session.encode("utf-8"))
    with _connection() as conn:
        conn.execute(
            "INSERT INTO telegram_sessions (user_id, session, phone, updated_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP) "
            "ON CONFLICT(user_id) DO UPDATE SET session=excluded.session, phone=excluded.phone, updated_at=CURRENT_TIMESTAMP",
            (int(user_id), encrypted, phone or ""),
        )
        conn.commit()


def get_telegram_session(user_id: int) -> str | None:
    with _connection() as conn:
        row = conn.execute(
            "SELECT session FROM telegram_sessions WHERE user_id = ?", (int(user_id),)
        ).fetchone()
    if row is None:
        return None
    try:
        return _cipher.decrypt(bytes(row[0])).decode("utf-8")
    except InvalidToken:
        with _connection() as conn:
            conn.execute("DELETE FROM telegram_sessions WHERE user_id = ?", (int(user_id),))
            conn.commit()
        return None


def get_telegram_session_info(user_id: int) -> dict[str, Any] | None:
    with _connection() as conn:
        row = conn.execute(
            "SELECT user_id, phone, updated_at FROM telegram_sessions WHERE user_id = ?",
            (int(user_id),),
        ).fetchone()
    return _dict(row)


def list_telegram_session_users() -> list[int]:
    with _connection() as conn:
        rows = conn.execute("SELECT user_id FROM telegram_sessions ORDER BY user_id").fetchall()
    return [int(row[0]) for row in rows]


def delete_telegram_session(user_id: int) -> bool:
    with _connection() as conn:
        cur = conn.execute("DELETE FROM telegram_sessions WHERE user_id = ?", (int(user_id),))
        conn.commit()
        return cur.rowcount > 0


# ---------- targeted Telegram chats ----------
def upsert_target(
    user_id: int,
    chat_id: str | int,
    chat_ref: str,
    title: str = "",
    username: str = "",
    chat_type: str = "",
) -> int:
    with _connection() as conn:
        existing = conn.execute(
            "SELECT user_id FROM target_chats WHERE chat_id = ? LIMIT 1", (str(chat_id),)
        ).fetchone()
        if existing is not None and int(existing[0]) != int(user_id):
            raise ValueError("Ye group/channel kisi doosre bot user ke target me already configured hai")
        conn.execute(
            "INSERT INTO target_chats "
            "(user_id, chat_id, chat_ref, title, username, chat_type) VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(user_id, chat_id) DO UPDATE SET "
            "chat_ref=excluded.chat_ref, title=excluded.title, username=excluded.username, "
            "chat_type=excluded.chat_type, updated_at=CURRENT_TIMESTAMP",
            (int(user_id), str(chat_id), str(chat_ref), title or "", username or "", chat_type or ""),
        )
        row = conn.execute(
            "SELECT id FROM target_chats WHERE user_id = ? AND chat_id = ?",
            (int(user_id), str(chat_id)),
        ).fetchone()
        conn.commit()
    return int(row[0])


def get_target(
    user_id: int,
    target_id: int | None = None,
    chat_id: str | int | None = None,
) -> dict[str, Any] | None:
    with _connection() as conn:
        if target_id is not None:
            row = conn.execute(
                "SELECT * FROM target_chats WHERE user_id = ? AND id = ?",
                (int(user_id), int(target_id)),
            ).fetchone()
        elif chat_id is not None:
            row = conn.execute(
                "SELECT * FROM target_chats WHERE user_id = ? AND chat_id = ?",
                (int(user_id), str(chat_id)),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM target_chats WHERE user_id = ? ORDER BY updated_at DESC, id DESC LIMIT 1",
                (int(user_id),),
            ).fetchone()
    return _dict(row)


def list_targets(user_id: int) -> list[dict[str, Any]]:
    with _connection() as conn:
        rows = conn.execute(
            "SELECT * FROM target_chats WHERE user_id = ? ORDER BY updated_at DESC, id DESC",
            (int(user_id),),
        ).fetchall()
    return [dict(row) for row in rows]


def set_target_start(user_id: int, target_id: int, message_id: int, source_url: str) -> bool:
    with _connection() as conn:
        cur = conn.execute(
            "UPDATE target_chats SET start_message_id = ?, start_url = ?, updated_at=CURRENT_TIMESTAMP "
            "WHERE id = ? AND user_id = ?",
            (int(message_id), source_url or "", int(target_id), int(user_id)),
        )
        conn.commit()
        return cur.rowcount > 0


def set_target_auto(user_id: int, target_id: int, enabled: bool) -> bool:
    with _connection() as conn:
        cur = conn.execute(
            "UPDATE target_chats SET auto_enabled = ?, updated_at=CURRENT_TIMESTAMP "
            "WHERE id = ? AND user_id = ?",
            (1 if enabled else 0, int(target_id), int(user_id)),
        )
        conn.commit()
        return cur.rowcount > 0


def delete_target(user_id: int, target_id: int) -> bool:
    with _connection() as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        cur = conn.execute(
            "DELETE FROM target_chats WHERE id = ? AND user_id = ?",
            (int(target_id), int(user_id)),
        )
        conn.commit()
        return cur.rowcount > 0


def auto_targets(chat_id: str | int) -> list[dict[str, Any]]:
    with _connection() as conn:
        rows = conn.execute(
            "SELECT * FROM target_chats WHERE chat_id = ? AND auto_enabled = 1 "
            "ORDER BY id LIMIT 1",
            (str(chat_id),),
        ).fetchall()
    return [dict(row) for row in rows]


def touch_target_message(target_id: int, message_id: int) -> None:
    with _connection() as conn:
        conn.execute(
            "UPDATE target_chats SET last_seen_message_id = MAX(last_seen_message_id, ?), "
            "updated_at=CURRENT_TIMESTAMP WHERE id = ?",
            (int(message_id), int(target_id)),
        )
        conn.commit()


# ---------- per-message durable processing state ----------
def ensure_target_item(
    target_id: int,
    message_id: int,
    source_chat_id: str | int,
    source_url: str = "",
    file_name: str = "",
) -> tuple[int, bool]:
    """Return (item id, created). Existing uploaded items are never duplicated."""
    with _connection() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO target_items "
            "(target_id, message_id, source_chat_id, source_url, file_name) VALUES (?, ?, ?, ?, ?)",
            (int(target_id), int(message_id), str(source_chat_id), source_url or "", file_name or ""),
        )
        row = conn.execute(
            "SELECT id FROM target_items WHERE target_id = ? AND message_id = ?",
            (int(target_id), int(message_id)),
        ).fetchone()
        if not cur.rowcount and row:
            conn.execute(
                "UPDATE target_items SET source_url=CASE WHEN source_url='' THEN ? ELSE source_url END, "
                "file_name=CASE WHEN file_name='' THEN ? ELSE file_name END WHERE id = ?",
                (source_url or "", file_name or "", int(row[0])),
            )
        conn.commit()
    return int(row[0]), bool(cur.rowcount)


def get_target_item(item_id: int) -> dict[str, Any] | None:
    with _connection() as conn:
        row = conn.execute("SELECT * FROM target_items WHERE id = ?", (int(item_id),)).fetchone()
    return _dict(row)


def mark_item_queued(item_id: int) -> None:
    with _connection() as conn:
        conn.execute(
            "UPDATE target_items SET state='queued', error='', started_at=NULL, completed_at=NULL WHERE id = ?",
            (int(item_id),),
        )
        conn.commit()


def mark_item_downloading(item_id: int) -> None:
    with _connection() as conn:
        conn.execute(
            "UPDATE target_items SET state='downloading', error='', started_at=CURRENT_TIMESTAMP WHERE id = ?",
            (int(item_id),),
        )
        conn.commit()


def mark_item_started(item_id: int) -> None:
    with _connection() as conn:
        conn.execute(
            "UPDATE target_items SET state='uploading', error='', started_at=CURRENT_TIMESTAMP WHERE id = ?",
            (int(item_id),),
        )
        conn.commit()


def mark_item_uploaded(item_id: int, link: str, file_name: str = "") -> None:
    with _connection() as conn:
        conn.execute(
            "UPDATE target_items SET state='uploaded', terabox_url=?, "
            "file_name=CASE WHEN ? <> '' THEN ? ELSE file_name END, "
            "error='', completed_at=CURRENT_TIMESTAMP WHERE id = ?",
            (link or "", file_name or "", file_name or "", int(item_id)),
        )
        conn.commit()


def mark_item_failed(item_id: int, error: str) -> None:
    with _connection() as conn:
        conn.execute(
            "UPDATE target_items SET state='failed', error=?, completed_at=CURRENT_TIMESTAMP WHERE id = ?",
            ((error or "Unknown error")[:2000], int(item_id)),
        )
        conn.commit()


def mark_item_skipped(item_id: int, reason: str = "") -> None:
    with _connection() as conn:
        conn.execute(
            "UPDATE target_items SET state='skipped', error=?, completed_at=CURRENT_TIMESTAMP WHERE id = ?",
            ((reason or "Skipped")[:2000], int(item_id)),
        )
        conn.commit()


def target_summary(target_id: int) -> dict[str, int]:
    with _connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS total, "
            "COALESCE(SUM(state='queued'), 0) AS queued, "
            "COALESCE(SUM(state='downloading'), 0) AS downloading, "
            "COALESCE(SUM(state='uploading'), 0) AS uploading, "
            "COALESCE(SUM(state='uploaded'), 0) AS uploaded, "
            "COALESCE(SUM(state='failed'), 0) AS failed, "
            "COALESCE(SUM(state='skipped'), 0) AS skipped "
            "FROM target_items WHERE target_id = ?",
            (int(target_id),),
        ).fetchone()
    return {key: int(row[key] or 0) for key in ("total", "queued", "downloading", "uploading", "uploaded", "failed", "skipped")}


def list_links(target_id: int, limit: int = 10000, offset: int = 0) -> list[dict[str, Any]]:
    with _connection() as conn:
        rows = conn.execute(
            "SELECT id, message_id, source_url, file_name, terabox_url, completed_at "
            "FROM target_items WHERE target_id = ? AND state = 'uploaded' "
            "ORDER BY id LIMIT ? OFFSET ?",
            (int(target_id), max(1, min(int(limit), 10000)), max(0, int(offset))),
        ).fetchall()
    return [dict(row) for row in rows]


def reset_failed_items(target_id: int) -> int:
    with _connection() as conn:
        cur = conn.execute(
            "UPDATE target_items SET state='queued', error='', started_at=NULL, completed_at=NULL "
            "WHERE target_id = ? AND state = 'failed'",
            (int(target_id),),
        )
        conn.commit()
        return cur.rowcount


def recover_inflight_items() -> int:
    """Make rows from an interrupted process visible for the next /batch retry."""
    with _connection() as conn:
        cur = conn.execute(
            "UPDATE target_items SET state='queued', error='Recovered after bot restart', "
            "started_at=NULL WHERE state IN ('downloading', 'uploading')"
        )
        conn.commit()
        return cur.rowcount
