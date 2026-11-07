import asyncio
import contextlib
import logging
import math
import os
import re
import shutil
import threading
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from urllib.parse import urlparse

import yt_dlp
from pyrogram import Client, filters, idle
from pyrogram.handlers import MessageHandler
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

import config
import db
import tgdl
from dashboard import start_dashboard
from terabox import TeraBox, TeraboxError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
# Idle media-DC reconnects ("null packet" restart) are normal; hide the noise.
for _n in ("pyrogram.connection", "pyrogram.session.auth", "pyrogram.dispatcher"):
    logging.getLogger(_n).setLevel(logging.WARNING)
logging.getLogger("pyrogram.session.session").setLevel(logging.WARNING)
logging.getLogger("pyrogram.client").setLevel(logging.WARNING)

config.validate_runtime_config()

db.recover_inflight_items()

os.makedirs(config.DOWNLOAD_DIR, exist_ok=True)

# Pyrogram's session contains Telegram authentication state; keep it private
# and colocated with the rest of the persistent runtime data.
os.umask(0o077)

app = Client(
    "terabox_uploader",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
    workdir=str(config.DATA_DIR),
    max_concurrent_transmissions=config.TELEGRAM_TOTAL_DOWNLOAD_WORKERS,
    sleep_threshold=30,
)

log = logging.getLogger("bot")

# kurigram/pyrogram forks renamed disable_web_page_preview -> link_preview_options.
try:
    from pyrogram.types import LinkPreviewOptions
    NO_PREVIEW = {"link_preview_options": LinkPreviewOptions(is_disabled=True)}
except Exception:
    NO_PREVIEW = {"disable_web_page_preview": True}


async def send_done(status, name: str, link: str):
    """Final success message. Link is always logged so it can never be lost."""
    log.info("UPLOAD DONE: %s -> %s", name, link)
    text = f"✅ **Upload ho gaya!**\n\n📁 `{name}`\n🔗 {link}"
    for kwargs in (NO_PREVIEW, {}):
        try:
            await status.edit_text(text, **kwargs)
            return
        except TypeError:
            continue  # this library version does not know that keyword
        except Exception as exc:
            log.warning("Could not edit final upload status: %s", exc)
            break

    # A FloodWait or an already-edited/deleted status message must not turn a
    # completed upload into a misleading ❌ error in the worker.
    for kwargs in (NO_PREVIEW, {}):
        try:
            await status.reply_text(text, **kwargs)
            return
        except TypeError:
            continue
        except Exception as exc:
            log.warning("Could not send final upload status: %s", exc)
            return

URL_RE = re.compile(r'https?://\S+')
TELEGRAM_MESSAGE_RE = re.compile(
    r"^https?://(?:t\.me|telegram\.me)/(?:s/)?(?P<path>[^?#]+)", re.I
)
waiting_for_token: set[int] = set()
# Private-chat wizard state. It deliberately lives in memory; the durable
# target itself is saved after each completed step, so a restart cannot lose
# an already configured target.
target_wizards: dict[int, dict] = {}
# Telegram user-account sessions are encrypted in SQLite. Live clients stay in
# memory only; they are recreated from the encrypted session after restart.
telegram_login_states: dict[int, dict] = {}
user_clients: dict[int, Client] = {}
# This semaphore remains as a safety net inside workers. The explicit FIFO queue
# below controls when each download/upload job is allowed to start.
upload_sem = asyncio.Semaphore(config.MAX_PARALLEL)
# These are process-wide budgets. They let each active job keep bounded
# Telegram/TeraBox connections without multiplying them unexpectedly.
download_range_slots = asyncio.Semaphore(config.TELEGRAM_TOTAL_DOWNLOAD_WORKERS)
upload_part_slots = threading.BoundedSemaphore(config.TERABOX_TOTAL_UPLOAD_THREADS)

@dataclass(slots=True)
class QueueJob:
    user_id: int
    generation: int
    run: Callable[[], Awaitable[None]]
    status: Message | None = None


job_queue: asyncio.Queue[QueueJob] = asyncio.Queue()
queue_workers: list[asyncio.Task] = []
active_tasks: defaultdict[int, set[asyncio.Task]] = defaultdict(set)
user_generations: defaultdict[int, int] = defaultdict(int)
pending_by_user: defaultdict[int, int] = defaultdict(int)
active_jobs = 0


async def _safe_job_status(status: Message | None, text: str) -> None:
    if status is None:
        return
    try:
        await status.edit_text(text)
    except Exception:
        pass


async def to_thread_wait_on_cancel(func, *args, **kwargs):
    """Run blocking network work without deleting its input on cancellation."""
    task = asyncio.create_task(asyncio.to_thread(func, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # asyncio.to_thread cannot kill its native thread. Wait for it to
        # settle before the caller's finally block removes downloaded files.
        await asyncio.shield(asyncio.gather(task, return_exceptions=True))
        raise


async def queue_worker(worker_id: int) -> None:
    """Run queued jobs in FIFO order; one worker is the default."""
    global active_jobs
    while True:
        job = await job_queue.get()
        pending_by_user[job.user_id] = max(0, pending_by_user[job.user_id] - 1)
        if pending_by_user[job.user_id] == 0:
            pending_by_user.pop(job.user_id, None)

        if job.generation != user_generations[job.user_id]:
            await _safe_job_status(job.status, "🛑 Job stop kar diya gaya (queue se skip).")
            job_queue.task_done()
            continue

        active_jobs += 1
        job_task = asyncio.create_task(job.run(), name=f"upload-job-{job.user_id}")
        active_tasks[job.user_id].add(job_task)
        try:
            await job_task
        except asyncio.CancelledError:
            # /stop cancels the child job, not this worker, so FIFO processing
            # continues with the next item.
            if job_task.cancelled():
                await _safe_job_status(job.status, "🛑 Job stop kar diya gaya.")
            else:
                job_task.cancel()
                await asyncio.gather(job_task, return_exceptions=True)
                raise
        except Exception:
            log.exception("Queue worker %s failed while processing a job", worker_id)
        finally:
            active_tasks[job.user_id].discard(job_task)
            if not active_tasks[job.user_id]:
                active_tasks.pop(job.user_id, None)
            active_jobs -= 1
            job_queue.task_done()


def ensure_queue_workers() -> None:
    """Start workers lazily after the Telegram event loop is running."""
    if any(not task.done() for task in queue_workers):
        return
    queue_workers.clear()
    for worker_id in range(config.MAX_PARALLEL):
        queue_workers.append(
            asyncio.create_task(
                queue_worker(worker_id + 1),
                name=f"upload-queue-worker-{worker_id + 1}",
            )
        )


async def enqueue_job(
    user_id: int,
    run: Callable[[], Awaitable[None]],
    status: Message | None = None,
) -> int:
    ensure_queue_workers()
    pending_by_user[user_id] += 1
    position = pending_by_user[user_id]
    await job_queue.put(QueueJob(user_id, user_generations[user_id], run, status))
    return position


# ---------- UI ----------
def menu(bound: bool) -> InlineKeyboardMarkup:
    if not bound:
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔑 Bind TeraBox Token", callback_data="bind")]]
        )
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔄 Change Token", callback_data="bind"),
                InlineKeyboardButton("🗑 Delete Config", callback_data="delete"),
            ],
            [InlineKeyboardButton("ℹ️ Status", callback_data="status")],
        ]
    )


BIND_HELP = (
    "🔑 **Apna TeraBox cookie bhejo.**\n\n"
    "Kaise milega:\n"
    "1. PC / Phone browser me dm.1024terabox.com par login karo\n"
    "2. F12 (Inspect) → Network tab kholo\n"
    "3. Koi bhi page refresh karo ya `/api/quota` request par click karo\n"
    "4. Request Headers me **Cookie** ki poori line copy karke yahan paste karo\n\n"
    "💡 *Tip: Poori Cookie (ndus + browserid + ndut_fmt) bhejne se upload bina kisi rukawat ke fast hota hai.*\n"
    "Cookie milte hi message turant delete kar diya jayega. /cancel se ruk sakte ho."
)


def human(n: float) -> str:
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} PB"


def speed_label(bytes_per_second: float) -> str:
    return f"{human(max(0.0, bytes_per_second))}/s"


def eta_label(cur: int, total: int, bytes_per_second: float) -> str:
    if total <= 0:
        return "ETA: unavailable"
    remaining = max(0, total - cur)
    if remaining == 0:
        return "ETA: done"
    if bytes_per_second <= 0:
        return "ETA: estimating…"
    seconds = max(1, math.ceil(remaining / bytes_per_second))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        duration = f"{hours}h {minutes}m"
    elif minutes:
        duration = f"{minutes}m {seconds}s"
    else:
        duration = f"{seconds}s"
    return f"ETA: ~{duration}"


def bar(cur: int, total: int) -> str:
    pct = cur / total if total else 0
    filled = int(pct * 12)
    return f"[{'█' * filled}{'░' * (12 - filled)}] {pct * 100:.0f}%"


STAGE_TEXT = {
    "hash": "🔐 File ka hash ban raha hai (badi file me kuch minute lag sakte hain)...",
    "precreate": "📡 TeraBox se upload session ban raha hai...",
    "upload": "⬆️ TeraBox Upload shuru...",
    "finish": "🔗 Share link ban raha hai...",
}


def make_stage_cb(loop, edit_status):
    def stage_cb(stage: str):
        text = STAGE_TEXT.get(stage)
        if text:
            asyncio.run_coroutine_threadsafe(edit_status(text), loop)
    return stage_cb


async def _disconnect_login_client(uid: int) -> None:
    state = telegram_login_states.pop(uid, None)
    client = (state or {}).get("client")
    if client is not None:
        with contextlib.suppress(Exception):
            await client.disconnect()


async def ensure_user_client(uid: int) -> Client | None:
    """Start the encrypted user session used for private source chats."""
    existing = user_clients.get(int(uid))
    if existing is not None and existing.is_connected:
        return existing
    session = db.get_telegram_session(uid)
    if not session:
        return None
    client = Client(
        f"telegram_user_{int(uid)}",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        session_string=session,
        in_memory=True,
        workdir=str(config.DATA_DIR),
        max_concurrent_transmissions=config.TELEGRAM_TOTAL_DOWNLOAD_WORKERS,
        sleep_threshold=30,
    )
    await client.start()
    media_filter = filters.video | filters.document | filters.audio | filters.animation | filters.video_note
    client.add_handler(
        MessageHandler(user_source_handler, (filters.group | filters.channel) & media_filter),
        group=20,
    )
    user_clients[int(uid)] = client
    log.info("Telegram user session connected for owner %s", uid)
    return client


async def source_client_for(uid: int) -> Client:
    """Prefer the user's MTProto session; fall back to the bot account."""
    if db.get_telegram_session(uid):
        try:
            client = await ensure_user_client(uid)
            if client is not None:
                return client
        except Exception as exc:
            log.warning("Telegram user session unavailable for %s: %s", uid, exc)
    return app


async def restore_user_clients() -> None:
    for uid in db.list_telegram_session_users():
        try:
            await ensure_user_client(uid)
        except Exception as exc:
            log.warning("Could not restore Telegram user session for %s: %s", uid, exc)


async def stop_user_clients() -> None:
    clients = list(user_clients.values())
    user_clients.clear()
    for client in clients:
        with contextlib.suppress(Exception):
            await client.stop()


async def _finish_telegram_login(uid: int, phone: str) -> None:
    state = telegram_login_states.get(uid) or {}
    client = state.get("client")
    if client is None:
        raise RuntimeError("Login session expire ho gaya; /login dobara bhejo")
    session = await client.export_session_string()
    await client.disconnect()
    telegram_login_states.pop(uid, None)
    db.save_telegram_session(uid, session, phone)
    await ensure_user_client(uid)


async def begin_telegram_login(m: Message, phone: str) -> None:
    uid = m.from_user.id
    phone = (phone or "").strip().replace(" ", "")
    if not re.fullmatch(r"\+[1-9]\d{6,14}", phone):
        await m.reply_text("❌ Phone number international format me bhejo, example `+919876543210`.")
        return
    await _disconnect_login_client(uid)
    client = Client(
        f"telegram_login_{uid}",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        in_memory=True,
        workdir=str(config.DATA_DIR),
    )
    try:
        await client.connect()
        sent = await client.send_code(phone)
        telegram_login_states[uid] = {
            "step": "code",
            "client": client,
            "phone": phone,
            "phone_code_hash": sent.phone_code_hash,
        }
        await m.reply_text(
            "📲 Telegram login code bheja gaya hai. Yahan sirf OTP/code bhejo.\n"
            "Agar 2-step password enabled hai to next message me password poocha jayega."
        )
    except Exception as exc:
        with contextlib.suppress(Exception):
            await client.disconnect()
        await m.reply_text(f"❌ Telegram login start nahi hua: `{exc}`")


async def telegram_login_input(m: Message) -> None:
    uid = m.from_user.id
    state = telegram_login_states.get(uid) or {}
    step = state.get("step")
    raw = (m.text or "").strip()
    if step == "phone":
        await begin_telegram_login(m, raw)
        return
    client = state.get("client")
    if client is None:
        telegram_login_states.pop(uid, None)
        await m.reply_text("❌ Login session expire ho gaya. `/login` dobara bhejo.")
        return
    with contextlib.suppress(Exception):
        await m.delete()
    try:
        if step == "code":
            try:
                await client.sign_in(state["phone"], state["phone_code_hash"], raw.replace(" ", ""))
            except Exception as exc:
                if exc.__class__.__name__ != "SessionPasswordNeeded":
                    raise
                state["step"] = "password"
                await m.reply_text("🔐 Telegram 2-step verification password bhejo. Message process hone ke baad delete ho jayega.")
                return
        elif step == "password":
            await client.check_password(raw)
        else:
            raise RuntimeError("Unknown login state")
        phone = state["phone"]
        await _finish_telegram_login(uid, phone)
        await m.reply_text(
            "✅ Telegram account login ho gaya.\n"
            "Ab private group/channel messages `/set`, `/batch` aur `/auto` se read/download ho sakte hain.\n"
            "Session encrypted SQLite me save hai. `/logout` se remove kar sakte ho."
        )
    except Exception as exc:
        await _disconnect_login_client(uid)
        await m.reply_text(f"❌ Telegram login failed: `{exc}`\n`/login` se dobara try karo.")


async def user_source_handler(client: Client, m: Message) -> None:
    """Live media updates received by a logged-in Telegram user session."""
    await auto_target_handler(client, m)


# ---------- target helpers ----------
@dataclass(slots=True)
class TelegramMessageRef:
    chat_ref: str | int
    message_id: int
    chat_id: str


def _chat_type(chat) -> str:
    value = getattr(getattr(chat, "type", None), "value", getattr(chat, "type", ""))
    return str(value or "")


def _chat_ref(chat) -> str:
    username = getattr(chat, "username", None)
    return f"@{username}" if username else str(chat.id)


def parse_telegram_message_url(raw: str) -> TelegramMessageRef | None:
    """Parse public and /c/ private Telegram message links without guessing."""
    value = (raw or "").strip().strip("<>[](){}.,;\"")
    match = TELEGRAM_MESSAGE_RE.search(value)
    if not match:
        return None
    parts = [part for part in match.group("path").split("/") if part]
    if len(parts) < 2:
        return None
    try:
        message_id = int(parts[-1])
    except ValueError:
        return None
    if message_id <= 0:
        return None
    if parts[0].lower() == "c" and len(parts) >= 3:
        internal = parts[1]
        if not internal.isdigit():
            return None
        chat_id = f"-100{internal}"
        return TelegramMessageRef(int(chat_id), message_id, chat_id)
    if parts[0].lower() == "b" and len(parts) >= 3:
        username = parts[1]
    else:
        username = parts[0]
    if username.startswith("@"):  # t.me/@name/... is accepted too
        username = username[1:]
    if not re.fullmatch(r"[A-Za-z0-9_]{3,}", username):
        return None
    return TelegramMessageRef(f"@{username}", message_id, f"@{username}")


def telegram_message_url(chat, message_id: int) -> str:
    username = getattr(chat, "username", None)
    if username:
        return f"https://t.me/{username}/{int(message_id)}"
    chat_id = str(getattr(chat, "id", ""))
    if chat_id.startswith("-100"):
        return f"https://t.me/c/{chat_id[4:]}/{int(message_id)}"
    return f"Telegram message {int(message_id)}"


def media_name(message: Message) -> str:
    media = pick_media(message)
    if media is None:
        return safe_name(f"message_{message.id}")
    ext = ".mp4" if (message.video or message.video_note or message.animation) else ""
    original = getattr(media, "file_name", None)
    if original:
        return safe_name(original)
    unique = getattr(media, "file_unique_id", None) or f"message_{message.id}"
    return safe_name(f"{unique}{ext}")


def target_line(target: dict, summary: dict | None = None) -> str:
    summary = summary or db.target_summary(target["id"])
    title = target.get("title") or target.get("chat_ref") or target.get("chat_id")
    auto = "ON" if target.get("auto_enabled") else "OFF"
    start = target.get("start_message_id") or "not set"
    return (
        f"🎯 **#{target['id']} {title}**\n"
        f"Chat: `{target.get('chat_ref') or target.get('chat_id')}` · Auto: **{auto}**\n"
        f"Start message: `{start}`\n"
        f"📦 Messages: {summary['total']} · ⏳ Queue: {summary['queued']} · "
        f"⬇️ Download: {summary['downloading']} · ⬆️ Upload: {summary['uploading']} · "
        f"✅ Uploaded: {summary['uploaded']} · ❌ Failed: {summary['failed']} · Skipped: {summary['skipped']}"
    )


async def send_target_links(uid: int, target_id: int, heading: str = "🔗 Uploaded TeraBox links"):
    target = db.get_target(uid, target_id=target_id)
    if not target:
        return
    links = db.list_links(target_id, limit=10000)
    if not links:
        await app.send_message(uid, f"{heading}\nIs target ke liye abhi uploaded link nahi hai.")
        return
    chunks: list[str] = []
    current = [heading, f"🎯 Target #{target_id}: {target.get('title') or target.get('chat_ref')}\n"]
    for index, item in enumerate(links, 1):
        name = (item.get("file_name") or f"message_{item['message_id']}").replace("`", "'")
        source = item.get("source_url") or ""
        line = f"{index}. 📁 {name}\n   {item.get('terabox_url', '')}"
        if source:
            line += f"\n   ↳ source: {source}"
        if sum(len(part) + 1 for part in current) + len(line) > 3800:
            chunks.append("\n".join(current))
            current = [f"🔗 Continued · target #{target_id}"]
        current.append(line)
    if len(current) > 1:
        chunks.append("\n".join(current))
    for chunk in chunks:
        await app.send_message(uid, chunk, **NO_PREVIEW)
        await asyncio.sleep(0.15)


async def send_target_report(uid: int, target_id: int, include_links: bool = True):
    target = db.get_target(uid, target_id=target_id)
    if not target:
        await app.send_message(uid, "❌ Target ID nahi mila. /targets se list dekho.")
        return
    await app.send_message(uid, target_line(target))
    if include_links:
        await send_target_links(uid, target_id)


async def resolve_chat(raw: str, client: Client | None = None):
    value = (raw or "").strip().strip("<>[](){}.,;\"")
    if not value:
        raise ValueError("Chat ID/username khaali hai")
    ref = parse_telegram_message_url(value)
    if ref:
        value = ref.chat_ref
    elif value.startswith("https://t.me/") or value.startswith("http://t.me/"):
        path = urlparse(value).path.strip("/").split("/")
        if path and path[0].lower() == "c" and len(path) >= 2 and path[1].isdigit():
            value = int(f"-100{path[1]}")
        elif path and path[0]:
            value = "@" + path[0].lstrip("@")
    elif re.fullmatch(r"-?\d+", value):
        value = int(value)
    elif not value.startswith("@"):  # @username is friendlier, both forms work
        value = "@" + value
    chat = await (client or app).get_chat(value)
    return chat


async def verify_target_access(chat, user_id: int, client: Client | None = None) -> None:
    """Verify source access using the logged-in user when available."""
    client = client or app
    kind = _chat_type(chat).lower()
    if kind in {"private", "bot", "user"}:
        raise ValueError("Sirf group, supergroup ya channel target kar sakte ho")

    def status_name(member) -> str:
        value = getattr(getattr(member, "status", None), "value", getattr(member, "status", ""))
        return str(value or "").rsplit(".", 1)[-1].lower()

    try:
        owner_member = await client.get_chat_member(chat.id, user_id)
    except Exception as exc:
        raise ValueError("Aapka Telegram account source chat me member/admin nahi hai") from exc
    owner_status = status_name(owner_member)
    if client is app:
        if owner_status not in {"owner", "creator", "administrator"}:
            raise ValueError("Bot-mode target set karne ke liye user source ka admin/owner hona chahiye")
    elif owner_status in {"left", "kicked", "banned", "restricted", ""}:
        raise ValueError("Logged-in Telegram account source chat ka member nahi hai")

    if client is app:
        try:
            bot_user = await app.get_me()
            bot_member = await app.get_chat_member(chat.id, bot_user.id)
        except Exception as exc:
            raise ValueError("Bot ko source group/channel me add karke read access do") from exc
        bot_status = status_name(bot_member)
        if bot_status in {"left", "kicked", "banned", "restricted", ""}:
            raise ValueError("Bot source group/channel ka member nahi hai")
        if kind == "channel" and bot_status not in {"owner", "creator", "administrator"}:
            raise ValueError("Channel posts/history ke liye bot ko channel admin banao")


async def configure_target_chat(m: Message, raw: str):
    uid = m.from_user.id
    source_client = await source_client_for(uid)
    try:
        chat = await resolve_chat(raw, source_client)
        await verify_target_access(chat, uid, source_client)
        chat_id = str(chat.id)
        target_id = db.upsert_target(
            uid, chat_id, _chat_ref(chat), getattr(chat, "title", "") or getattr(chat, "first_name", "") or "",
            getattr(chat, "username", "") or "", _chat_type(chat),
        )
        target_wizards[uid] = {"step": "start", "target_id": target_id, "chat_id": chat_id}
        await m.reply_text(
            f"✅ Target save ho gaya: **#{target_id} {getattr(chat, 'title', '') or _chat_ref(chat)}**\n\n"
            "Ab starting message ka Telegram URL bhejo, example:\n"
            "`https://t.me/SSC_CGL_DISCUSSION_GROUP_2026_27/42966`\n\n"
            "Isi message se purane messages ki taraf `/batch` scan hoga. `/cancel` se wizard rok sakte ho."
        )
    except Exception as exc:
        await m.reply_text(f"❌ Group/channel access nahi mila: `{exc}`\nID, @username ya public Telegram URL dobara bhejo.")


async def configure_target_start(m: Message, raw: str):
    uid = m.from_user.id
    wizard = target_wizards.get(uid) or {}
    target_id = wizard.get("target_id")
    ref = parse_telegram_message_url(raw)
    if not target_id or not ref:
        await m.reply_text("❌ Valid Telegram message URL bhejo, example `https://t.me/channel/12345`.")
        return
    target = db.get_target(uid, target_id=target_id)
    if not target:
        target_wizards.pop(uid, None)
        await m.reply_text("❌ Target mil nahi raha. /set se dobara configure karo.")
        return
    try:
        source_client = await source_client_for(uid)
        message = await source_client.get_messages(ref.chat_ref, ref.message_id)
        if not message or not getattr(message, "id", None):
            raise ValueError("message nahi mila")
        actual_chat_id = str(getattr(getattr(message, "chat", None), "id", ref.chat_id))
        if actual_chat_id != str(target["chat_id"]):
            await m.reply_text("❌ Ye message URL configured target se match nahi karta.")
            return
        source_url = telegram_message_url(message.chat, message.id)
        db.set_target_start(uid, target_id, message.id, source_url)
        target_wizards[uid] = {"step": "auto", "target_id": target_id}
        await m.reply_text(
            "✅ Starting message save ho gaya.\n\n"
            "Auto mode bhi set karna hai? `on` ya `off` bhejo.\n"
            "Baad me `/auto <target_id> on|off` se change kar sakte ho."
        )
    except Exception as exc:
        await m.reply_text(f"❌ Starting message read nahi hua: `{exc}`")


async def target_wizard_input(m: Message):
    uid = m.from_user.id
    step = (target_wizards.get(uid) or {}).get("step")
    raw = (m.text or "").strip()
    if step == "chat":
        await configure_target_chat(m, raw)
    elif step == "start":
        await configure_target_start(m, raw)
    elif step == "auto":
        target_id = (target_wizards.get(uid) or {}).get("target_id")
        if raw.lower() not in {"on", "off"}:
            await m.reply_text("Sirf `on` ya `off` bhejo, ya `/cancel`.")
            return
        db.set_target_auto(uid, int(target_id), raw.lower() == "on")
        target_wizards.pop(uid, None)
        await m.reply_text(
            f"✅ Auto mode **{raw.lower()}** ho gaya for target #{target_id}.\n"
            "Ab naye media messages automatically queue hoke TeraBox link bhejenge.\n\n"
            "Commands: `/batch 100`, `/batch all`, `/targetstatus`, `/links`."
        )


async def batch_job(client: Client, uid: int, target_id: int, count: int | None, status: Message):
    target = db.get_target(uid, target_id=target_id)
    if not target:
        await status.edit_text("❌ Target nahi mila.")
        return
    start_id = target.get("start_message_id")
    if not start_id:
        await status.edit_text("❌ Is target ka start message set nahi hai. /set se URL do.")
        return
    max_messages = count if count is not None else config.MAX_BATCH_MESSAGES
    max_messages = max(1, min(int(max_messages), config.MAX_BATCH_MESSAGES))
    source_client = await source_client_for(uid)
    chat_ref = target.get("chat_ref") or target.get("chat_id")
    scanned = 0
    discovered = 0
    uploaded = 0
    failed = 0
    seen_ids: set[int] = set()

    async def consume(message: Message) -> bool:
        nonlocal scanned, discovered, uploaded, failed
        if not message or not getattr(message, "id", None) or message.id in seen_ids:
            return True
        seen_ids.add(message.id)
        scanned += 1
        media = pick_media(message)
        if media is None:
            return scanned < max_messages
        item_id, created = db.ensure_target_item(
            target_id, message.id, target["chat_id"], telegram_message_url(message.chat, message.id), media_name(message)
        )
        item = db.get_target_item(item_id) or {}
        if not created and item.get("state") == "uploaded":
            return scanned < max_messages
        discovered += 1 if created else 0
        if not created and item.get("state") in {"downloading", "uploading"}:
            return scanned < max_messages
        db.mark_item_queued(item_id)
        await status.edit_text(
            f"📦 Batch target #{target_id}\n"
            f"🔎 Scanned: {scanned}/{max_messages} · 🎞 Media: {discovered}\n"
            f"✅ Uploaded: {uploaded} · ❌ Failed: {failed}\n"
            f"▶️ Current: message {message.id}"
        )
        ok, _, _ = await process_media_message(source_client, message, uid, status, item_id=item_id, announce=False)
        if ok:
            uploaded += 1
        else:
            failed += 1
        await status.edit_text(
            f"📦 Batch target #{target_id}\n"
            f"🔎 Scanned: {scanned}/{max_messages} · 🎞 Media: {discovered}\n"
            f"✅ Uploaded: {uploaded} · ❌ Failed: {failed}\n"
            f"⏭️ Next message scan ho raha hai..."
        )
        return scanned < max_messages

    try:
        first = await source_client.get_messages(chat_ref, int(start_id))
        await consume(first)
        if scanned < max_messages:
            async for message in source_client.get_chat_history(chat_ref, offset_id=int(start_id)):
                if scanned >= max_messages:
                    break
                if not await consume(message):
                    break
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        await _safe_job_status(status, f"❌ Batch scan ruk gaya: `{exc}`\nPartial items ke links `/links {target_id}` se mil jayenge.")
        with contextlib.suppress(Exception):
            await send_target_links(uid, target_id, "🔗 Partial batch uploaded links")
        return

    summary = db.target_summary(target_id)
    await status.edit_text(
        f"✅ Batch complete · target #{target_id}\n"
        f"🔎 Scanned: {scanned} · 🎞 New media: {discovered}\n"
        f"✅ Is run me uploaded: {uploaded} · ❌ Failed: {failed}\n"
        f"📊 Total uploaded: {summary['uploaded']} · Failed: {summary['failed']}"
    )
    await send_target_links(uid, target_id, "🔗 Batch uploaded links")


# ---------- commands ----------
@app.on_message(filters.command("start") & filters.private)
async def start(_, m: Message):
    bound = db.get_token(m.from_user.id) is not None
    text = (
        "👋 **TeraBox Uploader Bot**\n\n"
        "Pehle apna TeraBox account bind karo, uske baad jo bhi video bhejoge "
        "wo automatically TeraBox par upload hoga aur share link mil jayega.\n\n"
        "Target group/channel ke liye `/set` use karo."
    )
    if bound:
        text = (
            "✅ Aapka TeraBox account bind hai. Bas video/file ya direct link bhejo.\n\n"
            "🎯 Target setup: `/set` · Batch: `/batch 100` · Auto: `/auto <target_id> on`\n"
            "📊 Reports: `/targets`, `/targetstatus`, `/links`"
        )
    await m.reply_text(text, reply_markup=menu(bound))


@app.on_message(filters.command("login") & filters.private)
async def telegram_login_cmd(_, m: Message):
    uid = m.from_user.id
    waiting_for_token.discard(uid)
    target_wizards.pop(uid, None)
    if db.get_telegram_session(uid):
        info = db.get_telegram_session_info(uid) or {}
        await m.reply_text(
            f"✅ Telegram account already logged in{(' · ' + info.get('phone', '')) if info.get('phone') else ''}.\n"
            "Private groups/channels ke liye ye session use hoga. `/logout` ke baad `/login` se account change kar sakte ho."
        )
        return
    if len(m.command) > 1:
        await begin_telegram_login(m, m.command[1])
    else:
        telegram_login_states[m.from_user.id] = {"step": "phone"}
        await m.reply_text(
            "📱 Apna Telegram phone number international format me bhejo, example:\n"
            "`+919876543210`\n\n"
            "OTP aur 2FA password bot ke private chat me hi bhejna. `/cancel` se stop kar sakte ho."
        )


@app.on_message(filters.command("logout") & filters.private)
async def telegram_logout_cmd(_, m: Message):
    uid = m.from_user.id
    await _disconnect_login_client(uid)
    client = user_clients.pop(uid, None)
    if client is None and db.get_telegram_session(uid):
        with contextlib.suppress(Exception):
            client = await ensure_user_client(uid)
    if client is not None:
        with contextlib.suppress(Exception):
            await client.log_out()
        with contextlib.suppress(Exception):
            await client.stop()
    deleted = db.delete_telegram_session(uid)
    await m.reply_text(
        "✅ Telegram user session Telegram side se revoke aur local storage se delete ho gaya.\n"
        "Private source access ke liye `/login` dobara karo."
        if deleted else "Koi saved Telegram user session nahi tha."
    )


@app.on_message(filters.command("set") & filters.private)
async def set_target_cmd(_, m: Message):
    uid = m.from_user.id
    waiting_for_token.discard(uid)
    target_wizards[uid] = {"step": "chat"}
    if len(m.command) > 1:
        await configure_target_chat(m, " ".join(m.command[1:]))
        return
    await m.reply_text(
        "🎯 **Target group/channel setup**\n\n"
        "Pehle target group/channel ka ID, `@username` ya public Telegram URL bhejo.\n"
        "Bot ko us group me member/admin aur channel me admin/read access zaroor do."
    )


@app.on_message(filters.command(["targets", "targetlist"]) & filters.private)
async def targets_cmd(_, m: Message):
    targets = db.list_targets(m.from_user.id)
    if not targets:
        await m.reply_text("Koi target set nahi hai. `/set` se group/channel add karo.")
        return
    await m.reply_text("🎯 **Aapke targeted groups/channels**\n\n" + "\n\n".join(target_line(t) for t in targets))


@app.on_message(filters.command(["targetstatus", "target_status"]) & filters.private)
async def target_status_cmd(_, m: Message):
    uid = m.from_user.id
    target_id = int(m.command[1]) if len(m.command) > 1 and m.command[1].isdigit() else None
    target = db.get_target(uid, target_id=target_id) if target_id else db.get_target(uid)
    if not target:
        await m.reply_text("❌ Target nahi mila. `/targets` se IDs dekho.")
        return
    await m.reply_text(target_line(target))


@app.on_message(filters.command(["links", "targetlinks"]) & filters.private)
async def links_cmd(_, m: Message):
    uid = m.from_user.id
    target_id = int(m.command[1]) if len(m.command) > 1 and m.command[1].isdigit() else None
    target = db.get_target(uid, target_id=target_id) if target_id else db.get_target(uid)
    if not target:
        await m.reply_text("❌ Target nahi mila. `/targets` se IDs dekho.")
        return
    await send_target_links(uid, target["id"])


@app.on_message(filters.command("auto") & filters.private)
async def auto_cmd(_, m: Message):
    uid = m.from_user.id
    if len(m.command) < 2:
        await m.reply_text("Usage: `/auto <target_id> on|off`\nExample: `/auto 1 on`")
        return
    target_id = int(m.command[1]) if m.command[1].isdigit() else 0
    mode = m.command[2].lower() if len(m.command) > 2 else ""
    if mode not in {"on", "off"}:
        await m.reply_text("Mode `on` ya `off` hona chahiye.")
        return
    if not db.set_target_auto(uid, target_id, mode == "on"):
        await m.reply_text("❌ Target ID nahi mila.")
        return
    await m.reply_text(f"✅ Target #{target_id} ka auto mode **{mode}** ho gaya.")


@app.on_message(filters.command("batch") & filters.private)
async def batch_cmd(_, m: Message):
    uid = m.from_user.id
    args = list(m.command[1:])
    target_id = None
    count_arg = None
    if len(args) > 2:
        await m.reply_text("Usage: `/batch [count|all]` or `/batch <target_id> <count|all>`")
        return
    if len(args) >= 2:
        if not args[0].isdigit() or not db.get_target(uid, target_id=int(args[0])):
            await m.reply_text("❌ Target ID nahi mila. `/targets` se IDs dekho.")
            return
        target_id = int(args.pop(0))
    if args:
        count_arg = args[0].lower()
    target = db.get_target(uid, target_id=target_id) if target_id else db.get_target(uid)
    if not target:
        await m.reply_text("❌ Pehle `/set` se target configure karo.\nUsage: `/batch [count|all]` or `/batch <target_id> <count|all>`")
        return
    if count_arg is None:
        await m.reply_text(
            f"🎯 Target #{target['id']} ready hai. Kitne messages scan karu?\n"
            "`100`, `10000` ya `all` bhejo. `/cancel` se wizard rok sakte ho."
        )
        target_wizards[uid] = {"step": "batch_count", "target_id": target["id"]}
        return
    if count_arg == "all":
        count = None
    elif count_arg.isdigit() and int(count_arg) > 0:
        count = min(int(count_arg), config.MAX_BATCH_MESSAGES)
    else:
        await m.reply_text("Count `100`, `10000` ya `all` hona chahiye.")
        return
    status = await m.reply_text("⏳ Batch queue me add ho raha hai...")
    position = await enqueue_job(uid, lambda: batch_job(app, uid, target["id"], count, status), status)
    if position > 1:
        await _safe_job_status(status, f"⏳ Batch queue me waiting...\n📌 Pending position: {position}")


@app.on_message(filters.command("remove") & filters.private)
async def remove_target_cmd(_, m: Message):
    uid = m.from_user.id
    if len(m.command) < 2 or not m.command[1].isdigit():
        await m.reply_text("Usage: `/remove <target_id>`")
        return
    ok = db.delete_target(uid, int(m.command[1]))
    await m.reply_text("🗑 Target delete ho gaya." if ok else "❌ Target ID nahi mila.")


@app.on_message(filters.command(["bind", "change"]) & filters.private)
async def bind_cmd(_, m: Message):
    # /bind <token> direct bhi chalega
    target_wizards.pop(m.from_user.id, None)
    if len(m.command) > 1:
        await handle_token(m, " ".join(m.command[1:]))
        return
    waiting_for_token.add(m.from_user.id)
    await m.reply_text(BIND_HELP)


@app.on_message(filters.command(["delete", "delete_config"]) & filters.private)
async def delete_cmd(_, m: Message):
    waiting_for_token.discard(m.from_user.id)
    target_wizards.pop(m.from_user.id, None)
    ok = db.delete_token(m.from_user.id)
    await m.reply_text(
        "🗑 Config delete ho gaya." if ok else "Koi config saved nahi tha.",
        reply_markup=menu(False),
    )


@app.on_message(filters.command("cancel") & filters.private)
async def cancel_cmd(_, m: Message):
    waiting_for_token.discard(m.from_user.id)
    target_wizards.pop(m.from_user.id, None)
    await _disconnect_login_client(m.from_user.id)
    await m.reply_text("Cancelled.")


@app.on_message(filters.command("queue") & filters.private)
async def queue_cmd(_, m: Message):
    uid = m.from_user.id
    await m.reply_text(
        f"📋 **Queue status**\n\n"
        f"▶️ Active: {active_jobs}\n"
        f"⏳ Waiting (all users): {job_queue.qsize()}\n"
        f"👤 Your waiting jobs: {pending_by_user.get(uid, 0)}\n"
        f"⚙️ Queue workers: {config.MAX_PARALLEL}\n\n"
        "FIFO order me next job automatically start hoga."
    )


@app.on_message(filters.command(["stop", "stopall"]) & filters.private)
async def stop_cmd(_, m: Message):
    uid = m.from_user.id
    waiting_for_token.discard(uid)
    user_generations[uid] += 1
    tasks = list(active_tasks.get(uid, set()))
    for task in tasks:
        task.cancel()
    pending = pending_by_user.get(uid, 0)
    await m.reply_text(
        f"🛑 Stop request accepted.\n"
        f"Active jobs cancelled: {len(tasks)}\n"
        f"Waiting jobs marked for skip: {pending}\n\n"
        "Naya video bhejne par queue normal continue hogi."
    )


@app.on_message(filters.command(["status", "config"]) & filters.private)
async def status_cmd(_, m: Message):
    await send_status(m, m.from_user.id)


async def send_status(m: Message, uid: int):
    token = db.get_token(uid)
    if not token:
        targets = db.list_targets(uid)
        if targets:
            await m.reply_text(
                "❌ TeraBox account bind nahi hai. Pehle `/bind` karo.\n\n" +
                "\n\n".join(target_line(t) for t in targets),
                reply_markup=menu(False),
            )
        else:
            await m.reply_text("❌ Koi account bind nahi hai.", reply_markup=menu(False))
        return
    try:
        q = await to_thread_wait_on_cancel(TeraBox(token).validate)
        used = q.get("used", 0)
        total = q.get("total", 0)
        targets = db.list_targets(uid)
        tg_info = db.get_telegram_session_info(uid)
        telegram_text = "\n📱 Telegram user login: ✅" if tg_info else "\n📱 Telegram user login: ❌ (`/login` for private sources)"
        target_text = "\n\n🎯 **Target status**\n" + "\n\n".join(target_line(t) for t in targets) if targets else "\n\n🎯 Koi target configured nahi hai. `/set` se add karo."
        await m.reply_text(
            f"✅ Account bind hai\n💾 Used: {human(used)} / {human(total)}{telegram_text}{target_text}",
            reply_markup=menu(True),
        )
    except Exception as e:
        await m.reply_text(
            f"⚠️ Token invalid/expire lag raha hai: {e}\n/change se naya token do.",
            reply_markup=menu(True),
        )


# ---------- callbacks ----------
@app.on_callback_query()
async def cb(_, q: CallbackQuery):
    uid = q.from_user.id
    if q.data == "bind":
        target_wizards.pop(uid, None)
        waiting_for_token.add(uid)
        await q.message.reply_text(BIND_HELP)
    elif q.data == "delete":
        ok = db.delete_token(uid)
        waiting_for_token.discard(uid)
        await q.message.reply_text(
            "🗑 Config delete ho gaya." if ok else "Koi config nahi tha.",
            reply_markup=menu(False),
        )
    elif q.data == "status":
        await send_status(q.message, uid)
    await q.answer()


# ---------- token input ----------
async def handle_token(m: Message, raw: str):
    uid = m.from_user.id
    token = raw.strip().strip("`").replace("\r\n", "\n")
    if token.lower().startswith("cookie:"):
        token = token.split(":", 1)[1].strip()

    # If it's a bare ndus or ndus=value without semicolons
    if ";" not in token and "\n" not in token:
        token = re.sub(r"\s+", "", token)
        if token.lower().startswith("ndus="):
            token = token[5:]
    else:
        # Multi-line or semicolon-separated Cookie header
        cookies = []
        for line in re.split(r"[;\n]+", token):
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and value:
                cookies.append(f"{key}={value}")
        token = "; ".join(cookies)

    if not token or len(token) < 15:
        await m.reply_text("❌ Cookie token adhoora lag raha hai. Poora TeraBox Cookie bhejo.")
        return

    # Delete the message containing sensitive token
    try:
        await m.delete()
    except Exception:
        pass

    wait = await m.reply_text("⏳ Token verify ho raha hai...")
    try:
        tb = TeraBox(token)
        q = await to_thread_wait_on_cancel(tb.validate)
    except Exception as e:
        await wait.edit_text(
            "❌ Token verify nahi hua. Check karo ki browser me dm.1024terabox.com logged in hai.\n"
            f"Details: `{e}`\n\n/change se dobara poori cookie bhejo."
        )
        return

    db.save_token(uid, token)
    waiting_for_token.discard(uid)
    await wait.edit_text(
        f"✅ Account bind ho gaya!\n💾 Used: {human(q.get('used', 0))} / {human(q.get('total', 0))}\n\n"
        "Ab koi bhi video bhejo, automatically upload ho jayega.",
        reply_markup=menu(True),
    )


@app.on_message(
    filters.private & filters.text & ~filters.command(
        ["start", "login", "logout", "set", "targets", "targetlist", "targetstatus", "target_status", "links", "targetlinks", "auto", "batch", "remove", "bind", "change", "delete", "delete_config", "cancel", "stop", "stopall", "status", "config", "queue"]
    )
)
async def text_handler(_, m: Message):
    uid = m.from_user.id
    if uid in waiting_for_token:
        await handle_token(m, m.text)
        return
    if uid in telegram_login_states:
        await telegram_login_input(m)
        return
    wizard = target_wizards.get(uid) or {}
    if wizard.get("step") == "batch_count":
        raw = (m.text or "").strip().lower()
        if raw == "all":
            count = None
        elif raw.isdigit() and int(raw) > 0:
            count = min(int(raw), config.MAX_BATCH_MESSAGES)
        else:
            await m.reply_text(f"Count `100`, `10000` ya `all` hona chahiye (max {config.MAX_BATCH_MESSAGES}).")
            return
        target_id = int(wizard["target_id"])
        target_wizards.pop(uid, None)
        status = await m.reply_text("⏳ Batch queue me add ho raha hai...")
        await enqueue_job(uid, lambda: batch_job(app, uid, target_id, count, status), status)
        return
    if wizard:
        await target_wizard_input(m)
        return
    found = URL_RE.search(m.text or "")
    if found:
        url = found.group(0).rstrip(".,;:)]}>")
        if parse_telegram_message_url(url):
            await handle_telegram_url(m, url)
        else:
            await handle_url(m, url)
    else:
        await m.reply_text("Video/file, direct/m3u8 link ya Telegram message URL bhejo, ya /start dabao.")


# ---------- Telegram file -> TeraBox (video / pdf / etc.) ----------
def pick_media(m: Message):
    return m.video or m.document or m.audio or m.animation or m.video_note


def safe_name(name: str) -> str:
    name = re.sub(r"[\x00-\x1f\x7f]", "_", name)
    name = re.sub(r'[\\/:*?"<>|]', "_", name).strip()
    # Limit UTF-8 bytes, not Python characters; Telegram filenames may contain
    # emoji/CJK and Linux enforces a 255-byte component limit.
    encoded = (name or f"file_{int(time.time())}").encode("utf-8")[:120]
    while encoded:
        try:
            result = encoded.decode("utf-8").strip()
            if result:
                return result
        except UnicodeDecodeError:
            encoded = encoded[:-1]
            continue
        break
    return f"file_{int(time.time())}"


@app.on_message(
    filters.private
    & (filters.video | filters.document | filters.audio | filters.animation | filters.video_note)
)
async def file_handler(
    client: Client,
    m: Message,
    _queued: bool = False,
    _status: Message | None = None,
):
    media = pick_media(m)
    uid = m.from_user.id
    token = db.get_token(uid)
    if not token:
        await m.reply_text("❌ Pehle TeraBox account bind karo.", reply_markup=menu(False))
        return

    size = getattr(media, "file_size", 0) or 0
    if size > config.MAX_FILE_MB * 1024 * 1024:
        await m.reply_text(
            f"❌ File {config.MAX_FILE_MB} MB se badi hai.\n"
            "👉 Uska direct link bhejo, main link se upload kar dunga."
        )
        return

    ext = ".mp4" if (m.video or m.video_note or m.animation) else ""
    orig_name = getattr(media, "file_name", None)
    if orig_name:
        name = safe_name(orig_name)
    else:
        name = safe_name(f"{media.file_unique_id}{ext}")

    if not _queued:
        status = await m.reply_text("⏳ Queue me add ho raha hai...")
        position = await enqueue_job(
            uid, lambda: process_media_message(client, m, uid, status, announce=True), status
        )
        if position > 1:
            try:
                await status.edit_text(
                    f"⏳ Queue me waiting...\n📌 Pending position: {position}"
                )
            except Exception:
                pass
        return

    status = _status or await m.reply_text("⏳ Queue me hai...")
    await process_media_message(client, m, uid, status, announce=True)
    return


async def process_media_message(
    client: Client,
    m: Message,
    uid: int,
    status: Message,
    item_id: int | None = None,
    announce: bool = True,
) -> tuple[bool, str, str]:
    """Download one Telegram message to disk and upload it to TeraBox.

    This is the shared implementation for the original private-media flow,
    pasted Telegram message URLs, batch scans and live target auto mode.
    ``uid`` is always the account owner, never ``m.from_user``; channel posts
    do not have a sender user.
    """
    if item_id and getattr(m, "chat", None) and getattr(m, "id", None):
        # Telegram file references can expire while a queued batch is waiting.
        # Persisted item identity is stable, so refresh the message just before
        # downloading instead of relying on the captured event object.
        try:
            refreshed = await client.get_messages(m.chat.id, m.id)
            if refreshed:
                m = refreshed
        except Exception as exc:
            log.info("Message refresh failed for %s/%s; using captured message: %s", m.chat.id, m.id, exc)

    media = pick_media(m)
    name = media_name(m)
    local = ""

    async def edit_status(text: str):
        try:
            await status.edit_text(text)
        except Exception:
            pass

    if media is None:
        if item_id:
            db.mark_item_skipped(item_id, "Message me media nahi tha")
        await edit_status("⏭️ Is Telegram message me uploadable media nahi hai.")
        return False, "", name

    token = db.get_token(uid)
    if not token:
        if item_id:
            db.mark_item_failed(item_id, "TeraBox account bind nahi hai")
        await edit_status("❌ TeraBox account bind nahi hai; job skip kar diya.")
        return False, "", name

    size = getattr(media, "file_size", 0) or 0
    if size > config.MAX_FILE_MB * 1024 * 1024:
        detail = f"File {config.MAX_FILE_MB} MB se badi hai; direct link use karo."
        if item_id:
            db.mark_item_failed(item_id, detail)
        await edit_status(f"❌ {detail}")
        return False, "", name

    source_chat = getattr(getattr(m, "chat", None), "id", "private")
    source_tag = re.sub(r"[^A-Za-z0-9_-]", "_", str(source_chat))[:60]
    local = os.path.join(config.DOWNLOAD_DIR, f"{uid}_{source_tag}_{m.id}_{name}")
    if item_id:
        db.mark_item_downloading(item_id)

    async with upload_sem:
        try:
            if size:
                free = shutil.disk_usage(config.DOWNLOAD_DIR).free
                buffer = min(64 * 1024 * 1024, max(8 * 1024 * 1024, size // 100))
                required = size + buffer
                if free < required:
                    raise TeraboxError(
                        f"Disk space kam hai. Available: {human(free)}, required: about {human(required)}"
                    )

            loop = asyncio.get_running_loop()
            await edit_status("🔎 TeraBox login check ho raha hai...")
            tb = TeraBox(token)
            await to_thread_wait_on_cancel(tb.ensure_login)

            async def dl_progress(cur, total, rate):
                await edit_status(
                    f"⬇️ Telegram download ({config.TELEGRAM_DOWNLOAD_WORKERS} workers)\n"
                    f"{bar(cur, total)}\n{human(cur)} / {human(total)}\n"
                    f"⚡ Speed: {speed_label(rate)} · {eta_label(cur, total, rate)}"
                )

            await tgdl.download_media_parallel(
                client, media, local, size, dl_progress,
                config.TELEGRAM_DOWNLOAD_WORKERS, range_limiter=download_range_slots,
            )
            if not os.path.exists(local):
                raise FileNotFoundError(f"Downloaded file not found at {local}")

            actual_size = os.path.getsize(local)
            if item_id:
                db.mark_item_started(item_id)
            await edit_status(
                f"✅ Download complete ({human(actual_size)})\n⬆️ TeraBox uploading...\n"
                f"{bar(0, actual_size)}\n⚡ Speed: 0.0 B/s · ETA: estimating…"
            )

            ul_started = [time.monotonic()]
            ul_last_ui = [0.0, 0]

            def up_progress(cur, total):
                now = time.monotonic()
                if cur < ul_last_ui[1] or (now - ul_last_ui[0] < 5 and cur < total):
                    return
                ul_last_ui[0], ul_last_ui[1] = now, cur
                rate = cur / max(now - ul_started[0], 0.001)
                asyncio.run_coroutine_threadsafe(
                    edit_status(
                        f"⬆️ TeraBox upload\n{bar(cur, total)}\n"
                        f"{human(cur)} / {human(total)}\n⚡ Speed: {speed_label(rate)} · "
                        f"{eta_label(cur, total, rate)}"
                    ), loop
                )

            await to_thread_wait_on_cancel(tb.ensure_login, True)
            link = await to_thread_wait_on_cancel(
                tb.upload, local, name, config.REMOTE_FOLDER, up_progress,
                make_stage_cb(loop, edit_status), config.TERABOX_UPLOAD_THREADS,
                upload_part_slots,
            )
            if item_id:
                db.mark_item_uploaded(item_id, link, name)
            if announce:
                await send_done(status, name, link)
            return True, link, name
        except asyncio.CancelledError:
            if item_id:
                db.mark_item_failed(item_id, "Job cancelled")
            raise
        except TeraboxError as exc:
            detail = str(exc)
            if any(x in detail for x in ("31045", "user not exists", "errno=-6", "not login", "4000023")):
                tail = "TeraBox cookie expire/adhoori hai; /change se fresh cookie do."
            else:
                tail = "Agar error baar-baar aaye to /change se fresh cookie do."
            if item_id:
                db.mark_item_failed(item_id, detail)
            await edit_status(f"❌ TeraBox error:\n`{detail}`\n\n{tail}")
            return False, "", name
        except Exception as exc:
            detail = str(exc)
            if item_id:
                db.mark_item_failed(item_id, detail)
            await edit_status(f"❌ Error: `{detail}`")
            return False, "", name
        finally:
            if local and os.path.exists(local):
                with contextlib.suppress(OSError):
                    os.remove(local)


async def handle_telegram_url(
    m: Message,
    url: str,
    _queued: bool = False,
    _status: Message | None = None,
):
    uid = m.from_user.id
    if not db.get_token(uid):
        await m.reply_text("❌ Pehle TeraBox account bind karo.", reply_markup=menu(False))
        return
    if not _queued:
        status = await m.reply_text("⏳ Telegram message URL queue me add ho raha hai...")
        await enqueue_job(uid, lambda: handle_telegram_url(m, url, True, status), status)
        return
    status = _status or await m.reply_text("⏳ Telegram message fetch ho raha hai...")
    try:
        ref = parse_telegram_message_url(url)
        if not ref:
            raise ValueError("Telegram message URL invalid hai")
        source_client = await source_client_for(uid)
        message = await source_client.get_messages(ref.chat_ref, ref.message_id)
        if not message or not pick_media(message):
            raise ValueError("message me uploadable video/file nahi mila")
        await process_media_message(source_client, message, uid, status, announce=True)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        await _safe_job_status(status, f"❌ Telegram message read nahi hua: `{exc}`")


@app.on_message(
    (filters.group | filters.channel)
    & (filters.video | filters.document | filters.audio | filters.animation | filters.video_note)
)
async def auto_target_handler(client: Client, m: Message):
    """Receive new media posts for configured chats and enqueue them once."""
    if not getattr(m, "chat", None) or getattr(m, "outgoing", False):
        return
    sender = getattr(m, "from_user", None)
    if sender is not None and getattr(sender, "is_bot", False):
        return
    for target in db.auto_targets(m.chat.id):
        uid = int(target["user_id"])
        if not db.get_token(uid):
            log.warning("Auto target %s skipped: owner %s has no TeraBox token", target["id"], uid)
            continue
        item_id, created = db.ensure_target_item(
            target["id"], m.id, target["chat_id"], telegram_message_url(m.chat, m.id), media_name(m)
        )
        item = db.get_target_item(item_id) or {}
        if not created and item.get("state") in {"uploaded", "queued", "downloading", "uploading"}:
            continue
        db.mark_item_queued(item_id)
        db.touch_target_message(target["id"], m.id)
        try:
            status = await app.send_message(
                uid, f"🤖 Auto target #{target['id']}\n⏳ Message {m.id} queue me hai..."
            )
        except Exception as exc:
            log.warning("Cannot notify auto-target owner %s: %s", uid, exc)
            continue
        await enqueue_job(
            uid,
            lambda target=target, item_id=item_id, status=status: process_media_message(
                client, m, uid, status, item_id=item_id, announce=True
            ),
            status,
        )


# ---------- URL -> TeraBox (2GB+ ke liye) ----------
async def handle_url(
    m: Message,
    url: str,
    _queued: bool = False,
    _status: Message | None = None,
):
    uid = m.from_user.id
    token = db.get_token(uid)
    if not token:
        await m.reply_text("❌ Pehle TeraBox account bind karo.", reply_markup=menu(False))
        return

    if not _queued:
        status = await m.reply_text("⏳ Queue me add ho raha hai...")
        position = await enqueue_job(
            uid, lambda: handle_url(m, url, True, status), status
        )
        if position > 1:
            try:
                await status.edit_text(
                    f"⏳ Queue me waiting...\n📌 Pending position: {position}"
                )
            except Exception:
                pass
        return

    status = _status or await m.reply_text("⏳ Queue me hai...")
    token = db.get_token(uid)
    if not token:
        await status.edit_text("❌ TeraBox account bind nahi hai; job skip kar diya.")
        return

    folder = os.path.join(config.DOWNLOAD_DIR, f"{uid}_{m.id}")
    os.makedirs(folder, exist_ok=True)
    loop = asyncio.get_running_loop()

    async def edit_status(text):
        try:
            await status.edit_text(text)
        except Exception:
            pass

    async with upload_sem:
        try:
            await edit_status("🔎 TeraBox login check ho raha hai...")
            tb = TeraBox(token)
            await to_thread_wait_on_cancel(tb.ensure_login)

            last = [0.0]

            def hook(d):
                if d.get("status") != "downloading" or time.time() - last[0] < 3:
                    return
                last[0] = time.time()
                cur = d.get("downloaded_bytes") or 0
                total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                rate = float(d.get("speed") or 0)
                speed_line = f"\n⚡ Speed: {speed_label(rate)}" if rate else ""
                eta_line = f" · {eta_label(cur, total, rate)}" if total else " · ETA: unavailable"
                txt = (
                    f"⬇️ Link se download\n{bar(cur, total)}\n"
                    f"{human(cur)} / {human(total)}{speed_line}{eta_line}"
                    if total
                    else f"⬇️ Link se download\n{human(cur)}{speed_line}{eta_line}"
                )
                asyncio.run_coroutine_threadsafe(edit_status(txt), loop)

            opts = {
                "outtmpl": os.path.join(folder, "%(title).120B.%(ext)s"),
                "progress_hooks": [hook],
                "noplaylist": True,
                "quiet": True,
                "no_warnings": True,
                "retries": 10,
                "fragment_retries": 10,
                "file_access_retries": 5,
                "concurrent_fragment_downloads": config.URL_FRAGMENT_WORKERS,
                "http_chunk_size": 10 * 1024 * 1024,
                "merge_output_format": "mp4",
                "max_filesize": config.MAX_URL_GB * 1024**3,
            }

            def dl():
                with yt_dlp.YoutubeDL(opts) as y:
                    y.extract_info(url, download=True)

            await to_thread_wait_on_cancel(dl)

            files = [os.path.join(folder, f) for f in os.listdir(folder)]
            files = [f for f in files if os.path.isfile(f) and not f.endswith((".part", ".ytdl"))]
            if not files:
                raise TeraboxError("Link se file download nahi hui (size limit ya unsupported link).")
            local = max(files, key=os.path.getsize)
            name = safe_name(os.path.basename(local))

            # Parallel 4 MiB workers finish in bursts. Use elapsed average
            # instead of the last chunk interval, which shows false spikes.
            ul_started = [0.0]
            ul_last_ui = [0.0, 0]

            def up_progress(cur, total):
                now = time.monotonic()
                if ul_started[0] <= 0:
                    ul_started[0] = now
                # Telegram rate-limits messages.EditMessage. Ignore stale
                # callbacks so the displayed percentage never goes backward.
                if cur < ul_last_ui[1] or (now - ul_last_ui[0] < 5 and cur < total):
                    return
                ul_last_ui[0], ul_last_ui[1] = now, cur
                rate = cur / max(now - ul_started[0], 0.001)
                asyncio.run_coroutine_threadsafe(
                    edit_status(
                        f"⬆️ TeraBox Upload\n{bar(cur, total)}\n"
                        f"{human(cur)} / {human(total)}\n⚡ Speed: {speed_label(rate)} · "
                        f"{eta_label(cur, total, rate)}"
                    ),
                    loop,
                )

            await to_thread_wait_on_cancel(tb.ensure_login, True)
            ul_started[0] = time.monotonic()
            link = await to_thread_wait_on_cancel(
                tb.upload, local, name, config.REMOTE_FOLDER, up_progress,
                make_stage_cb(loop, edit_status), config.TERABOX_UPLOAD_THREADS,
                upload_part_slots,
            )
            await send_done(status, name, link)
        except TeraboxError as e:
            await status.edit_text(f"❌ Error:\n`{e}`")
        except Exception as e:
            await status.edit_text(f"❌ Error:\n`{e}`")
        finally:
            shutil.rmtree(folder, ignore_errors=True)


async def run_runtime() -> None:
    await app.start()
    try:
        await restore_user_clients()
        await idle()
    finally:
        await stop_user_clients()
        await app.stop()


if __name__ == "__main__":
    _, dashboard_url = start_dashboard()
    display_url = dashboard_url.split("?", 1)[0]
    suffix = " (auth token required)" if "?" in dashboard_url else ""
    print(f"Bot started · live logs: {display_url}{suffix}", flush=True)
    app.loop.run_until_complete(run_runtime())
