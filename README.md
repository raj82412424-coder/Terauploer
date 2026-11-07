# TeraBox Uploader Telegram Bot

Telegram par video/file bhejo → bot local disk par resilient download karta hai → TeraBox par parallel chunk upload karta hai → share link bhejta hai.

## Highlights

- Pyrogram/Kurigram-compatible Telegram bot.
- Telegram media ko disjoint 1 MiB ranges me bounded parallel workers se download karta hai.
- Har range me retry + resume; final file `.part` me banti hai aur success par atomic rename hota hai.
- FIFO upload queue: default one video at a time; every additional file/URL waits in order and starts automatically after the previous job finishes.
- Telegram download workers default `3` per job, with `6` total active streams across all jobs.
- TeraBox upload workers default `6` per job, with `8` total active parts across all jobs.
- Files up to 4096 MB are accepted from Telegram when the connected Telegram/Pyrogram route supports them; larger files use direct-link flow.
- Telegram user-session login: `/login` → phone → OTP → optional 2FA password; session Fernet-encrypted SQLite me save hota hai. `/logout` se revoke/delete karo. Isse bot aapke user account ke accessible private groups/channels ko read/download kar sakta hai.
- URL/m3u8/yt-dlp downloads me fragment retries aur configurable concurrency.
- `/queue` active job aur waiting jobs ki count dikhata hai.
- Targeted group/channel ingestion: `/set` wizard se chat + starting Telegram message set karo; `/batch 100`, `/batch 10000` ya `/batch all` se us point se archive scan karke media TeraBox par upload hota hai.
- `/auto <target_id> on|off` se configured group/channel ke naye media messages automatically queue hote hain.
- Target reports: `/targets`, `/targetstatus [id]`, `/links [id]`; durable SQLite state restart ke baad bhi uploaded links, failed items aur counters preserve karti hai.
- Private chat me pasted Telegram message URL (public `t.me/name/123` ya accessible private `/c/.../123`) ko bina command ke fetch/upload karta hai.
- Batch/auto flow me har item ka Telegram source URL aur TeraBox uploaded URL report hota hai, live download/upload progress bar ke saath.
- `/stop` current user ke active job ko cancel karta hai aur uske waiting jobs skip karta hai.
- Live dashboard with secret redaction and `/health` endpoint.

## Security first

Agar API ID/hash ya bot token chat, log, screenshot ya repository me expose ho gaya ho to Telegram/BotFather se turant rotate/revoke karo. Real values sirf deployment ke `.env` me rakho; is project ZIP me credentials hardcode na karo. TeraBox Cookie aur Telegram user session ko bhi password ki tarah treat karo. `/login` OTP/2FA sirf bot ke private chat me bhejo; kisi group/channel me kabhi nahi.

## Local setup

```bash
cp .env.example .env
# .env me API_ID, API_HASH aur BOT_TOKEN bharo
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python bot.py
```

`DATA_DIR` default `./data` rakho, taaki encryption key, SQLite DB aur downloads persistent ek hi directory me rahen. First start par `DATA_DIR/.encryption_key` restrictive permissions ke saath generate hoti hai. Existing encrypted tokens ke liye wahi purani key preserve karna zaroori hai.

## Docker deployment

```bash
cp .env.example .env
# .env me secrets bharo; production me strong LOG_WEB_TOKEN set karo
docker compose up -d --build
docker compose logs -f terabox-bot
```

Compose persistent named volume `/app/data` mount karta hai, isliye container user ko encryption key/SQLite likhne ki permission milti hai. Dashboard host ke localhost par bind hota hai. Dashboard public karna ho to reverse proxy + HTTPS + strong `LOG_WEB_TOKEN` use karo; Telegram polling ko incoming port ki zaroorat nahi hoti.

## Tuning

- `MAX_PARALLEL=1`: FIFO queue ko strictly one job at a time chalata hai. Zaroorat ho to `2`–`4` set karke limited parallel workers enable kar sakte ho; process-wide worker budgets active connections ko bounded rakhte hain.
- `TELEGRAM_DOWNLOAD_WORKERS=3` और `TELEGRAM_TOTAL_DOWNLOAD_WORKERS=6`: per-job और पूरे process की सीमा। Telegram FloodWait दिखे तो total `4` करो।
- `TERABOX_UPLOAD_THREADS=6` और `TERABOX_TOTAL_UPLOAD_THREADS=8`: per-job और पूरे process की सीमा। Server errors बढ़ें तो total `6` करो।
- `MAX_FILE_MB=4096`: 4 GB तक की Telegram file route support करे तो; Telegram/provider limit होने पर direct link use होगा.
- `TERABOX_MAX_RETRIES`, `TERABOX_CONNECT_TIMEOUT`, `TERABOX_READ_TIMEOUT`: unstable network ke liye.
- `URL_FRAGMENT_WORKERS=8`: direct URL downloads ke fragments; provider/server limit hone par kam karo.
- `MAX_BATCH_MESSAGES=10000`: `/batch all` ka safety cap. Large archive ko multiple configured start points/counts me process karo.

Parallelism ko blindly maximum karne se speed zaroori nahi badhti—Telegram/TeraBox rate limits, CPU, disk I/O aur VPS egress bottleneck ho sakte hain. Dashboard me retry/error logs dekhkar tuning karo.

## Targeted group/channel workflow

1. Private groups/channels ke liye pehle `/login` karke apna Telegram account authorize karo; us account ka source group/channel me access hona chahiye. Agar `/login` use nahi karte to bot account ko source group/supergroup me member/admin aur channel me admin/read access do.
2. Private chat me `/set` bhejo. Bot target ID/`@username`/public URL, phir starting message URL aur auto mode (`on`/`off`) poochega. Logged-in user session source access verify karega; bot-only mode me `/set` bhejne wala user source ka admin/owner hona chahiye.
3. `/batch 100` ya `/batch 10000` current target se utne messages scan karta hai. Kisi doosre configured target ke liye `/batch <target_id> 100` use karo. `/batch all` maximum `MAX_BATCH_MESSAGES` (default 10,000) tak safe scan karta hai; text-only messages skip hote hain, media durable queue me record hota hai.
4. `/auto <target_id> on` ke baad aane wale video/document/audio/animation/video-note messages automatic upload honge. Har target ka owner apne bound TeraBox account se upload karta hai.
5. `/targetstatus`, `/targets` se scanned/queued/downloading/uploading/uploaded/failed counts dekho. `/links [target_id]` se source URL ke saath saved TeraBox links dobara bhejo.

Batch natural Telegram history order me starting message se older messages ki taraf scan karta hai. Private source chats/channels ke liye logged-in user session ya bot account ko access hona chahiye. Same target/message dobara scan karne par already uploaded item duplicate nahi hota; failed item retry ho sakta hai.

## Supported flows

- `/start` → TeraBox account bind menu.
- `/login` → Telegram user account authorize karo (phone, OTP, optional 2FA); `/logout` session remove karta hai.
- `/bind` / `/change` → complete TeraBox Cookie header bind; sensitive message verify ke baad delete hota hai.
- `/set`, `/batch`, `/auto`, `/targets`, `/targetstatus`, `/links`, `/remove` → targeted source management.
- Private chat me direct Telegram message URL paste karo → us message ka media bina command upload hota hai.
- `/status`, `/delete`, `/cancel`, `/stop`, `/queue`.
- Video, document, audio, animation, video note.
- Direct URL, m3u8 aur yt-dlp-supported URL. `MAX_URL_GB` limit lagu hai.

Telegram bot/media route की वास्तविक size limit provider/client पर निर्भर है। `MAX_FILE_MB` केवल local guard है; limit से बाहर files के लिए direct link flow use करो। Local disk को हर concurrent file के size + safety buffer के लिए खाली रखो.

## TeraBox compatibility note

TeraBox ka official public upload API nahi hai; `terabox.py` website ke current web endpoints use karta hai. Cookie login, `jsToken`, `bdstoken`, precreate, locateupload, parallel part upload, create aur share-link flow included hai. Website endpoint change hone par TeraBox-side update ki zaroorat pad sakti hai.

## Checks

```bash
python -m compileall -q .
python test_upload.py  # real Cookie ke saath hi; upload test cloud storage consume karega
```
