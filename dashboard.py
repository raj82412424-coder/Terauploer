"""Small dependency-free live log dashboard for the Telegram bot."""
from __future__ import annotations

import html
import json
import logging
import os
import re
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote, urlparse

_MAX = 1000
_entries: deque[dict] = deque(maxlen=_MAX)
_next_id = 0
_condition = threading.Condition()
_server: ThreadingHTTPServer | None = None

_SECRET_PATTERNS = [
    (re.compile(r"(?i)((?:bot[_\s-]*token|bot[_\s-]*api[_\s-]*token)\s*[:=]\s*)[^\s,;]+"), r"\1[REDACTED]"),
    (re.compile(r"(?i)((?:ndus|ndut_fmt|ndut_fmv|csrfToken|bdstoken|jsToken|api_hash|encryption_key)\s*[:=]\s*)[^;\s]+"), r"\1[REDACTED]"),
    (re.compile(r"(?i)(cookie\s*[:=]\s*)[^\n]+"), r"\1[REDACTED]"),
    (re.compile(r"(?i)([?&]token=)[^&\s]+"), r"\1[REDACTED]"),
]


def redact(text: str) -> str:
    for pattern, replacement in _SECRET_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


class DashboardHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        global _next_id
        try:
            message = redact(self.format(record))
            item = {
                "id": _next_id,
                "ts": time.strftime("%H:%M:%S"),
                "level": record.levelname,
                "message": message,
            }
            with _condition:
                _next_id += 1
                _entries.append(item)
                _condition.notify_all()
        except Exception:
            pass


_PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>TeraBox Bot · Live Logs</title>
<style>
:root{color-scheme:dark}*{box-sizing:border-box}body{margin:0;background:#0b1020;color:#e5e7eb;font:14px ui-monospace,SFMono-Regular,Menlo,monospace}.wrap{max-width:1200px;margin:auto;padding:22px}.top{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:16px}h1{font:600 21px system-ui;margin:0}.state{color:#86efac;font:13px system-ui}.panel{background:#111827;border:1px solid #26334d;border-radius:12px;overflow:hidden}.toolbar{display:flex;gap:8px;align-items:center;padding:10px 12px;border-bottom:1px solid #26334d;font-family:system-ui}.toolbar button{background:#1d4ed8;color:#fff;border:0;border-radius:7px;padding:7px 12px;cursor:pointer}.toolbar button:hover{background:#2563eb}.toolbar label{margin-left:auto;color:#94a3b8}.logs{height:calc(100vh - 145px);min-height:360px;overflow:auto;padding:12px}.line{white-space:pre-wrap;word-break:break-word;padding:4px 0;border-bottom:1px solid #172033;font-size:13px}.time{color:#64748b;margin-right:8px}.INFO{color:#d1d5db}.WARNING{color:#fbbf24}.ERROR,.CRITICAL{color:#fb7185}.DEBUG{color:#93c5fd}.empty{color:#64748b;padding:30px;text-align:center}
</style></head><body><div class="wrap"><div class="top"><h1>● TeraBox Bot · Live Logs</h1><div class="state" id="state">Connecting…</div></div><div class="panel"><div class="toolbar"><button onclick="clearLogs()">Clear view</button><button onclick="togglePause()" id="pause">Pause</button><label>Auto-refresh: 1s · Secrets redacted</label></div><div class="logs" id="logs"><div class="empty">Waiting for bot logs…</div></div></div></div>
<script>
let after=-1,paused=false,items=[];const box=document.getElementById('logs'),state=document.getElementById('state');
function draw(){
  if(!items.length){
    box.innerHTML='<div class="empty">No log entries yet.</div>';
    return;
  }
  box.innerHTML=items.map(x=>`<div class="line ${x.level}"><span class="time">${x.ts}</span> ${escapeHtml(x.message)}</div>`).join('');
  box.scrollTop=box.scrollHeight;
}
function escapeHtml(s){return s.replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
function clearLogs(){items=[];draw()}
function togglePause(){paused=!paused;document.getElementById('pause').textContent=paused?'Resume':'Pause';state.textContent=paused?'Paused':'Live'}

async function poll(){
  if(paused)return;
  try{
    const urlParams=new URLSearchParams(window.location.search);
    const token=urlParams.get('token')||'';
    let reqUrl='/api/logs?after='+after;
    if(token){
      reqUrl+='&token='+encodeURIComponent(token);
    }
    let r=await fetch(reqUrl,{cache:'no-store'});
    if(r.status===401){
      state.textContent='Unauthorized (token required in ?token=...)';
      state.style.color='#fb7185';
      return;
    }
    if(!r.ok){
      state.textContent='Error '+r.status;
      return;
    }
    let d=await r.json();
    if(d.items&&d.items.length){
      items.push(...d.items);
      if(items.length>1000) items=items.slice(-1000);
      draw();
    }
    if(typeof d.next==='number') after=d.next;
    state.textContent='Live · '+new Date().toLocaleTimeString();
    state.style.color='#86efac';
  }catch(e){
    state.textContent='Disconnected';
    state.style.color='#fb7185';
  }
}
setInterval(poll,1000);
poll();
</script></body></html>"""


def _authorized(handler: BaseHTTPRequestHandler) -> bool:
    expected = os.getenv("LOG_WEB_TOKEN", "").strip()
    if not expected:
        return True
    query = parse_qs(urlparse(handler.path).query)
    supplied = query.get("token", [""])[0] or handler.headers.get("X-Log-Token", "")
    return supplied == expected


class _Handler(BaseHTTPRequestHandler):
    server_version = "TeraBoxLogDashboard/1.0"

    def log_message(self, *_args):
        return

    def do_GET(self):
        if not _authorized(self):
            self.send_response(401)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"Unauthorized: specify token in query ?token=YOUR_LOG_WEB_TOKEN")
            return

        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            body = _PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/health":
            body = b'{"ok":true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/api/logs":
            try:
                after = int(parse_qs(urlparse(self.path).query).get("after", ["-1"])[0])
            except ValueError:
                after = -1
            with _condition:
                items = [x for x in _entries if x["id"] > after]
                nxt = _next_id - 1 if _next_id > 0 else -1
            body = json.dumps({"items": items, "next": nxt}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()


def start_dashboard() -> tuple[ThreadingHTTPServer, str]:
    global _server
    handler = DashboardHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

    host = os.getenv("LOG_WEB_HOST", "127.0.0.1")
    port = int(os.getenv("LOG_WEB_PORT", "8080"))
    _server = ThreadingHTTPServer((host, port), _Handler)
    thread = threading.Thread(target=_server.serve_forever, name="log-dashboard", daemon=True)
    thread.start()
    token = os.getenv("LOG_WEB_TOKEN", "").strip()
    token_query = f"?token={quote(token, safe='')}" if token else ""
    url = f"http://{host}:{port}/{token_query}"
    safe_url = f"http://{host}:{port}/"
    auth_note = " (token required)" if token else ""
    logging.info("Live log dashboard running on %s%s", safe_url, auth_note)
    return _server, url


def stop_dashboard() -> None:
    if _server:
        _server.shutdown()
