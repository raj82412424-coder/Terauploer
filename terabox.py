"""
TeraBox web client (UNOFFICIAL endpoints, same ones the website uses).
Auth = user's TeraBox Cookie header (ndus, browserid, ndut_fmt, etc.).
"""
import hashlib
import json
import logging
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Optional
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter

log = logging.getLogger(__name__)

APP_ID = "250528"
DEFAULT_BASE = os.getenv("TERABOX_API_BASE", "https://dm.1024terabox.com").rstrip("/")
DEFAULT_WEB_BASE = os.getenv("TERABOX_WEB_BASE", "https://dm.1024terabox.com").rstrip("/")
CHUNK = 4 * 1024 * 1024
PART_MAX_RETRIES = max(1, min(20, int(os.getenv("TERABOX_MAX_RETRIES", "8"))))
PART_CONNECT_TIMEOUT = max(3, int(os.getenv("TERABOX_CONNECT_TIMEOUT", "15")))
PART_READ_TIMEOUT = max(10, int(os.getenv("TERABOX_READ_TIMEOUT", "60")))
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


class TeraboxError(Exception):
    pass


class TeraBox:
    def __init__(self, ndus: str, base_url: Optional[str] = None):
        self.base = (base_url or DEFAULT_BASE).rstrip("/")
        self.web_base = (base_url or DEFAULT_WEB_BASE).rstrip("/")
        self.s = requests.Session()
        # Keep enough reusable connections for parallel part uploads. HTTPS is
        # preferred below because the HTTP upload endpoint can return 408/read
        # request body timeouts on shared networks.
        adapter = HTTPAdapter(pool_connections=32, pool_maxsize=32)
        self.s.mount("https://", adapter)
        self.s.mount("http://", adapter)
        self._thread_local = threading.local()
        self.s.headers.update(
            {
                "User-Agent": UA,
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": self.web_base + "/",
                "Origin": self.web_base,
            }
        )

        raw = ndus.strip()
        if raw.lower().startswith("cookie:"):
            raw = raw.split(":", 1)[1].strip()
        pairs = {}
        # Parse cookie string, newline-separated or semicolon-separated
        if ";" in raw or "=" in raw or "\n" in raw:
            for item in re.split(r"[;\n]+", raw):
                item = item.strip()
                if "=" in item:
                    k, v = item.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    if k and v:
                        pairs[k] = v
        else:
            pairs["ndus"] = raw

        pairs.setdefault("lang", "en")

        # Set cookies across TeraBox domains
        for domain in (".1024terabox.com",):
            for k, v in pairs.items():
                self.s.cookies.set(k, v, domain=domain)

        # Also set explicit Cookie header for maximum compatibility with PCS servers
        self.cookie_header = "; ".join(f"{k}={v}" for k, v in pairs.items())
        self.s.headers["Cookie"] = self.cookie_header

        # Login/token state is initialized before any validate/upload call.
        self.js_token = ""
        self.bd_token = ""
        self._logged_in = False
        self._quota: dict = {}
        log.info("TeraBox session initialized: cookies=%s", sorted(pairs.keys()))

    def _part_session(self) -> requests.Session:
        """Return one requests session per upload worker.

        requests.Session keeps connection pools but is not documented as
        thread-safe. A session per worker preserves keep-alive speed without
        racing cookies/headers during parallel part uploads.
        """
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            adapter = HTTPAdapter(pool_connections=4, pool_maxsize=4, pool_block=True)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            session.headers.update(self.s.headers)
            session.cookies.update(self.s.cookies)
            self._thread_local.session = session
        return session

    def _candidate_bases(self) -> list[str]:
        values = [self.base, self.web_base, DEFAULT_BASE, DEFAULT_WEB_BASE]
        result = []
        for value in values:
            value = value.rstrip("/")
            if value and value not in result:
                result.append(value)
        return result

    # ---------- helpers ----------
    def _params(self, **extra) -> dict:
        p = {
            "app_id": APP_ID,
            "web": "1",
            "channel": "dubox",
            "clienttype": "0",
            "jsToken": self.js_token,
            "dp-logid": str(int(time.time() * 1000)) + str(random.randint(1000, 9999)),
        }
        if self.bd_token:
            p["bdstoken"] = self.bd_token
        p.update(extra)
        return p

    def _fetch_js_token(self) -> None:
        """Fetch required jsToken from TeraBox web page without crashing on decompression."""
        targets = [
            self.web_base + "/main",
            self.base + "/main",
            DEFAULT_WEB_BASE + "/main",
        ]
        # Remove duplicate URLs while preserving order
        seen = set()
        unique_targets = []
        for u in targets:
            if u not in seen:
                seen.add(u)
                unique_targets.append(u)

        for url in unique_targets:
            try:
                # Accept-Encoding: identity prevents zlib/gzip corrupt header crashes
                r = self.s.get(url, headers={"Accept-Encoding": "identity"}, timeout=15)
                text = requests.utils.unquote(r.text)
            except Exception as e:
                log.warning("Could not fetch page from %s: %s", url, e)
                continue

            # Extract jsToken
            js_patterns = (
                r'fn\(["\']([0-9A-Fa-f]{16,})["\']\)',
                r'fn\(%22([0-9A-Fa-f]{16,})%22\)',
                r'(?:jsToken|jstoken|jstok)\s*["\']?\s*[:=]\s*["\']?([0-9A-Fa-f]{16,})',
            )
            for pat in js_patterns:
                m = re.search(pat, text, re.I) or re.search(pat, r.text, re.I)
                if m:
                    self.js_token = m.group(1)
                    break

            # Extract bdstoken if available (optional on modern TeraBox)
            bd_patterns = (
                r'["\']bdstoken["\']\s*:\s*["\']([0-9A-Fa-f]{16,})["\']',
                r'(?:bdstoken|bdsto)\s*["\']?\s*[:=]\s*["\']?([0-9A-Fa-f]{16,})',
            )
            for pat in bd_patterns:
                m = re.search(pat, text, re.I) or re.search(pat, r.text, re.I)
                if m:
                    self.bd_token = m.group(1)
                    break

            if self.js_token:
                log.info(
                    "TeraBox tokens found: jsToken=%s (len=%d) bdstoken=%s",
                    bool(self.js_token),
                    len(self.js_token),
                    bool(self.bd_token),
                )
                return

    def _check(self, data: dict, what: str) -> dict:
        if not isinstance(data, dict) or data.get("errno", 0) != 0:
            errno = data.get("errno") if isinstance(data, dict) else "non-json"
            errmsg = data.get("errmsg") if isinstance(data, dict) else str(data)
            raise TeraboxError(f"{what} failed (errno={errno}): {errmsg or data}")
        return data

    # ---------- public ----------
    def validate(self) -> dict:
        """Returns quota info if token is valid, else raises."""
        try:
            self._fetch_js_token()
        except Exception:
            pass

        last_error = None
        bases_to_try = self._candidate_bases()
        # remove duplicates while preserving order
        seen = set()
        unique_bases = []
        for b in bases_to_try:
            if b not in seen:
                seen.add(b)
                unique_bases.append(b)

        for b in unique_bases:
            try:
                r = self.s.get(
                    b + "/api/quota",
                    params=self._params(checkfree="1", checkexpire="1"),
                    headers={"Accept-Encoding": "identity"},
                    timeout=20,
                )
                data = r.json()
                if data.get("errno", -1) == 0:
                    self.base = b
                    self.web_base = b
                    return data
                last_error = data.get("errmsg", f"errno={data.get('errno')}")
            except Exception as e:
                last_error = str(e)

        raise TeraboxError(f"Token invalid ya expire ho gaya ({last_error}).")

    BASES = ["https://dm.1024terabox.com"]

    def _fetch_bdstoken_api(self) -> None:
        """bdstoken fallback via template-variable API (page scraping often misses it)."""
        if self.bd_token:
            return
        try:
            r = self.s.get(
                self.base + "/api/gettemplatevariable",
                params=self._params(fields=json.dumps(["bdstoken"])),
                headers={"Accept-Encoding": "identity"},
                timeout=20,
            )
            data = r.json()
            tok = (data.get("result") or {}).get("bdstoken") if isinstance(data, dict) else None
            if tok:
                self.bd_token = tok
                log.info("bdstoken API fallback fetched via API")
        except Exception as e:
            log.warning("bdstoken API fallback failed: %s", e)

    def ensure_login(self, force: bool = False) -> dict:
        """Find the TeraBox domain where this cookie is really logged in, then
        (re)load jsToken/bdstoken FROM THAT domain. Call before every upload.
        Raises TeraboxError immediately if the cookie is not valid."""
        if self._logged_in and not force:
            return self._quota
        self.js_token = ""
        self.bd_token = ""
        q = self.validate()               # picks working self.base
        self.js_token = ""
        self.bd_token = ""
        self._fetch_js_token()            # tokens from the working domain
        self._fetch_bdstoken_api()
        self._quota = q
        self._logged_in = True
        log.info("TeraBox login OK on %s (jsToken=%s bdstoken=%s)",
                 self.base, bool(self.js_token), bool(self.bd_token))
        return q

    def _precreate(self, remote: str, ctx: dict, size: int) -> dict:
        r = self.s.post(
            self.base + "/api/precreate",
            params=self._params(),
            data={
                "path": remote,
                "autoinit": 1,
                "target_path": ctx["target"],
                "block_list": ctx["block_list"],
                "size": size,
                "rtype": 3,
                "local_mtime": ctx["mtime"],
                "file_limit_switch_v34": "true",
            },
            headers={"Accept-Encoding": "identity"},
            timeout=60,
        )
        pre = r.json()
        log.info("precreate: base=%s path=%s size=%s blocks=%s http=%s errno=%s",
                 self.base, remote, size, ctx["parts"], r.status_code,
                 pre.get("errno") if isinstance(pre, dict) else "non-json")
        return pre

    # ----- step-wise API (streaming & chunk upload) -----
    def start(self, name: str, size: int, md5s: list, folder: str, mtime: Optional[int] = None) -> dict:
        remote = f"/{folder}/{name}"
        ctx = {
            "name": name,
            "size": size,
            "remote": remote,
            "target": f"/{folder}/",
            "block_list": json.dumps(md5s),
            "parts": len(md5s),
            "mtime": mtime or int(time.time()),
            "rapid_upload": False,
            "fs_id": None,
            "uploadid": "",
        }

        # Make sure cookie/domain/tokens are valid for THIS instance.
        self.ensure_login()

        # Call precreate. errno -6 = "not logged in": refresh tokens, then try
        # the other TeraBox domains before giving up.
        pre = self._precreate(remote, ctx, size)
        if isinstance(pre, dict) and pre.get("errno") == -6:
            log.warning("precreate errno=-6 on %s - refreshing login/tokens", self.base)
            self.ensure_login(force=True)
            pre = self._precreate(remote, ctx, size)
        if isinstance(pre, dict) and pre.get("errno") == -6:
            original = self.base
            for alt in self._candidate_bases():
                if alt == original:
                    continue
                self.base = self.web_base = alt
                self.js_token = self.bd_token = ""
                try:
                    self._fetch_js_token()
                    self._fetch_bdstoken_api()
                    pre = self._precreate(remote, ctx, size)
                except Exception as e:
                    log.warning("precreate on %s failed: %s", alt, e)
                    continue
                if isinstance(pre, dict) and pre.get("errno") != -6:
                    break
            else:
                self.base = self.web_base = original
        if isinstance(pre, dict) and pre.get("errno") == -6:
            raise TeraboxError(
                "TeraBox cookie upload ke liye valid nahi (errno -6 user not login). "
                "Browser me dm.1024terabox.com par logout/login karke, Network tab se POORI Cookie "
                "(ndus + browserid + csrfToken + ndut_fmt ...) copy karo aur /change karo."
            )
        pre = self._check(pre, "precreate")

        # Check for rapid/instant upload: return_type == 2 means file already exists on cloud
        fs_id = pre.get("fs_id") or (
            pre.get("info", {}).get("fs_id") if isinstance(pre.get("info"), dict) else None
        )
        if fs_id or pre.get("return_type") == 2 or (isinstance(pre.get("block_list"), list) and len(pre.get("block_list")) == 0):
            ctx["rapid_upload"] = True
            ctx["fs_id"] = fs_id
            log.info("precreate: rapid/instant upload hit! fs_id=%s", fs_id)
            return ctx

        ctx["uploadid"] = pre.get("uploadid", "")
        ctx["needed_blocks"] = pre.get("block_list", list(range(len(md5s))))
        if not ctx["uploadid"]:
            raise TeraboxError(f"uploadid nahi mila: {pre}")

        # Locate upload hosts with candidate list
        ctx["host"] = "c-jp.terabox.com"
        ctx["hosts"] = ["c-jp.terabox.com", "c1-jp.terabox.com", "c2-jp.terabox.com"]
        for locate_url in (
            f"{self.base}/rest/2.0/pcs/file",
            "https://jp-data.terabox.com/rest/2.0/pcs/file",
            "https://d.terabox.com/rest/2.0/pcs/file",
        ):
            try:
                lr = self.s.get(
                    locate_url,
                    params={"method": "locateupload", "app_id": APP_ID},
                    headers={"Accept-Encoding": "identity"},
                    timeout=15,
                ).json()
                if isinstance(lr, dict):
                    server_list = lr.get("server") or lr.get("servers") or []
                    if isinstance(server_list, list) and server_list:
                        ctx["hosts"] = server_list
                    host = lr.get("host") or (ctx["hosts"][0] if ctx["hosts"] else ctx["host"])
                    parsed = urlparse(host if "://" in host else "http://" + host)
                    if parsed.hostname:
                        ctx["host"] = parsed.hostname
                    break
            except Exception as e:
                log.warning("locateupload fallback warning from %s: %s", locate_url, e)

        return ctx

    def put_part(self, ctx: dict, i: int, chunk: bytes) -> None:
        """Upload one 4 MiB part with keep-alive sessions and host failover."""
        if ctx.get("rapid_upload"):
            return

        candidate_hosts = [ctx.get("host", "")] + list(ctx.get("hosts", []))
        targets = []
        for host in candidate_hosts:
            raw = str(host).strip().rstrip("/")
            if not raw:
                continue
            parsed = urlparse(raw if "://" in raw else f"//{raw}")
            hostname = parsed.netloc or parsed.path
            if not hostname:
                continue
            for target in (f"https://{hostname}", f"http://{hostname}"):
                if target not in targets:
                    targets.append(target)
        if not targets:
            raise TeraboxError(f"chunk {i}: TeraBox upload host nahi mila")

        session = self._part_session()
        last_error = None
        max_attempts = max(PART_MAX_RETRIES, len(targets) * 2)
        for attempt in range(max_attempts):
            target_base = targets[attempt % len(targets)]
            try:
                response = session.post(
                    f"{target_base}/rest/2.0/pcs/superfile2",
                    params={
                        "method": "upload",
                        "type": "tmpfile",
                        "app_id": APP_ID,
                        "path": ctx["remote"],
                        "uploadid": ctx["uploadid"],
                        "partseq": str(i),
                    },
                    files={"file": (ctx["name"], chunk)},
                    headers={"Accept-Encoding": "identity"},
                    timeout=(PART_CONNECT_TIMEOUT, PART_READ_TIMEOUT),
                )
                try:
                    body = response.json()
                except Exception:
                    body = {"raw": response.text[:300]}
                if not isinstance(body, dict):
                    body = {"raw": str(body)[:300]}

                if not isinstance(body, dict) or not body or "raw" in body:
                    last_error = (
                        f"{target_base}: http {response.status_code} invalid upload response"
                    )
                    log.warning(
                        "part %s attempt %s/%s failed: %s",
                        i, attempt + 1, max_attempts, last_error,
                    )
                else:
                    err_code = body.get("error_code", body.get("errno"))
                    if response.status_code >= 400 or err_code not in (0, None):
                        err_msg = body.get("error_msg", body.get("errmsg", response.text[:200]))
                        last_error = (
                            f"{target_base}: http {response.status_code} "
                            f"code={err_code} {err_msg}"
                        )
                        log.warning(
                            "part %s attempt %s/%s failed: %s",
                            i, attempt + 1, max_attempts, last_error,
                        )
                    else:
                        # Future parts prefer the endpoint that just succeeded.
                        ctx["host"] = target_base
                        return
            except Exception as exc:  # timeout / connection reset / etc.
                last_error = exc
                log.warning(
                    "part %s attempt %s/%s error: %r",
                    i, attempt + 1, max_attempts, exc,
                )

            # Small jitter prevents all workers retrying the same host together.
            delay = min(0.5 * (2 ** min(attempt, 4)), 4.0) + random.uniform(0, 0.25)
            time.sleep(delay)

        raise TeraboxError(
            f"chunk {i} upload failed after {max_attempts} attempts: {last_error}"
        )

    def finish(self, ctx) -> str:
        fs_id = ctx.get("fs_id")

        if not ctx.get("rapid_upload") or not fs_id:
            r = self.s.post(
                self.base + "/api/create",
                params=self._params(),
                data={
                    "path": ctx["remote"],
                    "size": ctx["size"],
                    "uploadid": ctx["uploadid"],
                    "target_path": ctx["target"],
                    "block_list": ctx["block_list"],
                    "local_mtime": ctx["mtime"],
                    "isdir": 0,
                    "rtype": 1,
                },
                headers={"Accept-Encoding": "identity"},
                timeout=60,
            )
            created = self._check(r.json(), "create")
            log.info("create: http=%s errno=%s resp=%s", r.status_code, created.get("errno"), created)
            fs_id = created.get("fs_id")
            if not fs_id and isinstance(created.get("info"), dict):
                fs_id = created["info"].get("fs_id")
            if not fs_id and isinstance(created.get("info"), list) and created["info"]:
                fs_id = created["info"][0].get("fs_id")
            if not fs_id:
                raise TeraboxError(f"fs_id nahi mila: {created}")

        # Generate share link using both fid_list (required) and path_list
        share_data = {
            "schannel": 0,
            "channel_list": "[0]",
            "period": 0,
            "pwd": "",
            "public": 1,
            "path_list": json.dumps([ctx["remote"]]),
        }
        if fs_id:
            share_data["fid_list"] = f"[{fs_id}]"

        sh = {}
        for share_url in (self.base + "/share/pset", self.base + "/api/share/pset"):
            try:
                r = self.s.post(
                    share_url,
                    params=self._params(),
                    data=share_data,
                    headers={"Accept-Encoding": "identity"},
                    timeout=60,
                )
                sh = r.json()
                log.info("share/pset (%s): http=%s errno=%s resp=%s", share_url, r.status_code, sh.get("errno"), sh)
                if isinstance(sh, dict) and sh.get("errno", 0) == 0:
                    link = sh.get("link") or ""
                    short = sh.get("shorturl") or ""
                    if short.startswith("http"):
                        return short
                    if link:
                        return link
                    if short:
                        return f"https://1024terabox.com/s/{short}"
            except Exception as e:
                log.warning("Share request to %s failed: %s", share_url, e)

        # Direct download / stream link generation via PCS
        direct_link = self.get_download_link(ctx["remote"])
        if direct_link:
            log.info("Direct download link generated for %s: %s", ctx["remote"], direct_link[:80])
            return direct_link

        # If share link endpoint failed or returned non-zero errno, return file uploaded confirmation with path
        err_msg = sh.get("errmsg") or f"errno={sh.get('errno', 'unknown')}" if isinstance(sh, dict) else "unknown"
        return f"File uploaded to `{ctx['remote']}` (Share link generated nahi ho saka: {err_msg})"

    def get_download_link(self, remote_path: str) -> Optional[str]:
        """Generate a direct high-speed PCS download/streaming URL for the remote file."""
        try:
            r = self.s.get(
                f"{self.base}/rest/2.0/pcs/file",
                params={"method": "download", "app_id": APP_ID, "path": remote_path},
                allow_redirects=False,
                timeout=20,
            )
            loc = r.headers.get("Location")
            if loc:
                return loc
        except Exception as e:
            log.warning("get_download_link failed for %s: %s", remote_path, e)
        return None

    # ----- local file upload -----
    def upload(
        self,
        path,
        name,
        folder,
        progress=None,
        stage=None,
        threads: int = 4,
        thread_limiter: Optional[threading.Semaphore] = None,
    ) -> str:
        """Hash -> precreate -> parallel part upload -> create -> share link.

        progress(done_bytes, total_bytes) is called from worker threads (serialised).
        stage(name) is called with 'hash', 'precreate', 'upload', 'finish'.
        """
        def _stage(x):
            if stage:
                try:
                    stage(x)
                except Exception:
                    pass

        size = os.path.getsize(path)
        if size <= 0:
            raise TeraboxError("File empty hai (0 bytes).")

        _stage("hash")
        md5s = []
        with open(path, "rb") as f:
            while True:
                c = f.read(CHUNK)
                if not c:
                    break
                md5s.append(hashlib.md5(c).hexdigest())

        _stage("precreate")
        ctx = self.start(name, size, md5s, folder, int(os.path.getmtime(path)))

        if ctx.get("rapid_upload"):
            if progress:
                progress(size, size)
        else:
            _stage("upload")
            total_parts = len(md5s)
            raw_needed = ctx.get("needed_blocks") or []
            needed = []
            for value in raw_needed:
                try:
                    index = int(value)
                except (TypeError, ValueError):
                    continue
                if 0 <= index < total_parts:
                    needed.append(index)
            # Some TeraBox responses return an empty or malformed block list;
            # uploading every part is safer than silently producing a partial file.
            needed = list(dict.fromkeys(needed))
            if not needed:
                needed = list(range(total_parts))

            done = 0
            lock = threading.Lock()
            threads = max(1, min(int(threads), 8, len(needed)))
            fd = os.open(path, os.O_RDONLY)

            def work(i: int):
                chunk = os.pread(fd, CHUNK, i * CHUNK)
                if not chunk:
                    raise TeraboxError(f"part {i}: local file read empty")
                acquired = False
                if thread_limiter is not None:
                    thread_limiter.acquire()
                    acquired = True
                try:
                    self.put_part(ctx, i, chunk)
                finally:
                    if acquired:
                        thread_limiter.release()
                return len(chunk)

            try:
                with ThreadPoolExecutor(max_workers=threads) as ex:
                    futs = [ex.submit(work, i) for i in needed]
                    try:
                        for fut in as_completed(futs):
                            n = fut.result()
                            with lock:
                                done += n
                                if progress:
                                    progress(min(done, size), size)
                    except BaseException:
                        for f_ in futs:
                            f_.cancel()
                        raise
            finally:
                os.close(fd)

        _stage("finish")
        return self.finish(ctx)
