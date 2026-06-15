"""
Thunder-MT: SmartStrm 迅雷云盘多线程流加速引擎 (forked from qy527145/url_proxy)

Changes from upstream:
  - Removed pre-flight Range: bytes=0-0 request (token consumption bug)
  - Lazy file info extraction from first download chunk
  - Added SS smartstrm_fid -> direct URL resolution
  - Content-Type correction via file extension / ?fext= parameter
  - Persistent FastAPI server with dynamic URL sessions
  - Environment variable configuration
"""

import os
import re
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from typing import Optional

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from humanfriendly import parse_size


EXT_MIME = {
    ".mkv":  "video/x-matroska",
    ".mp4":  "video/mp4",
    ".mov":  "video/quicktime",
    ".avi":  "video/x-msvideo",
    ".webm": "video/webm",
    ".ts":   "video/mp2t",
    ".flv":  "video/x-flv",
    ".wmv":  "video/x-ms-wmv",
    ".m4v":  "video/mp4",
}


def _env(k, default):
    v = os.getenv(k)
    return v if v else default


LISTEN_HOST   = _env("LISTEN_HOST", "0.0.0.0")
LISTEN_PORT   = int(_env("LISTEN_PORT", "8010"))
TRUNK_SIZE    = _env("TRUNK", "32M")
SPLIT_SIZE    = _env("SPLIT", "3M")
CONNS         = int(_env("CONNS", "4"))
TIMEOUT       = int(_env("TIMEOUT", "30"))
SESSION_TTL   = int(_env("SESSION_TTL", "120"))


def mime_by_ext(url: str, content_type: str) -> str:
    if content_type and not content_type.startswith("application/octet-stream") \
            and not content_type.startswith("binary/octet-stream") \
            and not content_type.startswith("binary/"):
        return content_type
    parsed = urllib.parse.urlparse(url)
    ext = os.path.splitext(parsed.path)[1].lower()
    if not ext:
        fe = urllib.parse.parse_qs(parsed.query).get("fext", [""])[0].lower()
        if fe:
            ext = "." + fe
    return EXT_MIME.get(ext, content_type)


def resolve_direct_url(ss_url: str, timeout: int = TIMEOUT) -> str:
    """GET SS smartstrm_fid address → extract 302 Location → return Xunlei direct URL."""
    with httpx.Client(timeout=httpx.Timeout(timeout), follow_redirects=False) as c:
        resp = c.get(ss_url)
        if resp.status_code in (301, 302, 307, 308):
            loc = resp.headers.get("Location", "")
            if loc:
                return loc
            raise RuntimeError(f"SS returned {resp.status_code} without Location")
        ct = resp.headers.get("Content-Type", "")
        if resp.status_code == 200 and (ct.startswith("video/") or ct.startswith("audio/")):
            return ss_url
        raise RuntimeError(f"unexpected SS response: {resp.status_code}")


class URLSource:
    def __init__(self, url: str, conns: int = CONNS):
        self.url = url
        self.session = httpx.Client(
            limits=httpx.Limits(max_connections=conns, max_keepalive_connections=conns)
        )

    def get(self, begin: int, end: int):
        resp = self.session.get(
            self.url, headers={"Range": f"bytes={begin}-{end}"}
        )
        ct = resp.headers.get("Content-Type", "application/octet-stream")
        total = int(resp.headers.get("Content-Length", "0"))
        cr = resp.headers.get("Content-Range", "")
        if cr:
            total = int(cr.split("/")[-1])
        return resp.content, begin, end, total, ct


class Spliter:
    def __init__(self, *, begin=None, end=None, length=None):
        if begin is not None and end is not None:
            self.begin = begin
            self.length = end - begin + 1
        elif length:
            self.begin = 0
            self.length = length
        else:
            raise Exception("spliter: missing begin/end or length")

    def iter(self, split: int):
        begin = self.begin
        end = self.begin + self.length - 1
        left, right = begin, begin + split - 1
        while right <= end:
            yield left, right
            left, right = right + 1, right + split
        if left <= end:
            yield left, end

    def sub_split(self, trunk: int):
        for b, e in self.iter(split=trunk):
            yield Spliter(begin=b, end=e)


class URLProxy:
    def __init__(self, url: str, trunk: int, split: int, conns: int):
        self.url = url
        self.trunk = trunk
        self.split = split
        self.conns = conns
        self.source = URLSource(url, conns)
        self._content_type: Optional[str] = None
        self._content_length: int = 0
        self._info_ready = threading.Event()
        self._info_lock = threading.Lock()

    @property
    def content_type(self) -> str:
        self._info_ready.wait()
        return self._content_type or "application/octet-stream"

    @property
    def content_length(self) -> int:
        self._info_ready.wait()
        return self._content_length

    def _set_info(self, total: int, ct: str):
        with self._info_lock:
            if not self._info_ready.is_set():
                self._content_length = total
                self._content_type = mime_by_ext(self.url, ct)
                self._info_ready.set()

    def stream(self, begin=None, end=None, split=None):
        if begin is None:
            begin = 0
        if end is None:
            self._info_ready.wait()
            end = self._content_length - 1
        if split is None:
            split = self.split
        spliter = Spliter(begin=begin, end=end)
        executor = ThreadPoolExecutor(max_workers=self.conns)

        for future in as_completed(
            [executor.submit(self.source.get, b, e) for b, e in spliter.iter(split=split)]
        ):
            content, b, e, total, ct = future.result()
            if not self._info_ready.is_set():
                self._set_info(total, ct)
            yield content, b, e

    def sorted_stream(self, begin=None, end=None, trunk=None, split=None):
        if begin is None:
            begin = 0
        if end is None:
            self._info_ready.wait()
            end = self._content_length - 1
        if trunk is None:
            trunk = self.trunk
        if split is None:
            split = self.split
        spliter = Spliter(begin=begin, end=end)
        for l, r in spliter.iter(split=trunk):
            buf = BytesIO()
            for data, b, e in self.stream(begin=l, end=r, split=split):
                buf.seek(b - l)
                buf.write(data)
            yield buf.getvalue()


SS_CACHE: dict[str, tuple[str, float]] = {}
SS_CACHE_LOCK = threading.Lock()

app = FastAPI()
sessions: dict[str, URLProxy] = {}
sessions_lock = threading.Lock()
sessions_last_access: dict[str, float] = {}


def _session_gc():
    while True:
        time.sleep(30)
        now = time.time()
        with sessions_lock:
            stale = [k for k, t in sessions_last_access.items() if now - t > SESSION_TTL]
            for k in stale:
                sessions.pop(k, None)
                sessions_last_access.pop(k, None)
                print(f"session GC: {k[:80]}...")


@app.get("/stream")
async def stream_handler(url: str, request: Request):
    # Resolve SS URL → direct URL (with cache)
    direct_url = None
    with SS_CACHE_LOCK:
        entry = SS_CACHE.get(url)
        if entry and time.time() < entry[1]:
            direct_url = entry[0]
    if direct_url is None:
        direct_url = resolve_direct_url(url)
        with SS_CACHE_LOCK:
            SS_CACHE[url] = (direct_url, time.time() + 30)

    # Get or create session
    trunk = parse_size(TRUNK_SIZE, True)
    split = parse_size(SPLIT_SIZE, True)
    with sessions_lock:
        if direct_url not in sessions:
            sessions[direct_url] = URLProxy(direct_url, trunk, split, CONNS)
            print(f"session start: url={direct_url[:80]}...")
        sessions_last_access[direct_url] = time.time()
        proxy = sessions[direct_url]

    # Wait for content length (first chunk must arrive)
    proxy._info_ready.wait()
    size = proxy.content_length

    range_str = request.headers.get("Range")
    begin = 0
    end = size - 1

    if range_str:
        match = re.compile(r'bytes=(\d+)-(\d*)').match(range_str)
        if match:
            b_str, e_str = match.groups()
            begin = int(b_str) if b_str else 0
            end = int(e_str) if e_str else size - 1

    end_out = min(begin + trunk, size) - 1
    if end_out < end:
        end = end_out

    headers = {
        "Content-Type": proxy.content_type,
        "Content-Range": f"bytes {begin}-{end}/{size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(end - begin + 1),
    }

    return StreamingResponse(
        proxy.sorted_stream(begin=begin, end=end),
        status_code=206 if range_str else 200,
        headers=headers,
    )


@app.get("/health")
async def health():
    with sessions_lock:
        n = len(sessions)
    return {"status": "ok", "sessions": n}


if __name__ == "__main__":
    threading.Thread(target=_session_gc, daemon=True).start()
    uvicorn.run(app, host=LISTEN_HOST, port=LISTEN_PORT, log_level="info", http="h11")
