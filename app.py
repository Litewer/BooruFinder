import base64
import ctypes
import hashlib
import json
import os
import sys
import time
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener, urlopen
import xml.etree.ElementTree as ET


SOURCE_CONFIG = {
    "rule34": {
        "name": "Rule34",
        "base_url": "https://api.rule34.xxx/index.php",
        "post_url": "https://rule34.xxx/index.php?page=post&s=view&id={id}",
        "cred_prefix": "rule34",
    },
    "gelbooru": {
        "name": "Gelbooru",
        "base_url": "https://gelbooru.com/index.php",
        "post_url": "https://gelbooru.com/index.php?page=post&s=view&id={id}",
        "cred_prefix": "gelbooru",
    },
}

VIDEO_EXTENSIONS = {".mp4", ".webm", ".mov", ".m4v", ".avi", ".mkv"}

APP_DIR = Path(os.getenv("APPDATA", str(Path.home()))) / "BooruFinder"
SECURE_FILE = APP_DIR / "secure_store.json"
DPAPI_SCOPE = b"BooruFinder.LocalSecureStore"
DOWNLOADS_DIR = Path.home() / "Downloads" / "BooruFinder"

CACHE_MAX_ITEMS = 300
POST_CACHE_TTL_SEC = 120
TAG_CACHE_TTL_SEC = 90

_POST_CACHE = {}
_TAG_CACHE = {}
_CACHE_LOCK = Lock()


def static_dir() -> str:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, "static")
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


def to_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def now_ts() -> float:
    return time.time()


def _cache_hash(parts) -> str:
    raw = "||".join(str(p) for p in parts).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _cache_get(cache_store: dict, key: str):
    with _CACHE_LOCK:
        item = cache_store.get(key)
        if not item:
            return None
        expires_at, payload = item
        if expires_at < now_ts():
            cache_store.pop(key, None)
            return None
        return payload


def _cache_set(cache_store: dict, key: str, payload, ttl_sec: int):
    with _CACHE_LOCK:
        cache_store[key] = (now_ts() + ttl_sec, payload)
        if len(cache_store) > CACHE_MAX_ITEMS:
            for stale_key in list(cache_store.keys())[: max(1, len(cache_store) - CACHE_MAX_ITEMS)]:
                cache_store.pop(stale_key, None)


def _safe_filename(name: str, default="file.bin") -> str:
    raw = (name or "").strip()
    if not raw:
        return default
    blocked = '<>:"/\\|?*'
    cleaned = "".join("_" if ch in blocked else ch for ch in raw)
    cleaned = cleaned.strip(". ").strip()
    return cleaned or default


def _downloads_dir() -> Path:
    try:
        DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
        return DOWNLOADS_DIR
    except Exception:  # noqa: BLE001
        fallback = APP_DIR / "downloads"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


def storage_mode() -> str:
    return "dpapi" if os.name == "nt" else "plain"


class DATA_BLOB(ctypes.Structure):
    _fields_ = [("cbData", ctypes.c_uint32), ("pbData", ctypes.POINTER(ctypes.c_ubyte))]


def _to_blob(raw: bytes):
    if not raw:
        return DATA_BLOB(0, None), None
    buf = ctypes.create_string_buffer(raw, len(raw))
    blob = DATA_BLOB(len(raw), ctypes.cast(buf, ctypes.POINTER(ctypes.c_ubyte)))
    return blob, buf


def _blob_to_bytes(blob: DATA_BLOB) -> bytes:
    if not blob.cbData or not blob.pbData:
        return b""
    return ctypes.string_at(blob.pbData, blob.cbData)


def _dpapi_protect(raw: bytes) -> bytes:
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    in_blob, _ = _to_blob(raw)
    scope_blob, _ = _to_blob(DPAPI_SCOPE)
    out_blob = DATA_BLOB()

    ok = crypt32.CryptProtectData(
        ctypes.byref(in_blob),
        None,
        ctypes.byref(scope_blob),
        None,
        None,
        0,
        ctypes.byref(out_blob),
    )
    if not ok:
        raise OSError("CryptProtectData failed")

    try:
        return _blob_to_bytes(out_blob)
    finally:
        kernel32.LocalFree(out_blob.pbData)


def _dpapi_unprotect(raw: bytes) -> bytes:
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    in_blob, _ = _to_blob(raw)
    scope_blob, _ = _to_blob(DPAPI_SCOPE)
    out_blob = DATA_BLOB()

    ok = crypt32.CryptUnprotectData(
        ctypes.byref(in_blob),
        None,
        ctypes.byref(scope_blob),
        None,
        None,
        0,
        ctypes.byref(out_blob),
    )
    if not ok:
        raise OSError("CryptUnprotectData failed")

    try:
        return _blob_to_bytes(out_blob)
    finally:
        kernel32.LocalFree(out_blob.pbData)


def encrypt_bytes(raw: bytes) -> dict:
    if os.name == "nt":
        protected = _dpapi_protect(raw)
        return {"enc": "dpapi", "data": base64.b64encode(protected).decode("ascii")}
    return {"enc": "plain", "data": base64.b64encode(raw).decode("ascii")}


def decrypt_bytes(payload: dict) -> bytes:
    enc = (payload or {}).get("enc", "")
    data_text = (payload or {}).get("data", "")
    raw = base64.b64decode(data_text.encode("ascii")) if data_text else b""

    if enc == "dpapi":
        if os.name != "nt":
            raise OSError("DPAPI payload on non-Windows system")
        return _dpapi_unprotect(raw)
    return raw


def default_secure_state():
    return {
        "credentials": {
            source_id: {"user_id": "", "api_key": ""} for source_id in SOURCE_CONFIG
        },
        "network": {"proxy_url": ""},
    }


def normalize_secure_state(state: dict):
    state = state or {}
    normalized = default_secure_state()

    cred_block = state.get("credentials", {})
    if isinstance(cred_block, dict):
        for source_id in SOURCE_CONFIG:
            source_state = cred_block.get(source_id, {})
            if isinstance(source_state, dict):
                normalized["credentials"][source_id]["user_id"] = str(source_state.get("user_id", "")).strip()
                normalized["credentials"][source_id]["api_key"] = str(source_state.get("api_key", "")).strip()

    net_block = state.get("network", {})
    if isinstance(net_block, dict):
        normalized["network"]["proxy_url"] = str(net_block.get("proxy_url", "")).strip()

    return normalized


def load_secure_state():
    if not SECURE_FILE.exists():
        return default_secure_state()
    try:
        raw = SECURE_FILE.read_text(encoding="utf-8")
        container = json.loads(raw)
        decrypted = decrypt_bytes(container)
        payload = json.loads(decrypted.decode("utf-8"))
        return normalize_secure_state(payload)
    except Exception:  # noqa: BLE001
        return default_secure_state()


def save_secure_state(state: dict):
    normalized = normalize_secure_state(state)
    APP_DIR.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(normalized, ensure_ascii=False).encode("utf-8")
    encrypted = encrypt_bytes(raw)
    SECURE_FILE.write_text(json.dumps(encrypted), encoding="utf-8")


def clear_secure_state():
    try:
        SECURE_FILE.unlink(missing_ok=True)
    except Exception:  # noqa: BLE001
        pass


def normalize_url(url_value: str) -> str:
    if not url_value:
        return ""
    if url_value.startswith("//"):
        return f"https:{url_value}"
    return url_value


def media_type_from_url(url_value: str) -> str:
    if not url_value:
        return "image"
    lower = url_value.lower()
    for ext in VIDEO_EXTENSIONS:
        if lower.endswith(ext):
            return "video"
    return "image"


def xml_node_to_dict(node: ET.Element) -> dict:
    data = dict(node.attrib or {})
    for child in list(node):
        key = str(child.tag or "").strip()
        if not key:
            continue
        value = (child.text or "").strip()
        if value:
            data[key] = value
    return data


def normalize_post(source_id: str, attrs: dict) -> dict:
    post_id = str(attrs.get("id", "")).strip()
    file_url = normalize_url(
        attrs.get("file_url")
        or attrs.get("image")
        or attrs.get("sample_url")
        or attrs.get("jpeg_url")
        or ""
    )
    if source_id == "gelbooru" and file_url and not file_url.startswith(("http://", "https://")):
        directory = str(attrs.get("directory", "")).strip().strip("/")
        if directory:
            file_url = f"https://img2.gelbooru.com/images/{directory}/{file_url}"
    preview_url = normalize_url(attrs.get("preview_url") or attrs.get("sample_url") or file_url)
    sample_url = normalize_url(attrs.get("sample_url") or file_url)
    post_url = SOURCE_CONFIG[source_id]["post_url"].format(id=post_id) if post_id else ""

    if not post_id or not file_url:
        return {}

    return {
        "id": post_id,
        "source_id": source_id,
        "source_name": SOURCE_CONFIG[source_id]["name"],
        "post_url": post_url,
        "file_url": file_url,
        "sample_url": sample_url,
        "preview_url": preview_url,
        "width": to_int(attrs.get("width")),
        "height": to_int(attrs.get("height")),
        "score": to_int(attrs.get("score")),
        "rating": str(attrs.get("rating", "")).strip(),
        "tags": str(attrs.get("tags", "")).strip(),
        "change": to_int(attrs.get("change")),
        "created_at": str(attrs.get("created_at", "")).strip(),
        "media_type": media_type_from_url(file_url),
    }


def parse_source_list(raw_sources: str):
    requested = [s.strip().lower() for s in (raw_sources or "").split(",") if s.strip()]
    if not requested:
        return list(SOURCE_CONFIG.keys())
    return [src for src in requested if src in SOURCE_CONFIG]


def parse_credentials(query: dict, secure_state: dict, headers):
    credentials = {}
    secure_creds = (secure_state or {}).get("credentials", {})

    for source_id, cfg in SOURCE_CONFIG.items():
        prefix = cfg["cred_prefix"]
        query_user_id = (query.get(f"{prefix}_user_id", [""])[0] or "").strip()
        query_api_key = (query.get(f"{prefix}_api_key", [""])[0] or "").strip()
        header_user_id = (headers.get(f"X-{prefix}-User-Id", "") or "").strip()
        header_api_key = (headers.get(f"X-{prefix}-Api-Key", "") or "").strip()

        saved = secure_creds.get(source_id, {}) if isinstance(secure_creds, dict) else {}
        saved_user_id = str(saved.get("user_id", "")).strip()
        saved_api_key = str(saved.get("api_key", "")).strip()

        env_user_id = (os.getenv(f"{prefix.upper()}_USER_ID", "") or "").strip()
        env_api_key = (os.getenv(f"{prefix.upper()}_API_KEY", "") or "").strip()

        credentials[source_id] = {
            "user_id": query_user_id or header_user_id or saved_user_id or env_user_id,
            "api_key": query_api_key or header_api_key or saved_api_key or env_api_key,
        }
    return credentials


def parse_network_config(query: dict, secure_state: dict, headers):
    query_proxy = (query.get("proxy_url", [""])[0] or "").strip()
    header_proxy = (headers.get("X-Booru-Proxy-Url", "") or "").strip()
    saved_proxy = str((secure_state or {}).get("network", {}).get("proxy_url", "")).strip()
    env_proxy = (os.getenv("BOORU_PROXY", "") or "").strip()

    proxy_url = query_proxy or header_proxy or saved_proxy or env_proxy
    if proxy_url and not proxy_url.startswith(("http://", "https://")):
        proxy_url = ""
    return {"proxy_url": proxy_url}


def strict_https(url: str):
    parsed = urlparse(url)
    if parsed.scheme.lower() != "https":
        raise RuntimeError("Only HTTPS endpoints are allowed")


def allowed_media_host(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    if not host:
        return False
    return host.endswith("rule34.xxx") or host.endswith("gelbooru.com")


def open_http(request: Request, network_config: dict, timeout: int):
    proxy_url = str((network_config or {}).get("proxy_url", "")).strip()
    if proxy_url:
        opener = build_opener(ProxyHandler({"http": proxy_url, "https": proxy_url}))
        return opener.open(request, timeout=timeout)
    return urlopen(request, timeout=timeout)


def download_media_file(url: str, network_config: dict, timeout=60):
    normalized_url = (url or "").strip()
    strict_https(normalized_url)
    if not allowed_media_host(normalized_url):
        raise RuntimeError("Host is not allowed for download")

    parsed = urlparse(normalized_url)
    file_name = _safe_filename(Path(parsed.path).name, "media.bin")
    target_dir = _downloads_dir()
    target_path = target_dir / file_name

    stem = target_path.stem
    suffix = target_path.suffix
    counter = 1
    while target_path.exists():
        target_path = target_dir / f"{stem}_{counter}{suffix}"
        counter += 1

    request = Request(
        normalized_url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BooruFinder/1.0",
            "Accept": "*/*",
            "DNT": "1",
            "Cache-Control": "no-store",
        },
    )
    size = 0
    with open_http(request, network_config, timeout=timeout) as response:
        with target_path.open("wb") as out_file:
            while True:
                chunk = response.read(1024 * 256)
                if not chunk:
                    break
                out_file.write(chunk)
                size += len(chunk)

    return {"path": str(target_path), "size": size, "filename": target_path.name}


def media_referer(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    if host.endswith("rule34.xxx"):
        return "https://rule34.xxx/"
    if host.endswith("gelbooru.com"):
        return "https://gelbooru.com/"
    return ""


def fetch_source_posts(
    source_id: str,
    tags: str,
    page: int,
    limit: int,
    sort_mode: str,
    credentials=None,
    network_config=None,
    timeout=25,
):
    credentials = credentials or {}
    network_config = network_config or {}
    cred_fingerprint = _cache_hash([credentials.get("user_id", ""), credentials.get("api_key", "")])
    net_fingerprint = _cache_hash([network_config.get("proxy_url", "")])
    cache_key = _cache_hash([source_id, tags.strip(), page, limit, sort_mode, cred_fingerprint, net_fingerprint])
    cached = _cache_get(_POST_CACHE, cache_key)
    if cached is not None:
        return [dict(item) for item in cached]

    tag_query = tags.strip()
    if sort_mode == "popular":
        if "sort:score" not in tag_query:
            tag_query = (tag_query + " sort:score").strip()

    params = {
        "page": "dapi",
        "s": "post",
        "q": "index",
        "tags": tag_query,
        "pid": str(page),
        "limit": str(limit),
    }
    if credentials.get("user_id") and credentials.get("api_key"):
        params["user_id"] = credentials["user_id"]
        params["api_key"] = credentials["api_key"]

    url = f"{SOURCE_CONFIG[source_id]['base_url']}?{urlencode(params)}"
    strict_https(url)

    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BooruFinder/1.0",
            "Accept": "*/*",
            "DNT": "1",
            "Cache-Control": "no-store",
        },
    )
    with open_http(request, network_config, timeout=timeout) as response:
        payload = response.read()

    root = ET.fromstring(payload)
    if root.tag.lower() == "error":
        raise RuntimeError((root.text or "API error").strip())

    posts = []
    for post_el in root.findall("post"):
        normalized = normalize_post(source_id, xml_node_to_dict(post_el))
        if normalized:
            posts.append(normalized)
    _cache_set(_POST_CACHE, cache_key, posts, POST_CACHE_TTL_SEC)
    return posts


def fetch_source_tags(
    source_id: str,
    term: str,
    limit: int,
    credentials=None,
    network_config=None,
    timeout=25,
):
    credentials = credentials or {}
    network_config = network_config or {}
    pattern = term.strip()
    if not pattern:
        return []

    cred_fingerprint = _cache_hash([credentials.get("user_id", ""), credentials.get("api_key", "")])
    net_fingerprint = _cache_hash([network_config.get("proxy_url", "")])
    cache_key = _cache_hash([source_id, pattern.lower(), limit, cred_fingerprint, net_fingerprint])
    cached = _cache_get(_TAG_CACHE, cache_key)
    if cached is not None:
        return [dict(item) for item in cached]

    lowered = pattern.lower()
    merged = {}

    # Exact tag lookup to surface the canonical tag (e.g. "cum")
    exact_params = {
        "page": "dapi",
        "s": "tag",
        "q": "index",
        "name": pattern,
        "limit": "1",
    }
    if credentials.get("user_id") and credentials.get("api_key"):
        exact_params["user_id"] = credentials["user_id"]
        exact_params["api_key"] = credentials["api_key"]

    exact_url = f"{SOURCE_CONFIG[source_id]['base_url']}?{urlencode(exact_params)}"
    strict_https(exact_url)
    exact_request = Request(
        exact_url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BooruFinder/1.0",
            "Accept": "*/*",
            "DNT": "1",
            "Cache-Control": "no-store",
        },
    )
    try:
        with open_http(exact_request, network_config, timeout=timeout) as response:
            exact_payload = response.read()
        exact_root = ET.fromstring(exact_payload)
        if exact_root.tag.lower() != "error":
            for tag_el in exact_root.findall("tag"):
                tag_data = xml_node_to_dict(tag_el)
                name = str(tag_data.get("name", "")).strip()
                if name:
                    merged[name] = max(merged.get(name, 0), to_int(tag_data.get("count"), 0))
    except Exception:  # noqa: BLE001
        pass

    # Related tags from real posts in this source (better relevance than name_pattern)
    posts = fetch_source_posts(
        source_id,
        pattern,
        0,
        max(30, min(limit * 8, 80)),
        "popular",
        credentials,
        network_config,
        timeout,
    )
    for post in posts:
        for tag in str(post.get("tags", "")).split():
            tag_clean = tag.strip()
            if not tag_clean:
                continue
            if lowered not in tag_clean.lower():
                continue
            merged[tag_clean] = merged.get(tag_clean, 0) + 1

    ordered_names = sorted(merged.items(), key=lambda x: (-to_int(x[1]), x[0]))[:limit]
    result = [
        {
            "name": name,
            "count": to_int(count, 0),
            "source_id": source_id,
            "source_name": SOURCE_CONFIG[source_id]["name"],
        }
        for name, count in ordered_names
    ]
    _cache_set(_TAG_CACHE, cache_key, result, TAG_CACHE_TTL_SEC)
    return result


def sort_posts(posts, mode: str):
    if mode == "popular":
        return sorted(posts, key=lambda p: (p.get("score", 0), p.get("change", 0)), reverse=True)
    return sorted(posts, key=lambda p: (p.get("change", 0), to_int(p.get("id"))), reverse=True)


def dedupe_posts(posts):
    seen = set()
    unique = []
    for item in posts:
        key = item.get("file_url") or f"{item.get('source_id')}:{item.get('id')}"
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


class AppHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        # Disable browser caching so UI updates are visible immediately.
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def log_message(self, format_text, *args):
        return

    def send_json(self, status: int, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def read_json_body(self):
        length = max(0, to_int(self.headers.get("Content-Length"), 0))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:  # noqa: BLE001
            return {}

    def handle_api_sources(self):
        self.send_json(
            200,
            {
                "sources": [
                    {"id": source_id, "name": config["name"]}
                    for source_id, config in SOURCE_CONFIG.items()
                ]
            },
        )

    def handle_api_security(self):
        state = load_secure_state()
        self.send_json(
            200,
            {
                "storage": storage_mode(),
                "secure_store_exists": SECURE_FILE.exists(),
                "proxy_active": bool(state.get("network", {}).get("proxy_url")),
            },
        )

    def handle_api_secure_config_get(self):
        self.send_json(200, load_secure_state())

    def handle_api_secure_config_save(self):
        payload = self.read_json_body()
        normalized = normalize_secure_state(payload)
        save_secure_state(normalized)
        self.send_json(200, {"ok": True, "storage": storage_mode()})

    def handle_api_secure_config_clear(self):
        clear_secure_state()
        self.send_json(200, {"ok": True})

    def handle_api_download(self):
        payload = self.read_json_body()
        media_url = str(payload.get("url", "")).strip()
        if not media_url:
            self.send_json(400, {"error": "Missing media URL"})
            return

        secure_state = load_secure_state()
        network_config = parse_network_config({}, secure_state, self.headers)
        try:
            info = download_media_file(media_url, network_config, timeout=120)
            self.send_json(
                200,
                {
                    "ok": True,
                    "downloaded_to": info["path"],
                    "filename": info["filename"],
                    "size": info["size"],
                },
            )
        except Exception as exc:  # noqa: BLE001
            self.send_json(400, {"error": str(exc)})

    def handle_api_downloads_open(self):
        path = _downloads_dir()
        try:
            if os.name == "nt":
                os.startfile(str(path))  # type: ignore[attr-defined]
            self.send_json(200, {"ok": True, "path": str(path)})
        except Exception as exc:  # noqa: BLE001
            self.send_json(400, {"error": str(exc), "path": str(path)})

    def handle_api_media(self, query):
        media_url = (query.get("url", [""])[0] or "").strip()
        if not media_url:
            self.send_json(400, {"error": "Missing media URL"})
            return

        try:
            strict_https(media_url)
            if not allowed_media_host(media_url):
                raise RuntimeError("Host is not allowed for media proxy")
        except Exception as exc:  # noqa: BLE001
            self.send_json(400, {"error": str(exc)})
            return

        secure_state = load_secure_state()
        network_config = parse_network_config(query, secure_state, self.headers)

        upstream_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BooruFinder/1.0",
            "Accept": "*/*",
            "DNT": "1",
            "Cache-Control": "no-store",
        }
        referer = media_referer(media_url)
        if referer:
            upstream_headers["Referer"] = referer
            upstream_headers["Origin"] = referer.rstrip("/")
        range_header = (self.headers.get("Range", "") or "").strip()
        if range_header:
            upstream_headers["Range"] = range_header

        request = Request(media_url, headers=upstream_headers)
        try:
            with open_http(request, network_config, timeout=90) as response:
                status = to_int(getattr(response, "status", 0) or response.getcode(), 200)
                self.send_response(status)
                for header_name in (
                    "Content-Type",
                    "Content-Length",
                    "Content-Range",
                    "Accept-Ranges",
                    "Last-Modified",
                    "ETag",
                ):
                    value = response.headers.get(header_name)
                    if value:
                        self.send_header(header_name, value)
                self.end_headers()

                while True:
                    chunk = response.read(1024 * 256)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
        except HTTPError as exc:
            body = b""
            try:
                body = exc.read()
            except Exception:  # noqa: BLE001
                body = b""
            self.send_response(exc.code or 502)
            content_type = ""
            if getattr(exc, "headers", None):
                content_type = exc.headers.get("Content-Type", "")
            self.send_header("Content-Type", content_type or "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)
        except Exception as exc:  # noqa: BLE001
            body = str(exc).encode("utf-8")
            self.send_response(502)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def handle_api_search(self, query):
        if query.get("adult", ["0"])[0] != "1":
            self.send_json(
                403,
                {"error": "Age gate is required. Set 'adult=1' to continue."},
            )
            return

        secure_state = load_secure_state()
        network_config = parse_network_config(query, secure_state, self.headers)
        credentials = parse_credentials(query, secure_state, self.headers)

        tags = (query.get("tags", [""])[0] or "").strip()
        page = max(0, to_int(query.get("page", ["0"])[0], 0))
        limit = max(1, min(100, to_int(query.get("limit", ["30"])[0], 30)))
        min_score = max(0, to_int(query.get("min_score", ["0"])[0], 0))
        sort_mode = (query.get("sort", ["new"])[0] or "new").strip().lower()
        sources = parse_source_list(query.get("sources", [""])[0])

        if not sources:
            self.send_json(400, {"error": "No valid sources selected."})
            return

        results = []
        errors = []
        has_more = False

        with ThreadPoolExecutor(max_workers=len(sources)) as executor:
            futures = {
                executor.submit(
                    fetch_source_posts,
                    source_id,
                    tags,
                    page,
                    limit,
                    sort_mode,
                    credentials.get(source_id, {}),
                    network_config,
                ): source_id
                for source_id in sources
            }
            for future in as_completed(futures):
                source_id = futures[future]
                try:
                    source_items = future.result()
                    results.extend(source_items)
                    if len(source_items) >= limit:
                        has_more = True
                except Exception as exc:  # noqa: BLE001
                    errors.append({"source": source_id, "message": str(exc)})

        deduped = dedupe_posts(results)
        filtered = [item for item in deduped if item.get("score", 0) >= min_score]
        ordered = sort_posts(filtered, sort_mode)
        self.send_json(
            200,
            {
                "page": page,
                "limit": limit,
                "min_score": min_score,
                "sort": sort_mode,
                "sources": sources,
                "raw_count": len(deduped),
                "count": len(ordered),
                "has_more": has_more,
                "items": ordered,
                "errors": errors,
            },
        )

    def handle_api_tags(self, query):
        if query.get("adult", ["0"])[0] != "1":
            self.send_json(403, {"error": "Age gate is required. Set 'adult=1' to continue."})
            return

        term = (query.get("term", [""])[0] or "").strip()
        if len(term) < 2:
            self.send_json(200, {"term": term, "suggestions": [], "errors": []})
            return

        secure_state = load_secure_state()
        network_config = parse_network_config(query, secure_state, self.headers)
        credentials = parse_credentials(query, secure_state, self.headers)

        limit = max(1, min(30, to_int(query.get("limit", ["12"])[0], 12)))
        sources = parse_source_list(query.get("sources", [""])[0])

        if not sources:
            self.send_json(400, {"error": "No valid sources selected."})
            return

        suggestions = []
        errors = []
        with ThreadPoolExecutor(max_workers=len(sources)) as executor:
            futures = {
                executor.submit(
                    fetch_source_tags,
                    source_id,
                    term,
                    limit,
                    credentials.get(source_id, {}),
                    network_config,
                ): source_id
                for source_id in sources
            }
            for future in as_completed(futures):
                source_id = futures[future]
                try:
                    suggestions.extend(future.result())
                except Exception as exc:  # noqa: BLE001
                    errors.append({"source": source_id, "message": str(exc)})

        ordered = sorted(
            suggestions,
            key=lambda t: (t.get("source_name", ""), -to_int(t.get("count")), t.get("name", "")),
        )
        self.send_json(
            200,
            {
                "term": term,
                "sources": sources,
                "suggestions": ordered,
                "errors": errors,
            },
        )

    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)

        if parsed.path == "/api/sources":
            self.handle_api_sources()
            return
        if parsed.path == "/api/security":
            self.handle_api_security()
            return
        if parsed.path == "/api/secure-config":
            self.handle_api_secure_config_get()
            return
        if parsed.path == "/api/search":
            self.handle_api_search(query)
            return
        if parsed.path == "/api/tags":
            self.handle_api_tags(query)
            return
        if parsed.path == "/api/media":
            self.handle_api_media(query)
            return

        if parsed.path in {"", "/"}:
            self.path = "/index.html"
        super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/secure-config":
            self.handle_api_secure_config_save()
            return
        if parsed.path == "/api/secure-config/clear":
            self.handle_api_secure_config_clear()
            return
        if parsed.path == "/api/download":
            self.handle_api_download()
            return
        if parsed.path == "/api/downloads/open":
            self.handle_api_downloads_open()
            return
        self.send_json(404, {"error": "Not found"})


def main():
    port = to_int(os.getenv("BOORU_PORT", "8765"), 8765)
    server = create_server(port)

    print(f"Booru Finder started: http://127.0.0.1:{port}")
    if os.getenv("BOORU_NO_BROWSER", "0") != "1":
        webbrowser.open(f"http://127.0.0.1:{port}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def create_server(port: int):
    handler = lambda *args, **kwargs: AppHandler(*args, directory=static_dir(), **kwargs)
    return ThreadingHTTPServer(("127.0.0.1", port), handler)


if __name__ == "__main__":
    main()
