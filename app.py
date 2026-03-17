import base64
import ctypes
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock, Thread
from urllib.parse import parse_qs, urlencode, urljoin, urlparse
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener, urlopen
import xml.etree.ElementTree as ET


SOURCE_CONFIG = {
    "rule34": {
        "name": "Rule34",
        "base_url": "https://api.rule34.xxx/index.php",
        "post_url": "https://rule34.xxx/index.php?page=post&s=view&id={id}",
        "cred_prefix": "rule34",
        "autocomplete": "app",
    },
    "gelbooru": {
        "name": "Gelbooru",
        "base_url": "https://gelbooru.com/index.php",
        "post_url": "https://gelbooru.com/index.php?page=post&s=view&id={id}",
        "cred_prefix": "gelbooru",
        "autocomplete": "native",
    },
}

VIDEO_EXTENSIONS = {".mp4", ".webm", ".mov", ".m4v", ".avi", ".mkv"}
HOME_WINDOWS = {"7d": 7, "30d": 30, "90d": 90, "180d": 180}

FEATURED_COLLECTIONS = {
    "rule34": [
        {"id": "top_posts", "title": "Top posts", "subtitle": "Most liked this source", "query": "", "sort": "popular"},
        {"id": "animated", "title": "Animated", "subtitle": "Loops and video", "query": "animated", "sort": "popular"},
        {"id": "genshin", "title": "Genshin Impact", "subtitle": "Fast jump-in", "query": "genshin_impact", "sort": "popular"},
        {"id": "resident_evil", "title": "Resident Evil", "subtitle": "Franchise collection", "query": "resident_evil", "sort": "popular"},
        {
            "id": "fnaf_help_wanted",
            "title": "Help Wanted",
            "subtitle": "Five Nights at Freddy's",
            "query": "five_nights_at_freddy's:_help_wanted",
            "sort": "popular",
        },
    ],
    "gelbooru": [
        {"id": "top_posts", "title": "Top posts", "subtitle": "Most liked this source", "query": "", "sort": "popular"},
        {"id": "animated", "title": "Animated", "subtitle": "GIF and video ready", "query": "animated", "sort": "popular"},
        {"id": "pokemon", "title": "Pokemon", "subtitle": "Popular series", "query": "pokemon", "sort": "popular"},
        {"id": "overwatch", "title": "Overwatch", "subtitle": "Trending characters", "query": "overwatch", "sort": "popular"},
        {"id": "resident_evil", "title": "Resident Evil", "subtitle": "Franchise collection", "query": "resident_evil", "sort": "popular"},
    ],
}

FALLBACK_NEWS = [
    {
        "id": "bf-2026-home-remake",
        "source_id": "app",
        "source_name": "Booru Finder",
        "title": "Home/search remake",
        "summary": "New home-first layout, source-aware autocomplete, featured collections, and cached trending tags.",
        "url": "",
        "published_at": "2026-03-16",
        "kind": "release",
    },
    {
        "id": "bf-2026-android-refresh",
        "source_id": "app",
        "source_name": "Booru Finder",
        "title": "Android mobile shell",
        "summary": "Bottom navigation, larger touch targets, lighter effects, and faster media browsing on phones.",
        "url": "",
        "published_at": "2026-03-16",
        "kind": "release",
    },
    {
        "id": "bf-2026-cache",
        "source_id": "app",
        "source_name": "Booru Finder",
        "title": "Cached trends and recent searches",
        "summary": "Trending windows now reuse SQLite snapshots, and recent searches are stored locally for fast home screen reuse.",
        "url": "",
        "published_at": "2026-03-16",
        "kind": "release",
    },
]

TREND_STOP_TAGS = {
    "1girl",
    "1boy",
    "2girls",
    "2boys",
    "3girls",
    "3boys",
    "solo",
    "animated",
    "video",
    "sound",
    "audio",
    "tagme",
    "hi_res",
    "highres",
    "absurdres",
    "english_text",
    "text",
    "commentary",
    "webm",
    "mp4",
    "female",
    "male",
    "human",
    "rating:e",
    "rating:q",
    "rating:s",
    "rating:g",
    "order:score",
    "sort:score",
}

APP_DIR = Path(os.getenv("APPDATA", str(Path.home()))) / "BooruFinder"
SECURE_FILE = APP_DIR / "secure_store.json"
CACHE_DB = APP_DIR / "booru_cache.sqlite3"
DPAPI_SCOPE = b"BooruFinder.LocalSecureStore"
DOWNLOADS_DIR = Path.home() / "Downloads" / "BooruFinder"

CACHE_MAX_ITEMS = 300
POST_CACHE_TTL_SEC = 120
TAG_CACHE_TTL_SEC = 90
TAG_COUNT_CACHE_TTL_SEC = 600
SQLITE_AUTOCOMPLETE_TTL_SEC = 6 * 60 * 60
SQLITE_FEATURED_TTL_SEC = 45 * 60
SQLITE_TREND_TTL_SEC = 30 * 60
SQLITE_NEWS_TTL_SEC = 60 * 60
BACKGROUND_REFRESH_INTERVAL_SEC = 45 * 60
AUTOCOMPLETE_CACHE_VERSION = "v8"

_POST_CACHE = {}
_TAG_CACHE = {}
_TAG_COUNT_CACHE = {}
_CACHE_LOCK = Lock()
_DB_LOCK = Lock()
_BACKGROUND_LOCK = Lock()
_BACKGROUND_STARTED = False


def static_dir() -> str:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, "static")
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


def to_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def to_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def now_ts() -> float:
    return time.time()


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


def db_connect():
    APP_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(CACHE_DB, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_cache_db():
    with _DB_LOCK:
        with db_connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS kv_cache (
                    kind TEXT NOT NULL,
                    cache_key TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    updated_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    PRIMARY KEY (kind, cache_key)
                );
                CREATE TABLE IF NOT EXISTS recent_queries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    display_query TEXT NOT NULL,
                    raw_query TEXT NOT NULL,
                    include_tags TEXT NOT NULL,
                    exclude_tags TEXT NOT NULL,
                    sources TEXT NOT NULL,
                    sort_mode TEXT NOT NULL,
                    rating TEXT NOT NULL,
                    min_score INTEGER NOT NULL,
                    created_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS trend_post_samples (
                    source_id TEXT NOT NULL,
                    window_key TEXT NOT NULL,
                    tag_name TEXT NOT NULL,
                    score REAL NOT NULL,
                    post_json TEXT NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (source_id, window_key, tag_name)
                );
                CREATE INDEX IF NOT EXISTS idx_recent_queries_created
                    ON recent_queries (created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_kv_cache_expires
                    ON kv_cache (kind, expires_at);
                """
            )


def db_cache_get(kind: str, cache_key: str):
    init_cache_db()
    with db_connect() as conn:
        row = conn.execute(
            "SELECT payload, expires_at FROM kv_cache WHERE kind = ? AND cache_key = ?",
            (kind, cache_key),
        ).fetchone()
        if not row:
            return None
        if to_int(row["expires_at"], 0) < int(now_ts()):
            conn.execute("DELETE FROM kv_cache WHERE kind = ? AND cache_key = ?", (kind, cache_key))
            conn.commit()
            return None
        try:
            return json.loads(row["payload"])
        except Exception:  # noqa: BLE001
            return None


def db_cache_set(kind: str, cache_key: str, payload, ttl_sec: int):
    init_cache_db()
    updated_at = int(now_ts())
    expires_at = updated_at + max(1, ttl_sec)
    raw = json.dumps(payload, ensure_ascii=False)
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO kv_cache(kind, cache_key, payload, updated_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(kind, cache_key)
            DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at, expires_at = excluded.expires_at
            """,
            (kind, cache_key, raw, updated_at, expires_at),
        )
        conn.execute("DELETE FROM kv_cache WHERE expires_at < ?", (updated_at - 24 * 60 * 60,))
        conn.commit()


def record_recent_query(display_query: str, raw_query: str, include_tags, exclude_tags, sources, sort_mode: str, rating: str, min_score: int):
    init_cache_db()
    display_value = str(display_query or "").strip()
    if not display_value:
        return
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO recent_queries(display_query, raw_query, include_tags, exclude_tags, sources, sort_mode, rating, min_score, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                display_value,
                str(raw_query or "").strip(),
                json.dumps(include_tags or [], ensure_ascii=False),
                json.dumps(exclude_tags or [], ensure_ascii=False),
                json.dumps(sources or [], ensure_ascii=False),
                str(sort_mode or "new"),
                str(rating or "any"),
                max(0, to_int(min_score, 0)),
                int(now_ts()),
            ),
        )
        conn.execute(
            """
            DELETE FROM recent_queries
            WHERE id NOT IN (
                SELECT id FROM recent_queries ORDER BY created_at DESC LIMIT 80
            )
            """
        )
        conn.commit()


def load_recent_queries(limit: int = 8):
    init_cache_db()
    with db_connect() as conn:
        rows = conn.execute(
            """
            SELECT id, display_query, raw_query, include_tags, exclude_tags, sources, sort_mode, rating, min_score, created_at
            FROM recent_queries
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (max(1, limit),),
        ).fetchall()

    items = []
    for row in rows:
        try:
            include_tags = json.loads(row["include_tags"] or "[]")
        except Exception:  # noqa: BLE001
            include_tags = []
        try:
            exclude_tags = json.loads(row["exclude_tags"] or "[]")
        except Exception:  # noqa: BLE001
            exclude_tags = []
        try:
            sources = json.loads(row["sources"] or "[]")
        except Exception:  # noqa: BLE001
            sources = []
        items.append(
            {
                "id": row["id"],
                "display_query": row["display_query"],
                "raw_query": row["raw_query"],
                "include_tags": include_tags,
                "exclude_tags": exclude_tags,
                "sources": sources,
                "sort_mode": row["sort_mode"],
                "rating": row["rating"],
                "min_score": to_int(row["min_score"], 0),
                "created_at": to_int(row["created_at"], 0),
            }
        )
    return items


def store_trend_samples(source_id: str, window_key: str, items):
    init_cache_db()
    timestamp = int(now_ts())
    with db_connect() as conn:
        conn.execute("DELETE FROM trend_post_samples WHERE source_id = ? AND window_key = ?", (source_id, window_key))
        for item in items or []:
            conn.execute(
                """
                INSERT OR REPLACE INTO trend_post_samples(source_id, window_key, tag_name, score, post_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    source_id,
                    window_key,
                    str(item.get("tag", "")),
                    to_float(item.get("score"), 0.0),
                    json.dumps(item.get("preview") or {}, ensure_ascii=False),
                    timestamp,
                ),
            )
        conn.commit()


def load_trend_samples(source_id: str, window_key: str, limit: int = 12):
    init_cache_db()
    with db_connect() as conn:
        rows = conn.execute(
            """
            SELECT tag_name, score, post_json
            FROM trend_post_samples
            WHERE source_id = ? AND window_key = ?
            ORDER BY score DESC
            LIMIT ?
            """,
            (source_id, window_key, max(1, limit)),
        ).fetchall()

    items = []
    for row in rows:
        try:
            preview = json.loads(row["post_json"] or "{}")
        except Exception:  # noqa: BLE001
            preview = {}
        items.append({"tag": row["tag_name"], "score": to_float(row["score"], 0.0), "preview": preview})
    return items


def normalize_url(url_value: str) -> str:
    if not url_value:
        return ""
    if url_value.startswith("//"):
        return f"https:{url_value}"
    return url_value


def absolutize_source_url(source_id: str, url_value: str, attrs: dict) -> str:
    normalized = normalize_url(url_value)
    if not normalized:
        return ""
    if normalized.startswith(("http://", "https://")):
        return normalized

    base_site_url = f"https://{urlparse(SOURCE_CONFIG[source_id]['post_url']).hostname}/"
    if normalized.startswith("/"):
        return urljoin(base_site_url, normalized)

    if source_id == "gelbooru":
        directory = str(attrs.get("directory", "")).strip().strip("/")
        if directory and "/" not in normalized:
            return f"https://img2.gelbooru.com/images/{directory}/{normalized}"

    return urljoin(base_site_url, normalized)


def normalize_tag_query(raw_tags: str) -> str:
    raw = str(raw_tags or "").replace("\r", " ").replace("\n", " ").replace("\t", " ")
    compact = " ".join(raw.split())
    if "," not in compact:
        return compact

    normalized_tags = []
    for chunk in compact.split(","):
        tag = chunk.strip()
        if not tag:
            continue
        negative = tag.startswith("-")
        if negative:
            tag = tag[1:].strip()
        normalized = "_".join(part for part in tag.split() if part)
        if not normalized:
            continue
        normalized_tags.append(f"-{normalized}" if negative else normalized)
    return " ".join(normalized_tags)


def normalize_tag_hint_term(term: str) -> str:
    raw = str(term or "").replace("\r", " ").replace("\n", " ").replace("\t", " ")
    compact = " ".join(raw.split()).strip()
    if not compact:
        return ""
    return compact.replace("_", " ")


def comparable_tag_text(value: str) -> str:
    normalized = normalize_tag_hint_term(str(value or "")).lower().replace("'", "")
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return " ".join(normalized.split())


def comparable_tag_tokens(value: str):
    return [token for token in comparable_tag_text(value).split() if token]


def normalize_search_text(value: str) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").replace("\t", " ").split()).strip()


def is_valid_tag_name(value: str) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    return not any(marker in raw.lower() for marker in ("\r", "\n", "\\r", "\\n"))


def tag_token_matches(query_token: str, candidate_token: str) -> bool:
    if not query_token or not candidate_token:
        return False
    if candidate_token == query_token:
        return True
    if candidate_token.rstrip("s") == query_token.rstrip("s"):
        return True
    if candidate_token.startswith(query_token):
        return True
    if query_token.startswith(candidate_token):
        min_candidate = max(2, len(query_token) - 1)
        return len(candidate_token) >= min_candidate
    return False


def contiguous_token_match(term_tokens, candidate_tokens):
    if not term_tokens or len(term_tokens) > len(candidate_tokens):
        return None
    for start in range(0, len(candidate_tokens) - len(term_tokens) + 1):
        if all(tag_token_matches(term_tokens[idx], candidate_tokens[start + idx]) for idx in range(len(term_tokens))):
            return start
    return None


def subsequence_token_match(term_tokens, candidate_tokens):
    if not term_tokens:
        return None
    indices = []
    start_idx = None
    cursor = 0
    for term_token in term_tokens:
        found = False
        while cursor < len(candidate_tokens):
            if tag_token_matches(term_token, candidate_tokens[cursor]):
                if start_idx is None:
                    start_idx = cursor
                indices.append(cursor)
                cursor += 1
                found = True
                break
            cursor += 1
        if not found:
            return None
    return start_idx, indices


def tag_match_rank(name: str, term: str):
    lowered_name = comparable_tag_text(name)
    lowered_term = comparable_tag_text(term)
    if not lowered_name or not lowered_term:
        return None
    if lowered_name == lowered_term:
        return 0
    if lowered_name.startswith(lowered_term):
        return 1

    term_tokens = comparable_tag_tokens(term)
    candidate_tokens = comparable_tag_tokens(name)

    contiguous_at = contiguous_token_match(term_tokens, candidate_tokens)
    if contiguous_at is not None:
        return 2 if contiguous_at == 0 else 3

    subsequence = subsequence_token_match(term_tokens, candidate_tokens)
    if subsequence is not None:
        start_idx, indices = subsequence
        span = indices[-1] - indices[0] + 1 if indices else len(term_tokens)
        gaps = max(0, span - len(term_tokens))
        if gaps <= 1:
            return 2 if start_idx == 0 else 3
        return 4 if start_idx == 0 else 5

    if lowered_term in lowered_name:
        return 6
    return None


def tag_hint_rank(name: str, term: str):
    return tag_match_rank(name, term) if tag_match_rank(name, term) is not None else 9


def autocomplete_effective_rank(name: str, term: str, count=0, match_rank=None):
    rank = to_int(match_rank, tag_hint_rank(name, term))
    total = to_int(count, 0)
    if rank == 0 and total <= 5:
        return 4
    if rank == 1 and total <= 5:
        return 3
    return rank


def derive_tag_aliases(name: str, term: str):
    raw_name = str(name or "").strip()
    if not is_valid_tag_name(raw_name):
        return []

    aliases = set()
    term_tokens = comparable_tag_tokens(term)
    candidate_tokens = comparable_tag_tokens(raw_name)

    if term_tokens and candidate_tokens:
        contiguous_at = contiguous_token_match(term_tokens, candidate_tokens)
        if contiguous_at == 0 and len(candidate_tokens) > len(term_tokens):
            for extra_tokens in (1, 2):
                stem_count = min(len(candidate_tokens), len(term_tokens) + extra_tokens)
                stem = "_".join(candidate_tokens[:stem_count]).strip("_")
                if stem and stem != comparable_tag_text(raw_name).replace(" ", "_") and tag_match_rank(stem, term) is not None:
                    aliases.add(stem)

    current = raw_name
    while True:
        match = re.search(r"_\(([^()]+)\)$", current)
        if not match:
            break
        qualifier = normalize_tag_hint_term(match.group(1)).replace(" ", "_")
        if qualifier and tag_match_rank(qualifier, term) is not None:
            aliases.add(qualifier)
        current = current[: match.start()].rstrip("_")
        if current and tag_match_rank(current, term) is not None:
            aliases.add(current)

    return [alias for alias in aliases if is_valid_tag_name(alias)]


def tag_seed_term(term: str) -> str:
    normalized = normalize_tag_hint_term(term)
    if not normalized:
        return ""
    parts = [part for part in normalized.split() if part]
    if not parts:
        return ""
    return parts[0]


def resolve_source_search_tags(
    source_id: str,
    raw_tags: str,
    credentials=None,
    network_config=None,
    timeout=10,
) -> str:
    normalized = normalize_tag_query(raw_tags)
    compact = normalize_tag_hint_term(raw_tags)
    if not compact or "," in str(raw_tags or ""):
        return normalized
    if "_" in compact or ":" in compact or "~" in compact:
        return normalized
    if " " not in compact:
        return normalized

    negative = compact.startswith("-")
    phrase = compact[1:].strip() if negative else compact
    canonical = canonicalize_tag_for_source(
        source_id,
        phrase,
        credentials,
        network_config,
        timeout=min(timeout, 12),
    )
    if not canonical:
        return normalized
    return f"-{canonical}" if negative else canonical


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
    file_url = absolutize_source_url(
        source_id,
        attrs.get("file_url")
        or attrs.get("image")
        or attrs.get("sample_url")
        or attrs.get("jpeg_url")
        or "",
        attrs,
    )
    sample_url = absolutize_source_url(
        source_id,
        attrs.get("sample_url") or attrs.get("jpeg_url") or attrs.get("image") or file_url,
        attrs,
    )
    preview_url = absolutize_source_url(
        source_id,
        attrs.get("preview_url") or attrs.get("sample_url") or attrs.get("jpeg_url") or file_url,
        attrs,
    )
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
    if "both" in requested:
        return list(SOURCE_CONFIG.keys())
    return [src for src in requested if src in SOURCE_CONFIG]


def parse_tag_list(query: dict, name: str):
    result = []
    for raw_value in query.get(name, []):
        for chunk in str(raw_value or "").split(","):
            normalized = normalize_tag_hint_term(chunk)
            if normalized:
                result.append(normalized)
    return result


def header_get(headers, name: str) -> str:
    try:
        return str(headers.get(name, "") or "").strip()
    except Exception:  # noqa: BLE001
        return ""


def has_source_auth(credentials: dict) -> bool:
    return bool(str((credentials or {}).get("user_id", "")).strip() and str((credentials or {}).get("api_key", "")).strip())


def parse_credentials(query: dict, secure_state: dict, headers):
    credentials = {}
    secure_creds = (secure_state or {}).get("credentials", {})

    for source_id, cfg in SOURCE_CONFIG.items():
        prefix = cfg["cred_prefix"]
        query_user_id = (query.get(f"{prefix}_user_id", [""])[0] or "").strip()
        query_api_key = (query.get(f"{prefix}_api_key", [""])[0] or "").strip()
        header_user_id = header_get(headers, f"X-{prefix}-User-Id")
        header_api_key = header_get(headers, f"X-{prefix}-Api-Key")

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


def credentials_from_state(secure_state: dict):
    return parse_credentials({}, secure_state or default_secure_state(), {})


def parse_network_config(query: dict, secure_state: dict, headers):
    query_proxy = (query.get("proxy_url", [""])[0] or "").strip()
    header_proxy = header_get(headers, "X-Booru-Proxy-Url")
    saved_proxy = str((secure_state or {}).get("network", {}).get("proxy_url", "")).strip()
    env_proxy = (os.getenv("BOORU_PROXY", "") or "").strip()

    proxy_url = query_proxy or header_proxy or saved_proxy or env_proxy
    if proxy_url and not proxy_url.startswith(("http://", "https://")):
        proxy_url = ""
    return {"proxy_url": proxy_url}


def network_from_state(secure_state: dict):
    return parse_network_config({}, secure_state or default_secure_state(), {})


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


def read_json_response(url: str, network_config: dict, timeout: int):
    strict_https(url)
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BooruFinder/2.0",
            "Accept": "application/json, text/plain, */*",
            "DNT": "1",
            "Cache-Control": "no-store",
        },
    )
    with open_http(request, network_config, timeout=timeout) as response:
        raw = response.read()
    return json.loads(raw.decode("utf-8", "ignore"))


def read_text_response(url: str, network_config: dict, timeout: int):
    strict_https(url)
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BooruFinder/2.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "DNT": "1",
            "Cache-Control": "no-store",
        },
    )
    with open_http(request, network_config, timeout=timeout) as response:
        return response.read().decode("utf-8", "ignore")


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
    tags = normalize_tag_query(tags)
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


def fetch_exact_tag_count(
    source_id: str,
    tag_name: str,
    credentials=None,
    network_config=None,
    timeout=15,
):
    credentials = credentials or {}
    network_config = network_config or {}
    normalized_name = str(tag_name or "").strip()
    if not normalized_name:
        return None

    cred_fingerprint = _cache_hash([credentials.get("user_id", ""), credentials.get("api_key", "")])
    net_fingerprint = _cache_hash([network_config.get("proxy_url", "")])
    cache_key = _cache_hash([source_id, "exact_tag", normalized_name.lower(), cred_fingerprint, net_fingerprint])
    cached = _cache_get(_TAG_COUNT_CACHE, cache_key)
    if cached is not None:
        return None if cached < 0 else cached

    params = {
        "page": "dapi",
        "s": "tag",
        "q": "index",
        "name": normalized_name,
        "limit": "1",
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

    exact_count = None
    try:
        with open_http(request, network_config, timeout=timeout) as response:
            payload = response.read()
        root = ET.fromstring(payload)
        if root.tag.lower() != "error":
            for tag_el in root.findall("tag"):
                tag_data = xml_node_to_dict(tag_el)
                current_name = str(tag_data.get("name", "")).strip()
                if current_name.lower() != normalized_name.lower():
                    continue
                exact_count = to_int(tag_data.get("count"), 0)
                break
    except Exception:  # noqa: BLE001
        exact_count = None

    if exact_count is None:
        try:
            params = {
                "page": "dapi",
                "s": "post",
                "q": "index",
                "tags": normalized_name,
                "pid": "0",
                "limit": "1",
            }
            if credentials.get("user_id") and credentials.get("api_key"):
                params["user_id"] = credentials["user_id"]
                params["api_key"] = credentials["api_key"]
            post_url = f"{SOURCE_CONFIG[source_id]['base_url']}?{urlencode(params)}"
            strict_https(post_url)
            post_request = Request(
                post_url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BooruFinder/1.0",
                    "Accept": "*/*",
                    "DNT": "1",
                    "Cache-Control": "no-store",
                },
            )
            with open_http(post_request, network_config, timeout=timeout) as response:
                post_payload = response.read()
            post_root = ET.fromstring(post_payload)
            if post_root.tag.lower() == "posts":
                exact_count = to_int(post_root.attrib.get("count"), 0)
        except Exception:  # noqa: BLE001
            exact_count = None

    _cache_set(_TAG_COUNT_CACHE, cache_key, exact_count if exact_count is not None else -1, TAG_COUNT_CACHE_TTL_SEC)
    return exact_count


def fetch_gelbooru_native_autocomplete(term: str, limit: int, network_config: dict, timeout: int):
    normalized_term = normalize_tag_hint_term(term)
    if len(normalized_term) < 2:
        return []

    variants = []
    if normalized_term not in variants:
        variants.append(normalized_term)
    tokens = comparable_tag_tokens(normalized_term)
    if tokens and tokens[0] not in variants:
        variants.append(tokens[0])

    candidates = {}
    for variant in variants:
        url = (
            "https://gelbooru.com/index.php?"
            + urlencode(
                {
                    "page": "autocomplete2",
                    "term": variant,
                    "type": "tag_query",
                    "limit": str(max(10, limit * 3)),
                }
            )
        )
        try:
            payload = read_json_response(url, network_config, timeout)
        except Exception:  # noqa: BLE001
            continue
        if not isinstance(payload, list):
            continue
        for item in payload:
            if not isinstance(item, dict):
                continue
            tag_name = str(item.get("value") or item.get("label") or "").strip()
            if not is_valid_tag_name(tag_name):
                continue
            rank = tag_match_rank(tag_name, normalized_term)
            if rank is None:
                continue
            existing = candidates.setdefault(
                tag_name,
                {
                    "name": tag_name,
                    "count": 0,
                    "local_hits": 0,
                    "native_hits": 0,
                    "match_rank": rank,
                },
            )
            existing["match_rank"] = min(to_int(existing.get("match_rank"), rank), rank)
            existing["count"] = max(to_int(existing.get("count"), 0), to_int(item.get("post_count"), 0))
            existing["native_hits"] += 1
    return list(candidates.values())


def merge_tag_candidate(candidates: dict, name: str, term: str, count=0, local_hits=0, native_hits=0):
    if not is_valid_tag_name(name):
        return
    rank = tag_match_rank(name, term)
    if rank is None:
        return
    item = candidates.setdefault(
        name,
        {
            "name": name,
            "count": 0,
            "local_hits": 0,
            "native_hits": 0,
            "match_rank": rank,
        },
    )
    item["count"] = max(to_int(item.get("count"), 0), to_int(count, 0))
    item["local_hits"] = to_int(item.get("local_hits"), 0) + to_int(local_hits, 0)
    item["native_hits"] = to_int(item.get("native_hits"), 0) + to_int(native_hits, 0)
    item["match_rank"] = min(to_int(item.get("match_rank"), rank), rank)


def tag_seed_terms(term: str):
    tokens = comparable_tag_tokens(term)
    if not tokens:
        return []
    if len(tokens) == 1:
        return [tokens[0]]
    seeds = []
    first_two = "_".join(tokens[:2])
    if len(tokens) >= 3:
        first_three = "_".join(tokens[:3])
        if first_three:
            seeds.append(first_three)
    if first_two and first_two not in seeds:
        seeds.append(first_two)
    return seeds


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
    pattern = normalize_tag_hint_term(term)
    if not pattern:
        return []
    auth_enabled = has_source_auth(credentials)

    cred_fingerprint = _cache_hash([credentials.get("user_id", ""), credentials.get("api_key", "")])
    net_fingerprint = _cache_hash([network_config.get("proxy_url", "")])
    cache_key = _cache_hash([source_id, pattern.lower(), limit, cred_fingerprint, net_fingerprint])
    cached = _cache_get(_TAG_CACHE, cache_key)
    if cached is not None:
        return [dict(item) for item in cached]

    db_cache_key = _cache_hash([AUTOCOMPLETE_CACHE_VERSION, source_id, pattern.lower(), limit])
    cached_db = db_cache_get("autocomplete", db_cache_key)
    if cached_db is not None:
        _cache_set(_TAG_CACHE, cache_key, cached_db, TAG_CACHE_TTL_SEC)
        return [dict(item) for item in cached_db]
    if not auth_enabled and source_id == "rule34":
        return []

    candidates = {}
    post_seed_limit = 30 if " " not in pattern else 22
    term_tokens = comparable_tag_tokens(pattern)

    if source_id == "gelbooru":
        for native_item in fetch_gelbooru_native_autocomplete(pattern, limit, network_config, min(timeout, 8)):
            merge_tag_candidate(
                candidates,
                native_item.get("name", ""),
                pattern,
                count=native_item.get("count", 0),
                native_hits=native_item.get("native_hits", 1),
            )

    for seed_query in tag_seed_terms(pattern):
        try:
            posts = fetch_source_posts(
                source_id,
                seed_query,
                0,
                post_seed_limit,
                "popular",
                credentials,
                network_config,
                min(timeout, 15),
            )
        except Exception:  # noqa: BLE001
            posts = []

        for post in posts:
            for raw_tag in str(post.get("tags", "")).split():
                merge_tag_candidate(candidates, raw_tag.strip(), pattern, local_hits=1)

    exact_name = pattern.replace(" ", "_")
    if exact_name:
        exact_count = fetch_exact_tag_count(
            source_id,
            exact_name,
            credentials,
            network_config,
            timeout=min(timeout, 8),
        )
        merge_tag_candidate(candidates, exact_name, pattern, count=to_int(exact_count, 0))

    def is_strong_candidate(item: dict):
        if to_int(item.get("match_rank"), 9) > 4:
            return False
        if to_int(item.get("native_hits"), 0) > 0:
            return True
        if to_int(item.get("local_hits"), 0) >= 2:
            return True
        if to_int(item.get("count"), 0) >= 25:
            return True
        return False

    has_strong_local_match = any(is_strong_candidate(item) for item in candidates.values())
    if auth_enabled and (len(term_tokens) >= 2 or (len(term_tokens) == 1 and len(term_tokens[0]) >= 3)):
        pattern_variants = []
        if len(term_tokens) == 1:
            single = term_tokens[0]
            if single:
                pattern_variants.append(single)
                pattern_variants.append(f"%{single}%")
        if len(term_tokens) >= 2:
            tail_pattern = "_".join(term_tokens[-2:])
            if tail_pattern:
                pattern_variants.append(f"%{tail_pattern}%")
            full_pattern = "_".join(term_tokens)
            if full_pattern and full_pattern not in pattern_variants:
                pattern_variants.append(f"%{full_pattern}%")

        seen_variants = set()
        for pattern_variant in pattern_variants:
            if pattern_variant in seen_variants:
                continue
            seen_variants.add(pattern_variant)
            pattern_params = {
                "page": "dapi",
                "s": "tag",
                "q": "index",
                "name_pattern": pattern_variant,
                "limit": str(max(limit * 12, 120)),
            }
            if credentials.get("user_id") and credentials.get("api_key"):
                pattern_params["user_id"] = credentials["user_id"]
                pattern_params["api_key"] = credentials["api_key"]
            pattern_url = f"{SOURCE_CONFIG[source_id]['base_url']}?{urlencode(pattern_params)}"
            strict_https(pattern_url)
            pattern_request = Request(
                pattern_url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BooruFinder/1.0",
                    "Accept": "*/*",
                    "DNT": "1",
                    "Cache-Control": "no-store",
                },
            )
            try:
                with open_http(pattern_request, network_config, timeout=min(timeout, 8)) as response:
                    pattern_payload = response.read()
                pattern_root = ET.fromstring(pattern_payload)
                if pattern_root.tag.lower() == "error":
                    continue
                for tag_el in pattern_root.findall("tag"):
                    tag_data = xml_node_to_dict(tag_el)
                    merge_tag_candidate(
                        candidates,
                        str(tag_data.get("name", "")).strip(),
                        pattern,
                        count=tag_data.get("count", 0),
                    )
                if any(to_int(item.get("match_rank"), 9) <= 3 for item in candidates.values()):
                    break
            except Exception:  # noqa: BLE001
                continue

    alias_targets = sorted(
        candidates.values(),
        key=lambda item: (
            autocomplete_effective_rank(item["name"], pattern, item.get("count"), item.get("match_rank")),
            -to_int(item.get("native_hits"), 0),
            -to_int(item.get("local_hits"), 0),
            len(item["name"]),
            item["name"],
        ),
    )[: max(limit * 10, 80)]
    derived_aliases = {}
    for item in alias_targets:
        for alias in derive_tag_aliases(item["name"], pattern):
            if alias == item["name"]:
                continue
            merge_tag_candidate(
                candidates,
                alias,
                pattern,
                local_hits=item.get("local_hits", 0),
                native_hits=item.get("native_hits", 0),
            )
            derived_aliases[alias] = candidates.get(alias)

    if auth_enabled and derived_aliases:
        alias_items = []
        seen_alias_names = set()
        for item in derived_aliases.values():
            if not item:
                continue
            name = str(item.get("name", "")).strip()
            if not name or name in seen_alias_names:
                continue
            seen_alias_names.add(name)
            alias_items.append(item)
        with ThreadPoolExecutor(max_workers=min(6, len(alias_items))) as executor:
            futures = {
                executor.submit(
                    fetch_exact_tag_count,
                    source_id,
                    item["name"],
                    credentials,
                    network_config,
                    min(timeout, 6),
                ): item
                for item in alias_items
            }
            for future in as_completed(futures):
                item = futures[future]
                try:
                    count = future.result()
                    item["count"] = max(to_int(item.get("count"), 0), to_int(count, 0))
                except Exception:  # noqa: BLE001
                    continue

    preliminary = sorted(
        candidates.values(),
        key=lambda item: (
            autocomplete_effective_rank(item["name"], pattern, item.get("count"), item.get("match_rank")),
            -to_int(item.get("native_hits"), 0),
            -to_int(item.get("local_hits"), 0),
            len(item["name"]),
            item["name"],
        ),
    )[: max(limit, 12)]

    count_targets = preliminary[: min(len(preliminary), max(limit, 6))] if auth_enabled else []
    if count_targets:
        with ThreadPoolExecutor(max_workers=min(6, len(count_targets))) as executor:
            futures = {
                executor.submit(
                    fetch_exact_tag_count,
                    source_id,
                    item["name"],
                    credentials,
                    network_config,
                    min(timeout, 6),
                ): item
                for item in count_targets
            }
            for future in as_completed(futures):
                item = futures[future]
                try:
                    count = future.result()
                    item["count"] = to_int(count, 0)
                except Exception:  # noqa: BLE001
                    item["count"] = to_int(item.get("count"), 0)

    ordered_names = sorted(
        preliminary,
        key=lambda item: (
            autocomplete_effective_rank(item["name"], pattern, item.get("count"), item.get("match_rank")),
            -to_int(item.get("count"), 0),
            -to_int(item.get("native_hits"), 0),
            -to_int(item.get("local_hits"), 0),
            len(item["name"]),
            item["name"],
        ),
    )[:limit]
    result = [
        {
            "value": item["name"],
            "label": item["name"],
            "count": to_int(item.get("count"), 0),
            "source_id": source_id,
            "source_name": SOURCE_CONFIG[source_id]["name"],
            "kind": "tag",
            "match_rank": to_int(item.get("match_rank"), 9),
        }
        for item in ordered_names
    ]
    _cache_set(_TAG_CACHE, cache_key, result, TAG_CACHE_TTL_SEC)
    db_cache_set("autocomplete", db_cache_key, result, SQLITE_AUTOCOMPLETE_TTL_SEC)
    return result


def rating_to_query_tag(rating: str) -> str:
    normalized = str(rating or "any").strip().lower()
    mapping = {
        "safe": "rating:s",
        "s": "rating:s",
        "general": "rating:g",
        "g": "rating:g",
        "questionable": "rating:q",
        "q": "rating:q",
        "explicit": "rating:e",
        "e": "rating:e",
    }
    return mapping.get(normalized, "")


def canonicalize_tag_for_source(
    source_id: str,
    raw_tag: str,
    credentials=None,
    network_config=None,
    timeout=12,
):
    normalized = normalize_tag_hint_term(raw_tag)
    if not normalized:
        return ""
    exact_name = "_".join(part for part in normalized.split() if part)
    exact_count = fetch_exact_tag_count(source_id, exact_name, credentials or {}, network_config or {}, min(timeout, 10))

    hints = fetch_source_tags(source_id, normalized, 8, credentials or {}, network_config or {}, timeout)
    if hints:
        best = hints[0]
        best_value = str(best.get("value", "")).strip()
        best_count = to_int(best.get("count"), 0)
        best_rank = autocomplete_effective_rank(best_value, normalized, best_count, best.get("match_rank"))
        if best_value:
            if to_int(exact_count, 0) > 0:
                if best_value == exact_name:
                    return exact_name
                if best_rank <= 3 and best_count >= max(to_int(exact_count, 0) * 3, to_int(exact_count, 0) + 50):
                    return best_value
                return exact_name
            if best_rank <= 4 and best_count > 0:
                return best_value

    if to_int(exact_count, 0) > 0:
        return exact_name
    return exact_name


def resolve_structured_query(
    source_id: str,
    include_tags,
    exclude_tags,
    raw_query: str,
    rating: str,
    credentials=None,
    network_config=None,
):
    rating_tag = rating_to_query_tag(rating)
    if str(raw_query or "").strip():
        resolved_query = normalize_tag_query(raw_query)
        if rating_tag and rating_tag not in resolved_query:
            resolved_query = f"{resolved_query} {rating_tag}".strip()
        return {
            "resolved_query": resolved_query,
            "resolved_include_tags": [],
            "resolved_exclude_tags": [],
        }

    resolved_include = []
    resolved_exclude = []
    for tag in include_tags or []:
        canonical = canonicalize_tag_for_source(source_id, tag, credentials or {}, network_config or {})
        if canonical:
            resolved_include.append(canonical)
    for tag in exclude_tags or []:
        canonical = canonicalize_tag_for_source(source_id, tag, credentials or {}, network_config or {})
        if canonical:
            resolved_exclude.append(canonical)

    query_parts = list(dict.fromkeys(resolved_include))
    query_parts.extend(f"-{tag}" for tag in dict.fromkeys(resolved_exclude))
    if rating_tag:
        query_parts.append(rating_tag)

    return {
        "resolved_query": " ".join(part for part in query_parts if part).strip(),
        "resolved_include_tags": list(dict.fromkeys(resolved_include)),
        "resolved_exclude_tags": list(dict.fromkeys(resolved_exclude)),
    }


def parse_post_timestamp(post: dict):
    change = to_int(post.get("change"), 0)
    if change > 946684800:
        return change
    created_at = str(post.get("created_at", "")).strip()
    if not created_at:
        return 0
    for fmt in ("%a %b %d %H:%M:%S %z %Y", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(created_at, fmt)
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:  # noqa: BLE001
            continue
    return 0


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


def preview_post_for_list(posts):
    if not posts:
        return None
    ordered = sort_posts(posts, "popular")
    return ordered[0]


def extract_trend_tags(post: dict):
    tags = []
    for raw_tag in str(post.get("tags", "")).split():
        tag = raw_tag.strip().lower()
        if not tag or tag in TREND_STOP_TAGS:
            continue
        if tag.startswith(("artist:", "copyright:", "character:", "meta:", "source:")):
            continue
        if len(tag) < 3 or tag.isdigit():
            continue
        tags.append(tag)
    return tags


def compute_trending_snapshot(source_id: str, window_key: str, credentials=None, network_config=None):
    credentials = credentials or {}
    network_config = network_config or {}
    days = HOME_WINDOWS.get(window_key, 7)
    pages = 2 if days <= 7 else 3 if days <= 30 else 4 if days <= 90 else 5
    posts = []
    with ThreadPoolExecutor(max_workers=pages) as executor:
        futures = {
            executor.submit(fetch_source_posts, source_id, "", page, 24, "new", credentials, network_config, 20): page
            for page in range(pages)
        }
        for future in as_completed(futures):
            try:
                posts.extend(future.result())
            except Exception:  # noqa: BLE001
                continue

    cutoff = int(now_ts()) - days * 24 * 60 * 60
    weighted = {}
    previews = {}
    for post in sort_posts(posts, "new"):
        post_ts = parse_post_timestamp(post)
        if post_ts and post_ts < cutoff:
            continue
        age_days = max(0.0, (int(now_ts()) - post_ts) / 86400.0) if post_ts else 0.0
        recency_weight = max(0.35, 2.0 - (age_days / max(days, 1)))
        score_weight = min(max(to_int(post.get("score"), 0), 0), 2500) / 900.0
        base_weight = recency_weight + score_weight
        for tag in extract_trend_tags(post):
            weighted[tag] = weighted.get(tag, 0.0) + base_weight
            previous = previews.get(tag)
            if previous is None or to_int(post.get("score"), 0) > to_int(previous.get("score"), 0):
                previews[tag] = post

    ordered_tags = sorted(weighted.items(), key=lambda item: (-item[1], item[0]))[:12]
    items = [
        {
            "tag": tag,
            "score": round(score, 2),
            "count": round(score, 1),
            "preview": previews.get(tag) or {},
            "source_id": source_id,
            "source_name": SOURCE_CONFIG[source_id]["name"],
        }
        for tag, score in ordered_tags
    ]
    store_trend_samples(source_id, window_key, items)
    return items


def load_trending_sections(sources, window_key: str, credentials_map: dict, network_config: dict, force=False):
    sections = []
    errors = []
    for source_id in sources:
        if not has_source_auth(credentials_map.get(source_id, {})):
            sections.append(
                {
                    "source_id": source_id,
                    "source_name": SOURCE_CONFIG[source_id]["name"],
                    "window": window_key,
                    "items": [],
                    "updated_at": iso_now(),
                }
            )
            errors.append({"source": source_id, "message": "Missing API credentials for source."})
            continue
        cache_key = _cache_hash([source_id, window_key])
        payload = None if force else db_cache_get("trending", cache_key)
        if payload is None:
            try:
                items = compute_trending_snapshot(source_id, window_key, credentials_map.get(source_id, {}), network_config)
                payload = {
                    "source_id": source_id,
                    "source_name": SOURCE_CONFIG[source_id]["name"],
                    "window": window_key,
                    "items": items,
                    "updated_at": iso_now(),
                }
                db_cache_set("trending", cache_key, payload, SQLITE_TREND_TTL_SEC)
            except Exception as exc:  # noqa: BLE001
                cached_samples = load_trend_samples(source_id, window_key, 12)
                if cached_samples:
                    payload = {
                        "source_id": source_id,
                        "source_name": SOURCE_CONFIG[source_id]["name"],
                        "window": window_key,
                        "items": [
                            {
                                "tag": item["tag"],
                                "score": item["score"],
                                "count": item["score"],
                                "preview": item["preview"],
                                "source_id": source_id,
                                "source_name": SOURCE_CONFIG[source_id]["name"],
                            }
                            for item in cached_samples
                        ],
                        "updated_at": iso_now(),
                    }
                else:
                    errors.append({"source": source_id, "message": str(exc)})
                    continue
        sections.append(payload)
    return {"window": window_key, "windows": list(HOME_WINDOWS.keys()), "sections": sections, "errors": errors}


def fetch_featured_sections(sources, credentials_map: dict, network_config: dict):
    sections = []
    errors = []
    for source_id in sources:
        if not has_source_auth(credentials_map.get(source_id, {})):
            sections.append({"source_id": source_id, "source_name": SOURCE_CONFIG[source_id]["name"], "items": []})
            errors.append({"source": source_id, "message": "Missing API credentials for source."})
            continue
        items = []
        for spec in FEATURED_COLLECTIONS.get(source_id, []):
            try:
                posts = fetch_source_posts(
                    source_id,
                    spec.get("query", ""),
                    0,
                    4,
                    spec.get("sort", "popular"),
                    credentials_map.get(source_id, {}),
                    network_config,
                    18,
                )
            except Exception as exc:  # noqa: BLE001
                errors.append({"source": source_id, "message": str(exc)})
                continue
            preview = preview_post_for_list(posts)
            if not preview:
                continue
            items.append(
                {
                    "id": spec["id"],
                    "title": spec["title"],
                    "subtitle": spec["subtitle"],
                    "query": spec["query"],
                    "sort": spec.get("sort", "popular"),
                    "preview": preview,
                }
            )
        sections.append({"source_id": source_id, "source_name": SOURCE_CONFIG[source_id]["name"], "items": items})
    return {"sections": sections, "errors": errors, "updated_at": iso_now()}


def load_featured_sections(sources, credentials_map: dict, network_config: dict, force=False):
    cache_key = _cache_hash(["featured", ",".join(sorted(sources))])
    payload = None if force else db_cache_get("featured", cache_key)
    if payload is None:
        payload = fetch_featured_sections(sources, credentials_map, network_config)
        db_cache_set("featured", cache_key, payload, SQLITE_FEATURED_TTL_SEC)
    return payload


def scrape_gelbooru_news(network_config: dict):
    html = read_text_response("https://gelbooru.com/index.php?page=forum&s=list", network_config, 12)
    pattern = re.compile(r'href="([^"]*page=forum(?:&amp;|&)s=view(?:&amp;|&)id=\d+[^"]*)"[^>]*>(.*?)</a>', re.IGNORECASE)
    items = []
    seen_titles = set()
    for href, raw_title in pattern.findall(html):
        title = re.sub(r"<.*?>", "", raw_title).strip()
        title = re.sub(r"\s+", " ", title)
        if len(title) < 4 or title.lower() in seen_titles:
            continue
        seen_titles.add(title.lower())
        items.append(
            {
                "id": f"gelbooru-{len(items) + 1}",
                "source_id": "gelbooru",
                "source_name": "Gelbooru",
                "title": title,
                "summary": "Forum / announcement feed",
                "url": urljoin("https://gelbooru.com/", href.replace("&amp;", "&")),
                "published_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "kind": "site",
            }
        )
        if len(items) >= 4:
            break
    return items


def fetch_news_items(sources, network_config: dict):
    items = []
    errors = []
    if "gelbooru" in sources:
        try:
            items.extend(scrape_gelbooru_news(network_config))
        except Exception as exc:  # noqa: BLE001
            errors.append({"source": "gelbooru", "message": str(exc)})
    for fallback in FALLBACK_NEWS:
        items.append(dict(fallback))
        if len(items) >= 6:
            break

    deduped = []
    seen = set()
    for item in items:
        key = (item.get("source_id"), item.get("title"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return {"items": deduped[:6], "errors": errors, "updated_at": iso_now()}


def load_news_items(sources, network_config: dict, force=False):
    cache_key = _cache_hash(["news", ",".join(sorted(sources))])
    payload = None if force else db_cache_get("news", cache_key)
    if payload is None:
        payload = fetch_news_items(sources, network_config)
        db_cache_set("news", cache_key, payload, SQLITE_NEWS_TTL_SEC)
    return payload


def build_display_query(include_tags, exclude_tags, raw_query: str):
    raw = normalize_search_text(raw_query)
    if raw:
        return raw
    parts = [normalize_search_text(tag) for tag in include_tags or [] if normalize_search_text(tag)]
    parts.extend(f"-{normalize_search_text(tag)}" for tag in exclude_tags or [] if normalize_search_text(tag))
    return ", ".join(parts)


def load_home_payload(sources, credentials_map: dict, network_config: dict, force=False):
    return {
        "sources": sources,
        "recent_queries": load_recent_queries(8),
        "featured": load_featured_sections(sources, credentials_map, network_config, force),
        "trending": load_trending_sections(sources, "7d", credentials_map, network_config, force),
        "news": load_news_items(sources, network_config, force),
        "updated_at": iso_now(),
    }


def start_background_refresh():
    global _BACKGROUND_STARTED
    with _BACKGROUND_LOCK:
        if _BACKGROUND_STARTED:
            return
        _BACKGROUND_STARTED = True

    def worker():
        while True:
            try:
                secure_state = load_secure_state()
                credentials_map = credentials_from_state(secure_state)
                network_config = network_from_state(secure_state)
                sources = list(SOURCE_CONFIG.keys())
                load_featured_sections(sources, credentials_map, network_config, force=True)
                load_news_items(sources, network_config, force=True)
                for window_key in HOME_WINDOWS:
                    load_trending_sections(sources, window_key, credentials_map, network_config, force=True)
            except Exception:  # noqa: BLE001
                pass
            time.sleep(BACKGROUND_REFRESH_INTERVAL_SEC)

    Thread(target=worker, name="booru-home-refresh", daemon=True).start()


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

    def require_adult(self, query):
        if query.get("adult", ["0"])[0] != "1":
            self.send_json(403, {"error": "Age gate is required. Set 'adult=1' to continue."})
            return False
        return True

    def handle_api_sources(self):
        self.send_json(
            200,
            {
                "sources": [
                    {"id": source_id, "name": config["name"]}
                    for source_id, config in SOURCE_CONFIG.items()
                ],
                "default_theme": "dark_ref",
                "theme_options": [
                    {"id": "dark_ref", "name": "Dark Ref"},
                    {"id": "pink_cyber", "name": "Pink Cyber Pastel"},
                    {"id": "old_neko", "name": "Old Neko"},
                    {"id": "retro_blue", "name": "Neo 2005 CRT"},
                ],
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
                "cache_db": str(CACHE_DB),
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

    def handle_api_autocomplete(self, query):
        if not self.require_adult(query):
            return

        term = normalize_tag_hint_term(query.get("term", [""])[0] or "")
        if len(term) < 2:
            self.send_json(200, {"term": term, "sources": [], "items": [], "errors": []})
            return

        limit = max(1, min(24, to_int(query.get("limit", ["12"])[0], 12)))
        sources = parse_source_list(query.get("sources", [""])[0])
        if not sources:
            self.send_json(400, {"error": "No valid sources selected."})
            return

        secure_state = load_secure_state()
        network_config = parse_network_config(query, secure_state, self.headers)
        credentials = parse_credentials(query, secure_state, self.headers)
        items = []
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
                    8,
                ): source_id
                for source_id in sources
            }
            for future in as_completed(futures):
                source_id = futures[future]
                try:
                    items.extend(future.result())
                except Exception as exc:  # noqa: BLE001
                    errors.append({"source": source_id, "message": str(exc)})

        ordered = sorted(
            items,
            key=lambda item: (
                autocomplete_effective_rank(
                    str(item.get("value", "")),
                    term,
                    item.get("count", 0),
                    item.get("match_rank"),
                ),
                -to_int(item.get("count"), 0),
                item.get("source_name", ""),
                item.get("value", ""),
            ),
        )
        self.send_json(200, {"term": term, "sources": sources, "items": ordered, "errors": errors})

    def handle_api_search(self, query):
        if not self.require_adult(query):
            return

        secure_state = load_secure_state()
        network_config = parse_network_config(query, secure_state, self.headers)
        credentials = parse_credentials(query, secure_state, self.headers)

        page = max(0, to_int(query.get("page", ["0"])[0], 0))
        limit = max(1, min(100, to_int(query.get("limit", ["30"])[0], 30)))
        min_score = max(0, to_int(query.get("min_score", ["0"])[0], 0))
        sort_mode = (query.get("sort", ["new"])[0] or "new").strip().lower()
        rating = (query.get("rating", ["any"])[0] or "any").strip().lower()
        sources = parse_source_list(query.get("sources", [""])[0])
        include_tags = parse_tag_list(query, "include_tags")
        exclude_tags = parse_tag_list(query, "exclude_tags")
        raw_query = normalize_search_text(query.get("raw_query", [""])[0] or "")
        legacy_tags = normalize_search_text(query.get("tags", [""])[0] or "")
        if not raw_query and legacy_tags:
            raw_query = legacy_tags

        if not sources:
            self.send_json(400, {"error": "No valid sources selected."})
            return
        if not raw_query and not include_tags and not exclude_tags:
            self.send_json(400, {"error": "Add at least one tag or raw query."})
            return

        results = []
        errors = []
        has_more = False
        resolved = {}

        with ThreadPoolExecutor(max_workers=len(sources)) as executor:
            resolve_futures = {
                executor.submit(
                    resolve_structured_query,
                    source_id,
                    include_tags,
                    exclude_tags,
                    raw_query,
                    rating,
                    credentials.get(source_id, {}),
                    network_config,
                ): source_id
                for source_id in sources
            }
            for future in as_completed(resolve_futures):
                source_id = resolve_futures[future]
                resolved[source_id] = future.result()

        with ThreadPoolExecutor(max_workers=len(sources)) as executor:
            futures = {
                executor.submit(
                    fetch_source_posts,
                    source_id,
                    resolved[source_id]["resolved_query"],
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
        record_recent_query(
            build_display_query(include_tags, exclude_tags, raw_query),
            raw_query,
            include_tags,
            exclude_tags,
            sources,
            sort_mode,
            rating,
            min_score,
        )
        self.send_json(
            200,
            {
                "page": page,
                "limit": limit,
                "min_score": min_score,
                "sort": sort_mode,
                "rating": rating,
                "sources": sources,
                "raw_count": len(deduped),
                "count": len(ordered),
                "has_more": has_more,
                "items": ordered,
                "errors": errors,
                "resolved_query": {source_id: resolved[source_id]["resolved_query"] for source_id in sources},
                "resolved_include_tags": {source_id: resolved[source_id]["resolved_include_tags"] for source_id in sources},
                "resolved_exclude_tags": {source_id: resolved[source_id]["resolved_exclude_tags"] for source_id in sources},
            },
        )

    def handle_api_tags(self, query):
        if not self.require_adult(query):
            return

        term = normalize_tag_hint_term(query.get("term", [""])[0] or "")
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

        def suggestion_rank(name: str):
            return tag_hint_rank(str(name or ""), term)

        ordered = sorted(
            suggestions,
            key=lambda t: (
                autocomplete_effective_rank(
                    str(t.get("value", "")),
                    term,
                    t.get("count", 0),
                    t.get("match_rank"),
                ),
                -to_int(t.get("count")),
                t.get("source_name", ""),
                t.get("value", ""),
            ),
        )
        self.send_json(
            200,
            {
                "term": term,
                "sources": sources,
                "suggestions": [
                    {
                        "name": item.get("value", ""),
                        "count": to_int(item.get("count"), 0),
                        "source_id": item.get("source_id"),
                        "source_name": item.get("source_name"),
                        "match_rank": to_int(item.get("match_rank"), 9),
                    }
                    for item in ordered
                ],
                "errors": errors,
            },
        )

    def handle_api_featured(self, query):
        if not self.require_adult(query):
            return
        sources = parse_source_list(query.get("sources", [""])[0])
        secure_state = load_secure_state()
        credentials_map = parse_credentials(query, secure_state, self.headers)
        network_config = parse_network_config(query, secure_state, self.headers)
        force = query.get("refresh", ["0"])[0] == "1"
        self.send_json(200, load_featured_sections(sources, credentials_map, network_config, force))

    def handle_api_trending(self, query):
        if not self.require_adult(query):
            return
        window_key = (query.get("window", ["7d"])[0] or "7d").strip().lower()
        if window_key not in HOME_WINDOWS:
            self.send_json(400, {"error": "Invalid window"})
            return
        sources = parse_source_list(query.get("sources", [""])[0])
        secure_state = load_secure_state()
        credentials_map = parse_credentials(query, secure_state, self.headers)
        network_config = parse_network_config(query, secure_state, self.headers)
        force = query.get("refresh", ["0"])[0] == "1"
        self.send_json(200, load_trending_sections(sources, window_key, credentials_map, network_config, force))

    def handle_api_news(self, query):
        if not self.require_adult(query):
            return
        sources = parse_source_list(query.get("sources", [""])[0])
        secure_state = load_secure_state()
        network_config = parse_network_config(query, secure_state, self.headers)
        force = query.get("refresh", ["0"])[0] == "1"
        self.send_json(200, load_news_items(sources, network_config, force))

    def handle_api_home(self, query):
        if not self.require_adult(query):
            return
        sources = parse_source_list(query.get("sources", [""])[0])
        secure_state = load_secure_state()
        credentials_map = parse_credentials(query, secure_state, self.headers)
        network_config = parse_network_config(query, secure_state, self.headers)
        force = query.get("refresh", ["0"])[0] == "1"
        self.send_json(200, load_home_payload(sources, credentials_map, network_config, force))

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
        if parsed.path == "/api/autocomplete":
            self.handle_api_autocomplete(query)
            return
        if parsed.path == "/api/featured":
            self.handle_api_featured(query)
            return
        if parsed.path == "/api/trending":
            self.handle_api_trending(query)
            return
        if parsed.path == "/api/news":
            self.handle_api_news(query)
            return
        if parsed.path == "/api/home":
            self.handle_api_home(query)
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
    init_cache_db()
    start_background_refresh()
    handler = lambda *args, **kwargs: AppHandler(*args, directory=static_dir(), **kwargs)
    return ThreadingHTTPServer(("127.0.0.1", port), handler)


if __name__ == "__main__":
    main()
