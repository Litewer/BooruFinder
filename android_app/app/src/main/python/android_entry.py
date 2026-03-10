import json
import os
import threading
from pathlib import Path

import app


_SERVER = None
_LOCK = threading.Lock()


def _default_secure_path() -> Path:
    return Path(__file__).resolve().parent / "default_secure_state.json"


def _load_default_secure_state():
    path = _default_secure_path()
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return app.normalize_secure_state(payload)
    except Exception:  # noqa: BLE001
        return None


def _ensure_preloaded_keys():
    if app.SECURE_FILE.exists():
        return
    default_state = _load_default_secure_state()
    if not default_state:
        return
    app.save_secure_state(default_state)


def start_server(port=8765):
    global _SERVER
    with _LOCK:
        if _SERVER is not None:
            return int(port)

        os.environ["BOORU_NO_BROWSER"] = "1"
        os.environ["BOORU_PORT"] = str(int(port))
        _ensure_preloaded_keys()

        _SERVER = app.create_server(int(port))
        thread = threading.Thread(target=_SERVER.serve_forever, name="booru-http", daemon=True)
        thread.start()
        return int(port)


def stop_server():
    global _SERVER
    with _LOCK:
        if _SERVER is None:
            return
        try:
            _SERVER.shutdown()
        finally:
            _SERVER.server_close()
            _SERVER = None
