import ctypes
import os
import sys
import threading
import urllib.request

import app

try:
    import webview
except Exception:  # noqa: BLE001
    webview = None


def _show_error(message: str):
    if os.name == "nt":
        ctypes.windll.user32.MessageBoxW(0, message, "Booru Finder", 0x10)
    else:
        print(message)


def _run_desktop_smoke(server, url: str):
    with urllib.request.urlopen(f"{url}/", timeout=8) as response:
        if response.status != 200:
            raise RuntimeError(f"Desktop smoke failed: status {response.status}")
    server.shutdown()
    server.server_close()


def main():
    if webview is None:
        _show_error("Missing dependency: pywebview. Install it and rebuild the app.")
        return 1

    requested_port = app.to_int(os.getenv("BOORU_PORT", "0"), 0)
    bind_port = requested_port if requested_port > 0 else 0
    server = app.create_server(bind_port)
    actual_port = server.server_address[1]
    url = f"http://127.0.0.1:{actual_port}"

    thread = threading.Thread(target=server.serve_forever, name="booru-http", daemon=True)
    thread.start()

    if os.getenv("BOORU_DESKTOP_SMOKE", "0") == "1":
        _run_desktop_smoke(server, url)
        return 0

    window = webview.create_window(
        "Booru Finder",
        url,
        width=1480,
        height=920,
        min_size=(960, 700),
    )
    try:
        webview.start(gui="edgechromium", debug=False)
    finally:
        try:
            server.shutdown()
        except Exception:  # noqa: BLE001
            pass
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
