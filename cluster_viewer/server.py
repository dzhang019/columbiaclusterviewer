from __future__ import annotations

import argparse
import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .collectors import build_dashboard_payload


ROOT = Path(__file__).resolve().parent.parent
STATIC_DIR = ROOT / "static"


def _read_static_file(path: Path) -> bytes:
    with open(path, "rb") as handle:
        return handle.read()


def _content_type(path: Path) -> str:
    suffix = path.suffix.lower()
    return {
        ".css": "text/css; charset=utf-8",
        ".js": "application/javascript; charset=utf-8",
        ".html": "text/html; charset=utf-8",
        ".json": "application/json; charset=utf-8",
    }.get(suffix, "application/octet-stream")


class ClusterViewerHandler(BaseHTTPRequestHandler):
    server_version = "ClusterViewer/0.1"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._serve_static("index.html")
            return
        if parsed.path == "/api/dashboard":
            self._serve_dashboard()
            return
        if parsed.path.startswith("/static/"):
            self._serve_static(parsed.path.removeprefix("/static/"))
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _serve_dashboard(self) -> None:
        payload = json.dumps(build_dashboard_payload()).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _serve_static(self, relative_path: str) -> None:
        safe_path = (STATIC_DIR / relative_path).resolve()
        if not safe_path.is_file() or STATIC_DIR not in safe_path.parents:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return
        payload = _read_static_file(safe_path)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", _content_type(safe_path))
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def run() -> None:
    parser = argparse.ArgumentParser(description="Run the simple cluster viewer.")
    parser.add_argument("--host", default=os.environ.get("CLUSTER_VIEWER_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("CLUSTER_VIEWER_PORT", "8000")))
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), ClusterViewerHandler)
    print(f"Cluster viewer listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down cluster viewer.")
    finally:
        server.server_close()
