from __future__ import annotations

import argparse
import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .collectors import build_dashboard_payload_from_snapshot, collect_live_snapshot
from .history import HistorySampler, HistoryStore


ROOT = Path(__file__).resolve().parent.parent
STATIC_DIR = ROOT / "static"


class ClusterViewerServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], request_handler_class, history_store: HistoryStore) -> None:
        super().__init__(server_address, request_handler_class)
        self.history_store = history_store


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
    server_version = "ClusterViewer/0.2"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._serve_static("index.html")
            return
        if parsed.path == "/api/dashboard":
            self._serve_dashboard(parse_qs(parsed.query))
            return
        if parsed.path.startswith("/static/"):
            self._serve_static(parsed.path.removeprefix("/static/"))
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _serve_dashboard(self, query: dict[str, list[str]]) -> None:
        range_key = query.get("range", ["1h"])[0]
        user_filter = query.get("user", [""])[0].strip()
        node_filter = query.get("node", [""])[0].strip()
        snapshot = collect_live_snapshot()
        history = self.server.history_store.load_history(range_key, user_filter=user_filter, node_filter=node_filter)
        payload = json.dumps(build_dashboard_payload_from_snapshot(snapshot, history=history)).encode("utf-8")
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
    parser.add_argument("--host", default=os.environ.get("CLUSTER_VIEWER_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("CLUSTER_VIEWER_PORT", "8000")))
    parser.add_argument(
        "--db-path",
        default=os.environ.get("CLUSTER_VIEWER_DB_PATH", str(ROOT / "cluster_viewer.sqlite3")),
    )
    parser.add_argument(
        "--sample-interval",
        type=int,
        default=int(os.environ.get("CLUSTER_VIEWER_SAMPLE_INTERVAL", "60")),
    )
    args = parser.parse_args()

    history_store = HistoryStore(args.db_path)
    history_store.save_snapshot(collect_live_snapshot())
    sampler = HistorySampler(history_store, collect_live_snapshot, args.sample_interval)
    sampler.start()

    server = ClusterViewerServer((args.host, args.port), ClusterViewerHandler, history_store)
    print(f"Cluster viewer listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down cluster viewer.")
    finally:
        sampler.stop()
        server.server_close()
