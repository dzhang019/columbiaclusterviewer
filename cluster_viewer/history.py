from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


RANGE_WINDOWS = {
    "live": {"delta": timedelta(minutes=15), "max_points": 60},
    "5m": {"delta": timedelta(minutes=5), "max_points": 30},
    "1h": {"delta": timedelta(hours=1), "max_points": 60},
    "1d": {"delta": timedelta(days=1), "max_points": 96},
    "1mo": {"delta": timedelta(days=30), "max_points": 120},
}


def normalize_range(range_key: str | None) -> str:
    return range_key if range_key in RANGE_WINDOWS else "1h"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _downsample(rows: list[dict[str, Any]], max_points: int) -> list[dict[str, Any]]:
    if len(rows) <= max_points:
        return rows
    step = len(rows) / max_points
    sampled = [rows[int(index * step)] for index in range(max_points)]
    if sampled[-1] != rows[-1]:
        sampled[-1] = rows[-1]
    return sampled


def _job_state_count(queue_status: dict[str, int], prefixes: tuple[str, ...]) -> int:
    total = 0
    for state, count in queue_status.items():
        if state.upper().startswith(prefixes):
            total += count
    return total


def _node_state_summary(node_states: dict[str, int]) -> dict[str, int]:
    summary = {"allocated": 0, "idle": 0, "down": 0, "drain": 0}
    for state, count in node_states.items():
        normalized = state.lower()
        if "alloc" in normalized or "mix" in normalized:
            summary["allocated"] += count
        if "idle" in normalized:
            summary["idle"] += count
        if "down" in normalized:
            summary["down"] += count
        if "drain" in normalized:
            summary["drain"] += count
    return summary


def _safe_int(value: str | None) -> int:
    if not value:
        return 0
    try:
        return int(value)
    except ValueError:
        return 0


class HistoryStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    collected_at TEXT NOT NULL,
                    hostname TEXT NOT NULL,
                    load1 REAL NOT NULL,
                    load5 REAL NOT NULL,
                    load15 REAL NOT NULL,
                    memory_percent REAL NOT NULL,
                    disk_percent REAL NOT NULL,
                    cpu_allocated INTEGER NOT NULL,
                    cpu_idle INTEGER NOT NULL,
                    cpu_other INTEGER NOT NULL,
                    total_jobs INTEGER NOT NULL,
                    running_jobs INTEGER NOT NULL,
                    pending_jobs INTEGER NOT NULL,
                    nodes_total INTEGER NOT NULL,
                    nodes_allocated INTEGER NOT NULL,
                    nodes_idle INTEGER NOT NULL,
                    nodes_down INTEGER NOT NULL,
                    nodes_drain INTEGER NOT NULL,
                    scheduler_available INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_samples_collected_at ON samples(collected_at);

                CREATE TABLE IF NOT EXISTS user_samples (
                    sample_id INTEGER NOT NULL,
                    user_name TEXT NOT NULL,
                    total_jobs INTEGER NOT NULL,
                    running_jobs INTEGER NOT NULL,
                    pending_jobs INTEGER NOT NULL,
                    PRIMARY KEY (sample_id, user_name),
                    FOREIGN KEY(sample_id) REFERENCES samples(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_user_samples_user_name ON user_samples(user_name);

                CREATE TABLE IF NOT EXISTS node_samples (
                    sample_id INTEGER NOT NULL,
                    node_name TEXT NOT NULL,
                    state TEXT NOT NULL,
                    cpu_allocated INTEGER NOT NULL,
                    cpu_idle INTEGER NOT NULL,
                    cpu_total INTEGER NOT NULL,
                    memory_mb INTEGER NOT NULL,
                    PRIMARY KEY (sample_id, node_name),
                    FOREIGN KEY(sample_id) REFERENCES samples(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_node_samples_node_name ON node_samples(node_name);
                """
            )

    def save_snapshot(self, snapshot: dict[str, Any]) -> None:
        system = snapshot["system"]
        scheduler = snapshot["scheduler"]
        queue_status = scheduler.get("queue_status", {})
        node_summary = _node_state_summary(scheduler.get("node_states", {}))

        with self._lock:
            with self._connect() as connection:
                cursor = connection.execute(
                    """
                    INSERT INTO samples (
                        collected_at, hostname, load1, load5, load15, memory_percent, disk_percent,
                        cpu_allocated, cpu_idle, cpu_other, total_jobs, running_jobs, pending_jobs,
                        nodes_total, nodes_allocated, nodes_idle, nodes_down, nodes_drain, scheduler_available
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot["generated_at"],
                        system["hostname"],
                        system["load_average"][0],
                        system["load_average"][1],
                        system["load_average"][2],
                        system["memory"]["percent_used"],
                        system["disk_root"]["percent_used"],
                        scheduler["cpu"]["allocated"],
                        scheduler["cpu"]["idle"],
                        scheduler["cpu"]["other"],
                        len(scheduler.get("jobs", [])),
                        _job_state_count(queue_status, ("R",)),
                        _job_state_count(queue_status, ("PD", "CF")),
                        len(scheduler.get("nodes", [])),
                        node_summary["allocated"],
                        node_summary["idle"],
                        node_summary["down"],
                        node_summary["drain"],
                        1 if scheduler.get("available") else 0,
                    ),
                )
                sample_id = cursor.lastrowid

                per_user: dict[str, dict[str, int]] = {}
                for job in scheduler.get("jobs", []):
                    user_name = job.get("user", "")
                    if not user_name:
                        continue
                    counts = per_user.setdefault(user_name, {"total": 0, "running": 0, "pending": 0})
                    counts["total"] += 1
                    state = job.get("state", "").upper()
                    if state.startswith("R"):
                        counts["running"] += 1
                    if state.startswith("PD") or state.startswith("CF"):
                        counts["pending"] += 1

                connection.executemany(
                    """
                    INSERT OR REPLACE INTO user_samples (
                        sample_id, user_name, total_jobs, running_jobs, pending_jobs
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        (sample_id, user_name, counts["total"], counts["running"], counts["pending"])
                        for user_name, counts in per_user.items()
                    ],
                )

                connection.executemany(
                    """
                    INSERT OR REPLACE INTO node_samples (
                        sample_id, node_name, state, cpu_allocated, cpu_idle, cpu_total, memory_mb
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            sample_id,
                            node.get("name", ""),
                            node.get("state", ""),
                            _safe_int(node.get("cpu_allocated", "0")),
                            _safe_int(node.get("cpu_idle", "0")),
                            _safe_int(node.get("cpu_total", "0")),
                            _safe_int(node.get("memory_mb", "0")),
                        )
                        for node in scheduler.get("nodes", [])
                        if node.get("name")
                    ],
                )

                cutoff = (_utc_now() - timedelta(days=45)).isoformat()
                connection.execute("DELETE FROM samples WHERE collected_at < ?", (cutoff,))
                connection.commit()

    def load_history(self, range_key: str, user_filter: str = "", node_filter: str = "") -> dict[str, Any]:
        normalized_range = normalize_range(range_key)
        config = RANGE_WINDOWS[normalized_range]
        since = (_utc_now() - config["delta"]).isoformat()
        user_like = f"%{user_filter.lower()}%"
        node_like = f"%{node_filter.lower()}%"

        with self._lock:
            with self._connect() as connection:
                cluster_rows = [
                    dict(row)
                    for row in connection.execute(
                        """
                        SELECT
                            collected_at, load1, memory_percent, cpu_allocated, cpu_idle, cpu_other,
                            total_jobs, running_jobs, pending_jobs, nodes_allocated, nodes_idle, nodes_down, nodes_drain
                        FROM samples
                        WHERE collected_at >= ?
                        ORDER BY collected_at ASC
                        """,
                        (since,),
                    )
                ]
                user_rows = [
                    dict(row)
                    for row in connection.execute(
                        """
                        SELECT
                            s.collected_at,
                            COALESCE(SUM(u.total_jobs), 0) AS total_jobs,
                            COALESCE(SUM(u.running_jobs), 0) AS running_jobs,
                            COALESCE(SUM(u.pending_jobs), 0) AS pending_jobs
                        FROM samples AS s
                        LEFT JOIN user_samples AS u
                            ON u.sample_id = s.id
                            AND (? = '' OR LOWER(u.user_name) LIKE ?)
                        WHERE s.collected_at >= ?
                        GROUP BY s.id
                        ORDER BY s.collected_at ASC
                        """,
                        (user_filter, user_like, since),
                    )
                ]
                node_rows = [
                    dict(row)
                    for row in connection.execute(
                        """
                        SELECT
                            s.collected_at,
                            COALESCE(SUM(n.cpu_allocated), 0) AS cpu_allocated,
                            COALESCE(SUM(n.cpu_idle), 0) AS cpu_idle,
                            COALESCE(SUM(n.cpu_total), 0) AS cpu_total,
                            COALESCE(COUNT(n.node_name), 0) AS matched_nodes
                        FROM samples AS s
                        LEFT JOIN node_samples AS n
                            ON n.sample_id = s.id
                            AND (? = '' OR LOWER(n.node_name) LIKE ?)
                        WHERE s.collected_at >= ?
                        GROUP BY s.id
                        ORDER BY s.collected_at ASC
                        """,
                        (node_filter, node_like, since),
                    )
                ]
                coverage = connection.execute(
                    "SELECT MIN(collected_at) AS oldest, MAX(collected_at) AS newest, COUNT(*) AS samples FROM samples"
                ).fetchone()

        return {
            "range": normalized_range,
            "cluster": _downsample(cluster_rows, config["max_points"]),
            "user": _downsample(user_rows, config["max_points"]),
            "node": _downsample(node_rows, config["max_points"]),
            "filters": {"user": user_filter, "node": node_filter},
            "coverage": {
                "oldest": coverage["oldest"] if coverage else None,
                "newest": coverage["newest"] if coverage else None,
                "samples": coverage["samples"] if coverage else 0,
            },
        }


class HistorySampler(threading.Thread):
    def __init__(self, store: HistoryStore, snapshot_builder, interval_seconds: int) -> None:
        super().__init__(daemon=True)
        self.store = store
        self.snapshot_builder = snapshot_builder
        self.interval_seconds = max(interval_seconds, 15)
        self._stop_event = threading.Event()

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.store.save_snapshot(self.snapshot_builder())
            except Exception:
                pass
            self._stop_event.wait(self.interval_seconds)

    def stop(self) -> None:
        self._stop_event.set()
