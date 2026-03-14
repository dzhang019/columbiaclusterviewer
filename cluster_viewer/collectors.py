from __future__ import annotations

import json
import math
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def _run_command(command: list[str], timeout: int = 5) -> dict[str, Any]:
    binary = shutil.which(command[0])
    if not binary:
        return {
            "ok": False,
            "command": " ".join(command),
            "stdout": "",
            "stderr": f"{command[0]} not found",
            "returncode": None,
        }

    completed = subprocess.run(
        [binary, *command[1:]],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    return {
        "ok": completed.returncode == 0,
        "command": " ".join(command),
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
        "returncode": completed.returncode,
    }


def _safe_int(value: str | None) -> int:
    if not value:
        return 0
    try:
        return int(value)
    except ValueError:
        return 0


def _safe_float(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0


@dataclass
class Metric:
    label: str
    value: str
    detail: str = ""
    status: str = "neutral"

    def as_dict(self) -> dict[str, str]:
        return {
            "label": self.label,
            "value": self.value,
            "detail": self.detail,
            "status": self.status,
        }


def collect_system_metrics() -> dict[str, Any]:
    uptime_seconds = 0.0
    load_averages = [0.0, 0.0, 0.0]
    if hasattr(os, "getloadavg"):
        load_averages = [round(value, 2) for value in os.getloadavg()]

    meminfo: dict[str, int] = {}
    if os.path.exists("/proc/meminfo"):
        with open("/proc/meminfo", "r", encoding="utf-8") as handle:
            for line in handle:
                key, raw_value = line.split(":", 1)
                meminfo[key] = _safe_int(raw_value.strip().split()[0]) * 1024

    if os.path.exists("/proc/uptime"):
        with open("/proc/uptime", "r", encoding="utf-8") as handle:
            uptime_seconds = _safe_float(handle.read().split()[0])

    total = meminfo.get("MemTotal", 0)
    available = meminfo.get("MemAvailable", 0)
    used = max(total - available, 0)
    memory_percent = round((used / total) * 100, 1) if total else 0.0

    statvfs = os.statvfs("/")
    disk_total = statvfs.f_blocks * statvfs.f_frsize
    disk_free = statvfs.f_bavail * statvfs.f_frsize
    disk_used = disk_total - disk_free
    disk_percent = round((disk_used / disk_total) * 100, 1) if disk_total else 0.0

    return {
        "hostname": os.uname().nodename,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "uptime_seconds": int(uptime_seconds),
        "load_average": load_averages,
        "cpu_count": os.cpu_count() or 0,
        "memory": {
            "total_bytes": total,
            "used_bytes": used,
            "available_bytes": available,
            "percent_used": memory_percent,
        },
        "disk_root": {
            "total_bytes": disk_total,
            "used_bytes": disk_used,
            "free_bytes": disk_free,
            "percent_used": disk_percent,
        },
    }


def collect_slurm_metrics() -> dict[str, Any]:
    sinfo = _run_command(["sinfo", "--Node", "--Format=%n|%t|%C|%m|%f"])
    squeue = _run_command(["squeue", "--noheader", "--Format=%i|%T|%u|%P|%M|%D|%R"])

    nodes: list[dict[str, str]] = []
    node_states: dict[str, int] = {}
    total_alloc = 0
    total_idle = 0
    total_other = 0

    if sinfo["ok"] and sinfo["stdout"]:
        for line in sinfo["stdout"].splitlines():
            node_name, state, cpu_field, memory_mb, features = (line.split("|") + ["", "", "", "", ""])[:5]
            alloc, idle, other, total = [_safe_int(part) for part in cpu_field.split("/")[:4]]
            total_alloc += alloc
            total_idle += idle
            total_other += other
            state_key = state.lower()
            node_states[state_key] = node_states.get(state_key, 0) + 1
            nodes.append(
                {
                    "name": node_name,
                    "state": state,
                    "cpu_allocated": str(alloc),
                    "cpu_idle": str(idle),
                    "cpu_other": str(other),
                    "cpu_total": str(total),
                    "memory_mb": memory_mb,
                    "features": features,
                }
            )

    jobs: list[dict[str, str]] = []
    queue_status: dict[str, int] = {}
    queue_by_user: dict[str, int] = {}
    if squeue["ok"] and squeue["stdout"]:
        for line in squeue["stdout"].splitlines():
            job_id, state, owner, partition, elapsed, nodes_count, reason = (line.split("|") + ["", "", "", "", "", "", ""])[:7]
            queue_status[state] = queue_status.get(state, 0) + 1
            queue_by_user[owner] = queue_by_user.get(owner, 0) + 1
            jobs.append(
                {
                    "job_id": job_id,
                    "state": state,
                    "user": owner,
                    "partition": partition,
                    "elapsed": elapsed,
                    "nodes": nodes_count,
                    "reason": reason,
                }
            )

    return {
        "available": sinfo["ok"] or squeue["ok"],
        "scheduler": "slurm",
        "commands": {
            "sinfo": sinfo,
            "squeue": squeue,
        },
        "nodes": nodes,
        "node_states": node_states,
        "cpu": {
            "allocated": total_alloc,
            "idle": total_idle,
            "other": total_other,
        },
        "jobs": jobs,
        "queue_status": queue_status,
        "queue_by_user": queue_by_user,
    }


def _human_bytes(value: int) -> str:
    if value <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    power = min(int(math.log(value, 1024)), len(units) - 1)
    scaled = value / (1024**power)
    return f"{scaled:.1f} {units[power]}"


def _format_duration(seconds: int) -> str:
    minutes, _ = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def collect_live_snapshot() -> dict[str, Any]:
    system = collect_system_metrics()
    scheduler = collect_slurm_metrics()
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "system": system,
        "scheduler": scheduler,
    }


def build_dashboard_payload() -> dict[str, Any]:
    return build_dashboard_payload_from_snapshot(collect_live_snapshot())


def build_dashboard_payload_from_snapshot(
    snapshot: dict[str, Any],
    history: dict[str, Any] | None = None,
) -> dict[str, Any]:
    system = snapshot["system"]
    scheduler = snapshot["scheduler"]

    metrics = [
        Metric(
            label="Host",
            value=system["hostname"],
            detail=f"Uptime {_format_duration(system['uptime_seconds'])}",
        ),
        Metric(
            label="Load Avg",
            value=" / ".join(str(item) for item in system["load_average"]),
            detail=f"{system['cpu_count']} logical CPUs",
            status="warn" if system["load_average"][0] > system["cpu_count"] else "good",
        ),
        Metric(
            label="Memory",
            value=f"{system['memory']['percent_used']}%",
            detail=f"{_human_bytes(system['memory']['used_bytes'])} / {_human_bytes(system['memory']['total_bytes'])}",
            status="warn" if system["memory"]["percent_used"] > 85 else "good",
        ),
        Metric(
            label="Root Disk",
            value=f"{system['disk_root']['percent_used']}%",
            detail=f"{_human_bytes(system['disk_root']['used_bytes'])} / {_human_bytes(system['disk_root']['total_bytes'])}",
            status="warn" if system["disk_root"]["percent_used"] > 85 else "good",
        ),
    ]

    if scheduler["available"]:
        metrics.extend(
            [
                Metric(
                    label="Queued Jobs",
                    value=str(sum(scheduler["queue_status"].values())),
                    detail=", ".join(f"{key}:{value}" for key, value in sorted(scheduler["queue_status"].items())),
                ),
                Metric(
                    label="Allocated CPUs",
                    value=str(scheduler["cpu"]["allocated"]),
                    detail=f"idle {scheduler['cpu']['idle']}, other {scheduler['cpu']['other']}",
                ),
            ]
        )
    else:
        metrics.append(
            Metric(
                label="Scheduler",
                value="Unavailable",
                detail="sinfo/squeue not found or not accessible from this host",
                status="warn",
            )
        )

    top_users = sorted(
        scheduler["queue_by_user"].items(),
        key=lambda item: item[1],
        reverse=True,
    )[:8]

    return {
        "generated_at": snapshot["generated_at"],
        "system": system,
        "scheduler": scheduler,
        "metrics": [metric.as_dict() for metric in metrics],
        "top_users": [{"user": user, "jobs": jobs} for user, jobs in top_users],
        "history": history or {},
    }


def dashboard_payload_json() -> bytes:
    return json.dumps(build_dashboard_payload()).encode("utf-8")
