from __future__ import annotations

import json
import math
import os
import re
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
    stderr = completed.stderr.strip()
    has_error_output = "error:" in stderr.lower()
    return {
        "ok": completed.returncode == 0 and not has_error_output,
        "command": " ".join(command),
        "stdout": completed.stdout.strip(),
        "stderr": stderr,
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


def _parse_key_value_fields(line: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for token in line.split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        fields[key] = value
    return fields


def _parse_gpu_total(gres_value: str) -> int:
    total = 0
    if not gres_value or gres_value == "(null)":
        return 0
    for item in gres_value.split(","):
        cleaned = item.split("(")[0].strip()
        if not cleaned.startswith("gpu"):
            continue
        parts = cleaned.split(":")
        if len(parts) >= 2 and parts[-1].isdigit():
            total += _safe_int(parts[-1])
        else:
            total += 1
    return total


def _parse_gpu_allocated(alloc_tres_value: str) -> int:
    total = 0
    if not alloc_tres_value:
        return 0
    for match in re.finditer(r"gres/gpu(?:[:/][^=,]+)?=(\d+)", alloc_tres_value):
        total += _safe_int(match.group(1))
    return total


def collect_nvidia_metrics() -> dict[str, Any]:
    gpu_query = _run_command(
        [
            "nvidia-smi",
            "--query-gpu=index,name,uuid,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw",
            "--format=csv,noheader,nounits",
        ]
    )
    process_query = _run_command(
        [
            "nvidia-smi",
            "--query-compute-apps=gpu_uuid,pid,process_name,used_gpu_memory",
            "--format=csv,noheader,nounits",
        ]
    )

    gpus: list[dict[str, str]] = []
    processes: list[dict[str, str]] = []
    total_utilization = 0.0
    total_memory_percent = 0.0

    if gpu_query["ok"] and gpu_query["stdout"]:
        for raw_line in gpu_query["stdout"].splitlines():
            parts = [part.strip() for part in raw_line.split(",")]
            if len(parts) < 8:
                continue
            index, name, uuid, utilization, memory_used, memory_total, temperature, power_draw = parts[:8]
            utilization_value = _safe_float(utilization)
            memory_used_value = _safe_float(memory_used)
            memory_total_value = _safe_float(memory_total)
            memory_percent = round((memory_used_value / memory_total_value) * 100, 1) if memory_total_value else 0.0
            total_utilization += utilization_value
            total_memory_percent += memory_percent
            gpus.append(
                {
                    "index": index,
                    "name": name,
                    "uuid": uuid,
                    "utilization_gpu": f"{utilization_value:.1f}",
                    "memory_used_mb": f"{memory_used_value:.1f}",
                    "memory_total_mb": f"{memory_total_value:.1f}",
                    "memory_percent": f"{memory_percent:.1f}",
                    "temperature_c": temperature,
                    "power_draw_w": power_draw,
                }
            )

    if process_query["ok"] and process_query["stdout"]:
        for raw_line in process_query["stdout"].splitlines():
            parts = [part.strip() for part in raw_line.split(",")]
            if len(parts) < 4:
                continue
            gpu_uuid, pid, process_name, used_gpu_memory = parts[:4]
            processes.append(
                {
                    "gpu_uuid": gpu_uuid,
                    "pid": pid,
                    "process_name": process_name,
                    "used_gpu_memory_mb": used_gpu_memory,
                }
            )

    visible_gpus = len(gpus)
    average_utilization = round(total_utilization / visible_gpus, 1) if visible_gpus else 0.0
    average_memory_percent = round(total_memory_percent / visible_gpus, 1) if visible_gpus else 0.0

    return {
        "available": gpu_query["ok"] and visible_gpus > 0,
        "commands": {
            "gpu_query": gpu_query,
            "process_query": process_query,
        },
        "visible_gpus": visible_gpus,
        "average_utilization_gpu": average_utilization,
        "average_memory_percent": average_memory_percent,
        "gpus": gpus,
        "processes": processes,
    }


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
    sinfo = _run_command(["sinfo", "--noheader", "--Node", "-o", "%N|%t|%C|%m|%f"])
    squeue = _run_command(["squeue", "--noheader", "-o", "%i|%T|%u|%P|%M|%D|%R"])
    scontrol_nodes = _run_command(["scontrol", "show", "nodes", "-o"])

    nodes: list[dict[str, str]] = []
    node_states: dict[str, int] = {}
    total_alloc = 0
    total_idle = 0
    total_other = 0
    gpu_details_by_node: dict[str, dict[str, int]] = {}
    gpu_total = 0
    gpu_allocated = 0
    gpu_nodes = 0

    if scontrol_nodes["ok"] and scontrol_nodes["stdout"]:
        for line in scontrol_nodes["stdout"].splitlines():
            fields = _parse_key_value_fields(line)
            node_name = fields.get("NodeName", "")
            if not node_name:
                continue
            node_gpu_total = _parse_gpu_total(fields.get("Gres", ""))
            node_gpu_allocated = _parse_gpu_allocated(fields.get("AllocTRES", ""))
            gpu_details_by_node[node_name] = {
                "gpu_total": node_gpu_total,
                "gpu_allocated": node_gpu_allocated,
                "gpu_idle": max(node_gpu_total - node_gpu_allocated, 0),
            }
            gpu_total += node_gpu_total
            gpu_allocated += node_gpu_allocated
            if node_gpu_total > 0:
                gpu_nodes += 1

    if sinfo["ok"] and sinfo["stdout"]:
        for line in sinfo["stdout"].splitlines():
            node_name, state, cpu_field, memory_mb, features = (line.split("|") + ["", "", "", "", ""])[:5]
            if node_name.upper() == "NODELIST" or memory_mb.upper() == "MEMORY":
                continue
            alloc, idle, other, total = [_safe_int(part) for part in cpu_field.split("/")[:4]]
            gpu_detail = gpu_details_by_node.get(node_name, {"gpu_total": 0, "gpu_allocated": 0, "gpu_idle": 0})
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
                    "gpu_total": str(gpu_detail["gpu_total"]),
                    "gpu_allocated": str(gpu_detail["gpu_allocated"]),
                    "gpu_idle": str(gpu_detail["gpu_idle"]),
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
            "scontrol_nodes": scontrol_nodes,
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
        "gpu": {
            "total": gpu_total,
            "allocated": gpu_allocated,
            "idle": max(gpu_total - gpu_allocated, 0),
            "gpu_nodes": gpu_nodes,
            "available": scontrol_nodes["ok"] and gpu_total > 0,
        },
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
    nvidia = collect_nvidia_metrics()
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "system": system,
        "scheduler": scheduler,
        "nvidia": nvidia,
    }


def build_dashboard_payload() -> dict[str, Any]:
    return build_dashboard_payload_from_snapshot(collect_live_snapshot())


def build_dashboard_payload_from_snapshot(
    snapshot: dict[str, Any],
    history: dict[str, Any] | None = None,
) -> dict[str, Any]:
    system = snapshot["system"]
    scheduler = snapshot["scheduler"]
    nvidia = snapshot.get("nvidia", {})

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
        if scheduler["gpu"]["total"] > 0:
            metrics.append(
                Metric(
                    label="Allocated GPUs",
                    value=str(scheduler["gpu"]["allocated"]),
                    detail=f"idle {scheduler['gpu']['idle']} / total {scheduler['gpu']['total']}",
                    status="warn" if scheduler["gpu"]["idle"] == 0 else "good",
                )
            )
        if nvidia.get("available"):
            metrics.append(
                Metric(
                    label="Host GPU Util",
                    value=f"{nvidia['average_utilization_gpu']}%",
                    detail=f"{nvidia['visible_gpus']} visible GPUs, mem {nvidia['average_memory_percent']}%",
                    status="warn" if nvidia["average_utilization_gpu"] > 95 else "good",
                )
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
        "nvidia": nvidia,
        "metrics": [metric.as_dict() for metric in metrics],
        "top_users": [{"user": user, "jobs": jobs} for user, jobs in top_users],
        "history": history or {},
    }


def dashboard_payload_json() -> bytes:
    return json.dumps(build_dashboard_payload()).encode("utf-8")
