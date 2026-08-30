"""Read-only environment diagnostics used by ``viskium doctor``."""

from __future__ import annotations

import os
import platform
import shutil
import stat
import sys
from pathlib import Path
from typing import Any

from viskium.config import EffectiveConfig


def _nearest_existing_directory(path: Path) -> Path | None:
    candidate = path
    while True:
        try:
            if candidate.is_dir():
                return candidate
        except OSError:
            return None
        if candidate.parent == candidate:
            return None
        candidate = candidate.parent


def _disk_report(path: Path) -> dict[str, Any]:
    probe_path = _nearest_existing_directory(path)
    if probe_path is None:
        return {
            "available": False,
            "probe_path": None,
            "total_bytes": None,
            "used_bytes": None,
            "free_bytes": None,
            "reason": "no_existing_ancestor",
        }

    try:
        usage = shutil.disk_usage(probe_path)
    except OSError as error:
        return {
            "available": False,
            "probe_path": str(probe_path),
            "total_bytes": None,
            "used_bytes": None,
            "free_bytes": None,
            "reason": f"disk_usage_failed:{type(error).__name__}",
        }

    return {
        "available": True,
        "probe_path": str(probe_path),
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "reason": None,
    }


def build_doctor_report(config: EffectiveConfig) -> dict[str, Any]:
    """Inspect the host without creating paths, opening devices, or writing probes."""

    data_root = config.storage.root
    inspection_error: str | None = None
    try:
        path_stat = data_root.stat()
    except FileNotFoundError:
        exists = False
        is_directory = None
    except OSError as error:
        exists = False
        is_directory = None
        inspection_error = type(error).__name__
    else:
        exists = True
        is_directory = stat.S_ISDIR(path_stat.st_mode)

    disk = _disk_report(data_root)
    issues: list[dict[str, str]] = []
    if inspection_error is not None:
        issues.append(
            {
                "severity": "error",
                "code": "data_root_inspection_failed",
                "message": f"The effective data root could not be inspected: {inspection_error}.",
            }
        )
    elif exists and not is_directory:
        issues.append(
            {
                "severity": "error",
                "code": "data_root_not_directory",
                "message": "The effective data root exists but is not a directory.",
            }
        )
    elif not exists:
        issues.append(
            {
                "severity": "warning",
                "code": "data_root_missing",
                "message": "The effective data root does not exist; doctor left it unchanged.",
            }
        )
    if not disk["available"]:
        issues.append(
            {
                "severity": "warning",
                "code": "volume_space_unavailable",
                "message": "Free space could not be resolved for the effective data root.",
            }
        )

    if any(issue["severity"] == "error" for issue in issues):
        status = "error"
    elif issues:
        status = "warning"
    else:
        status = "ok"

    return {
        "schema_version": 1,
        "status": status,
        "read_only": True,
        "python": {
            "version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "executable": sys.executable,
        },
        "system": {
            "os": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "cpu_count": os.cpu_count(),
        },
        "data_root": {
            "path": str(data_root),
            "source": config.storage.root_source,
            "exists": exists,
            "is_directory": is_directory,
            "disk": disk,
        },
        "config": {
            "path": str(config.config_file),
            "loaded": config.config_loaded,
        },
        "issues": issues,
    }


__all__ = ["build_doctor_report"]
