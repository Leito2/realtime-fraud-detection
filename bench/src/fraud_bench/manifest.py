"""Run manifest: everything needed to reproduce or distrust a result (PLAN.md section 5.5)."""
from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import yaml

from fraud_bench.stack import ROOT


def _run(cmd: list[str]) -> str:
    try:
        return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, encoding="utf-8",
                              timeout=20).stdout.strip()
    except Exception:
        return ""


def _model_version(tracking_uri: str = "http://localhost:5000", name: str = "fraud-champion",
                   alias: str = "champion") -> str | None:
    q = urllib.parse.urlencode({"name": name, "alias": alias})
    try:
        with urllib.request.urlopen(f"{tracking_uri}/api/2.0/mlflow/registered-models/alias?{q}",
                                    timeout=3) as r:
            return str(json.load(r)["model_version"]["version"])
    except Exception:
        return None


def collect(run_id: str, experiment: str, params: dict) -> dict:
    """Facts about the machine, the code and the configuration under test."""
    docker = _run(["docker", "info", "--format", "{{json .}}"])
    info = json.loads(docker) if docker.startswith("{") else {}
    wslconfig = Path.home() / ".wslconfig"
    cfg = {name: yaml.safe_load((ROOT / "config" / name).read_text(encoding="utf-8"))
           for name in ("scorer.yaml", "thresholds.yaml") if (ROOT / "config" / name).exists()}
    return {
        "run_id": run_id,
        "experiment": experiment,
        "started_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "params": params,
        "git": {"sha": _run(["git", "rev-parse", "HEAD"]),
                "dirty": bool(_run(["git", "status", "--porcelain"]))},
        "host": {"os": platform.platform(), "cpu": platform.processor(), "cpu_count": os.cpu_count(),
                 "python": sys.version.split()[0],
                 "wslconfig": wslconfig.read_text(encoding="utf-8") if wslconfig.exists() else None},
        "docker": {"server_version": info.get("ServerVersion"), "ncpu": info.get("NCPU"),
                   "mem_total_bytes": info.get("MemTotal")},
        "model_version": _model_version(),
        "effective_config": cfg,
        "compose_images": _run(["docker", "compose", "--profile", "core", "images", "--format", "json"]),
    }
