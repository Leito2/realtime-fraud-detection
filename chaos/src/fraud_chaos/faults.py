"""Fault injectors. Each one returns a short description of what it did, for the report."""
from __future__ import annotations

import json
import random
import time
import urllib.request

from fraud_bench.stack import COMPOSE, TOOLS, sh

TOXIPROXY = "http://localhost:8474"


def _compose(*args: str) -> str:
    r = sh([*COMPOSE, *args])
    if r.returncode:
        raise RuntimeError(f"docker compose {' '.join(args)} failed: {r.stderr.strip()[:300]}")
    return r.stdout.strip()


def kill(service: str, restart_after_s: float | None = 3, **_) -> str:
    """SIGKILL a service. Docker does not apply a restart policy to a container that `docker kill` stopped
    (it counts as a manual stop), so the injector plays the orchestrator: it starts the container again after
    ``restart_after_s``. ``None`` leaves it dead for the whole run."""
    _compose("kill", service)                                   # SIGKILL: no graceful shutdown
    if restart_after_s is None:
        return f"SIGKILL to {service}; it stays down"
    time.sleep(restart_after_s)
    _compose("start", service)
    return f"SIGKILL to {service}; the orchestrator (this tool) starts it again after {restart_after_s:g} s"


def stop_then_start(service: str, down_s: float = 20, **_) -> str:
    _compose("stop", service)
    time.sleep(down_s)
    _compose("start", service)
    return f"{service} stopped for {down_s:.0f} s, then started"


def scale_kill(service: str, replicas: int = 2, **_) -> str:
    """Run N copies, then kill one of them (a process dying, not the service)."""
    _compose("up", "-d", "--scale", f"{service}={replicas}", "--no-recreate", service)
    time.sleep(15)                                              # the group settles
    ids = _compose("ps", "-q", service).split()
    victim = sorted(ids)[0]
    sh(["docker", "kill", victim])
    return f"{replicas} {service} processes were running; SIGKILL to {victim[:12]}"


def _toxiproxy(method: str, path: str, body: dict | None = None) -> dict:
    req = urllib.request.Request(f"{TOXIPROXY}{path}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        text = r.read().decode()
    return json.loads(text) if text else {}


def toxic(proxy: str, kind: str, attributes: dict, duration_s: float, **_) -> str:
    """Add a Toxiproxy toxic for a while (latency, timeout, bandwidth, ...) and remove it."""
    name = f"chaos-{kind}"
    _toxiproxy("POST", f"/proxies/{proxy}/toxics",
               {"name": name, "type": kind, "stream": "downstream", "toxicity": 1.0,
               "attributes": attributes})
    time.sleep(duration_s)
    _toxiproxy("DELETE", f"/proxies/{proxy}/toxics/{name}")
    return f"Toxiproxy {kind} {attributes} on {proxy} for {duration_s:.0f} s"


def proxy_down(proxy: str, duration_s: float, **_) -> str:
    _toxiproxy("POST", f"/proxies/{proxy}", {"enabled": False})
    time.sleep(duration_s)
    _toxiproxy("POST", f"/proxies/{proxy}", {"enabled": True})
    return f"Toxiproxy {proxy} disabled for {duration_s:.0f} s (connection refused)"


POISON = [
    b"this is not json",
    b"{\"payment_id\": \"x\", \"amount\": ",                        # truncated
    b"{\"payment_id\": \"p-bad-1\", \"user_id\": \"u_000001\", \"amount\": \"abc\"}",     # wrong type
    b"\x00\xff\xfe binary junk",
    b"{}",
]


def poison(topic: str, count: int, bootstrap: str = "localhost:9092", **_) -> str:
    from confluent_kafka import Producer

    p = Producer({"bootstrap.servers": bootstrap})
    rng = random.Random(7)
    for i in range(count):
        p.produce(topic, key=f"poison-{i}", value=rng.choice(POISON))
    p.flush(10)
    return f"{count} malformed records written to {topic}"


def overload(rate: int, duration_s: float, **_) -> str:
    """A second generator on top of the steady one: the load doubles for a while."""
    r = sh([*TOOLS, "generator", "--rate", str(rate), "--duration", str(int(duration_s)), "--stream-seed",
            str(random.randint(1, 10**6)), "--users", "20000", "--zipf", "0.7"])
    if r.returncode:
        raise RuntimeError(r.stderr[:300])
    return f"extra {rate} ev/s for {duration_s:.0f} s on top of the steady load"


def mlflow_down_and_corrupt_model(down_s: float = 60, **_) -> str:
    """MLflow stops and a corrupt model is registered under the champion alias, then MLflow returns."""
    from fraud_chaos.registry import register_corrupt_champion

    _compose("stop", "mlflow")
    time.sleep(down_s / 2)
    _compose("start", "mlflow")
    time.sleep(10)
    version = register_corrupt_champion()
    time.sleep(down_s / 2)
    return f"MLflow down for {down_s / 2:.0f} s; then corrupt version {version} set as champion"


INJECTORS = {"kill": kill, "stop": stop_then_start, "scale_kill": scale_kill, "toxic": toxic,
             "proxy_down": proxy_down, "poison": poison, "overload": overload,
             "mlflow_corrupt": mlflow_down_and_corrupt_model}


def inject(fault: dict) -> str:
    spec = dict(fault)
    kind = spec.pop("type")
    return INJECTORS[kind](**spec)
