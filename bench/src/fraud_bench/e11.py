"""Experiment E11 (PLAN.md 14.4): REST/JSON vs gRPC unary vs gRPC bidirectional streaming, same model.

    uv run python -m fraud_bench.e11 rest        --target http://localhost:8000 --rate 300 --duration 60
    uv run python -m fraud_bench.e11 grpc        --target localhost:50051       --rate 300 --duration 60
    uv run python -m fraud_bench.e11 grpc-stream --target localhost:50051       --rate 300 --duration 60

Open loop, like the Kafka path (PLAN.md 5): request ``i`` is *scheduled* at ``t0 + i / rate`` and its latency
runs from that instant, not from when the client managed to send it, so a slow server cannot hide its own
queueing (coordinated omission). The warm-up is discarded. Run it inside a Linux container
(`docker compose --profile tools run --rm e11 ...`): asyncio on Windows has a ~15 ms clock tick.
If the client itself fell behind its schedule
(p99 start lag over 5 ms) the run is marked invalid, the same rule the generator uses.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path

VALID_START_LAG_MS = 5.0


@dataclass
class Sample:
    scheduled: float
    started: float
    done: float = 0.0
    error: str | None = None

    @property
    def latency_ms(self) -> float:
        return (self.done - self.scheduled) * 1000

    @property
    def start_lag_ms(self) -> float:
        return (self.started - self.scheduled) * 1000


@dataclass
class Summary:
    mode: str
    rate: float
    count: int
    errors: dict[str, int]
    p50: float
    p95: float
    p99: float
    max: float
    start_lag_p99_ms: float
    valid: bool
    request_bytes: int | None = None
    response_bytes: int | None = None
    extra: dict = field(default_factory=dict)


def percentile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return float("nan")
    return sorted_values[min(len(sorted_values) - 1, int(q * len(sorted_values)))]


def summarize(mode: str, rate: float, samples: list[Sample], warmup_s: float = 0.0) -> Summary:
    t0 = min((s.scheduled for s in samples), default=0.0)
    kept = [s for s in samples if s.scheduled - t0 >= warmup_s]
    ok = sorted(s.latency_ms for s in kept if s.error is None)
    lags = sorted(s.start_lag_ms for s in kept)
    start_lag = percentile(lags, 0.99)
    errors = dict(Counter(s.error for s in kept if s.error))
    return Summary(mode=mode, rate=rate, count=len(kept), errors=errors,
                   p50=percentile(ok, 0.50), p95=percentile(ok, 0.95), p99=percentile(ok, 0.99),
                   max=ok[-1] if ok else float("nan"), start_lag_p99_ms=start_lag,
                   valid=bool(kept) and start_lag <= VALID_START_LAG_MS)


def fine_timers() -> None:
    """Windows rounds timer sleeps up to ~15.6 ms, which would make a 300/s schedule fire in bursts."""
    if sys.platform == "win32":
        import ctypes

        ctypes.windll.winmm.timeBeginPeriod(1)


async def open_loop(rate: float, duration_s: float, send: Callable[[int], Awaitable[None]]) -> list[Sample]:
    """Fire ``send(i)`` on a fixed schedule; latency is measured from the scheduled instant."""
    fine_timers()
    total = int(rate * duration_s)
    samples: list[Sample] = []
    tasks: list[asyncio.Task] = []

    async def one(i: int, scheduled: float) -> None:
        sample = Sample(scheduled=scheduled, started=time.perf_counter())
        samples.append(sample)
        try:
            await send(i)
        except Exception as exc:                          # the code of the failure is the result
            code = getattr(exc, "code", None)             # grpc.RpcError exposes the status code
            sample.error = str(code() if callable(code) else type(exc).__name__)
        sample.done = time.perf_counter()

    t0 = time.perf_counter() + 0.05
    for i in range(total):
        scheduled = t0 + i / rate
        delay = scheduled - time.perf_counter()
        if delay > 0:
            await asyncio.sleep(delay)
        tasks.append(asyncio.create_task(one(i, scheduled)))
    await asyncio.gather(*tasks)
    return samples


def payload_pool(n: int = 2000, users: int = 20_000, zipf: float = 0.7, seed: int = 11) -> list[dict]:
    """Realistic payments (generator habits and amounts), pre-built so the client does no work per call."""
    from generator.logic import Population, next_payment

    pop, rng = Population(users, seed=42, zipf=zipf), random.Random(seed)
    return [next_payment(pop, rng, 0, 0) for _ in range(n)]


def to_json_body(p: dict) -> dict:
    keys = ("payment_id", "user_id", "card_id", "amount", "currency", "merchant_id", "mcc", "country",
            "device_id", "ip_country", "channel")
    return {k: p[k] for k in keys}


def to_proto(p: dict):
    from google.protobuf.timestamp_pb2 import Timestamp

    from fraud.v1 import scoring_pb2 as pb

    ts = Timestamp()
    ts.GetCurrentTime()
    channel = {"web": pb.CHANNEL_WEB, "app": pb.CHANNEL_APP, "pos": pb.CHANNEL_POS}[p["channel"]]
    return pb.ScoreRequest(**{k: p[k] for k in ("payment_id", "user_id", "card_id", "amount", "currency",
                                                "merchant_id", "mcc", "country", "device_id", "ip_country")},
                           channel=channel, event_time=ts)


async def run_rest(target: str, pool: list[dict], rate: float,
                   duration_s: float) -> tuple[list[Sample], dict]:
    import httpx

    sizes: dict = {}
    limits = httpx.Limits(max_connections=200, max_keepalive_connections=200)
    async with httpx.AsyncClient(base_url=target, limits=limits, timeout=2.0) as client:
        sizes["request_bytes"] = len(json.dumps(to_json_body(pool[0])).encode())

        async def send(i: int) -> None:
            r = await client.post("/score", json=to_json_body(pool[i % len(pool)]))
            r.raise_for_status()
            sizes.setdefault("response_bytes", len(r.content))
        return await open_loop(rate, duration_s, send), sizes


async def run_grpc(target: str, pool: list[dict], rate: float, duration_s: float, api_key: str | None,
                   deadline_s: float) -> tuple[list[Sample], dict]:
    from fraud.client import api_key_metadata, open_aio_channel
    from fraud.v1 import scoring_pb2_grpc as pb_grpc

    requests = [to_proto(p) for p in pool]
    sizes = {"request_bytes": requests[0].ByteSize()}
    meta = api_key_metadata(api_key)
    async with open_aio_channel(target) as channel:
        stub = pb_grpc.FraudScoringStub(channel)

        async def send(i: int) -> None:
            r = await stub.Score(requests[i % len(requests)], timeout=deadline_s, metadata=meta)
            sizes.setdefault("response_bytes", r.ByteSize())
        return await open_loop(rate, duration_s, send), sizes


async def run_grpc_stream(target: str, pool: list[dict], rate: float, duration_s: float, api_key: str | None,
                          deadline_s: float) -> tuple[list[Sample], dict]:
    """One long-lived bidirectional stream; responses come back in request order."""
    from fraud.client import api_key_metadata, open_aio_channel
    from fraud.v1 import scoring_pb2_grpc as pb_grpc

    requests = [to_proto(p) for p in pool]
    sizes = {"request_bytes": requests[0].ByteSize()}
    pending: dict[int, asyncio.Future] = {}
    async with open_aio_channel(target) as channel:
        stub = pb_grpc.FraudScoringStub(channel)
        call = stub.ScoreStream(metadata=api_key_metadata(api_key))
        order: asyncio.Queue = asyncio.Queue()

        async def reader() -> None:
            async for response in call:
                sizes.setdefault("response_bytes", response.ByteSize())
                i = await order.get()
                future = pending.pop(i)
                if not future.done():                  # a response after the client's deadline is still read
                    future.set_result(None)

        reader_task = asyncio.create_task(reader())

        write_lock = asyncio.Lock()                    # grpc.aio allows one pending write per call

        async def send(i: int) -> None:
            future = asyncio.get_running_loop().create_future()
            pending[i] = future
            async with write_lock:                      # request order == response order on one stream
                order.put_nowait(i)
                try:
                    await call.write(requests[i % len(requests)])
                except BaseException:
                    order.get_nowait()                  # nothing was sent: keep order aligned with responses
                    raise
            await asyncio.wait_for(future, deadline_s)

        samples = await open_loop(rate, duration_s, send)
        await call.done_writing()
        reader_task.cancel()
        return samples, sizes


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["rest", "grpc", "grpc-stream"])
    ap.add_argument("--target", required=True)
    ap.add_argument("--rate", type=float, default=300)
    ap.add_argument("--duration", type=float, default=60)
    ap.add_argument("--warmup", type=float, default=10)
    ap.add_argument("--deadline-ms", type=float, default=250, help="client deadline (proto default: 50)")
    ap.add_argument("--api-key", default=None)
    ap.add_argument("--out", type=Path, default=None, help="append the summary to this JSON lines file")
    a = ap.parse_args()
    pool = payload_pool()
    deadline = a.deadline_ms / 1000
    if a.mode == "rest":
        samples, sizes = asyncio.run(run_rest(a.target, pool, a.rate, a.duration))
    elif a.mode == "grpc":
        samples, sizes = asyncio.run(run_grpc(a.target, pool, a.rate, a.duration, a.api_key, deadline))
    else:
        samples, sizes = asyncio.run(run_grpc_stream(a.target, pool, a.rate, a.duration, a.api_key, deadline))
    s = summarize(a.mode, a.rate, samples, a.warmup)
    s.request_bytes, s.response_bytes = sizes.get("request_bytes"), sizes.get("response_bytes")
    line = json.dumps(s.__dict__)
    print(line)
    if a.out:
        a.out.parent.mkdir(parents=True, exist_ok=True)
        with a.out.open("a", encoding="utf-8") as f:
            f.write(line + "\n")


if __name__ == "__main__":
    main()
