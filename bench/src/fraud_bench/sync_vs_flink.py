"""How far is synchronous (REST/gRPC) velocity from Flink's window? ADR-0012 promised a measured answer.

    uv run python -m fraud_bench.sync_vs_flink --delays 0 0.5 1 2 --out docs/results/m7c-sync-vs-flink

The synchronous path has no window of its own: it takes the Feature Writer's stored state for the user (the
Flink window as of the user's previous payment) and adds the current payment (``fraudcore.sync_velocity``).
Two things make that differ from the Flink row for the same payment:

* **expiry**: payments that left the 10-minute window since the previous payment are still in the stored
  state (sync over-counts);
* **writer delay**: the previous payment's state reaches Redis ``writer_delay_s`` after it happened; a payment
  that arrives sooner does not see it (sync under-counts).

Both are replayed here on the simulated 60-day history, with Flink's window taken from
``fraudcore.velocity_reference`` (which the parity test pins to the real Flink job).
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from fraudcore.features import sync_velocity, velocity_reference

FIELDS = ("f_cnt_10m", "f_sum_10m", "f_max_10m", "f_small_tx_cnt_10m")


def residuals(events: list[dict], writer_delay_s: float) -> list[dict]:
    """Per event: sync velocity minus the Flink-equivalent velocity. ``events`` are in arrival order."""
    reference = velocity_reference(events)
    stored: dict[str, list[tuple[float, dict]]] = {}        # per user: (visible_at, state)
    out = []
    for e, ref in zip(events, reference, strict=True):
        states = stored.setdefault(e["user_id"], [])
        snapshot = None
        for visible_at, state in reversed(states):
            if visible_at <= e["ts"]:
                snapshot = state
                break
        sync = sync_velocity(snapshot, e["amount"], e["ts"])
        out.append({
            "cnt_error": sync["f_cnt_10m"] - ref["f_cnt_10m"],
            "sum_error": round(sync["f_sum_10m"] - ref["f_sum_10m"], 6),
            "max_error": round(sync["f_max_10m"] - ref["f_max_10m"], 6),
            "small_error": sync["f_small_tx_cnt_10m"] - ref["f_small_tx_cnt_10m"],
        })
        states.append((e["ts"] + writer_delay_s, {**ref, "ts": e["ts"]}))
        del states[:-8]                                      # only the most recent states can be visible
    return out


def summarize(rows: list[dict]) -> dict:
    cnt = np.array([r["cnt_error"] for r in rows])
    exact = [r["cnt_error"] == 0 and r["sum_error"] == 0 and r["max_error"] == 0 and r["small_error"] == 0
             for r in rows]
    return {
        "events": len(rows), "exact_share": sum(exact) / len(rows),
        "cnt_error_min": int(cnt.min()), "cnt_error_max": int(cnt.max()),
        "cnt_error_mean": float(cnt.mean()), "cnt_abs_error_p99": float(np.percentile(np.abs(cnt), 99)),
        "over_count_share": float((cnt > 0).mean()), "under_count_share": float((cnt < 0).mean()),
    }


def load_events(path: str, limit: int | None) -> list[dict]:
    import polars as pl

    frame = pl.read_parquet(path).select("user_id", "event_time", "amount").sort("event_time")
    if limit:
        frame = frame.head(limit)
    ts = frame["event_time"].dt.epoch("us").to_numpy() / 1e6
    return [{"user_id": u, "ts": float(t), "amount": float(a)}
            for u, t, a in zip(frame["user_id"], ts, frame["amount"], strict=True)]


def main() -> None:
    ap = argparse.ArgumentParser(description="sync (REST/gRPC) velocity vs the Flink window")
    ap.add_argument("--data", default="data/dataset_cache.parquet")
    ap.add_argument("--delays", type=float, nargs="+", default=[0.0, 0.5, 1.0, 2.0])
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--out", default="docs/results/m7c-sync-vs-flink")
    args = ap.parse_args()
    events = load_events(args.data, args.limit)
    results = {}
    for delay in args.delays:
        results[str(delay)] = summarize(residuals(events, delay))
        r = results[str(delay)]
        print(f"writer delay {delay:4.1f} s: exact {r['exact_share']:.4f}  over {r['over_count_share']:.4f}"
              f"  under {r['under_count_share']:.4f}  |cnt err| p99 {r['cnt_abs_error_p99']:.0f}", flush=True)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.with_suffix(".json").write_text(json.dumps({"events": len(events), "by_writer_delay_s": results},
                                                   indent=2), encoding="utf-8")
    lines = ["# Synchronous velocity vs the Flink window (ADR-0012)", "",
             f"{len(events):,} simulated payments replayed per writer delay. `exact` = all four velocity "
             "features equal to Flink's. `over` / `under` = share of payments where the count is too high / "
             "too low.", "",
             "| writer delay (s) | exact | over-count | under-count | mean count error | p99 abs error |",
             "|---|---|---|---|---|---|"]
    for delay, r in results.items():
        lines.append(f"| {delay} | {r['exact_share']:.2%} | {r['over_count_share']:.2%} | "
                     f"{r['under_count_share']:.2%} | {r['cnt_error_mean']:+.4f} | "
                     f"{r['cnt_abs_error_p99']:.0f} |")
    out.with_suffix(".md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {out.with_suffix('.md')}")


if __name__ == "__main__":
    main()
