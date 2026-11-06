"""Parquet history -> profiles -> Redis (PLAN.md §3.4). Run on demand: `make profiles`."""
import argparse
import os
from datetime import UTC, datetime

import polars as pl
import redis

from fraudcore.profiles import build_profiles
from profile_builder.logic import batched, to_redis


def main() -> None:
    ap = argparse.ArgumentParser(description="Build user profiles into Redis")
    ap.add_argument("--history", required=True, help="Parquet written by generator --offline")
    ap.add_argument("--redis", default=os.environ.get("REDIS_URL", "redis://redis:6379/0"))
    ap.add_argument("--as-of", help="ISO timestamp; default now (UTC)")
    args = ap.parse_args()

    as_of = datetime.fromisoformat(args.as_of) if args.as_of else datetime.now(UTC)
    history = pl.scan_parquet(args.history).select(
        "user_id", "amount", "country", "device_id", "event_time").collect()
    profiles = build_profiles(history, as_of)

    r = redis.Redis.from_url(args.redis)
    written = 0
    for chunk in batched(profiles.iter_rows(named=True), 1000):
        pipe = r.pipeline(transaction=False)
        for row in chunk:
            key, mapping = to_redis(row)
            pipe.hset(key, mapping=mapping)
        pipe.execute()
        written += len(chunk)
    print(f"profiles written: {written}", flush=True)


if __name__ == "__main__":
    main()
