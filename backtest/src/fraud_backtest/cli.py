"""Commands of the M6b job: ``publish`` a dataset, ``run`` a backtest, ``drift`` report (``make backtest``).

Runs on the host (``uv run backtest ...``) or as a one-off container (``docker compose --profile tools run
--rm backtest ...``): the same code, since it only talks to MinIO, MLflow and Postgres over the network.
"""
import argparse
import io
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import polars as pl
from fraud_training.dataset import build_dataset, temporal_split

from fraud_backtest.datalake import Lake, client_from_env
from fraud_backtest.drift_report import drift_report
from fraud_backtest.job import backtest, load_registry_models, to_markdown
from fraudcore.features import FEATURE_ORDER

DEFAULT_KEY = "training/history_full.parquet"


def lake_from_env() -> Lake:
    return Lake(client_from_env(dict(os.environ)), os.environ.get("S3_BUCKET", "fraud-data"))


def cmd_publish(a: argparse.Namespace) -> None:
    info = lake_from_env().publish(a.path, a.key)
    print(f"published {info['key']}  version {info['version_id']}  sha256 {info['sha256'][:16]}...  "
          f"{info['size'] / 1e6:.1f} MB")


def _history(lake: Lake, key: str, version_id: str | None) -> tuple[pl.DataFrame, str]:
    data = lake.fetch_verified(key, version_id)                       # raises if the bytes do not match
    version = version_id or lake._client.head_object(Bucket=lake._bucket, Key=key)["VersionId"]
    return pl.read_parquet(io.BytesIO(data)), version


def cmd_run(a: argparse.Namespace) -> None:
    lake = lake_from_env()
    history, version = _history(lake, a.key, a.version_id)
    models = load_registry_models(a.tracking_uri, a.model)
    if not models:
        sys.exit("no model in the registry has the alias champion or challenger")
    df = backtest(history, models, a.valid_days, a.test_days)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    title = (f"Backtest of {', '.join(f'{m.name} v{m.version}' for m in models)} on {a.key} "
             f"(version {version[:8]}, {a.test_days} test days)")
    md = to_markdown(df, title)
    lake.put_report(f"backtest-{stamp}.md", md.encode())
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(md, encoding="utf-8")
    print(md)


def _served_features(dsn: str, limit: int) -> pd.DataFrame:
    import psycopg

    with psycopg.connect(dsn) as conn:
        rows = conn.execute("SELECT features FROM decisions WHERE features IS NOT NULL "
                            "ORDER BY decided_at DESC LIMIT %s", (limit,)).fetchall()
    return pd.DataFrame([r[0] for r in rows], columns=FEATURE_ORDER).astype(float)


def cmd_drift(a: argparse.Namespace) -> None:
    lake = lake_from_env()
    history, _ = _history(lake, a.key, a.version_id)
    train, _, _ = temporal_split(build_dataset(history), a.valid_days, a.test_days)
    reference = train.select(FEATURE_ORDER).to_pandas().astype(float).sample(
        n=min(a.reference_rows, train.height), random_state=0)
    current = _served_features(a.dsn, a.window)
    if current.empty:
        sys.exit("the audit store has no served features (start the scorer with LOG_FEATURES=1)")
    result = drift_report(reference, current)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    lake.put_report(f"drift-{stamp}.html", result.html.encode())
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(result.html, encoding="utf-8")
    print(f"{len(current)} served rows vs {len(reference)} training rows")
    print(f"drifted features ({result.share:.0%}): {', '.join(result.drifted) or 'none'}")
    for name, p in sorted(result.p_values.items(), key=lambda kv: kv[1])[:6]:
        print(f"  {name:22s} K-S p-value {p:.2g}")
    print(f"report: {a.out}")


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("publish", help="upload a dataset as a new object version with a manifest")
    p.add_argument("path", type=Path)
    p.add_argument("--key", default=DEFAULT_KEY)
    p.set_defaults(fn=cmd_publish)

    def common(q: argparse.ArgumentParser) -> None:
        q.add_argument("--key", default=DEFAULT_KEY)
        q.add_argument("--version-id", default=None, help="object version (default: the latest)")
        q.add_argument("--valid-days", type=int, default=10)
        q.add_argument("--test-days", type=int, default=10)

    r = sub.add_parser("run", help="backtest champion and challenger on a versioned dataset")
    common(r)
    r.add_argument("--tracking-uri", default=os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    r.add_argument("--model", default="fraud-champion")
    r.add_argument("--out", type=Path, default=Path("docs/results/m6b-backtest.md"))
    r.set_defaults(fn=cmd_run)

    d = sub.add_parser("drift", help="Evidently report: training features vs the ones served")
    common(d)
    d.add_argument("--dsn", default=os.environ.get("POSTGRES_DSN", "postgresql://fraud:fraud@localhost:5432/fraud"))
    d.add_argument("--window", type=int, default=5000, help="most recent served rows to compare")
    d.add_argument("--reference-rows", type=int, default=20000)
    d.add_argument("--out", type=Path, default=Path("docs/results/m6b-drift-report.html"))
    d.set_defaults(fn=cmd_drift)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
