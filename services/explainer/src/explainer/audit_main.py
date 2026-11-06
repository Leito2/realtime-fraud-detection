"""CLI: audit the LLM explanations stored in Postgres.  ``uv run explainer-audit --limit 500``."""
import argparse
import os
import sys

import psycopg

from explainer.adapters.mlflow_booster import MlflowBoosterSource
from explainer.adapters.xgboost_shap import XgboostShap
from explainer.application.audit import AuditRow, audit

SQL = """
SELECT e.payment_id, split_part(d.model_version, ':', 2), d.score, e.status, e.text, d.features
FROM explanations e JOIN decisions d USING (payment_id)
WHERE e.status = 'llm' ORDER BY e.created_at DESC LIMIT %s
"""


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--dsn", default=os.environ.get("POSTGRES_DSN", "postgresql://fraud:fraud@localhost:5432/fraud"))
    ap.add_argument("--tracking-uri", default=os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    args = ap.parse_args()
    with psycopg.connect(args.dsn) as conn:
        rows = [AuditRow(payment_id=p, model_version=v, score=float(s), status=st, text=t or "", features=f)
                for p, v, s, st, t, f in conn.execute(SQL, (args.limit,)).fetchall()]
    report = audit(rows, XgboostShap(MlflowBoosterSource(args.tracking_uri, "fraud-champion")))
    print(f"audited={report.audited} unsupported={report.unsupported} "
          f"rate={report.unsupported_claim_rate:.1%} not_auditable={report.not_auditable}")
    for payment_id, claims in report.examples:
        print(f"  {payment_id}: {', '.join(claims)}")


if __name__ == "__main__":
    main()
