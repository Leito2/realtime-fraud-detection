"""DecisionLookup over the audit store (PostgreSQL): what ``GetDecision`` answers for a dispute."""
import psycopg

from scorer.application.ports import StoredDecision

SQL = ("SELECT payment_id, decision, score, model_version, threshold_set, degraded "
       "FROM decisions WHERE payment_id = %s")


class PostgresDecisionLookup:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def get(self, payment_id: str) -> StoredDecision | None:
        with psycopg.connect(self._dsn, connect_timeout=2) as conn:
            row = conn.execute(SQL, (payment_id,)).fetchone()
        return StoredDecision(*row) if row else None
