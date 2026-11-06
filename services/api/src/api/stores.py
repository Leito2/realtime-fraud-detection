"""Adapters behind the API's protocols: Postgres (audit store), Redis (online features), MLflow (registry)."""
import psycopg
import redis
from psycopg.rows import dict_row

from fraudcore.keys import profile_key, velocity_key

DECISION_SQL = """
SELECT d.payment_id, d.user_id, d.decision, d.score, d.model_version, d.threshold_set, d.degraded,
       d.decided_at, l.is_fraud AS label_is_fraud, l.scenario AS label_scenario,
       e.status AS explanation_status, e.text AS explanation, e.top_features
FROM decisions d
LEFT JOIN labels l USING (payment_id)
LEFT JOIN explanations e USING (payment_id)
WHERE d.payment_id = %s
"""


class PostgresDecisions:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def decision(self, payment_id: str) -> dict | None:
        with psycopg.connect(self._dsn, row_factory=dict_row) as conn:
            row = conn.execute(DECISION_SQL, (payment_id,)).fetchone()
        if row is None:
            return None
        row["decided_at"] = row["decided_at"].isoformat()
        return row


class RedisProfiles:
    def __init__(self, url: str) -> None:
        self._redis = redis.Redis.from_url(url, decode_responses=True)

    def features(self, user_id: str) -> dict | None:
        profile = self._redis.hgetall(profile_key(user_id))
        velocity = self._redis.hgetall(velocity_key(user_id))
        if not profile and not velocity:
            return None
        return {"profile": profile, "velocity": velocity}


class MlflowChampion:
    def __init__(self, tracking_uri: str, model_name: str, alias: str = "champion") -> None:
        import mlflow

        self._client = mlflow.MlflowClient(tracking_uri=tracking_uri)
        self._name, self._alias = model_name, alias

    def champion(self) -> dict | None:
        try:
            mv = self._client.get_model_version_by_alias(self._name, self._alias)
        except Exception:
            return None
        return {"name": self._name, "alias": self._alias, "version": mv.version,
                "run_id": mv.run_id, "tags": dict(mv.tags)}
