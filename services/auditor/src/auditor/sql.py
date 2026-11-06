"""SQL text for the audit store (infra/postgres/init.sql). Names are whitelisted, values are bound."""

COLUMNS = {
    "decisions": {"payment_id", "user_id", "decision", "score", "model_version", "threshold_set", "degraded",
                  "features"},
    "labels": {"payment_id", "is_fraud", "scenario"},
    "shadow_scores": {"payment_id", "model_version", "score", "decision"},
    "explanations": {"payment_id", "status", "text", "top_features"},
}
JSON_COLUMNS = {"features", "top_features"}


def insert_sql(table: str, columns: list[str]) -> str:
    if table not in COLUMNS:
        raise ValueError(f"unknown table {table!r}")
    unknown = set(columns) - COLUMNS[table]
    if unknown:
        raise ValueError(f"unknown columns for {table}: {sorted(unknown)}")
    marks = ", ".join("%s::jsonb" if c in JSON_COLUMNS else "%s" for c in columns)
    return (f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({marks}) "
            "ON CONFLICT (payment_id) DO NOTHING")
