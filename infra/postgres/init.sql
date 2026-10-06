-- Audit store (PLAN.md §3.11). Idempotent inserts use ON CONFLICT (payment_id) DO NOTHING.
CREATE TABLE IF NOT EXISTS decisions (
  payment_id     TEXT PRIMARY KEY,
  user_id        TEXT NOT NULL,
  decision       TEXT NOT NULL CHECK (decision IN ('APPROVE','REVIEW','BLOCK')),
  score          DOUBLE PRECISION NOT NULL,
  model_version  TEXT NOT NULL,
  threshold_set  TEXT NOT NULL,
  degraded       BOOLEAN NOT NULL DEFAULT FALSE,
  features       JSONB,                       -- logged served features (train on what you served)
  decided_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS labels (
  payment_id TEXT PRIMARY KEY,
  is_fraud   BOOLEAN NOT NULL,
  scenario   TEXT,
  labeled_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS shadow_scores (
  payment_id    TEXT PRIMARY KEY,
  model_version TEXT NOT NULL,
  score         DOUBLE PRECISION NOT NULL,
  decision      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS explanations (
  payment_id TEXT PRIMARY KEY,
  status     TEXT NOT NULL CHECK (status IN ('llm','template','skipped')),
  text       TEXT,
  top_features JSONB
);

CREATE OR REPLACE VIEW v_confusion_by_model AS
SELECT 'champion' AS model, d.decision <> 'APPROVE' AS flagged, l.is_fraud, COUNT(*) AS n
FROM decisions d JOIN labels l USING (payment_id) GROUP BY 2, 3
UNION ALL
SELECT 'challenger', s.decision <> 'APPROVE', l.is_fraud, COUNT(*)
FROM shadow_scores s JOIN labels l USING (payment_id) GROUP BY 2, 3;

CREATE OR REPLACE VIEW v_feedback_dataset AS
SELECT d.payment_id, d.features, l.is_fraud, d.decided_at
FROM decisions d JOIN labels l USING (payment_id)
WHERE d.features IS NOT NULL;
