from datetime import UTC, datetime

import numpy as np
import polars as pl
import pytest
from fraud_backtest.job import BacktestModel, backtest, to_markdown
from generator.logic import Population
from generator.offline import generate_history

from fraudcore.features import FEATURE_ORDER
from fraudcore.rules import rules_score


@pytest.fixture(scope="module")
def history() -> pl.DataFrame:
    return generate_history(Population(n_users=400, seed=5), seed=5, start=datetime(2026, 8, 1, tzinfo=UTC),
                            days=14, payments_per_day=1_500)


def rules(X: np.ndarray) -> np.ndarray:
    return np.array([rules_score(dict(zip(FEATURE_ORDER, row, strict=True))) for row in X], dtype=np.float32)


@pytest.fixture(scope="module")
def models() -> list[BacktestModel]:
    rng = np.random.default_rng(0)
    return [BacktestModel("champion", "1", rules, review=0.5, block=0.9),
            BacktestModel("coin-flip", "0", lambda X: rng.random(len(X)).astype(np.float32), review=0.5,
                          block=0.9)]


def test_the_report_has_one_row_per_model_segment_and_metric(history, models):
    df = backtest(history, models, valid_days=3, test_days=3)
    assert set(df.columns) == {"model", "version", "segment", "metric", "value"}
    assert {"champion", "coin-flip"} == set(df["model"])
    overall = df[df["segment"] == "overall"]
    assert {"pr_auc", "recall_at_fpr_1pct", "cost", "review_rate"} <= set(overall["metric"])
    assert any(s not in {"overall"} for s in df["segment"])                    # per-scenario recall rows


def test_a_real_model_beats_a_coin_flip_on_the_same_test_days(history, models):
    df = backtest(history, models, valid_days=3, test_days=3)
    pr = df[(df["segment"] == "overall") & (df["metric"] == "pr_auc")].set_index("model")["value"]
    assert pr["champion"] > pr["coin-flip"]


def test_markdown_puts_models_side_by_side(history, models):
    md = to_markdown(backtest(history, models, valid_days=3, test_days=3))
    header = next(line for line in md.splitlines() if line.startswith("| segment"))
    assert "champion" in header and "coin-flip" in header
    assert "pr_auc" in md and "overall" in md
