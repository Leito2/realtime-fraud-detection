"""IEEE-CIS offline benchmark (PLAN.md 12.2): preparation of the public dataset for the same training code."""

import numpy as np
import polars as pl
from fraud_training.ieee_cis import amount_baseline_scores, prepare, temporal_split_by_time


def frame() -> pl.DataFrame:
    return pl.DataFrame({
        "TransactionID": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "isFraud": [0, 0, 1, 0, 0, 0, 1, 0, 0, 1],
        "TransactionDT": [100, 50, 300, 200, 400, 600, 500, 700, 800, 900],
        "TransactionAmt": [10.0, 20.0, 500.0, 30.0, 40.0, 50.0, 700.0, 60.0, 70.0, 900.0],
        "ProductCD": ["W", "C", "W", None, "R", "W", "C", "W", "H", "W"],
        "card4": ["visa", "visa", None, "mastercard", "visa", "visa", "visa", "visa", "visa", "visa"],
        "V1": [1.0, None, 2.0, 3.0, None, 1.0, 1.0, 2.0, 2.0, 3.0],
    })


def test_prepare_drops_ids_encodes_categoricals_and_keeps_missing_as_nan():
    X, y, t, names = prepare(frame())
    assert "TransactionID" not in names and "isFraud" not in names and "TransactionDT" not in names
    assert X.dtype == np.float32 and X.shape == (10, len(names))
    assert set(np.unique(y)) == {0, 1}
    col = names.index("ProductCD")
    assert np.isnan(X[:, col]).sum() == 1                   # the missing category stays missing
    assert np.isnan(X[:, names.index("V1")]).sum() == 2     # numeric NaN is not imputed
    assert list(t) == sorted(t)                              # prepare returns rows in time order


def test_temporal_split_never_trains_on_the_future():
    _, _, t, _ = prepare(frame())
    tr, va, te = temporal_split_by_time(t, valid_frac=0.2, test_frac=0.2)
    assert t[tr].max() < t[va].min() <= t[va].max() < t[te].min()
    assert len(tr) + len(va) + len(te) == len(t)


def test_amount_baseline_ranks_bigger_amounts_as_riskier():
    X, y, _, names = prepare(frame())
    scores = amount_baseline_scores(X, names)
    assert scores[np.argmax(X[:, names.index("TransactionAmt")])] == scores.max()
