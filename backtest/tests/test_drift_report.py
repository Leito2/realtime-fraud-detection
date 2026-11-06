import numpy as np
import pandas as pd
from fraud_backtest.drift_report import drift_report


def frames(shift: float, n: int = 800):
    rng = np.random.default_rng(0)
    ref = pd.DataFrame({"amount": rng.lognormal(4, 0.8, n), "f_cnt_10m": rng.poisson(2, n) + 1.0})
    cur = pd.DataFrame({"amount": rng.lognormal(4 + shift, 0.8, n), "f_cnt_10m": rng.poisson(2, n) + 1.0})
    return ref, cur


def test_a_shifted_feature_is_reported_as_drifted_and_a_stable_one_is_not():
    ref, cur = frames(shift=1.0)
    result = drift_report(ref, cur)
    assert result.drifted == ["amount"]
    assert result.share == 0.5 and result.p_values["amount"] < 0.05 < result.p_values["f_cnt_10m"]


def test_no_shift_means_no_drift():
    ref, cur = frames(shift=0.0)
    assert drift_report(ref, cur).drifted == []


def test_the_html_report_is_self_contained():
    ref, cur = frames(shift=1.0)
    html = drift_report(ref, cur).html
    assert html.lstrip().lower().startswith("<") and "amount" in html


def test_only_the_requested_columns_are_compared():
    ref, cur = frames(shift=1.0)
    result = drift_report(ref, cur, columns=["f_cnt_10m"])
    assert result.drifted == [] and set(result.p_values) == {"f_cnt_10m"}
