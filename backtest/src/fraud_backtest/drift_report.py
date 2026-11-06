"""Feature drift report with Evidently: training reference vs the features the scorer served.

The reference is the dataset the model was trained on; the current window is ``decisions.features`` in the
audit store (PLAN.md 13.1). Evidently runs on demand (``make drift-report``), not as a permanent service.
"""
from dataclasses import dataclass

import pandas as pd
from evidently import Report
from evidently.presets import DataDriftPreset


@dataclass(frozen=True)
class DriftResult:
    html: str
    drifted: list[str]
    share: float                    # share of compared columns that drifted
    p_values: dict[str, float]      # two-sample K-S p-value per column (small = different distributions)


def drift_report(reference: pd.DataFrame, current: pd.DataFrame,
                 columns: list[str] | None = None) -> DriftResult:
    cols = columns or [c for c in reference.columns if c in current.columns]
    snapshot = Report([DataDriftPreset()]).run(current[cols], reference[cols])
    metrics = snapshot.dict()["metrics"]
    p_values, drifted, share = {}, [], 0.0
    for m in metrics:
        cfg = m["config"]
        if cfg["type"].endswith("DriftedColumnsCount"):
            share = float(m["value"]["share"])
        elif cfg["type"].endswith("ValueDrift"):
            p_values[cfg["column"]] = float(m["value"])
            if float(m["value"]) < cfg["threshold"]:
                drifted.append(cfg["column"])
    return DriftResult(snapshot.get_html_str(as_iframe=False), drifted, share, p_values)
