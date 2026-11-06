"""Population Stability Index of a score distribution against the one the model was trained on (PLAN.md 4.11).

Pure numpy, shared by training (which stores the reference next to the model) and the scorer (which watches
the live scores). Rule of thumb: < 0.1 stable, 0.1-0.25 moderate shift, > 0.25 significant shift.
"""
import numpy as np

EPS = 1e-4


def reference_from_scores(scores, bins: int = 10) -> dict:
    """Quantile bin edges and the share of ``scores`` in each bin; duplicate edges (equal scores) merge."""
    s = np.asarray(scores, dtype=np.float64)
    edges = np.unique(np.quantile(s, np.linspace(0, 1, bins + 1)))
    edges[0], edges[-1] = 0.0, 1.0
    edges = np.unique(edges)
    if len(edges) < 2:
        edges = np.array([0.0, 1.0])
    counts, _ = np.histogram(s, bins=edges)
    fractions = counts / max(counts.sum(), 1)
    return {"edges": [float(e) for e in edges], "fractions": [float(f) for f in fractions]}


def psi(reference: dict, scores) -> float:
    s = np.asarray(scores, dtype=np.float64)
    expected = np.asarray(reference["fractions"], dtype=np.float64)
    if s.size == 0 or expected.size < 2:
        return 0.0
    counts, _ = np.histogram(np.clip(s, 0.0, 1.0), bins=np.asarray(reference["edges"]))
    actual = counts / counts.sum()
    return float(np.sum((actual - expected) * np.log((actual + EPS) / (expected + EPS))))
