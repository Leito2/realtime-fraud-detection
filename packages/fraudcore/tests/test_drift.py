import numpy as np
import pytest

from fraudcore.drift import psi, reference_from_scores


def test_reference_has_matching_edges_and_fractions_that_sum_to_one():
    scores = np.random.default_rng(0).beta(1, 20, size=5000)
    ref = reference_from_scores(scores, bins=10)
    assert len(ref["edges"]) == len(ref["fractions"]) + 1
    assert sum(ref["fractions"]) == pytest.approx(1.0)
    assert ref["edges"][0] == 0.0 and ref["edges"][-1] == 1.0


def test_psi_of_the_same_distribution_is_near_zero():
    rng = np.random.default_rng(1)
    ref = reference_from_scores(rng.beta(1, 20, size=20_000))
    assert psi(ref, rng.beta(1, 20, size=5_000)) < 0.02


def test_psi_grows_when_the_score_distribution_shifts():
    rng = np.random.default_rng(2)
    ref = reference_from_scores(rng.beta(1, 20, size=20_000))
    mild = psi(ref, rng.beta(1, 12, size=5_000))
    strong = psi(ref, rng.beta(2, 6, size=5_000))
    assert 0.02 < mild < strong
    assert strong > 0.25                            # the conventional "significant shift" line


def test_psi_survives_empty_bins_and_empty_input():
    ref = reference_from_scores(np.random.default_rng(3).beta(1, 20, size=2000))
    assert np.isfinite(psi(ref, np.full(300, 0.99)))
    assert psi(ref, np.array([])) == 0.0


def test_a_constant_reference_collapses_to_one_bin_and_never_drifts():
    ref = reference_from_scores(np.zeros(500))
    assert psi(ref, np.random.default_rng(4).random(200)) == 0.0
