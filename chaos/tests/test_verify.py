import math

from fraud_chaos.verify import Decided, longest_gap_s, recovery_s, timeline, verify


def d(pid, t, lat=10.0, degraded=False):
    return Decided(pid, t, lat, degraded=degraded)


def test_lost_duplicated_and_unknown_are_told_apart():
    v = verify(["a", "b", "c", "d"], [d("a", 1), d("b", 1), d("b", 2), d("b", 3), d("zzz", 1)])
    assert v.lost == ["c", "d"] and v.lost_count == 2
    assert v.duplicated == {"b": 3} and v.duplicate_extra == 2
    assert v.unknown == 1 and v.decided_unique == 2 and v.produced == 4


def test_a_clean_run_has_nothing_lost_or_duplicated():
    v = verify(["a", "b"], [d("a", 1), d("b", 2)])
    assert v.lost == [] and v.duplicated == {} and v.unknown == 0


def steady(fault_s, outage=(0, 12), rate=20, slow=(12, 20)):
    """Decisions at ``rate``/s, none during the outage, slow right after it."""
    out = []
    for sec in range(-30, 120):
        if outage[0] <= sec < outage[1]:
            continue
        lat = 400.0 if slow[0] <= sec < slow[1] else 12.0
        out += [d(f"{sec}-{i}", fault_s + sec + i / rate, lat) for i in range(rate)]
    return out


def test_recovery_is_when_rate_and_p95_are_back_for_a_sustained_stretch():
    tl = timeline(steady(1000.0), fault_s=1000.0)
    assert recovery_s(tl, budget_ms=80) == 20.0          # outage 0-12, slow 12-20, healthy from second 20


def test_a_system_that_never_recovers_reports_none():
    tl = timeline(steady(1000.0, outage=(0, 500)), fault_s=1000.0)
    assert recovery_s(tl, budget_ms=80) is None


def test_the_longest_gap_is_the_outage_the_caller_saw():
    assert round(longest_gap_s(steady(1000.0), 970.0, 1100.0)) == 12


def test_empty_seconds_have_no_p95_instead_of_a_fake_zero():
    tl = timeline([d("a", 1000.5)], fault_s=1000.0, lo=0, hi=2)
    assert tl.count == [1, 0, 0] and not math.isnan(tl.p95_ms[0]) and math.isnan(tl.p95_ms[1])
