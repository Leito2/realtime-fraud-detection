import random

import pytest

from fraudcore.tracing import Traceparent, new_traceparent, parse_traceparent


def test_new_traceparent_is_valid_w3c_and_sampled():
    tp = new_traceparent(random.Random(1))
    parts = tp.split("-")
    assert parts[0] == "00" and len(parts[1]) == 32 and len(parts[2]) == 16 and parts[3] == "01"
    assert int(parts[1], 16) != 0 and int(parts[2], 16) != 0


def test_roundtrip_and_determinism():
    a = new_traceparent(random.Random(7))
    assert a == new_traceparent(random.Random(7))
    parsed = parse_traceparent(a)
    assert isinstance(parsed, Traceparent) and parsed.sampled is True
    assert f"00-{parsed.trace_id}-{parsed.span_id}-01" == a


@pytest.mark.parametrize("bad", [
    None, "", "garbage", "00-abc-def-01",
    "00-" + "0" * 32 + "-" + "1" * 16 + "-01",          # all-zero trace id
    "ff-" + "1" * 32 + "-" + "1" * 16 + "-01",          # unsupported version
])
def test_invalid_headers_parse_to_none(bad):
    assert parse_traceparent(bad) is None


def test_unsampled_flag_is_respected():
    tp = parse_traceparent("00-" + "a" * 32 + "-" + "b" * 16 + "-00")
    assert tp is not None and tp.sampled is False
