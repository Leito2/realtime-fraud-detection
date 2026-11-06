"""The versioned data lake on the real S3 service (not the mock). Needs `make up PROFILE=ops`."""
import uuid

import pytest

pytest.importorskip("boto3")
from fraud_backtest.datalake import DigestMismatch, Lake, client_from_env  # noqa: E402


@pytest.fixture()
def lake():
    client = client_from_env({})
    try:
        client.list_buckets()
    except Exception:
        pytest.skip("the S3 store is not reachable (make up PROFILE=ops)")
    return Lake(client, bucket="fraud-data")


def test_versions_are_kept_and_a_wrong_digest_is_refused(lake, tmp_path):
    key = f"it/{uuid.uuid4().hex}.bin"
    a, b = tmp_path / "a", tmp_path / "b"
    a.write_bytes(b"first")
    b.write_bytes(b"second")
    first, second = lake.publish(a, key), lake.publish(b, key)
    assert first["version_id"] != second["version_id"]
    assert lake.fetch_verified(key, first["version_id"]) == b"first"
    assert lake.fetch_verified(key) == b"second"
    with pytest.raises(DigestMismatch):
        lake.fetch_verified(key, first["version_id"], expected_sha256=second["sha256"])
