import json

import boto3
import pytest
from fraud_backtest.datalake import DigestMismatch, Lake
from moto import mock_aws


@pytest.fixture()
def lake(tmp_path):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1", aws_access_key_id="k", aws_secret_access_key="s")
        yield Lake(client, bucket="fraud-data")


def write(tmp_path, name, content: bytes):
    path = tmp_path / name
    path.write_bytes(content)
    return path


def test_publish_enables_versioning_and_returns_digest_and_version(lake, tmp_path):
    info = lake.publish(write(tmp_path, "h.parquet", b"v1"), "training/history.parquet")
    assert info["key"] == "training/history.parquet" and info["version_id"]
    assert len(info["sha256"]) == 64
    assert lake.versioning_enabled()


def test_every_publish_is_a_new_version_and_old_ones_stay_readable(lake, tmp_path):
    first = lake.publish(write(tmp_path, "a", b"first"), "training/h.parquet")
    second = lake.publish(write(tmp_path, "b", b"second"), "training/h.parquet")
    assert first["version_id"] != second["version_id"]
    assert lake.fetch("training/h.parquet", first["version_id"]) == b"first"
    assert lake.fetch("training/h.parquet") == b"second"                       # latest by default
    assert [v["version_id"] for v in lake.versions("training/h.parquet")][0] == second["version_id"]


def test_the_manifest_is_written_next_to_the_object_and_checked_on_read(lake, tmp_path):
    info = lake.publish(write(tmp_path, "a", b"payload"), "training/h.parquet")
    manifest = json.loads(lake.fetch("training/h.parquet.manifest.json"))
    assert manifest["sha256"] == info["sha256"] and manifest["version_id"] == info["version_id"]
    assert lake.fetch_verified("training/h.parquet", info["version_id"]) == b"payload"


def test_a_tampered_object_fails_digest_verification(lake, tmp_path):
    info = lake.publish(write(tmp_path, "a", b"payload"), "training/h.parquet")
    lake._client.put_object(Bucket="fraud-data", Key="training/h.parquet", Body=b"tampered")  # new version
    with pytest.raises(DigestMismatch):
        lake.fetch_verified("training/h.parquet", None, expected_sha256=info["sha256"])


def test_reports_are_stored_under_reports(lake):
    key = lake.put_report("backtest-1.md", b"# report")
    assert key == "reports/backtest-1.md" and lake.fetch(key) == b"# report"
