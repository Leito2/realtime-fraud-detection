"""Local data lake on MinIO (S3 API) with object versioning and a content-hash manifest (PLAN.md 13.1).

Layout: ``training/`` (histories), ``features/``, ``raw/`` and ``reports/``. Every ``publish`` creates a
new object version plus a manifest object (``<key>.manifest.json``) holding the SHA-256, so a backtest can
name the exact bytes it ran on and refuse bytes that do not match.
"""
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path


class DigestMismatch(Exception):
    pass


def client_from_env(env: dict):
    import boto3

    return boto3.client(
        "s3", endpoint_url=env.get("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=env.get("S3_ACCESS_KEY", "fraud"),
        aws_secret_access_key=env.get("S3_SECRET_KEY", "fraudfraud"), region_name="us-east-1")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class Lake:
    def __init__(self, client, bucket: str = "fraud-data") -> None:
        self._client, self._bucket = client, bucket

    def ensure(self) -> None:
        names = {b["Name"] for b in self._client.list_buckets()["Buckets"]}
        if self._bucket not in names:
            self._client.create_bucket(Bucket=self._bucket)
        self._client.put_bucket_versioning(Bucket=self._bucket, VersioningConfiguration={"Status": "Enabled"})

    def versioning_enabled(self) -> bool:
        return self._client.get_bucket_versioning(Bucket=self._bucket).get("Status") == "Enabled"

    def publish(self, path: Path, key: str) -> dict:
        self.ensure()
        digest = sha256_file(path)
        with open(path, "rb") as f:
            put = self._client.put_object(Bucket=self._bucket, Key=key, Body=f)
        info = {"key": key, "version_id": put["VersionId"], "sha256": digest, "size": path.stat().st_size,
                "file": path.name, "published_at": datetime.now(UTC).isoformat()}
        self._client.put_object(Bucket=self._bucket, Key=f"{key}.manifest.json",
                                Body=json.dumps(info, indent=2).encode())
        return info

    def fetch(self, key: str, version_id: str | None = None) -> bytes:
        args = {"Bucket": self._bucket, "Key": key}
        if version_id:
            args["VersionId"] = version_id
        return self._client.get_object(**args)["Body"].read()

    def versions(self, key: str) -> list[dict]:
        """Versions of one key, newest first."""
        listing = self._client.list_object_versions(Bucket=self._bucket, Prefix=key)
        found = [v for v in listing.get("Versions", []) if v["Key"] == key]
        found.sort(key=lambda v: v["LastModified"], reverse=True)
        return [{"version_id": v["VersionId"], "size": v["Size"], "last_modified": v["LastModified"],
                 "latest": v["IsLatest"]} for v in found]

    def manifest_for(self, key: str, version_id: str) -> dict | None:
        for v in self.versions(f"{key}.manifest.json"):
            manifest = json.loads(self.fetch(f"{key}.manifest.json", v["version_id"]))
            if manifest["version_id"] == version_id:
                return manifest
        return None

    def fetch_verified(self, key: str, version_id: str | None = None,
                       expected_sha256: str | None = None) -> bytes:
        """The object's bytes, checked against the manifest (or ``expected_sha256``): trustworthy lineage."""
        if version_id is None:
            version_id = self._client.head_object(Bucket=self._bucket, Key=key)["VersionId"]
        data = self.fetch(key, version_id)
        expected = expected_sha256
        if expected is None:
            manifest = self.manifest_for(key, version_id)
            if manifest is None:
                raise DigestMismatch(f"no manifest for {key} version {version_id}")
            expected = manifest["sha256"]
        actual = hashlib.sha256(data).hexdigest()
        if actual != expected:
            raise DigestMismatch(f"{key}: sha256 {actual[:12]} does not match {expected[:12]}")
        return data

    def put_report(self, name: str, body: bytes) -> str:
        self.ensure()
        key = f"reports/{name}"
        self._client.put_object(Bucket=self._bucket, Key=key, Body=body)
        return key
