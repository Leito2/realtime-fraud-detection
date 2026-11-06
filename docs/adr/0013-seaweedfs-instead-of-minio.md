# ADR-0013: SeaweedFS behind the S3 API instead of MinIO

- **Status:** accepted
- **Date:** 2026-11-06
- **Origin:** Implementation, M6b

## Context
MinIO stopped publishing container images and binaries in 2025; none of the registries the plan relied on served a usable one.

## Decision
The data lake is SeaweedFS exposing the S3 API with versioning. Code talks to S3 through boto3 and a SHA-256 manifest, so the server is replaceable.

## Consequences
Nothing in the repository names the server except the compose service and `infra/s3/s3.json`.
