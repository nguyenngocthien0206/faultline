"""The landing bucket: the S3 bucket where ingestion drops files for the warehouse.

faultline reads and writes it to inject landing faults (a file delivered twice,
late, truncated or with a renamed column), and empties a workspace's prefix on
teardown. Keys always start with the workspace name, and nothing is deleted
without validating that name first. Requires the optional `reference` extra.
"""

from __future__ import annotations

from typing import Any

from faultline.connectors.workspace import validate_workspace_name


def _boto3() -> Any:
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise ImportError(
            "the S3 connector needs boto3: pip install 'faultline[reference]'"
        ) from exc
    return boto3


class LandingBucket:
    """A thin S3 client for one bucket. Path-style addressing (RustFS, MinIO)."""

    def __init__(
        self,
        endpoint_url: str,
        *,
        access_key: str,
        secret_key: str,
        bucket: str = "landing",
        region: str = "us-east-1",
    ) -> None:
        self.endpoint_url = endpoint_url
        self.bucket = bucket
        self._settings = {
            "endpoint_url": endpoint_url,
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "region_name": region,
        }
        self._client: Any = None

    @property
    def client(self) -> Any:
        """Created on first use: building the adapter opens nothing."""
        if self._client is None:
            from botocore.config import Config

            self._client = _boto3().client(
                "s3", config=Config(s3={"addressing_style": "path"}), **self._settings
            )
        return self._client

    def list(self, prefix: str) -> list[str]:
        """Every key under `prefix`, sorted."""
        keys: list[str] = []
        pages = self.client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=prefix
        )
        for page in pages:
            keys += [obj["Key"] for obj in page.get("Contents", [])]
        return sorted(keys)

    def get(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def put(self, key: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)

    def delete_workspace(self, workspace: str) -> int:
        """Delete every key under `<workspace>/`. Returns how many were deleted."""
        keys = self.list(f"{validate_workspace_name(workspace)}/")
        for start in range(0, len(keys), 1000):  # the API's limit per request
            batch = [{"Key": k} for k in keys[start : start + 1000]]
            self.client.delete_objects(Bucket=self.bucket, Delete={"Objects": batch})
        return len(keys)
