from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import Iterator
from typing import Any

import aioboto3
import boto3
import pytest
from docker.errors import DockerException
from testcontainers.localstack import LocalStackContainer

from aws_s3_ohfuck.cli import RunReport, TargetMode, TargetSpec, run_restore


def _create_bucket(s3_client: Any, bucket: str, region: str) -> None:
    if region == "us-east-1":
        s3_client.create_bucket(Bucket=bucket)
    else:
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": region},
        )


def _new_bucket_name() -> str:
    return f"aws-s3-ohfuck-{uuid.uuid4().hex[:20]}"


@pytest.fixture(scope="session")
def localstack_s3() -> Iterator[dict[str, Any]]:
    if os.getenv("RUN_LIVE_TESTS") != "1":
        pytest.skip("Set RUN_LIVE_TESTS=1 to run live S3 container tests.")

    try:
        with LocalStackContainer().with_services("s3") as container:
            endpoint_url = container.get_url()
            region_name = container.region_name
            access_key = "testcontainers-localstack"
            secret_key = "testcontainers-localstack"

            sync_s3 = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                region_name=region_name,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )

            yield {
                "endpoint_url": endpoint_url,
                "region_name": region_name,
                "access_key": access_key,
                "secret_key": secret_key,
                "sync_s3": sync_s3,
            }
    except DockerException as exc:
        pytest.skip(f"Docker is unavailable for live tests: {exc}")


def _run_restore_live(
    *,
    endpoint_url: str,
    region_name: str,
    access_key: str,
    secret_key: str,
    target: TargetSpec,
    versions: int | None,
    target_bucket: str | None = None,
) -> RunReport:
    session = aioboto3.Session(
        region_name=region_name,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    async def _inner() -> RunReport:
        async with session.client("s3", endpoint_url=endpoint_url) as s3_client:
            return await run_restore(
                s3_client=s3_client,
                target=target,
                versions=versions,
                as_of_timestamp=None,
                ignore_delete_markers=False,
                target_bucket=target_bucket,
                target_region=region_name,
                max_workers=8,
                session_region=region_name,
            )

    return asyncio.run(_inner())


@pytest.mark.live
def test_live_restore_single_object_previous_version(
    localstack_s3: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("aws_s3_ohfuck.cli.prompt_confirm", lambda _: None)

    s3 = localstack_s3["sync_s3"]
    region_name = localstack_s3["region_name"]
    bucket = _new_bucket_name()
    key = "demo/object.txt"

    _create_bucket(s3, bucket, region_name)
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})

    s3.put_object(Bucket=bucket, Key=key, Body=b"v1")
    s3.put_object(Bucket=bucket, Key=key, Body=b"v2")
    s3.put_object(Bucket=bucket, Key=key, Body=b"v3")

    report = _run_restore_live(
        endpoint_url=localstack_s3["endpoint_url"],
        region_name=region_name,
        access_key=localstack_s3["access_key"],
        secret_key=localstack_s3["secret_key"],
        target=TargetSpec(bucket=bucket, mode=TargetMode.EXACT, key=key),
        versions=None,
    )

    assert report.restored == 1
    assert report.failed == 0
    latest_body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    assert latest_body == b"v2"


@pytest.mark.live
def test_live_restore_prefix_many_keys_parallel(
    localstack_s3: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("aws_s3_ohfuck.cli.prompt_confirm", lambda _: None)

    s3 = localstack_s3["sync_s3"]
    region_name = localstack_s3["region_name"]
    bucket = _new_bucket_name()
    prefix = "batch"

    _create_bucket(s3, bucket, region_name)
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})

    keys = [f"{prefix}/file-{i}.txt" for i in range(25)]
    for key in keys:
        s3.put_object(Bucket=bucket, Key=key, Body=b"old")
        s3.put_object(Bucket=bucket, Key=key, Body=b"new")

    report = _run_restore_live(
        endpoint_url=localstack_s3["endpoint_url"],
        region_name=region_name,
        access_key=localstack_s3["access_key"],
        secret_key=localstack_s3["secret_key"],
        target=TargetSpec(bucket=bucket, mode=TargetMode.PREFIX, prefix=prefix),
        versions=1,
    )

    assert report.failed == 0
    assert report.restored == len(keys)
    for key in keys:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        assert body == b"old"


@pytest.mark.live
def test_live_restore_to_missing_target_bucket_prompts_create(
    localstack_s3: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("aws_s3_ohfuck.cli.prompt_confirm", lambda _: None)

    s3 = localstack_s3["sync_s3"]
    region_name = localstack_s3["region_name"]
    source_bucket = _new_bucket_name()
    target_bucket = _new_bucket_name()
    key = "targeted/thing.txt"

    _create_bucket(s3, source_bucket, region_name)
    s3.put_bucket_versioning(Bucket=source_bucket, VersioningConfiguration={"Status": "Enabled"})

    s3.put_object(Bucket=source_bucket, Key=key, Body=b"first")
    s3.put_object(Bucket=source_bucket, Key=key, Body=b"second")

    report = _run_restore_live(
        endpoint_url=localstack_s3["endpoint_url"],
        region_name=region_name,
        access_key=localstack_s3["access_key"],
        secret_key=localstack_s3["secret_key"],
        target=TargetSpec(bucket=source_bucket, mode=TargetMode.EXACT, key=key),
        versions=1,
        target_bucket=target_bucket,
    )

    assert report.restored == 1
    assert report.failed == 0

    s3.head_bucket(Bucket=target_bucket)
    copied = s3.get_object(Bucket=target_bucket, Key=key)["Body"].read()
    assert copied == b"first"
