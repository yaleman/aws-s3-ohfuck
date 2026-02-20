from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import click
import pytest
from botocore.exceptions import ClientError
from click.testing import CliRunner

from aws_s3_ohfuck.cli import (
    TargetMode,
    TargetSpec,
    VersionEntry,
    build_restore_plan,
    cli,
    default_max_workers,
    parse_as_of_timestamp,
    parse_s3_url,
    resolve_max_workers,
    run_restore,
    select_version_as_of,
    select_version_by_depth,
)


class _AsyncPaginator:
    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self._pages = pages

    async def _iterate(self) -> Any:
        for page in self._pages:
            yield page

    def paginate(self, **_: Any) -> Any:
        return self._iterate()


class _FakeAsyncS3Client:
    def __init__(
        self,
        keys: list[str],
        versions_by_key: dict[str, list[dict[str, Any]]],
        fail_on_copy: set[str] | None = None,
    ) -> None:
        self._keys = keys
        self._versions_by_key = versions_by_key
        self._fail_on_copy = fail_on_copy or set()
        self.copy_calls: list[dict[str, Any]] = []

    async def get_bucket_versioning(self, **_: Any) -> dict[str, str]:
        return {"Status": "Enabled"}

    async def head_bucket(self, **_: Any) -> None:
        return None

    async def create_bucket(self, **_: Any) -> None:
        return None

    def get_paginator(self, name: str) -> _AsyncPaginator:
        if name == "list_objects_v2":
            return _AsyncPaginator([{"Contents": [{"Key": key} for key in self._keys]}])
        if name == "list_object_versions":
            return _AsyncPaginator([])
        raise AssertionError(f"Unexpected paginator: {name}")

    def get_paginator_for_key(self, key: str) -> _AsyncPaginator:
        return _AsyncPaginator(self._versions_by_key.get(key, []))

    async def copy_object(self, **kwargs: Any) -> None:
        self.copy_calls.append(kwargs)
        key = kwargs["Key"]
        if key in self._fail_on_copy:
            raise ClientError(
                {
                    "Error": {
                        "Code": "InternalError",
                        "Message": "simulated copy failure",
                    }
                },
                "CopyObject",
            )


class _PerKeyVersionClient(_FakeAsyncS3Client):
    def get_paginator(self, name: str) -> _AsyncPaginator:
        if name == "list_objects_v2":
            return _AsyncPaginator([{"Contents": [{"Key": key} for key in self._keys]}])
        if name == "list_object_versions":
            return _VersionPaginatorProxy(self)
        raise AssertionError(f"Unexpected paginator: {name}")


class _VersionPaginatorProxy(_AsyncPaginator):
    def __init__(self, client: _FakeAsyncS3Client) -> None:
        self._client = client

    def paginate(self, **kwargs: Any) -> Any:
        key = kwargs["Prefix"]
        pages = self._client._versions_by_key.get(key, [])
        return _AsyncPaginator(pages).paginate()



def _entries() -> list[VersionEntry]:
    return [
        VersionEntry(
            version_id="latest-delete",
            last_modified=datetime(2026, 2, 19, 12, 0, tzinfo=timezone.utc),
            is_delete_marker=True,
            is_latest=True,
        ),
        VersionEntry(
            version_id="v2",
            last_modified=datetime(2026, 2, 19, 11, 0, tzinfo=timezone.utc),
            is_delete_marker=False,
            is_latest=False,
        ),
        VersionEntry(
            version_id="v1",
            last_modified=datetime(2026, 2, 19, 10, 0, tzinfo=timezone.utc),
            is_delete_marker=False,
            is_latest=False,
        ),
    ]



def _versions_page(*version_specs: tuple[str, datetime, bool]) -> list[dict[str, Any]]:
    versions: list[dict[str, Any]] = []
    delete_markers: list[dict[str, Any]] = []
    for version_id, timestamp, is_delete in version_specs:
        payload = {
            "Key": "placeholder",
            "VersionId": version_id,
            "LastModified": timestamp,
            "IsLatest": version_id == "latest",
        }
        if is_delete:
            delete_markers.append(payload)
        else:
            versions.append(payload)
    return [{"Versions": versions, "DeleteMarkers": delete_markers}]



def _key_versions(key: str, *version_specs: tuple[str, datetime, bool]) -> list[dict[str, Any]]:
    page = _versions_page(*version_specs)[0]
    for item in page["Versions"]:
        item["Key"] = key
    for item in page["DeleteMarkers"]:
        item["Key"] = key
    return [page]



def test_parse_s3_url_bucket_root() -> None:
    target = parse_s3_url("s3://mybucket")
    assert target == TargetSpec(bucket="mybucket", mode=TargetMode.BUCKET_ALL, prefix="")



def test_parse_s3_url_bucket_star() -> None:
    target = parse_s3_url("s3://mybucket/*")
    assert target == TargetSpec(bucket="mybucket", mode=TargetMode.BUCKET_ALL, prefix="")



def test_parse_s3_url_prefix_star() -> None:
    target = parse_s3_url("s3://mybucket/path/to/*")
    assert target == TargetSpec(bucket="mybucket", mode=TargetMode.PREFIX, prefix="path/to")



def test_parse_s3_url_exact_key() -> None:
    target = parse_s3_url("s3://mybucket/a/b/c.txt")
    assert target == TargetSpec(bucket="mybucket", mode=TargetMode.EXACT, key="a/b/c.txt")



def test_parse_s3_url_rejects_nontrailing_wildcard() -> None:
    with pytest.raises(click.ClickException):
        parse_s3_url("s3://mybucket/a*b.txt")



def test_parse_as_of_timestamp_zulu() -> None:
    parsed = parse_as_of_timestamp("2026-02-20T10:30:00Z")
    assert parsed == datetime(2026, 2, 20, 10, 30, 0, tzinfo=timezone.utc)



def test_parse_as_of_timestamp_naive_defaults_to_utc() -> None:
    parsed = parse_as_of_timestamp("2026-02-20T10:30:00")
    assert parsed == datetime(2026, 2, 20, 10, 30, 0, tzinfo=timezone.utc)



def test_select_version_by_depth_includes_delete_markers() -> None:
    selected = select_version_by_depth(_entries(), versions_back=1, ignore_delete_markers=False)
    assert selected is not None
    assert selected.version_id == "v2"



def test_select_version_by_depth_ignores_delete_markers() -> None:
    selected = select_version_by_depth(_entries(), versions_back=1, ignore_delete_markers=True)
    assert selected is not None
    assert selected.version_id == "v1"



def test_select_version_as_of() -> None:
    selected = select_version_as_of(
        _entries(),
        as_of=datetime(2026, 2, 19, 10, 30, tzinfo=timezone.utc),
        ignore_delete_markers=False,
    )
    assert selected is not None
    assert selected.version_id == "v1"



def test_default_max_workers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("os.cpu_count", lambda: 4)
    assert default_max_workers() == 20
    assert resolve_max_workers(None) == 20
    assert resolve_max_workers(7) == 7



def test_run_restore_defaults_to_previous_version(monkeypatch: pytest.MonkeyPatch) -> None:
    key = "my/file.txt"
    versions_by_key = {
        key: _key_versions(
            key,
            ("latest", datetime(2026, 2, 19, 11, 0, tzinfo=timezone.utc), False),
            ("previous", datetime(2026, 2, 19, 10, 0, tzinfo=timezone.utc), False),
        )
    }
    client = _PerKeyVersionClient(keys=[key], versions_by_key=versions_by_key)

    monkeypatch.setattr("aws_s3_ohfuck.cli.prompt_confirm", lambda _: None)
    report = asyncio.run(
        run_restore(
            s3_client=client,
            target=TargetSpec(bucket="mybucket", mode=TargetMode.BUCKET_ALL, prefix=""),
            versions=None,
            as_of_timestamp=None,
            ignore_delete_markers=False,
            target_bucket=None,
            target_region=None,
            max_workers=4,
            session_region="us-east-1",
        )
    )

    assert report.restored == 1
    assert report.failed == 0
    assert report.skipped == 0
    assert len(client.copy_calls) == 1
    assert client.copy_calls[0]["CopySource"]["VersionId"] == "previous"



def test_build_restore_plan_splits_insufficient() -> None:
    now = datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc)
    key_ok = "ok.txt"
    key_short = "short.txt"
    versions_by_key = {
        key_ok: _key_versions(
            key_ok,
            ("latest", now, False),
            ("previous", now.replace(hour=11), False),
        ),
        key_short: _key_versions(key_short, ("latest", now, False)),
    }
    client = _PerKeyVersionClient(keys=[key_ok, key_short], versions_by_key=versions_by_key)

    plan = asyncio.run(
        build_restore_plan(
            s3_client=client,
            bucket="mybucket",
            keys=[key_ok, key_short],
            versions=None,
            as_of_timestamp=None,
            ignore_delete_markers=False,
            max_workers=4,
        )
    )

    assert sorted(plan.insufficient_keys) == [key_short]
    assert sorted(candidate.key for candidate in plan.ready) == [key_ok]



def test_stop_scheduling_after_first_copy_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc)
    keys = ["a.txt", "b.txt", "c.txt"]
    versions_by_key = {
        key: _key_versions(
            key,
            ("latest", now, False),
            ("previous", now.replace(hour=11), False),
        )
        for key in keys
    }
    client = _PerKeyVersionClient(keys=keys, versions_by_key=versions_by_key, fail_on_copy={"a.txt"})

    monkeypatch.setattr("aws_s3_ohfuck.cli.prompt_confirm", lambda _: None)
    report = asyncio.run(
        run_restore(
            s3_client=client,
            target=TargetSpec(bucket="mybucket", mode=TargetMode.BUCKET_ALL, prefix=""),
            versions=None,
            as_of_timestamp=None,
            ignore_delete_markers=False,
            target_bucket=None,
            target_region=None,
            max_workers=1,
            session_region="us-east-1",
        )
    )

    assert report.failed == 1
    assert report.restored == 0
    assert report.skipped == 2
    assert len(client.copy_calls) == 1



def test_cli_rejects_versions_with_as_of() -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "s3://mybucket/my/file.txt",
            "--versions",
            "2",
            "--as-of-timestamp",
            "2026-02-20T10:30:00Z",
        ],
    )
    assert result.exit_code != 0
    assert "either --versions or --as-of-timestamp" in result.output



def test_cli_validates_max_workers() -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["s3://mybucket/my/file.txt", "--max-workers", "0"],
    )
    assert result.exit_code != 0
    assert "x>=1" in result.output
