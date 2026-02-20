from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
import click
from click.testing import CliRunner

from aws_s3_ohfuck.cli import (
    TargetMode,
    TargetSpec,
    VersionEntry,
    cli,
    parse_as_of_timestamp,
    parse_s3_url,
    run_restore,
    select_version_as_of,
    select_version_by_depth,
)


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


class _FakePaginator:
    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self._pages = pages

    def paginate(self, **_: Any) -> list[dict[str, Any]]:
        return self._pages


class _FakeS3Client:
    def __init__(self) -> None:
        self.copy_calls: list[dict[str, Any]] = []

    def get_bucket_versioning(self, **_: Any) -> dict[str, str]:
        return {"Status": "Enabled"}

    def get_paginator(self, name: str) -> _FakePaginator:
        if name == "list_objects_v2":
            return _FakePaginator([{"Contents": [{"Key": "my/file.txt"}]}])
        if name == "list_object_versions":
            return _FakePaginator(
                [
                    {
                        "Versions": [
                            {
                                "Key": "my/file.txt",
                                "VersionId": "latest",
                                "LastModified": datetime(
                                    2026, 2, 19, 11, 0, tzinfo=timezone.utc
                                ),
                                "IsLatest": True,
                            },
                            {
                                "Key": "my/file.txt",
                                "VersionId": "previous",
                                "LastModified": datetime(
                                    2026, 2, 19, 10, 0, tzinfo=timezone.utc
                                ),
                                "IsLatest": False,
                            },
                        ]
                    }
                ]
            )
        raise AssertionError(f"Unexpected paginator: {name}")

    def copy_object(self, **kwargs: Any) -> None:
        self.copy_calls.append(kwargs)


def test_run_restore_defaults_to_previous_version(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("aws_s3_ohfuck.cli.prompt_confirm", lambda _: None)
    monkeypatch.setattr("aws_s3_ohfuck.cli.choose_insufficient_action", lambda _: None)
    client = _FakeS3Client()
    target = TargetSpec(bucket="mybucket", mode=TargetMode.BUCKET_ALL, prefix="")

    report = run_restore(
        s3_client=client,
        target=target,
        versions=None,
        as_of_timestamp=None,
        ignore_delete_markers=False,
        target_bucket=None,
        target_region=None,
    )

    assert report.restored == 1
    assert report.failed == 0
    assert len(client.copy_calls) == 1
    copy_source = client.copy_calls[0]["CopySource"]
    assert copy_source["VersionId"] == "previous"


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
