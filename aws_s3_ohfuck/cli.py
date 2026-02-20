from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from urllib.parse import urlparse

import boto3
import click
import questionary
from botocore.exceptions import ClientError

S3Client = Any


class TargetMode(str, Enum):
    EXACT = "exact"
    PREFIX = "prefix"
    BUCKET_ALL = "bucket_all"


@dataclass(frozen=True)
class TargetSpec:
    bucket: str
    mode: TargetMode
    key: str | None = None
    prefix: str | None = None


@dataclass(frozen=True)
class VersionEntry:
    version_id: str
    last_modified: datetime
    is_delete_marker: bool
    is_latest: bool


@dataclass
class RunReport:
    restored: int = 0
    skipped: int = 0
    failed: int = 0


class InsufficientChoice(str, Enum):
    SKIP_THIS = "skip_this"
    SKIP_ALL = "skip_all"
    ABORT = "abort"


def parse_s3_url(raw_url: str) -> TargetSpec:
    parsed = urlparse(raw_url)
    if parsed.scheme != "s3":
        raise click.ClickException(f"Invalid S3 URL '{raw_url}': scheme must be 's3://'.")

    bucket = parsed.netloc.strip()
    if not bucket:
        raise click.ClickException(f"Invalid S3 URL '{raw_url}': bucket is required.")

    path = parsed.path.lstrip("/")
    if not path or path == "*":
        return TargetSpec(bucket=bucket, mode=TargetMode.BUCKET_ALL, prefix="")

    if path.endswith("/*"):
        prefix = path[:-2]
        return TargetSpec(bucket=bucket, mode=TargetMode.PREFIX, prefix=prefix)

    if "*" in path:
        raise click.ClickException(
            f"Invalid S3 URL '{raw_url}': '*' is only supported as a trailing wildcard."
        )

    return TargetSpec(bucket=bucket, mode=TargetMode.EXACT, key=path)


def parse_as_of_timestamp(raw_value: str) -> datetime:
    value = raw_value.strip()
    if value.endswith("Z"):
        value = f"{value[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise click.BadParameter(
            "--as-of-timestamp must be an ISO-8601 datetime, "
            "for example 2026-02-20T10:30:00Z"
        ) from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def check_versioning_enabled(s3_client: S3Client, bucket: str) -> None:
    response = s3_client.get_bucket_versioning(Bucket=bucket)
    status = response.get("Status")
    if status != "Enabled":
        raise click.ClickException(
            f"Bucket '{bucket}' versioning is not enabled (Status={status!r})."
        )


def bucket_exists(s3_client: S3Client, bucket: str) -> bool:
    try:
        s3_client.head_bucket(Bucket=bucket)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchBucket", "NotFound"}:
            return False
        raise


def list_candidate_keys(s3_client: S3Client, target: TargetSpec) -> list[str]:
    if target.mode == TargetMode.EXACT:
        if target.key is None:
            return []
        return [target.key]

    prefix = target.prefix or ""
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []

    for page in paginator.paginate(Bucket=target.bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item.get("Key")
            if isinstance(key, str) and key:
                keys.append(key)

    return keys


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    raise click.ClickException("Encountered S3 version without valid LastModified timestamp.")


def list_object_versions(s3_client: S3Client, bucket: str, key: str) -> list[VersionEntry]:
    paginator = s3_client.get_paginator("list_object_versions")
    entries: list[VersionEntry] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=key):
        for item in page.get("Versions", []):
            item_key = item.get("Key")
            version_id = item.get("VersionId")
            if item_key != key or not isinstance(version_id, str):
                continue
            entries.append(
                VersionEntry(
                    version_id=version_id,
                    last_modified=_coerce_datetime(item.get("LastModified")),
                    is_delete_marker=False,
                    is_latest=bool(item.get("IsLatest", False)),
                )
            )

        for item in page.get("DeleteMarkers", []):
            item_key = item.get("Key")
            version_id = item.get("VersionId")
            if item_key != key or not isinstance(version_id, str):
                continue
            entries.append(
                VersionEntry(
                    version_id=version_id,
                    last_modified=_coerce_datetime(item.get("LastModified")),
                    is_delete_marker=True,
                    is_latest=bool(item.get("IsLatest", False)),
                )
            )

    entries.sort(key=lambda entry: (entry.last_modified, entry.is_latest), reverse=True)
    return entries


def _filter_entries(entries: list[VersionEntry], ignore_delete_markers: bool) -> list[VersionEntry]:
    if not ignore_delete_markers:
        return entries
    return [entry for entry in entries if not entry.is_delete_marker]


def select_version_by_depth(
    entries: list[VersionEntry], versions_back: int, ignore_delete_markers: bool
) -> VersionEntry | None:
    filtered = _filter_entries(entries, ignore_delete_markers)
    target_index = versions_back
    if target_index >= len(filtered):
        return None
    return filtered[target_index]


def select_version_as_of(
    entries: list[VersionEntry], as_of: datetime, ignore_delete_markers: bool
) -> VersionEntry | None:
    filtered = _filter_entries(entries, ignore_delete_markers)
    for entry in filtered:
        if entry.last_modified <= as_of:
            return entry
    return None


def render_selection_mode(
    versions: int | None, as_of_timestamp: datetime | None, ignore_delete_markers: bool
) -> str:
    marker_mode = "excluding delete markers" if ignore_delete_markers else "including delete markers"
    if as_of_timestamp is not None:
        return f"as-of timestamp {as_of_timestamp.isoformat()} ({marker_mode})"
    if versions is None:
        return f"default previous version (1 step back, {marker_mode})"
    return f"{versions} step(s) back from head ({marker_mode})"


def prompt_confirm(message: str) -> None:
    result = questionary.confirm(message, default=False).ask()
    if result is not True:
        raise click.Abort()


def choose_insufficient_action(key: str) -> InsufficientChoice:
    selection = questionary.select(
        f"Not enough versions for '{key}'. How should this be handled?",
        choices=[
            {"name": "Skip this key", "value": InsufficientChoice.SKIP_THIS.value},
            {
                "name": "Skip this and all future insufficient keys",
                "value": InsufficientChoice.SKIP_ALL.value,
            },
            {"name": "Abort run", "value": InsufficientChoice.ABORT.value},
        ],
    ).ask()
    if selection is None:
        return InsufficientChoice.ABORT
    return InsufficientChoice(selection)


def ensure_target_bucket(s3_client: S3Client, target_bucket: str, target_region: str | None) -> None:
    if bucket_exists(s3_client, target_bucket):
        return

    prompt_confirm(f"Target bucket '{target_bucket}' does not exist. Create it now?")

    create_region = target_region or boto3.session.Session().region_name or "us-east-1"
    if create_region == "us-east-1":
        s3_client.create_bucket(Bucket=target_bucket)
    else:
        s3_client.create_bucket(
            Bucket=target_bucket,
            CreateBucketConfiguration={"LocationConstraint": create_region},
        )


def copy_version_to_destination(
    s3_client: S3Client,
    src_bucket: str,
    key: str,
    version_id: str,
    dst_bucket: str,
) -> None:
    s3_client.copy_object(
        Bucket=dst_bucket,
        Key=key,
        CopySource={"Bucket": src_bucket, "Key": key, "VersionId": version_id},
    )


def _sample_keys(keys: list[str], limit: int = 5) -> str:
    if not keys:
        return "(none)"
    sample = keys[:limit]
    if len(keys) > limit:
        sample.append("...")
    return ", ".join(sample)


def run_restore(
    s3_client: S3Client,
    target: TargetSpec,
    versions: int | None,
    as_of_timestamp: datetime | None,
    ignore_delete_markers: bool,
    target_bucket: str | None,
    target_region: str | None,
) -> RunReport:
    report = RunReport()
    destination_bucket = target_bucket or target.bucket

    check_versioning_enabled(s3_client, target.bucket)
    keys = list_candidate_keys(s3_client, target)
    if not keys:
        click.echo("No matching keys found. No changes made.")
        return report

    selection = render_selection_mode(versions, as_of_timestamp, ignore_delete_markers)
    click.echo(f"Matched keys: {len(keys)}")
    click.echo(f"Destination bucket: {destination_bucket}")
    click.echo(f"Selection mode: {selection}")
    click.echo(f"Sample keys: {_sample_keys(keys)}")
    prompt_confirm("Proceed with restore operations?")

    if target_bucket:
        ensure_target_bucket(s3_client, target_bucket, target_region)

    skip_all_insufficient = False

    for key in keys:
        entries = list_object_versions(s3_client, target.bucket, key)
        selected: VersionEntry | None
        if as_of_timestamp is not None:
            selected = select_version_as_of(entries, as_of_timestamp, ignore_delete_markers)
        else:
            versions_back = versions if versions is not None else 1
            selected = select_version_by_depth(entries, versions_back, ignore_delete_markers)

        if selected is None:
            if skip_all_insufficient:
                click.echo(f"Skipping '{key}': insufficient historical versions.")
                report.skipped += 1
                continue

            action = choose_insufficient_action(key)
            if action == InsufficientChoice.SKIP_THIS:
                click.echo(f"Skipping '{key}': insufficient historical versions.")
                report.skipped += 1
                continue
            if action == InsufficientChoice.SKIP_ALL:
                skip_all_insufficient = True
                click.echo(f"Skipping '{key}': insufficient historical versions.")
                report.skipped += 1
                continue
            raise click.Abort()

        try:
            copy_version_to_destination(
                s3_client=s3_client,
                src_bucket=target.bucket,
                key=key,
                version_id=selected.version_id,
                dst_bucket=destination_bucket,
            )
        except ClientError as exc:
            report.failed += 1
            click.echo(f"Failed to restore '{key}': {exc}")
            continue

        report.restored += 1
        click.echo(f"Restored '{key}' from version '{selected.version_id}'.")

    return report


@click.command()
@click.argument("s3_url", type=str)
@click.option(
    "--versions",
    type=click.IntRange(min=1),
    default=None,
    help="How many steps back from the current head version to restore.",
)
@click.option(
    "--as-of-timestamp",
    type=str,
    default=None,
    help="ISO-8601 timestamp; restore to the latest version at or before this time.",
)
@click.option(
    "--ignore-delete-markers",
    is_flag=True,
    default=False,
    help="Ignore delete markers when selecting rollback candidates.",
)
@click.option(
    "--target-bucket",
    type=str,
    default=None,
    help="Destination bucket for restored copies. Defaults to source bucket.",
)
@click.option(
    "--target-region",
    type=str,
    default=None,
    help="Region used when creating a missing target bucket.",
)
def cli(
    s3_url: str,
    versions: int | None,
    as_of_timestamp: str | None,
    ignore_delete_markers: bool,
    target_bucket: str | None,
    target_region: str | None,
) -> None:
    if versions is not None and as_of_timestamp is not None:
        raise click.ClickException("Use either --versions or --as-of-timestamp, not both.")

    parsed_target = parse_s3_url(s3_url)
    parsed_timestamp = parse_as_of_timestamp(as_of_timestamp) if as_of_timestamp is not None else None
    s3_client = boto3.client("s3")

    report = run_restore(
        s3_client=s3_client,
        target=parsed_target,
        versions=versions,
        as_of_timestamp=parsed_timestamp,
        ignore_delete_markers=ignore_delete_markers,
        target_bucket=target_bucket,
        target_region=target_region,
    )

    click.echo(
        f"Completed. Restored={report.restored}, Skipped={report.skipped}, Failed={report.failed}."
    )
    if report.failed > 0:
        raise click.ClickException("One or more restores failed.")
