from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Callable, Coroutine, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, TypeVar, cast
from urllib.parse import urlparse

import aioboto3
import click
import questionary
from botocore.exceptions import ClientError

AsyncS3Client = Any
T = TypeVar("T")
U = TypeVar("U")


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


@dataclass(frozen=True)
class PlannedRestore:
    key: str
    version_id: str


@dataclass(frozen=True)
class RestorePlan:
    ready: list[PlannedRestore]
    insufficient_keys: list[str]


@dataclass
class RunReport:
    restored: int = 0
    skipped: int = 0
    failed: int = 0


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


def default_max_workers() -> int:
    return min(32, (os.cpu_count() or 1) * 5)


def resolve_max_workers(requested: int | None) -> int:
    return requested if requested is not None else default_max_workers()


async def _iterate_pages(pages: Any) -> AsyncIterator[dict[str, Any]]:
    if hasattr(pages, "__aiter__"):
        async for page in cast(Any, pages):
            yield cast(dict[str, Any], page)
        return

    for page in cast(Iterable[Any], pages):
        yield cast(dict[str, Any], page)


async def check_versioning_enabled(s3_client: AsyncS3Client, bucket: str) -> None:
    response = await s3_client.get_bucket_versioning(Bucket=bucket)
    status = response.get("Status")
    if status != "Enabled":
        raise click.ClickException(
            f"Bucket '{bucket}' versioning is not enabled (Status={status!r})."
        )


async def bucket_exists(s3_client: AsyncS3Client, bucket: str) -> bool:
    try:
        await s3_client.head_bucket(Bucket=bucket)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchBucket", "NotFound"}:
            return False
        raise


async def list_candidate_keys(s3_client: AsyncS3Client, target: TargetSpec) -> list[str]:
    if target.mode == TargetMode.EXACT:
        if target.key is None:
            return []
        return [target.key]

    prefix = target.prefix or ""
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=target.bucket, Prefix=prefix)

    keys: list[str] = []
    async for page in _iterate_pages(pages):
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


async def list_object_versions(
    s3_client: AsyncS3Client, bucket: str, key: str
) -> list[VersionEntry]:
    paginator = s3_client.get_paginator("list_object_versions")
    pages = paginator.paginate(Bucket=bucket, Prefix=key)

    entries: list[VersionEntry] = []
    async for page in _iterate_pages(pages):
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


async def _run_worker_pool(
    items: list[T], max_workers: int, worker: Callable[[T], Coroutine[Any, Any, U]]
) -> list[U]:
    if not items:
        return []

    iterator = iter(items)
    active: set[asyncio.Task[U]] = set()
    results: list[U] = []

    def schedule_one() -> bool:
        try:
            item = next(iterator)
        except StopIteration:
            return False
        active.add(asyncio.create_task(worker(item)))
        return True

    for _ in range(min(max_workers, len(items))):
        if not schedule_one():
            break

    while active:
        done, pending = await asyncio.wait(active, return_when=asyncio.FIRST_COMPLETED)
        active = set(pending)

        for task in done:
            results.append(task.result())

        while len(active) < max_workers:
            if not schedule_one():
                break

    return results


@dataclass(frozen=True)
class _SelectionResult:
    key: str
    selected: VersionEntry | None


async def build_restore_plan(
    s3_client: AsyncS3Client,
    bucket: str,
    keys: list[str],
    versions: int | None,
    as_of_timestamp: datetime | None,
    ignore_delete_markers: bool,
    max_workers: int,
) -> RestorePlan:
    async def process_key(key: str) -> _SelectionResult:
        entries = await list_object_versions(s3_client, bucket, key)
        selected: VersionEntry | None
        if as_of_timestamp is not None:
            selected = select_version_as_of(entries, as_of_timestamp, ignore_delete_markers)
        else:
            versions_back = versions if versions is not None else 1
            selected = select_version_by_depth(entries, versions_back, ignore_delete_markers)
        return _SelectionResult(key=key, selected=selected)

    results = await _run_worker_pool(keys, max_workers=max_workers, worker=process_key)

    ready: list[PlannedRestore] = []
    insufficient: list[str] = []

    for result in results:
        if result.selected is None:
            insufficient.append(result.key)
            continue
        ready.append(PlannedRestore(key=result.key, version_id=result.selected.version_id))

    return RestorePlan(ready=ready, insufficient_keys=insufficient)


def render_selection_mode(
    versions: int | None, as_of_timestamp: datetime | None, ignore_delete_markers: bool
) -> str:
    marker_mode = (
        "excluding delete markers" if ignore_delete_markers else "including delete markers"
    )
    if as_of_timestamp is not None:
        return f"as-of timestamp {as_of_timestamp.isoformat()} ({marker_mode})"
    if versions is None:
        return f"default previous version (1 step back, {marker_mode})"
    return f"{versions} step(s) back from head ({marker_mode})"


def prompt_confirm(message: str) -> None:
    result = questionary.confirm(message, default=False).ask()
    if result is not True:
        raise click.Abort()


def _sample_keys(keys: list[str], limit: int = 5) -> str:
    if not keys:
        return "(none)"
    sample = keys[:limit]
    if len(keys) > limit:
        sample.append("...")
    return ", ".join(sample)


def prompt_continue_with_insufficient(insufficient_keys: list[str]) -> None:
    click.echo(f"Insufficient history for {len(insufficient_keys)} key(s).")
    click.echo(f"Sample insufficient keys: {_sample_keys(insufficient_keys)}")
    prompt_confirm("Continue and skip these keys?")


async def ensure_target_bucket(
    s3_client: AsyncS3Client,
    target_bucket: str,
    target_region: str | None,
    session_region: str | None,
) -> None:
    if await bucket_exists(s3_client, target_bucket):
        return

    prompt_confirm(f"Target bucket '{target_bucket}' does not exist. Create it now?")

    create_region = target_region or session_region or "us-east-1"
    if create_region == "us-east-1":
        await s3_client.create_bucket(Bucket=target_bucket)
    else:
        await s3_client.create_bucket(
            Bucket=target_bucket,
            CreateBucketConfiguration={"LocationConstraint": create_region},
        )


@dataclass(frozen=True)
class _CopyResult:
    key: str
    success: bool
    error: str | None = None


async def _copy_version_to_destination(
    s3_client: AsyncS3Client,
    src_bucket: str,
    destination_bucket: str,
    planned: PlannedRestore,
) -> _CopyResult:
    try:
        await s3_client.copy_object(
            Bucket=destination_bucket,
            Key=planned.key,
            CopySource={
                "Bucket": src_bucket,
                "Key": planned.key,
                "VersionId": planned.version_id,
            },
        )
    except ClientError as exc:
        return _CopyResult(key=planned.key, success=False, error=str(exc))

    return _CopyResult(key=planned.key, success=True)


async def execute_copy_plan(
    s3_client: AsyncS3Client,
    source_bucket: str,
    destination_bucket: str,
    plan: RestorePlan,
    max_workers: int,
) -> RunReport:
    report = RunReport()
    if not plan.ready:
        return report

    active: set[asyncio.Task[_CopyResult]] = set()
    next_index = 0
    stop_scheduling = False

    def schedule_next() -> bool:
        nonlocal next_index
        if next_index >= len(plan.ready):
            return False
        planned = plan.ready[next_index]
        next_index += 1
        active.add(
            asyncio.create_task(
                _copy_version_to_destination(
                    s3_client=s3_client,
                    src_bucket=source_bucket,
                    destination_bucket=destination_bucket,
                    planned=planned,
                )
            )
        )
        return True

    while len(active) < max_workers:
        if not schedule_next():
            break

    while active:
        done, pending = await asyncio.wait(active, return_when=asyncio.FIRST_COMPLETED)
        active = set(pending)

        for task in done:
            result = task.result()
            if result.success:
                report.restored += 1
                click.echo(f"Restored '{result.key}'.")
                continue

            report.failed += 1
            click.echo(f"Failed to restore '{result.key}': {result.error}")
            stop_scheduling = True

        if stop_scheduling:
            continue

        while len(active) < max_workers:
            if not schedule_next():
                break

    if stop_scheduling and next_index < len(plan.ready):
        skipped_due_to_failure = len(plan.ready) - next_index
        report.skipped += skipped_due_to_failure
        click.echo(
            "Skipping "
            f"{skipped_due_to_failure} remaining key(s) because a copy operation failed."
        )

    return report


async def run_restore(
    s3_client: AsyncS3Client,
    target: TargetSpec,
    versions: int | None,
    as_of_timestamp: datetime | None,
    ignore_delete_markers: bool,
    target_bucket: str | None,
    target_region: str | None,
    max_workers: int,
    session_region: str | None,
) -> RunReport:
    destination_bucket = target_bucket or target.bucket

    await check_versioning_enabled(s3_client, target.bucket)
    keys = await list_candidate_keys(s3_client, target)
    if not keys:
        click.echo("No matching keys found. No changes made.")
        return RunReport()

    selection = render_selection_mode(versions, as_of_timestamp, ignore_delete_markers)
    click.echo(f"Matched keys: {len(keys)}")
    click.echo(f"Destination bucket: {destination_bucket}")
    click.echo(f"Selection mode: {selection}")
    click.echo(f"Max workers: {max_workers}")
    click.echo(f"Sample keys: {_sample_keys(keys)}")
    prompt_confirm("Proceed with restore operations?")

    if target_bucket:
        await ensure_target_bucket(s3_client, target_bucket, target_region, session_region)

    plan = await build_restore_plan(
        s3_client=s3_client,
        bucket=target.bucket,
        keys=keys,
        versions=versions,
        as_of_timestamp=as_of_timestamp,
        ignore_delete_markers=ignore_delete_markers,
        max_workers=max_workers,
    )

    if plan.insufficient_keys:
        prompt_continue_with_insufficient(plan.insufficient_keys)

    report = RunReport(skipped=len(plan.insufficient_keys))

    if not plan.ready:
        click.echo("No eligible keys found after selection. No copy operations performed.")
        return report

    copy_report = await execute_copy_plan(
        s3_client=s3_client,
        source_bucket=target.bucket,
        destination_bucket=destination_bucket,
        plan=plan,
        max_workers=max_workers,
    )

    report.restored = copy_report.restored
    report.failed = copy_report.failed
    report.skipped += copy_report.skipped
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
@click.option(
    "--max-workers",
    type=click.IntRange(min=1),
    default=None,
    help="Maximum number of concurrent S3 operations. Defaults to a conservative auto value.",
)
def cli(
    s3_url: str,
    versions: int | None,
    as_of_timestamp: str | None,
    ignore_delete_markers: bool,
    target_bucket: str | None,
    target_region: str | None,
    max_workers: int | None,
) -> None:
    if versions is not None and as_of_timestamp is not None:
        raise click.ClickException("Use either --versions or --as-of-timestamp, not both.")

    parsed_target = parse_s3_url(s3_url)
    parsed_timestamp = (
        parse_as_of_timestamp(as_of_timestamp) if as_of_timestamp is not None else None
    )
    resolved_workers = resolve_max_workers(max_workers)

    session = aioboto3.Session()

    async def _run() -> RunReport:
        async with session.client("s3") as s3_client:
            return await run_restore(
                s3_client=s3_client,
                target=parsed_target,
                versions=versions,
                as_of_timestamp=parsed_timestamp,
                ignore_delete_markers=ignore_delete_markers,
                target_bucket=target_bucket,
                target_region=target_region,
                max_workers=resolved_workers,
                session_region=session.region_name,
            )

    report = asyncio.run(_run())

    click.echo(
        f"Completed. Restored={report.restored}, Skipped={report.skipped}, Failed={report.failed}."
    )
    if report.failed > 0:
        raise click.ClickException("One or more restores failed.")
