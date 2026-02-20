# Implement aws-s3-ohfuck S3 Version Rollback CLI (with Timestamp Restore)

## Summary

Build a Python CLI (via uv) that restores S3 objects to an earlier state using S3 versioning and copy_object with VersionId, with safe interactive prompts
(questionary) and strict quality gates (ty check, ruff check).

## Scope and Restore Selection

- Input target positional s3_url supports:
  - s3://bucket
  - s3://bucket/*
  - s3://bucket/key.txt
  - s3://bucket/prefix/*
- Verify source bucket versioning is Enabled before any restore operations.
- Restore destination:
  - default: overwrite source object in source bucket
  - optional: write to --target-bucket (same key path)
- Selection modes:
  - --versions N: rollback by N from head (include delete markers by default)
  - --as-of-timestamp TS: choose most recent version with LastModified <= TS
  - --ignore-delete-markers: exclude delete markers from selection pool
- --versions and --as-of-timestamp are mutually exclusive (CLI validation error if both set).
- If neither selector is provided, default rollback target is the previous version from head (equivalent to one-step rollback).

## CLI/API Changes

- Replace scaffold main.py with Click app entrypoint.
- Add script entry in pyproject.toml, e.g. aws-s3-ohfuck = "main:cli".
- CLI interface:
  - positional: s3_url

  - options:
    - --versions INTEGER
    - --as-of-timestamp TEXT (parse ISO-8601 to timezone-aware datetime)
    - --ignore-delete-markers
    - --target-bucket TEXT
    - --target-region TEXT
- Prompting:
  - single preflight confirmation before file operations
  - prompt to create --target-bucket if missing
  - prompt on insufficient history (skip / skip-all / abort)

## Internal Design

- TargetSpec, VersionEntry, RunReport dataclasses.
- Core functions:
  - parse_s3_url
  - check_versioning_enabled
  - list_candidate_keys
  - list_versions_for_key
  - select_version_by_depth
  - select_version_as_of
  - ensure_target_bucket
  - restore_key
  - run_restore
- Selection logic:
  - normalize and optionally filter delete markers
  - depth mode:
    - explicit --versions N => index N from head semantics
    - default mode => immediate previous version
  - timestamp mode:
    - pick newest entry at or before timestamp
    - if none, trigger insufficient-history flow

## Error Handling and UX

- Invalid URL / invalid timestamp / conflicting selectors -> clear non-zero exit.
- Versioning disabled -> non-zero exit.
- No matching keys -> no-op with message, exit 0.
- Copy failures are collected and summarized; exit non-zero if any failures.
- User declines confirmation -> clean abort with zero copy operations.

## Dependencies and Tooling

- Add runtime deps: boto3, click, questionary.
- Add dev deps: ruff, ty, pytest.
- Configure lint/type settings for Python 3.13 in pyproject.toml.
- Update uv.lock.

## Tests and Acceptance

- URL parsing tests for all supported patterns.
- Selector tests:
  - depth selection including delete markers
  - --ignore-delete-markers
  - timestamp selection boundaries and timezone handling
  - mutual exclusivity and default previous-version behavior
- Prompt flow tests (preflight, bucket creation, insufficient history choices).
- Restore call tests validate CopySource and destination behavior.
- Completion gates:
  - uv run ty check passes
  - uv run ruff check passes

## Assumptions Locked

- Wildcard processing is recursive.
- Batch confirmation is one summary prompt.
- Default selection includes delete markers unless --ignore-delete-markers.
- --versions and --as-of-timestamp are mutually exclusive.
- If no selector is provided, rollback defaults to previous version.
- Target bucket creation defaults to AWS current default region; --target-region overrides.>
