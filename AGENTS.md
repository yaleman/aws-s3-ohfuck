# AGENTS.md

## Purpose

`aws-s3-ohfuck` restores S3 objects to historical versions using S3 versioning. This tool is intentionally safety-first and interactive.

## Project layout

- `aws_s3_ohfuck/cli.py`: main CLI implementation
- `main.py`: thin entrypoint wrapper
- `tests/test_main.py`: unit tests for parsing, selection logic, and restore workflow behavior

## Local workflow

- Install deps / sync lockfile: `uv lock`
- Run tests: `uv run pytest -q`
- Run live S3 integration tests: `RUN_LIVE_TESTS=1 uv run pytest -m live -q`
- Type checks (required): `uv run ty check`
- Lint checks (required): `uv run ruff check`

No task is complete unless `uv run ty check` and `uv run ruff check` pass.

Live tests require Docker and use `testcontainers` + LocalStack S3.

## Behavioral constraints

- Always verify source bucket versioning is enabled before restore work.
- Always require explicit confirmation before any copy operations.
- Support URL targets:
  - `s3://bucket`
  - `s3://bucket/*`
  - `s3://bucket/key`
  - `s3://bucket/prefix/*`
- Use `copy_object` with `CopySource.VersionId` for restoration.
- Respect selection semantics:
  - default (no selector): previous version (1 step back)
  - `--versions N`
  - `--as-of-timestamp` (mutually exclusive with `--versions`)
  - `--ignore-delete-markers`
- `--target-bucket` must prompt for bucket creation if missing.

## Parallel execution policy

- Parallelism is bounded by `--max-workers` (or auto default).
- Insufficient-history keys are handled with a batch prompt.
- On first copy failure, stop scheduling new copy tasks; allow already in-flight tasks to complete.
- Report restored/skipped/failed totals and return non-zero if failures occurred.

## Notes for future changes

- Keep prompts and safety checks explicit and difficult to bypass accidentally.
- Keep CLI behavior stable unless README and tests are updated in the same change.
