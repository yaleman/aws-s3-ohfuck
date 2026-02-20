# aws-s3-ohfuck

Restore S3 objects to historical versions with a safety-first CLI.

This tool:

- validates bucket versioning is enabled,
- lets you target a whole bucket, prefix, or single object,
- restores by copying a historical `VersionId` over the destination key,
- asks for confirmation before any write operations.

## Requirements

- Python 3.13+
- AWS credentials configured in your environment
- IAM permissions for:
  - `s3:GetBucketVersioning`
  - `s3:ListBucket`
  - `s3:ListBucketVersions`
  - `s3:GetObjectVersion`
  - `s3:PutObject`
  - optional target-bucket creation permissions (`s3:CreateBucket`)

## Install / run

```bash
uv lock
uv run aws-s3-ohfuck --help
```

## Usage

```bash
aws-s3-ohfuck [OPTIONS] S3_URL
```

`S3_URL` supports:

- `s3://mybucket`
- `s3://mybucket/*`
- `s3://mybucket/path/to/file.txt`
- `s3://mybucket/prefix/*`

## Selection options

- `--versions N`
  - rollback by N steps from head (includes delete markers by default)
- `--as-of-timestamp 2026-02-20T10:30:00Z`
  - restore to latest version at or before timestamp
- `--ignore-delete-markers`
  - filter out delete markers when selecting versions

`--versions` and `--as-of-timestamp` are mutually exclusive.

If neither is set, default rollback target is the previous version (1 step back).

## Destination options

- default destination: source bucket (in-place restore)
- `--target-bucket BUCKET`
  - restore into another bucket without overwriting source keys
- `--target-region REGION`
  - region used only when creating a missing target bucket

If `--target-bucket` does not exist, the CLI prompts before creating it.

## Parallelism and scale

For large buckets/prefixes, restore work is parallelized.

- `--max-workers N`
  - bound concurrent S3 operations
  - default is an automatic conservative value

Safety behavior in parallel mode:

- one preflight confirmation before writes,
- insufficient-history keys are summarized and confirmed in batch,
- first copy failure stops scheduling new copies (in-flight work completes).

## Examples

Rollback one object by 2 versions:

```bash
uv run aws-s3-ohfuck s3://mybucket/path/to/file.txt --versions 2
```

Restore everything under a prefix as-of a timestamp:

```bash
uv run aws-s3-ohfuck s3://mybucket/archive/* \
  --as-of-timestamp 2026-02-20T10:30:00Z
```

Restore an entire bucket into a new target bucket with 20 workers:

```bash
uv run aws-s3-ohfuck s3://mybucket/* \
  --target-bucket mybucket-restored \
  --target-region us-west-2 \
  --max-workers 20
```

## Development

Run checks locally:

```bash
uv run pytest -q
uv run ty check
uv run ruff check
```

No change is complete unless `ty check` and `ruff check` pass.
