# aws-s3-ohfuck

> [!NOTE]
>
> This is mostly vibe-coded nonsense, use at your own risk. Worse, I touched some of the code so I'd probably say not to use it at all.
>
> I'm going to archive this for now but email me if you want to do dev on it or whatever :D

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

or use `uvx`:

```bash
$ uvx --with 'git+https://github.com/yaleman/aws-s3-ohfuck' aws-s3-ohfuck --help
Usage: aws-s3-ohfuck [OPTIONS] S3_URL

Options:
  --versions INTEGER RANGE     How many steps back from the current head
                               version to restore.  [x>=1]
  --as-of-timestamp TEXT       ISO-8601 timestamp; restore to the latest
                               version at or before this time.
  --ignore-delete-markers      Ignore delete markers when selecting rollback
                               candidates.
  --target-bucket TEXT         Destination bucket for restored copies.
                               Defaults to source bucket.
  --target-region TEXT         Region used when creating a missing target
                               bucket.
  --max-workers INTEGER RANGE  Maximum number of concurrent S3 operations.
                               Defaults to a conservative auto value.  [x>=1]
  --help                       Show this message and exit.
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

## Live Integration Tests (Testcontainers + S3)

Live tests use a Dockerized LocalStack S3 service via `testcontainers`.

Run them explicitly:

```bash
RUN_LIVE_TESTS=1 uv run pytest -m live -q
```

Notes:

- Docker must be running.
- Live tests are skipped unless `RUN_LIVE_TESTS=1` is set.
- Standard `uv run pytest -q` remains fast and runs unit tests by default.
