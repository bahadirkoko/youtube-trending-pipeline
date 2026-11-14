# CLAUDE.md

Serverless YouTube trending data pipeline on AWS (portfolio project). Medallion
architecture: Bronze (raw JSON/CSV) → Silver (typed Parquet) → Gold (4 analytics
tables), queried with Athena. Orchestrated by Step Functions on a daily
EventBridge schedule. See `README.md` for the overview, `docs/architecture.md`
for design decisions, `docs/runbook.md` for operations.

## Environment facts

- AWS account `743116070631`, region `us-east-1`, IAM user `bahadirdev`.
- `infra/envs/dev` is the live, deployed environment. **`infra/envs/prod` is code
  only — validated in CI, intentionally never applied. Never run `terraform apply`
  in it.**
- Terraform state is remote (S3, from `infra/bootstrap`). Resource names follow
  `yt-data-pipeline-<thing>-dev`.
- The pipeline runs daily at 11:00 UTC and sends SNS emails on success/failure.
  The schedule is live: changes to Lambdas/Glue jobs affect tomorrow's run.
- 10 YouTube regions are enabled: CA, DE, FR, GB, IN, JP, KR, MX, RU, US.

## Commands

- `make help` lists all targets. Use Makefile targets over raw commands.
- `make check` = ruff + terraform fmt check + all 136 tests. **Must pass before
  every commit.** `make test-fast` skips the Spark tests (marker: `spark`).
- Spark tests need Java 17 and set PYSPARK_PYTHON themselves.
- `make pipeline-run` starts a real Step Functions execution (costs ~4¢, sends
  email). Don't run it casually.
- `make query Q=sql/queries/<file>.sql` runs an Athena query.
- Terraform: always `plan` before `apply`; show the user the plan and wait for
  their OK before any `apply`.

## Conventions

- Python 3.12, ruff (line length 100, rules E/F/I/B/UP), pyright-clean. Pre-commit
  hooks are installed and use the venv's tool versions (`language: system`).
- No Docker anywhere — Glue jobs are plain PySpark files that also run locally in
  tests; keep them importable and runnable both ways (`run(spark, src, dst)` style).
- Column lists are duplicated on purpose across Spark jobs, Terraform tables,
  `sql/ddl/`, and the data-quality gate — `tests/unit/test_schema_contracts.py`
  enforces they match. When changing a schema, update all four and the test will
  tell you what you missed.
- Views are cumulative in YouTube data: per-channel/category totals must use each
  video's **peak** views, never sum daily views.
- Glue writes use dynamic partition overwrite; keep it that way so re-runs replace
  partitions instead of appending.
- EventBridge uses a classic rule (not Scheduler) because the events role trusts
  `events.amazonaws.com`.
- Makefile recipes need real tabs.

## CI

GitHub Actions on every push to main: ruff, pytest (with Java/Spark), terraform
fmt + `init -backend=false` + validate for bootstrap, dev and prod. No AWS
credentials in CI and no `terraform plan` there — keep it that way.
