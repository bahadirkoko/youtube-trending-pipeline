# Architecture and design notes

This document explains how the pipeline is put together and why. For operating it (alerts,
recovery, rebuild) see [`runbook.md`](runbook.md). For the quick overview see the
[README](../README.md).

![Architecture](architecture.png)

## 1. Data flow

| Layer | Location | Contents | Written by |
|---|---|---|---|
| Bronze | `s3://<bronze>/youtube/raw_statistics/region=XX/date=YYYY-MM-DD/hour=HH/*.json` | Raw API responses, exactly as received. Historical Kaggle CSVs and category reference JSON also land here. | Ingestion Lambda, one-time Kaggle load |
| Silver | `s3://<silver>/youtube/trending_videos/region=XX/` | One typed, deduplicated Parquet table of trending videos, plus the category lookup. | `bronze_to_silver` Glue job, `json_to_parquet` Lambda |
| Gold | `s3://<gold>/youtube/<table>/region=XX/` | Four analytics tables built from Silver. | `silver_to_gold` Glue job |

Bronze is never modified after it is written, so Silver and Gold can always be rebuilt from it.
That is the main reason the raw layer exists.

### Gold tables

| Table | Grain | Used for |
|---|---|---|
| `video_trending_history` | one row per video per region | How long and how high each video trended |
| `trending_analytics` | one row per region per trending day | Daily volume and engagement |
| `channel_analytics` | one row per channel per region | Which channels trend most |
| `category_analytics` | one row per category per region | Which categories trend most |

YouTube view counts are cumulative: a video that trends for 10 days reports a larger number each
day. Summing daily views would count the same views many times, so the channel and category
tables use each video's **peak** views once.

## 2. Orchestration

A Step Functions state machine runs the pipeline in this order:

1. **Ingest.** The ingestion Lambda fetches every configured region. A region that fails is
   recorded and reported but does not stop the other regions.
2. **Parallel Silver.** `bronze_to_silver` (Glue) and `json_to_parquet` (Lambda) run side by
   side because they do not depend on each other.
3. **Quality gate.** The data quality Lambda checks the Silver table. This is a gate, not a
   report: if it fails, Gold is not built.
4. **Gold.** `silver_to_gold` (Glue) runs only after the gate passes.
5. **Notify.** A success email lists the regions ingested, any that failed, and the rows checked.

Glue jobs use the `.sync` integration, so the state machine waits for the job to finish and fails
if the job fails. Each task has retries for transient errors and a catch that routes any
failure to a notify-failure state, so every failed run produces an email that names the step.

It runs daily at 11:00 UTC from an EventBridge rule. The rule's `state` follows the
`pipeline_schedule_enabled` variable, which lets a new environment be deployed disabled and
switched on after a green manual run.

## 3. Data quality gate

Six checks run against Silver on every execution:

| Check | What it catches |
|---|---|
| Row count | Empty or collapsed tables |
| Files readable | Corrupt or partially written Parquet |
| Schema | Missing, extra or wrongly typed columns |
| Null rates | A source field that silently stopped arriving |
| Value ranges | Negative counts, impossible dates, unknown regions |
| Freshness | Ingestion that stopped. Applied only to rows from the live API, so the historical Kaggle data cannot make it fail. |

Thresholds are environment variables with sensible defaults and can be overridden per execution
through the event. The Lambda returns a structured result, and on failure it also publishes the
failed checks to SNS.

## 4. Design decisions and trade-offs

**Partition by region, not by date.** The data is small (hundreds of thousands of rows), and the
natural access pattern is "this region". Partitioning by date as well would create many tiny
files. With 10 regions the partition count stays small and every query can prune by region.

**Partition projection instead of crawlers.** The region values are a known, fixed list, so the
Glue tables declare them as an enum projection. New data is queryable immediately, there is
nothing to crawl or repair, and no crawler cost or schedule to maintain. The trade-off is that
adding a region means updating the projection (and the quality gate's list).

**Plain PySpark on Glue.** The transformations run on a real Spark engine, and the same Python
files run unchanged in the local test suite. There is no Docker image or custom runtime to
maintain.

**Dynamic partition overwrite.** Each run overwrites only the partitions it produced, so
re-running a day replaces that day's output rather than appending duplicates.

**Step Functions rather than a scheduler plus scripts.** It gives retries, per-step failure
handling, a visual execution history and native Glue and Lambda integrations without running a
server.

**EventBridge rule rather than EventBridge Scheduler.** The events IAM role trusts
`events.amazonaws.com`, which rules use. Scheduler would need a different trust policy.

**Alerts on absence as well as failure.** Besides alarms for failed executions and Lambda errors,
a "pipeline not running" alarm fires when no execution has happened for two days. That catches
the failure that does not produce an error: a disabled or broken schedule.

**One module per component.** Each part of the stack (`data_lake`, `ingestion`, `glue_jobs`,
`orchestration`, `monitoring`, ...) is a Terraform module that takes the environment as input.
`dev` and `prod` call the same modules with different settings.

## 5. Infrastructure and environments

- **State** lives in S3 with lockfile-based locking. A small `bootstrap` configuration creates the
  state bucket, because Terraform cannot create the bucket that stores its own state.
- **IAM** uses four roles (Lambda, Glue, Step Functions, EventBridge), each limited to the
  buckets, functions and topic it needs.
- **`dev`** is the deployed environment. It allows force-destroying buckets and deleting
  secrets immediately, so it can be torn down and rebuilt.
- **`prod`** uses the same modules with safer settings: buckets are never force-destroyed, the
  API-key secret stays recoverable for 30 days, and the Athena workgroup is protected. It is
  validated in CI on every push but intentionally never applied, to avoid paying for a second
  copy of a portfolio project.

## 6. Testing strategy

| Level | What it covers | Needs |
|---|---|---|
| Unit | Each Lambda handler (with moto), each Spark job (local Spark), the state machine definition (structure, transitions, timeouts, retries) | Java for Spark tests |
| Contract | Column lists in the Spark jobs, Terraform tables, DDL files and quality gate stay identical | Nothing |
| Integration | Raw API JSON through the real Silver job, the real quality gate, and the real Gold job, checking row counts match at each step | Java |
| Static | ruff, `terraform fmt`, `terraform validate` for every environment | Nothing |

Everything above runs in GitHub Actions without AWS credentials. `make test-fast` skips the
Spark tests for quick feedback while editing.

## 7. Failure modes

| What breaks | What happens | How you find out |
|---|---|---|
| One region's API call fails | Other regions continue; the region is listed as failed | Success email shows it under "failed at ingestion" |
| Source data is bad or missing | Quality gate fails, Gold is not built, previous Gold stays intact | Gate failure email with the failed checks |
| A Glue job fails | The step fails after retries and the execution stops | Failure email and the `pipeline-failed` alarm |
| A Lambda errors repeatedly | Alarm fires | Lambda error alarm emails |
| The schedule stops firing | No execution runs | `pipeline-not-running` alarm after two days |

## 8. Known limitations and next steps

- Only the trending lists are collected, not per-video comments or channel statistics.
- Gold is rebuilt in full for each region on every run; incremental builds would be needed at
  much larger volumes.
- Table formats could move to Apache Iceberg for schema evolution and upserts.
- A BI layer (QuickSight or a notebook) on top of Athena would make the Gold tables easier to
  explore.
- A CD workflow could apply `dev` from CI using GitHub's OIDC federation (no stored AWS keys).
