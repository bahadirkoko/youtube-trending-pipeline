
# Runbook

What to do when the YouTube trending pipeline raises an alert, plus the common
operations. Names below are for the `dev` environment in `us-east-1`.

Run commands from `infra/envs/dev` (so `terraform output` works) unless noted.

## At a glance

| Thing | Name |
|---|---|
| State machine | `yt-data-pipeline-orchestrator-dev` |
| Schedule | EventBridge rule `yt-data-pipeline-orchestrator-dev-schedule`, daily 11:00 UTC |
| Lambdas | `yt-data-pipeline-youtube-ingestion-dev`, `-json-to-parquet-dev`, `-data-quality-dev` |
| Glue jobs | `yt-data-pipeline-bronze-to-silver-dev`, `yt-data-pipeline-silver-to-gold-dev` |
| Alerts | SNS topic `yt-data-pipeline-alerts-dev` (email) |
| Dashboard | `terraform output dashboard_url` |
| Athena workgroup | `yt-data-pipeline-dev` (1 GB scan cap per query) |
| Databases | `yt_pipeline_bronze_dev`, `yt_pipeline_silver_dev`, `yt_pipeline_gold_dev` |

The flow: **Ingest** (API to Bronze) → **Silver** (categories Lambda and Bronze-to-Silver
Glue job, in parallel) → **Data quality gate** → **Gold** (Silver-to-Gold Glue job) →
success email. A failure at any step sends a failure email and stops the run.

## Important: what can and cannot be recovered

- **Silver and Gold can always be rebuilt** from Bronze. Re-running the pipeline is safe
  (every step is idempotent).
- **A missed ingestion day cannot be recovered.** The YouTube API only returns what is
  trending *now*. If ingestion fails for a day, that day's live data is gone for good.
  Fix ingestion problems first and quickly.
- The Kaggle history (2017-2018) can be re-uploaded at any time with `make backfill`.

## Alerts and what to do

You may get two emails for one failure: one from the state machine itself and one from
the CloudWatch alarm. That is intentional, the alarm is the backstop.

### "YouTube pipeline: FAILED" email, or alarm `pipeline-failed`

1. Find the failing step:

   ```bash
   SM=$(terraform output -raw state_machine_arn)
   aws stepfunctions list-executions --state-machine-arn "$SM" --max-items 5 \
     --query 'executions[].[name,status,startDate]' --output table
   aws stepfunctions get-execution-history --execution-arn <EXECUTION_ARN> --reverse-order \
     --max-items 6 --query 'events[].[type,stateEnteredEventDetails.name,taskFailedEventDetails.error,taskFailedEventDetails.cause]' --output text
   ```

   The email also has a link straight to the execution in the console.
2. Go to the matching section below, fix the cause, then re-run (see "Run the pipeline now").

An execution that was stopped on purpose also trips this alarm (status `ABORTED`).

### Failed in `Ingest`, or "NoRegionIngested"

Cause is nearly always the YouTube API: bad or expired key, or the daily quota (10,000
units) used up.

```bash
aws logs tail /aws/lambda/yt-data-pipeline-youtube-ingestion-dev --since 1h
```

- `API key not valid` / 400 / 403 `keyInvalid`: set a new key with `make set-secret`.
- `quotaExceeded`: wait until the quota resets (midnight Pacific time), then re-run. Do
  not loop re-runs, each one spends more quota.
- Some regions failing but others fine does **not** stop the run. The success email lists
  the failed regions, and the log shows why.

### `DataQualityFailed` (the gate said no)

The gate emails its own report (`data quality FAILED (k of 6 checks)`) listing every
failed check. Gold was **not** rebuilt, so Gold still holds yesterday's good data.

| Failed check | Usually means | First step |
|---|---|---|
| `row_count` | Silver is empty or a region is tiny | Was Bronze written? List `s3://<bronze>/youtube/raw_statistics/` |
| `files_readable` | A Parquet file is corrupt or half-written | Re-run the Bronze-to-Silver job |
| `schema` | A column is missing or has a new type | Compare with `SILVER_COLUMNS` in `src/glue_jobs/bronze_to_silver.py` |
| `null_rate` | The API changed a field or a source broke | Query the region in Athena, look at the null column |
| `value_ranges` | Negative counts, huge views, ratio outside 0..1, bad dates | Query the offending region in Athena |
| `freshness` | No live API data newer than 48 hours | Ingestion or Glue did not run, check the last executions |

To look at the gate's full result without waiting for a run:

```bash
aws lambda invoke --function-name yt-data-pipeline-data-quality-dev \
  --cli-binary-format raw-in-base64-out --payload '{}' /tmp/dq.json >/dev/null && python3 -m json.tool /tmp/dq.json
```

Thresholds live in `infra/modules/data_quality/variables.tf`. To test a looser setting
once, without changing anything, pass overrides:
`--payload '{"thresholds":{"max_age_hours":96}}'`. Do not raise a threshold just to make
an alert go away without understanding the cause.

### Failed in `BronzeToSilver` or `BuildGold` (a Glue job)

```bash
aws glue get-job-runs --job-name yt-data-pipeline-bronze-to-silver-dev --max-results 3 \
  --query 'JobRuns[].[Id,JobRunState,ErrorMessage]' --output text
aws logs tail /aws-glue/jobs/error --since 1h
```

(Use `yt-data-pipeline-silver-to-gold-dev` for Gold.) Typical causes: out of memory on a
big input (raise `number_of_workers` in `infra/modules/glue_jobs/variables.tf`), or a
Silver/Bronze schema change. Glue jobs are never auto-retried on failure, on purpose.

### Alarm `*-lambda-errors`

A Lambda raised an error. Find it:

```bash
aws logs tail /aws/lambda/<function-name> --since 1h
```

The alarm turns itself back to `OK` after the next clean 5 minutes.

### Alarm `pipeline-not-running`

No execution started for 2 days in a row, so the schedule is not firing.

```bash
aws events describe-rule --name yt-data-pipeline-orchestrator-dev-schedule \
  --query '[State,ScheduleExpression]' --output text
terraform output pipeline_schedule_state
```

If the rule is `DISABLED`, set `pipeline_schedule_enabled` to `true` in
`infra/envs/dev/variables.tf` and `terraform apply`. Do not enable or disable the rule
with the CLI: Terraform owns it and would undo the change.

### No emails arriving at all

```bash
aws sns list-subscriptions-by-topic --topic-arn "$(terraform output -raw alerts_topic_arn)" \
  --query 'Subscriptions[].[Endpoint,SubscriptionArn]' --output table
```

`PendingConfirmation` means the confirmation email was never clicked (check spam).
After a `terraform destroy` and re-apply the subscription must be confirmed again.

## Common operations

### Run the pipeline now

```bash
EXEC=$(aws stepfunctions start-execution --state-machine-arn "$(terraform output -raw state_machine_arn)" \
  --query executionArn --output text)
aws stepfunctions describe-execution --execution-arn "$EXEC" --query status --output text
```

A full run takes about 5-6 minutes and costs roughly 10 cents (two small Glue runs).

### Stop a running execution

```bash
aws stepfunctions stop-execution --execution-arn "$EXEC" --cause "why"
```

### Re-run only one stage

```bash
aws glue start-job-run --job-name yt-data-pipeline-silver-to-gold-dev   # Gold only
aws glue start-job-run --job-name yt-data-pipeline-bronze-to-silver-dev # Silver only
```

### Query the data

```bash
./scripts/run_query.sh sql/queries/01_top_channels.sql
./scripts/run_query.sh sql/queries/07_gold_reconciliation.sql   # Silver vs Gold row counts
```

### Rotate the YouTube API key

```bash
make set-secret
```

### Add a region

Add its code to `youtube_regions` in `infra/envs/dev/variables.tf` and `terraform apply`.
Ingestion currently fetches only the regions in that list. The Athena tables already know
all 10 region folders.

### Pause everything

Set `pipeline_schedule_enabled` to `false` and `terraform apply`. Nothing is deleted.

### Test that alerts still work

Deliberately break something harmless and check that the email arrives:

```bash
# 1. A Lambda error (invalid setting, touches no data)
aws lambda invoke --function-name yt-data-pipeline-data-quality-dev \
  --cli-binary-format raw-in-base64-out --payload '{"thresholds":{"nonsense":1}}' /tmp/x.json

# 2. An aborted execution
EXEC=$(aws stepfunctions start-execution --state-machine-arn "$(terraform output -raw state_machine_arn)" \
  --query executionArn --output text); sleep 3
aws stepfunctions stop-execution --execution-arn "$EXEC" --error AlarmTest --cause "alarm test"
```

Expect two `ALARM:` emails within about 3-6 minutes. They clear on their own.

## Cost controls

- Glue: about 4 cents per job run, two runs per pipeline run (about $3 a month at one run
  a day). Both jobs stop after 30 minutes at the latest.
- Athena: queries are cancelled above 1 GB scanned.
- Logs: 14 days. Glue creates its own `/aws-glue/` log groups with no expiry, so run
  `./scripts/set_log_retention.sh` after the first run of any new Glue job.
- Lambda, Step Functions, SNS, EventBridge and CloudWatch alarms are negligible at this
  volume.

## Rebuilding from nothing (after `terraform destroy`)

`dev` allows buckets to be destroyed with their contents, so a destroy deletes all data.

1. `terraform apply` in `infra/bootstrap` first if the state bucket is gone, then in `infra/envs/dev`.
2. Click the SNS confirmation email.
3. `make set-secret` (the secret exists but is empty after a rebuild).
4. `make backfill` to re-upload the Kaggle history to Bronze.
5. Run the pipeline once by hand and wait for the success email.
6. `./scripts/set_log_retention.sh`.

Live API history from before the rebuild is not recoverable (see the top of this file).

## Known behaviours (not bugs)

- Kaggle history ends in June 2018 and the live data starts in October 2026, so daily
  charts jump across an 8-year gap.
- The live API returns about 50 videos per region per day, the Kaggle files about 200.
- Channels are identified by `channel_title`, because the Kaggle files have no channel id.
- Channel and category totals add up each video's *peak* views, never daily views, since
  YouTube view counts are cumulative.
- The gate's `freshness` check only looks at live API rows, never at the Kaggle history.
