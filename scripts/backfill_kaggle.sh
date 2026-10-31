#!/usr/bin/env bash
# Upload the Kaggle historical files from data/ to the Bronze bucket.
#
#   data/raw/<RR>videos.csv            -> s3://<bronze>/youtube/kaggle_raw/region=<RR>/
#   data/reference/<RR>_category_id.json -> s3://<bronze>/youtube/kaggle_reference/region=<RR>/
#
# Safe to re-run (same keys are simply overwritten).
# Usage:  ./scripts/backfill_kaggle.sh [--dry-run]
# Optional env vars: ENVIRONMENT (dev), PROJECT_NAME (yt-data-pipeline), AWS_REGION (us-east-1)

set -euo pipefail

ENVIRONMENT="${ENVIRONMENT:-dev}"
PROJECT_NAME="${PROJECT_NAME:-yt-data-pipeline}"
REGION="${AWS_REGION:-us-east-1}"

DRY_RUN=()
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=(--dryrun)
  echo "DRY RUN: nothing will be uploaded"
fi

cd "$(dirname "$0")/.."   # repo root

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
BUCKET="${PROJECT_NAME}-bronze-${ACCOUNT_ID}-${REGION}-${ENVIRONMENT}"

if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "ERROR: bucket '$BUCKET' not found or not accessible."
  echo "Run 'terraform apply' in infra/envs/${ENVIRONMENT} first."
  exit 1
fi
echo "Bronze bucket: $BUCKET"

shopt -s nullglob
csv_count=0
json_count=0

for file in data/raw/*videos.csv; do
  name="$(basename "$file")"          # e.g. USvideos.csv
  region="${name:0:2}"                # US
  aws s3 cp "$file" "s3://${BUCKET}/youtube/kaggle_raw/region=${region}/${name}" \
    --content-type text/csv --only-show-errors ${DRY_RUN[@]+"${DRY_RUN[@]}"}
  echo "  csv  ${region}  ${name}"
  csv_count=$((csv_count + 1))
done

for file in data/reference/*_category_id.json; do
  name="$(basename "$file")"          # e.g. US_category_id.json
  region="${name:0:2}"
  aws s3 cp "$file" "s3://${BUCKET}/youtube/kaggle_reference/region=${region}/${name}" \
    --content-type application/json --only-show-errors ${DRY_RUN[@]+"${DRY_RUN[@]}"}
  echo "  json ${region}  ${name}"
  json_count=$((json_count + 1))
done

if [[ $csv_count -eq 0 || $json_count -eq 0 ]]; then
  echo "ERROR: expected files not found (csv: $csv_count, json: $json_count). Check data/raw and data/reference."
  exit 1
fi

echo "Done: ${csv_count} CSV and ${json_count} JSON files."
echo "Verify: aws s3 ls s3://${BUCKET}/youtube/kaggle_raw/ --recursive --human-readable"