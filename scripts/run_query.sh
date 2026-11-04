#!/usr/bin/env bash
# Run a .sql file in the project's Athena workgroup and print the result.
#
#   ./scripts/run_query.sh sql/queries/01_top_channels.sql
#
# Needs AWS credentials (aws sts get-caller-identity should work) and Terraform
# applied, because the workgroup name is read from `terraform output`.

set -euo pipefail

if [ $# -ne 1 ] || [ ! -f "$1" ]; then
  echo "usage: $0 path/to/query.sql" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKGROUP="$(terraform -chdir="$ROOT/infra/envs/dev" output -raw athena_workgroup)"

QID="$(aws athena start-query-execution \
  --work-group "$WORKGROUP" \
  --query-string "$(cat "$1")" \
  --query QueryExecutionId --output text)"

while true; do
  STATE="$(aws athena get-query-execution --query-execution-id "$QID" \
    --query 'QueryExecution.Status.State' --output text)"
  case "$STATE" in
    SUCCEEDED) break ;;
    FAILED | CANCELLED)
      aws athena get-query-execution --query-execution-id "$QID" \
        --query 'QueryExecution.Status.StateChangeReason' --output text >&2
      exit 1
      ;;
  esac
  sleep 1
done

aws athena get-query-results --query-execution-id "$QID" \
  --query 'ResultSet.Rows[*].Data[*].VarCharValue' --output text | column -t -s $'\t'

SCANNED="$(aws athena get-query-execution --query-execution-id "$QID" \
  --query 'QueryExecution.Statistics.DataScannedInBytes' --output text)"
echo
echo "scanned ${SCANNED} bytes"