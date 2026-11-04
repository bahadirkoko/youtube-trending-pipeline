#!/usr/bin/env bash
# Set a retention period on the CloudWatch log groups that AWS Glue creates itself.
#
#   ./scripts/set_log_retention.sh        # 14 days (same as the Terraform-managed groups)
#   ./scripts/set_log_retention.sh 30     # 30 days
#
# Terraform owns the Lambda and Step Functions log groups and sets their retention.
# Glue creates /aws-glue/... groups on its own, with "never expire". Run this after
# the first Glue run, and again whenever a new Glue job starts logging.

set -euo pipefail

DAYS="${1:-14}"

GROUPS_FOUND="$(aws logs describe-log-groups --log-group-name-prefix /aws-glue/ \
  --query 'logGroups[].logGroupName' --output text)"

if [ -z "$GROUPS_FOUND" ]; then
  echo "no /aws-glue/ log groups found, nothing to do"
  exit 0
fi

for group in $GROUPS_FOUND; do
  aws logs put-retention-policy --log-group-name "$group" --retention-in-days "$DAYS"
  echo "retention ${DAYS} days: ${group}"
done