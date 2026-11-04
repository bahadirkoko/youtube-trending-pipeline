#!/usr/bin/env bash
# Store the YouTube API key in Secrets Manager (input is hidden, never printed or saved).
set -euo pipefail

ENVIRONMENT="${ENVIRONMENT:-dev}"
PROJECT_NAME="${PROJECT_NAME:-yt-data-pipeline}"

read -rsp "Paste YouTube API key (hidden), then press Enter: " YT_KEY
echo
aws secretsmanager put-secret-value \
  --secret-id "${PROJECT_NAME}/${ENVIRONMENT}/youtube-api-key" \
  --secret-string "$YT_KEY" > /dev/null
unset YT_KEY
echo "Secret updated."
