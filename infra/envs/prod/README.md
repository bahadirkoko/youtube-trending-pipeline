
# prod environment

Same modules as `dev` with production settings (see the diff against `../dev`).

This environment is **validated in CI but intentionally never applied** — the project
runs live in `dev` only. To deploy it for real you would also need to:

1. Put a real `alert_email` in a `terraform.tfvars`.
2. Run `terraform init`, `plan` and `apply` in this folder.
3. Store the YouTube API key in the new prod secret (`make set-secret`-style, prod name).
4. Run `scripts/set_log_retention.sh` against the prod Glue log groups.
