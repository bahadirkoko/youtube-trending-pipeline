# Common tasks. Run `make` or `make help` to list them.
# Activate the virtualenv first:  source .venv/bin/activate

.DEFAULT_GOAL := help

.PHONY: help install format lint test test-fast check tf-fmt tf-fmt-check tf-validate \
    pipeline-run query backfill backfill-dry set-secret

help: ## Show this list
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

# ---------- setup ----------
install: ## Install dev dependencies and the git hooks
	pip install -r requirements-dev.txt
	pre-commit install

# ---------- Python quality ----------
format: ## Reformat all Python code
	ruff check --fix .
	ruff format .

lint: ## Lint and check formatting (changes nothing)
	ruff check .
	ruff format --check .

test: ## Run all tests (the Spark ones need Java and take about a minute)
	pytest -q

test-fast: ## Tests without Spark (about 1 second)
	pytest -m "not spark" -q

check: lint tf-fmt-check test ## Everything CI runs, locally

# ---------- Terraform ----------
tf-fmt: ## Format all Terraform files
	terraform fmt -recursive infra

tf-fmt-check: ## Fail if any Terraform file is not formatted
	terraform fmt -check -recursive infra

tf-validate: ## terraform validate for bootstrap, dev and prod (run terraform init in each first)
	terraform -chdir=infra/bootstrap validate
	terraform -chdir=infra/envs/dev validate
	terraform -chdir=infra/envs/prod validate

# ---------- Operating the pipeline ----------
pipeline-run: ## Start one pipeline execution now
	aws stepfunctions start-execution --query executionArn --output text \
	    --state-machine-arn "$$(terraform -chdir=infra/envs/dev output -raw state_machine_arn)"

query: ## Run an Athena query file:  make query Q=sql/queries/01_top_channels.sql
	@test -n "$(Q)" || (echo "usage: make query Q=path/to/query.sql" && exit 1)
	./scripts/run_query.sh $(Q)

backfill: ## Upload the Kaggle CSV and JSON files to Bronze
	./scripts/backfill_kaggle.sh

backfill-dry: ## Show what backfill would upload, without uploading
	./scripts/backfill_kaggle.sh --dry-run

set-secret: ## Store the YouTube API key in Secrets Manager
	./scripts/set_youtube_key.sh
