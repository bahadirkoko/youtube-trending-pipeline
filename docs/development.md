# Local development

How to set up the project on your machine, run the checks, and make common changes safely.
Nothing in this guide needs an AWS account. For deploying see the [README](../README.md#deploy-to-aws),
and for operating the live pipeline see [`runbook.md`](runbook.md).

## Prerequisites

| Tool      | Version                     | Needed for                                    |
| --------- | --------------------------- | --------------------------------------------- |
| Python    | 3.12                        | everything                                    |
| Java      | 17                          | the Spark tests (Glue jobs run locally)       |
| Terraform | 1.x (CI uses 1.16.4)        | `terraform fmt` and `validate`                |
| AWS CLI   | v2                          | only for deploying and operating, not for tests |

On macOS with Homebrew:

```bash
brew install python@3.12 openjdk@17 terraform awscli
```

Make sure `java -version` prints 17. If it doesn't, put Homebrew's JDK on your `PATH`:

```bash
export PATH="$(brew --prefix openjdk@17)/bin:$PATH"   # add to ~/.zshrc to keep it
```

There is no Docker anywhere in the project. The Glue jobs are plain PySpark files, and the
`pyspark` package from pip includes everything Spark needs apart from Java.

## First-time setup

```bash
git clone https://github.com/bahadirkoko/youtube-trending-pipeline.git
cd youtube-trending-pipeline

make install                 # creates .venv with Python 3.12, installs everything, sets up git hooks
source .venv/bin/activate
```

`make install` is safe to re-run. It creates `.venv` with `python3.12` only if it does not exist
yet, and stops with a message if `python3.12` is missing or an existing `.venv` uses another
Python version. To use a Python 3.12 binary with a different name, pass it in:
`make install PYTHON=/path/to/python3.12`.

Dependencies come from `requirements-dev.txt`, which includes `requirements-ci.txt`. Every
package CI uses (pyspark, ruff, pytest, moto, ...) is therefore installed at the exact same
version locally, and the rest are local-only tools like Jupyter.

Then confirm everything works:

```bash
make check
```

You should see ruff and `terraform fmt` pass and **136 passed** from pytest.

> If pytest reports `96 passed` and some skipped tests instead, Java was not found. The Spark
> test files skip themselves when `java` is not on the `PATH`, so a missing Java does not fail
> the run. It just hides 40 tests. Fix Java before trusting a green result.

### Local `.env` (optional)

```bash
cp .env.example .env
```

and add a YouTube API key if you want to call the API from a notebook or script. The deployed
pipeline never reads `.env`; it gets the key from Secrets Manager (`make set-secret`). `.env` is
gitignored.

## Everyday commands

Activate the venv first (`source .venv/bin/activate`). `make help` lists every target.

| Command          | What it does                                                    | Time       |
| ---------------- | --------------------------------------------------------------- | ---------- |
| `make test-fast` | Tests without Spark                                             | ~5 seconds |
| `make test`      | All tests, including Spark                                      | ~35 seconds |
| `make lint`      | `ruff check` and `ruff format --check` (changes nothing)        | instant    |
| `make format`    | Fix lint issues and reformat Python                             | instant    |
| `make tf-fmt`    | Format Terraform files                                          | instant    |
| `make check`     | lint + Terraform format check + all tests: what CI runs         | ~35 seconds |

`make check` must pass before every commit.

Run a single test file or test while you work:

```bash
pytest tests/unit/test_silver_to_gold.py -q
pytest -k "peak_views" -q
```

## Pre-commit hooks

`make install` installs a git hook that runs on every `git commit`:

- whitespace, end-of-file, YAML/TOML/JSON syntax, merge-conflict markers
- blocks files over 1 MB and private keys
- `ruff check --fix` and `ruff format` on Python files
- `terraform fmt` on Terraform files
- `nbstripout` on notebooks, so outputs are never committed

ruff, terraform and nbstripout run from your own environment, so the venv has to be active when
you commit. If a hook changes a file, the commit stops: look at the change, `git add` it, and
commit again.

```bash
pre-commit run --all-files   # run every hook by hand
```

## Tests

```text
tests/
  conftest.py                         fake AWS credentials for every test
  unit/
    test_youtube_ingestion.py         ingestion Lambda (moto)
    test_json_to_parquet.py           categories Lambda (moto)
    test_data_quality.py              quality gate checks and thresholds (moto)
    test_bronze_to_silver.py          Silver Glue job on local Spark      [spark]
    test_silver_to_gold.py            Gold Glue job on local Spark        [spark]
    test_pipeline_definition.py       Step Functions definition: states, retries, timeouts
    test_schema_contracts.py          column lists match across code, Terraform and DDL
  integration/
    test_pipeline_data_contract.py    API JSON -> Silver -> gate -> Gold  [spark]
```

- Tests can never reach a real AWS account: `conftest.py` sets fake credentials and removes
  `AWS_PROFILE` for every test, and AWS calls go to moto.
- Tests marked `spark` start a local Spark session and set `PYSPARK_PYTHON` themselves.
  `make test-fast` skips them with `-m "not spark"`.
- Glue jobs expose `run(spark, src, dst)`, so a test passes local temp folders where Glue
  passes S3 paths. Keep new jobs in the same shape.

## Making common changes

### Changing a schema (adding or renaming a column)

The column lists are copied on purpose, and `test_schema_contracts.py` fails until they all
agree. Update:

1. **The Spark job:** `SILVER_COLUMNS` in `src/glue_jobs/bronze_to_silver.py`, or `GOLD_SCHEMAS`
   in `src/glue_jobs/silver_to_gold.py`.
2. **The Glue table in Terraform:** `infra/modules/silver_tables/main.tf` or
   `infra/modules/gold_tables/main.tf`.
3. **For Gold, the DDL file:** `sql/ddl/gold_<table>.sql`.
4. **For Silver, the quality gate:** `EXPECTED_TYPES` in `src/lambdas/data_quality/handler.py`.
5. **The docs:** [`data-dictionary.md`](data-dictionary.md). No test checks this one.

Then run `make check`. Deploying the change needs a `terraform plan` and `apply` in
`infra/envs/dev`, and the next pipeline run rewrites the data with the new column.

### Changing a Lambda or Glue job

Edit the file under `src/`, add or update its tests, and run `make check`. The code only goes
live after a `terraform apply` in `infra/envs/dev`, but once applied it is used by the next
scheduled run at 11:00 UTC. Always run `terraform plan` first and read it before applying.

### Changing Terraform

```bash
make tf-fmt
terraform -chdir=infra/envs/dev init -backend=false   # once, if you have no AWS access
make tf-validate
```

`make tf-validate` needs `terraform init` in `infra/bootstrap`, `infra/envs/dev` and
`infra/envs/prod` first. `-backend=false` is enough for validating, and is what CI does.
`infra/envs/prod` is validated but **never applied**.

## CI

GitHub Actions ([`.github/workflows/ci.yml`](../.github/workflows/ci.yml)) runs on every push to
`main` and every pull request:

| Job       | Runs                                                                |
| --------- | ------------------------------------------------------------------- |
| Lint      | `ruff check .` and `ruff format --check .`                          |
| Tests     | `pytest -q` with Java 17, dependencies from `requirements-ci.txt`   |
| Terraform | `terraform fmt -check` and `init -backend=false` + `validate` for bootstrap, dev and prod |

CI has no AWS credentials and never runs `terraform plan`, so a green CI run tells you the code
and Terraform are valid, not that a deploy will succeed.

## Troubleshooting

| Problem                                              | Fix                                                                                 |
| ---------------------------------------------------- | ----------------------------------------------------------------------------------- |
| Fewer than 136 tests pass and some are skipped       | Java is missing or not on the `PATH`. Check `java -version`.                        |
| Spark tests fail with `Python worker` or version errors | Run pytest from the activated venv, so Spark uses the same Python.               |
| A commit is rejected by `ruff` or `terraform fmt`    | The hook already fixed the file. `git add` it and commit again.                     |
| `pre-commit` or `ruff` not found when committing     | Activate the venv: `source .venv/bin/activate`.                                     |
| `make tf-validate` fails with "init required"        | Run `terraform -chdir=<dir> init -backend=false` in each of the three directories.  |
