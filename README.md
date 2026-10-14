# GitHub Events Pipeline

A batch data pipeline built with PySpark. It pulls hourly event files from GH Archive, tracks the state of every file, retries failures up to a limit, validates the schema, and runs the data through quality check, transform and publish stages.

Built to show how a batch pipeline can stay in control of its own state: every file is known, every failure is counted, and a bad file does not get lost.

Tech stack: Python · PySpark · SQLite · GH Archive · pytest

## Architecture

The pipeline runs as one Python process. `gh_events/pipeline.py` calls the extract stage, starts Spark, registers the files in a SQLite state database, then runs four stages in order on those files. Each stage prints how long it took, and the run ends with a status report and a list of any failed files.

extract → schema validation → quality check → transform → publish

## Pipeline layers

**Extract** (`gh_events/stages/extract.py`) downloads the hourly GH Archive files. It also decides how late files are handled: a file counts as on time inside a window of `LATE_WINDOW_HOURS` (2), and `BACKFILL_MODE` switches that check off for historical runs.

**State tracking** (`gh_events/state_store.py`) keeps a `file_state` table in SQLite with one row per file. `discover_files` registers new files as discovered and never resets or duplicates a file it has already seen. When a stage fails on a file, `handle_stage_failure` adds a retry and keeps the file's state. Once a file reaches `MAX_RETRIES` (2), it is marked FAILED.

**Schema validation** (`gh_events/stages/schema_validation.py`) flattens each event and checks 7 fields: `id`, `type`, `created_at`, `actor.id`, `actor.login`, `repo.id`, `repo.name`. A missing field or a wrong type is caught. Extra fields are allowed. The comparison lives in its own function, `compare_schema`, so it can be tested without Spark.

**Quality check, transform and publish** (`gh_events/stages/`) run after validation, in that order.

**Monitoring** (`gh_events/monitoring.py`) prints the pipeline status and the files that failed.

**Analysis** (`analysis/queries.py`) holds the queries run on the published data.

## Setup

```
pip install -r requirements.txt
pip install pytest
```

`requirements.txt` has pyspark and requests. pytest is installed separately.

Set `GH_PROJECT_DIR` to the folder where the pipeline should keep its data. The processing date and the retry and late-file settings are in `gh_events/config.py`.

## Run

From the project root:

```
python3 -m gh_events.pipeline
```

## Tests

9 tests, all passing:

- 3 for the retry logic in `handle_stage_failure`
- 2 for `discover_files`
- 4 for `compare_schema`

From the project root:

```
python3 -m pytest -v
```

## Repo structure

```
github-events-pipeline/
├── gh_events/
│   ├── config.py                   # dates, retry limit, late window, paths
│   ├── pipeline.py                 # entry point, runs the stages in order
│   ├── state_store.py              # file state in SQLite, retry logic
│   ├── monitoring.py               # status and failed file reports
│   ├── spark_session.py            # Spark session setup
│   └── stages/
│       ├── extract.py              # download and late-file handling
│       ├── schema_validation.py    # schema check and compare_schema
│       ├── quality_check.py
│       ├── transform.py
│       └── publish.py
├── analysis/
│   └── queries.py                  # analysis queries
├── tests/
│   ├── test_state_store.py         # retry and discover_files tests
│   └── test_schema_validation.py   # compare_schema tests
├── DECISIONS.md                    # design notes
└── requirements.txt
```

## Roadmap

Things I plan to add:

- Download timeout and retry in the extract stage, so a network error skips one file instead of stopping the run
- Logging in place of print statements
- Processing date read from an environment variable, defaulting to today
- Tests for the late-arrival routing and the time helpers
- GitHub Actions to run the tests on every push
- A performance baseline and a Dockerfile