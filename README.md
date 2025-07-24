# SRG RM Copilot v2


This repository contains utilities for interacting with the Wheelhouse API and performing ETL workflows.

## Running the daily ETL

A small CLI is provided to pull metrics for your listings and persist them to Parquet.  You can invoke it using the module runner.  By default it pulls data for yesterday (America/Chicago timezone), but you can pass a date explicitly.

```bash
# Pull metrics for yesterday
python -m srg_rm_copilot etl

# Pull metrics for a specific date
python -m srg_rm_copilot etl --date 2025-07-01
```

Before running the tool set the following environment variables so that the client knows where to reach the Wheelhouse API and how to authenticate:

* `WHEELHOUSE_BASE_URL` – base URL for the Wheelhouse API, e.g. `https://api.wheelhouse.com/v1`
* `WHEELHOUSE_API_KEY` – optional bearer token for authenticating to the API

The ETL will fetch all listing IDs from the `/listings` endpoint and then download metrics for the specified date.  The results are persisted as Parquet files under `data/raw/{listing_id}/{YYYY-MM-DD}.parquet` relative to the current working directory.

## Nightly ETL workflow

A scheduled GitHub Actions workflow runs the ETL every day at **06:05\u00a0UTC** using the default date (yesterday). The workflow installs the package, runs the CLI, prints how many Parquet files were written and the first few file paths, and uploads a small `health.json` report.

- **Changing the schedule:** Edit the `cron` expression in `.github/workflows/nightly_etl.yml` to adjust when the job runs. The current value of `'5 6 * * *'` means 6:05\u00a0UTC every day.
- **Viewing logs & artifacts:** Go to the **Actions** tab and open the **Nightly ETL** workflow. Each run shows the console output, including counts and file paths, and exposes a downloadable **health** artifact containing a JSON report with the run timestamp and counts.
