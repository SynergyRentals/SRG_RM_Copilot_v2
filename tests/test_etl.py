"""Tests for the ETL script using pytest and pytest_httpx."""
import os
import pandas as pd
from pathlib import Path
from typer.testing import CliRunner

from srg_rm_copilot.etl import app



def test_etl_happy_path(tmp_path, httpx_mock):
    """Verify that the ETL writes parquet files when the API responds normally."""
    base_url = "http://testserver"
    # Prepare mocked API responses
    listings_response = [
        {"id": 1},
        {"id": 2},
    ]
    metrics_1 = [
        {"date": "2025-07-01", "value": 100},
        {"date": "2025-07-01", "value": 200},
    ]
    metrics_2 = [
        {"date": "2025-07-01", "value": 300},
    ]
    httpx_mock.add_response(
        method="GET", url=f"{base_url}/listings", json=listings_response
    )
    httpx_mock.add_response(
        method="GET", url=f"{base_url}/listings/1/metrics", json=metrics_1
    )
    httpx_mock.add_response(
        method="GET", url=f"{base_url}/listings/2/metrics", json=metrics_2
    )

    # Set environment variables and run CLI
    env = {
        "WHEELHOUSE_BASE_URL": base_url,
    }
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as fs:
        result = runner.invoke(app, ["etl", "--date", "2025-07-01"], env=env)
        assert result.exit_code == 0, result.output
        # Verify that output parquet files exist and contain the expected number of rows
        file1 = Path("data/raw/1/2025-07-01.parquet")
        file2 = Path("data/raw/2/2025-07-01.parquet")
        assert file1.is_file()
        assert file2.is_file()
        df1 = pd.read_parquet(file1)
        df2 = pd.read_parquet(file2)
        assert len(df1) == len(metrics_1)
        assert len(df2) == len(metrics_2)


def test_etl_rate_limit(tmp_path, httpx_mock, monkeypatch):
    """Verify that the ETL retries on HTTP 429 responses and still writes output."""
    base_url = "http://testserver"
    # The first call to /listings returns 429, then returns the list on retry
    httpx_mock.add_response(method="GET", url=f"{base_url}/listings", status_code=429)
    httpx_mock.add_response(method="GET", url=f"{base_url}/listings", json=[{"id": 1}])
    # Metrics call returns an empty list
    httpx_mock.add_response(method="GET", url=f"{base_url}/listings/1/metrics", json=[])

    # Avoid actually sleeping during tests by patching time.sleep to a no-op
    import time as time_module
    monkeypatch.setattr(time_module, "sleep", lambda *_, **__: None)

    env = {"WHEELHOUSE_BASE_URL": base_url}
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as fs:
        result = runner.invoke(app, ["etl", "--date", "2025-07-01"], env=env)
        assert result.exit_code == 0
        # A parquet file should still be produced for listing 1, even though metrics are empty
        file1 = Path("data/raw/1/2025-07-01.parquet")
        assert file1.is_file()
