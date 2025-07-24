import os
from pathlib import Path
from typer.testing import CliRunner
from srg_rm_copilot.etl import app


def test_cli_accepts_date(httpx_mock, tmp_path, monkeypatch):
    """Ensure the CLI accepts a date argument and exits successfully."""
    # Set environment variables for base URL and API key
    os.environ["WHEELHOUSE_BASE_URL"] = "http://testserver"
    os.environ["WHEELHOUSE_API_KEY"] = "testtoken"

    date_str = "2025-07-01"
    # Mock the /listings endpoint
    httpx_mock.add_response(
        url="http://testserver/listings",
        status_code=200,
        json=[{"id": 1}],
    )
    # Mock the metrics endpoint for listing 1 on the given date
    httpx_mock.add_response(
        url=f"http://testserver/listings/1/metrics?start_date={date_str}&end_date={date_str}",
        status_code=200,
        json=[{"date": date_str, "value": 123}],
    )

    # Change to a temporary directory so data is written there
    monkeypatch.chdir(tmp_path)

    runner = CliRunner()
    result = runner.invoke(app, ["etl", "--date", date_str])
    assert result.exit_code == 0
