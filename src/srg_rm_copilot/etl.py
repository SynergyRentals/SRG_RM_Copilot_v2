"""CLI to run a daily Wheelhouse ETL.

This module implements a Typer application that downloads a list of listings and
fetches daily metrics for each listing.  The metrics for each listing are
persisted as Parquet files under the `data/raw/{listing_id}/{YYYY-MM-DD}.parquet`
folder structure.

The base URL of the Wheelhouse API and an optional API key are read from
environment variables.  Retries with exponential backoff are applied when the
server responds with HTTP 429 (rate limiting).
"""
from __future__ import annotations

import os
import time
import datetime as dt
from typing import List, Optional, Union

import httpx
import pandas as pd
import typer

# Create a Typer application.  The ETL command will be registered as a
# subcommand on this app.  When invoking ``python -m srg_rm_copilot etl`` the
# @app.command decorator ensures that the ``etl`` function is executed.
app = typer.Typer(add_completion=False)



def _get_with_retry(
    client: httpx.Client,
    url: str,
    headers: dict[str, str] | None = None,
    params: dict[str, Union[str, int]] | None = None,
    max_retries: int = 3,
    backoff_factor: float = 1.0,
) -> dict | list:
    """Perform a GET request with retry/backoff on HTTP 429.

    Args:
        client: An httpx.Client used to perform the request.
        url: The absolute URL to call.
        headers: Optional request headers.
        params: Optional query parameters.
        max_retries: The maximum number of attempts (including the initial attempt).
        backoff_factor: Base multiplier for exponential backoff (seconds).

    Returns:
        The JSON-decoded response body on success.

    Raises:
        httpx.HTTPStatusError: If a non-429 error status is returned or
            max_retries is exceeded.
    """
    attempt = 0
    while True:
        response = client.get(url, headers=headers or {}, params=params)
        if response.status_code == 429 and attempt < max_retries - 1:
            # Exponential backoff: wait 2^attempt * backoff_factor seconds
            delay = (2 ** attempt) * backoff_factor
            time.sleep(delay)
            attempt += 1
            continue
        # Raise for any HTTP error other than 429
        response.raise_for_status()
        return response.json()



def _extract_listing_ids(listings_json: Union[dict, list]) -> List[str]:
    """Normalize the listing payload into a list of IDs.

    The ``/listings`` endpoint can return a list of IDs, a list of objects
    containing an ``id`` field, or an object with a ``results`` field.  This
    helper normalizes those variations.

    Args:
        listings_json: The decoded JSON returned from the ``/listings`` endpoint.

    Returns:
        A list of listing identifiers.
    """
    ids: List[str] = []
    if isinstance(listings_json, list):
        for item in listings_json:
            if isinstance(item, dict) and "id" in item:
                ids.append(str(item["id"]))
            else:
                ids.append(str(item))
    elif isinstance(listings_json, dict):
        # Some APIs return {"results": [...]}
        results = listings_json.get("results")
        if isinstance(results, list):
            ids.extend(
                str(item["id"]) if isinstance(item, dict) and "id" in item else str(item)
                for item in results
            )
        # fall back to keys if listing IDs are keys
        else:
            ids.extend(str(k) for k in listings_json.keys())
    return ids


@app.command()
def etl(date: Optional[str] = typer.Option(None, help="Date to pull in YYYY-MM-DD format. Defaults to yesterday.")) -> None:
    """Run the daily Wheelhouse ETL.

    Args:
        date: Optional date string (YYYY-MM-DD).  Defaults to yesterday.
    """
    # Determine the target date (default to yesterday)
    if date:
        try:
            target_date = dt.datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise typer.BadParameter(f"Invalid date format: {date}. Use YYYY-MM-DD.") from exc
    else:
        # Use America/Chicago time zone implicitly by subtracting a day from local date
        target_date = dt.date.today() - dt.timedelta(days=1)

    base_url = os.environ.get("WHEELHOUSE_BASE_URL")
    if not base_url:
        typer.echo("Environment variable WHEELHOUSE_BASE_URL must be set.", err=True)
        raise typer.Exit(code=1)
    base_url = base_url.rstrip("/")
    api_key = os.environ.get("WHEELHOUSE_API_KEY")
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}

    # Use a persistent client for efficiency
    with httpx.Client(timeout=30.0) as client:
        # Fetch list of listings
        listings_url = f"{base_url}/listings"
        listings_json = _get_with_retry(client, listings_url, headers=headers)
        listing_ids = _extract_listing_ids(listings_json)
        if not listing_ids:
            typer.echo("No listings returned from /listings endpoint.")
            return

        for listing_id in listing_ids:
            metrics_url = f"{base_url}/listings/{listing_id}/metrics"
            params = {
                "start_date": str(target_date),
                "end_date": str(target_date),
            }
            metrics_json = _get_with_retry(client, metrics_url, headers=headers, params=params)
            # Normalize metrics into a list of records
            if metrics_json is None:
                records: List[dict] = []
            elif isinstance(metrics_json, list):
                records = metrics_json  # type: ignore[assignment]
            elif isinstance(metrics_json, dict):
                # Some APIs nest results under a top-level key
                if "data" in metrics_json and isinstance(metrics_json["data"], list):
                    records = metrics_json["data"]  # type: ignore[assignment]
                elif "results" in metrics_json and isinstance(metrics_json["results"], list):
                    records = metrics_json["results"]  # type: ignore[assignment]
                else:
                    records = [metrics_json]  # type: ignore[list-item]
            else:
                records = []

            df = pd.DataFrame(records)
            # Construct output directory and write to Parquet
            out_dir = os.path.join("data", "raw", str(listing_id))
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, f"{target_date}.parquet")
            # If no columns, create empty parquet file with zero columns
            df.to_parquet(out_path, index=False)
            typer.echo(f"Wrote metrics for listing {listing_id} to {out_path}")
