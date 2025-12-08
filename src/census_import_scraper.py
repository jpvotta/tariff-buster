"""
Census International Trade Data scraper workflow using Flyte 2.0.
Downloads import data by 10-digit HS code and country from Census API.
"""

import asyncio
import os
from datetime import datetime
from typing import List
import flyte
from flyte import Cache
import httpx
import pandas as pd
from pydantic import BaseModel

class CensusImportRecord(BaseModel):
    hs_code: str  # I_COMMODITY
    hs_description: str  # I_COMMODITY_SDESC
    country_code: str  # CTY_CODE
    country_name: str  # CTY_NAME
    import_value_monthly: str  # GEN_VAL_MO (keeping as string since API returns strings)
    year: str  # YEAR
    month: str  # MONTH


# Environment for Census API scraping tasks
env = flyte.TaskEnvironment(
    name="census_imports",
    resources=flyte.Resources(cpu=2, memory="10Gi"),
    image=flyte.Image.from_debian_base().with_pip_packages(
        "httpx",
        "pydantic",
        "pandas",
        "pyarrow",
        "unionai-reuse>=0.1.9",
    ),
    secrets=[flyte.Secret(key="votta-census-api-key", as_env_var="CENSUS_API_KEY")],
    reusable=flyte.ReusePolicy(
        replicas=(1, 2),
        idle_ttl=120,
        concurrency=10,
        scaledown_ttl=120,
    ),
)


@env.task(cache=Cache(behavior="override", version_override="v1.3"), retries=3)
async def fetch_imports_by_hs_prefix(
    hs_prefix: str,
    year: int,
    month: int,
) -> List[CensusImportRecord]:
    """
    Fetch import data for all HS codes starting with a given prefix.

    Uses wildcard pattern matching to get all codes starting with prefix.

    Args:
        hs_prefix: HS code prefix (e.g., "01", "1", "84")
        year: Year (e.g., 2024)
        month: Month (1-12)

    Returns:
        List of CensusImportRecord objects
    """
    api_key = os.environ.get("CENSUS_API_KEY", "")

    # Format month as 2-digit string
    month_str = f"{month:02d}"

    # Build the API URL with parameters
    base_url = "https://api.census.gov/data/timeseries/intltrade/imports/hs"

    # Use wildcard for HS commodity to get all codes starting with prefix
    hs_pattern = f"{hs_prefix}*"

    params = {
        "get": "I_COMMODITY,I_COMMODITY_SDESC,CTY_CODE,CTY_NAME,GEN_VAL_MO,YEAR,MONTH",
        "YEAR": str(year),
        "MONTH": month_str,
        "I_COMMODITY": hs_pattern,
        "COMM_LVL": "HS10",  # Get 10-digit HS codes
        "SUMMARY_LVL": "DET",  # Individual countries (not groupings)
    }

    if api_key:
        params["key"] = api_key

    print(f"Fetching imports for HS prefix {hs_prefix}, {year}-{month_str}")

    async with httpx.AsyncClient() as client:
        response = await client.get(base_url, params=params, timeout=600)

        # Handle no results (HTTP 204)
        if response.status_code == 204:
            print(f"No data for HS prefix {hs_prefix} in {year}-{month_str}")
            return []

        response.raise_for_status()
        data = response.json()

    # Convert from Census format to list of Pydantic models
    if not data or len(data) < 2:
        print(f"Empty response for HS prefix {hs_prefix}")
        return []

    # First row is headers, rest are data
    headers = data[0]
    records = []

    # Map Census API field names to our model field names
    field_mapping = {
        "I_COMMODITY": "hs_code",
        "I_COMMODITY_SDESC": "hs_description",
        "CTY_CODE": "country_code",
        "CTY_NAME": "country_name",
        "GEN_VAL_MO": "import_value_monthly",
        "YEAR": "year",
        "MONTH": "month",
    }

    for row in data[1:]:
        # Build dict with mapped field names
        record_data = {}
        for i, header in enumerate(headers):
            if header in field_mapping:
                value = row[i] if i < len(row) else ""
                # Convert None to empty string
                record_data[field_mapping[header]] = value if value is not None else ""

        records.append(CensusImportRecord(**record_data))

    print(f"Retrieved {len(records)} import records for HS prefix {hs_prefix}")
    return records


@env.task()
async def scrape_census_imports_workflow(
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
    hs_prefixes: List[str],
) -> pd.DataFrame:
    """
    Main workflow to scrape Census import data by HS code and country.

    Scrapes all data from the start year/month to the end year/month (inclusive).

    Args:
        start_year: Starting year (e.g., 2024)
        start_month: Starting month (1-12)
        end_year: Ending year (e.g., 2025)
        end_month: Ending month (1-12)
        hs_prefixes: List of HS code prefixes to fetch (e.g., ["01", "02", "03"])
                    Use ["01", "02", ..., "99"] for all

    Returns:
        DataFrame with all Census import records fetched
    """

    # Generate list of (year, month) tuples from start to end
    year_month_pairs = []
    year = start_year
    month = start_month

    while (year < end_year) or (year == end_year and month <= end_month):
        year_month_pairs.append((year, month))

        # Move to next month
        month += 1
        if month > 12:
            month = 1
            year += 1

    print(f"Starting Census import scrape")
    print(f"Start: {start_year}-{start_month:02d}")
    print(f"End: {end_year}-{end_month:02d}")
    print(f"Total months to fetch: {len(year_month_pairs)}")
    print(f"HS prefixes: {hs_prefixes}")

    all_records = []

    # Loop through all year/month combinations
    for year, month in year_month_pairs:
        print(f"Fetching data for {year}-{month:02d}")

        # Fetch all prefixes in parallel for this year/month
        fetch_tasks = []
        with flyte.group(f"fetch-{year}-{month:02d}"):
            for hs_prefix in hs_prefixes:
                fetch_tasks.append(
                    fetch_imports_by_hs_prefix(hs_prefix, year, month)
                )

            results = await asyncio.gather(*fetch_tasks)

        # Flatten results
        for result in results:
            all_records.extend(result)

        print(f"Fetched {len(all_records)} total records so far")

    print(f"Workflow completed")
    print(f"Total records fetched: {len(all_records)}")

    # Define column order
    columns = ['year', 'month', 'hs_code', 'hs_description', 'country_code', 'country_name', 'import_value_monthly']

    # Convert list of Pydantic models to DataFrame
    if all_records:
        df = pd.DataFrame([record.model_dump() for record in all_records])
        # Reorder columns
        df = df[columns]
    else:
        # Create empty DataFrame with correct columns
        df = pd.DataFrame(columns=columns)

    print(f"DataFrame shape: {df.shape}")

    return df


if __name__ == "__main__":
    flyte.init_from_config()

    # Generate all two-digit HS prefixes (01-99)
    all_prefixes = [f"{i:02d}" for i in range(1, 100)]

    run = flyte.run(
        scrape_census_imports_workflow,
        start_year=2024,
        start_month=1,
        end_year=2025,
        end_month=8,
        hs_prefixes=all_prefixes,
    )

    print(f"Run name: {run.name}")
    print(f"Run URL: {run.url}")
