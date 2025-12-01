"""
Census International Trade Data scraper workflow using Flyte 2.0.
Downloads import data by 10-digit HS code and country from Census API.
"""

import asyncio
import os
import sqlite3
import time
from typing import List, Dict, Any, Optional
import flyte
from flyte.io import File
import requests

# Environment for Census API scraping tasks
env = flyte.TaskEnvironment(
    name="census_imports",
    resources=flyte.Resources(cpu=2, memory="4Gi"),
    image=flyte.Image.from_debian_base().with_pip_packages(
        "requests",
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


@env.task()
async def fetch_imports_by_hs_prefix(
    hs_prefix: str,
    year: int,
    month: int,
    max_retries: int = 3,
    base_delay: float = 2.0
) -> List[Dict[str, Any]]:
    """
    Fetch import data for all HS codes starting with a given prefix.

    Uses wildcard pattern matching as recommended in Census API docs (page 13)
    to split large queries into manageable chunks.

    Args:
        hs_prefix: HS code prefix (e.g., "01", "1", "84")
        year: Year (e.g., 2024)
        month: Month (1-12)
        max_retries: Maximum retry attempts
        base_delay: Base delay for exponential backoff

    Returns:
        List of import records with cleaned None values
    """
    api_key = os.environ.get("CENSUS_API_KEY", "")

    # Format month as 2-digit string
    month_str = f"{month:02d}"

    # Build the API URL with parameters
    base_url = "https://api.census.gov/data/timeseries/intltrade/imports/hs"

    # Use wildcard for HS commodity to get all codes starting with prefix
    hs_pattern = f"{hs_prefix}*"

    params = {
        "get": "I_COMMODITY,I_COMMODITY_SDESC,CTY_CODE,CTY_NAME,GEN_VAL_MO",
        "YEAR": str(year),
        "MONTH": month_str,
        "I_COMMODITY": hs_pattern,
        "COMM_LVL": "HS10",  # Get 10-digit HS codes
        "SUMMARY_LVL": "DET",  # Individual countries (not groupings)
    }

    if api_key:
        params["key"] = api_key

    for attempt in range(max_retries):
        try:
            print(f"Fetching imports for HS prefix {hs_prefix}, {year}-{month_str} (attempt {attempt + 1}/{max_retries})")

            response = requests.get(
                base_url,
                params=params,
                timeout=60
            )

            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', base_delay * (2 ** attempt)))
                print(f"Rate limited. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                continue

            # Handle no results (HTTP 204)
            if response.status_code == 204:
                print(f"No data for HS prefix {hs_prefix} in {year}-{month_str}")
                return []

            response.raise_for_status()
            data = response.json()

            # Convert from Census format to list of dicts
            if not data or len(data) < 2:
                print(f"Empty response for HS prefix {hs_prefix}")
                return []

            # First row is headers, rest are data
            headers = data[0]
            records = []

            for row in data[1:]:
                record = {}
                for i, header in enumerate(headers):
                    value = row[i] if i < len(row) else None
                    # Clean None values to avoid pickling issues
                    record[header] = value if value is not None else ""
                records.append(record)

            print(f"Retrieved {len(records)} import records for HS prefix {hs_prefix}")
            return records

        except requests.exceptions.RequestException as e:
            print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"All retry attempts exhausted for HS prefix {hs_prefix}")
                raise

    raise Exception(f"Failed to fetch data for HS prefix {hs_prefix}")


@env.task()
async def write_all_census_data(
    all_batch_results: List[List[Dict[str, Any]]],
    year: int,
    month: int
) -> File:
    """
    Write all Census import records to a single database file.

    Args:
        all_batch_results: List of batches, where each batch is a list of import records
        year: Year of data
        month: Month of data

    Returns:
        Flyte File reference to the completed database
    """
    db_path = "census_imports.db"
    print(f"Creating database at: {db_path}")

    # Remove existing file if it exists
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS import_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hs_code TEXT,
            hs_description TEXT,
            country_code TEXT,
            country_name TEXT,
            import_value_mo INTEGER,
            year INTEGER,
            month INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hs_code ON import_data(hs_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_country_code ON import_data(country_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_year_month ON import_data(year, month)")

    # Insert all records
    total_records = 0
    insert_batch_size = 1000

    for batch_num, batch_records in enumerate(all_batch_results, 1):
        if not batch_records:
            continue

        print(f"Writing batch {batch_num}: {len(batch_records)} records")

        for i in range(0, len(batch_records), insert_batch_size):
            sub_batch = batch_records[i:i + insert_batch_size]
            cursor.executemany("""
                INSERT INTO import_data (
                    hs_code, hs_description, country_code, country_name,
                    import_value_mo, year, month
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                (
                    record.get('I_COMMODITY', ''),
                    record.get('I_COMMODITY_SDESC', ''),
                    record.get('CTY_CODE', ''),
                    record.get('CTY_NAME', ''),
                    int(record.get('GEN_VAL_MO', 0)) if record.get('GEN_VAL_MO') and record.get('GEN_VAL_MO') != '' else 0,
                    year,
                    month,
                )
                for record in sub_batch
            ])

        total_records += len(batch_records)

    conn.commit()
    row_count = cursor.execute("SELECT COUNT(*) FROM import_data").fetchone()[0]
    conn.close()

    print(f"Database complete: {row_count} total records written to {db_path}")

    return await File.from_local(db_path)


@env.task()
async def scrape_census_imports_workflow(
    year: int = 2024,
    month: int = 10,
    hs_prefixes: Optional[List[str]] = None,
    parallel_batch_size: int = 5,
    delay_between_batches: float = 1.0,
) -> File:
    """
    Main workflow to scrape Census import data by HS code and country.

    Implements the parallelization strategy recommended in Census API docs (page 13):
    - Splits requests by HS code prefix (first 1-2 digits)
    - Makes parallel requests for different prefixes
    - Respects rate limits with delays between batches

    Args:
        year: Year of data to fetch
        month: Month of data to fetch (1-12)
        hs_prefixes: List of HS code prefixes to fetch (e.g., ["01", "02", "84"])
                    If None, fetches all codes starting with 0-9
        parallel_batch_size: Number of parallel requests per batch
        delay_between_batches: Delay in seconds between batches

    Returns:
        SQLite database file with all import data
    """
    print(f"Starting Census import scrape for {year}-{month:02d}")

    # If no prefixes specified, use single-digit prefixes (0-9)
    if hs_prefixes is None:
        # Start with single-digit prefixes for first digit 0-9
        hs_prefixes = [str(i) for i in range(10)]

    print(f"Will fetch data for {len(hs_prefixes)} HS prefixes: {hs_prefixes}")
    print(f"Parallel batch size: {parallel_batch_size}, Delay: {delay_between_batches}s")

    all_batch_results = []
    total_records = 0
    batch_count = 0

    # Process prefixes in batches for parallelization
    for i in range(0, len(hs_prefixes), parallel_batch_size):
        batch_prefixes = hs_prefixes[i:i + parallel_batch_size]
        batch_count += 1

        print(f"\n=== Processing batch {batch_count}: prefixes {batch_prefixes} ===")

        # Fetch data for this batch in parallel
        fetch_tasks = []
        with flyte.group(f"fetch-prefixes-{'-'.join(batch_prefixes)}"):
            for prefix in batch_prefixes:
                fetch_tasks.append(
                    fetch_imports_by_hs_prefix(prefix, year, month)
                )

            batch_results = await asyncio.gather(*fetch_tasks)

        # Flatten and collect all records from this batch
        all_records = []
        for result in batch_results:
            all_records.extend(result)

        if all_records:
            all_batch_results.append(all_records)
            total_records += len(all_records)

        print(f"Batch {batch_count} complete: {len(all_records)} records")
        print(f"Progress: {i + len(batch_prefixes)}/{len(hs_prefixes)} prefixes, {total_records} total records")

        # Add delay between batches (except for last batch)
        if i + parallel_batch_size < len(hs_prefixes):
            print(f"Waiting {delay_between_batches}s before next batch...")
            await asyncio.sleep(delay_between_batches)

    print(f"\n=== Fetching completed ===")
    print(f"Prefixes processed: {len(hs_prefixes)}")
    print(f"Total records fetched: {total_records}")
    print(f"Period: {year}-{month:02d}")

    # Write all data to database in a single task
    print(f"\n=== Writing {total_records} records to database ===")
    with flyte.group("write-database"):
        db_file = await write_all_census_data(all_batch_results, year, month)

    print(f"=== Workflow completed ===")
    return db_file


if __name__ == "__main__":
    flyte.init_from_config()

    # Example: Fetch October 2024 data for all HS codes
    # You can also specify specific prefixes like: hs_prefixes=["84", "85", "87"]
    run = flyte.with_runcontext(mode="local").run(
        scrape_census_imports_workflow,
        year=2024,
        month=10,
        hs_prefixes=["0", "1"],  # Start with just 0 and 1 for testing
        parallel_batch_size=2,
        
    )

    # run = flyte.run(
    #     scrape_census_imports_workflow,
    #     year=2024,
    #     month=10,
    #     hs_prefixes=["0", "1"],  # Start with just 0 and 1 for testing
    #     parallel_batch_size=2,
        
    # )

    print(f"Run name: {run.name}")
    print(f"Run URL: {run.url}")
