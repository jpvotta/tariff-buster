"""
Harmonized Tariff Schedule scraper workflow using Flyte 2.0.
Scrapes HTS data from https://hts.usitc.gov/ and stores in SQLite.
"""

import asyncio
import os
import sqlite3
import time
from typing import List, Dict, Any
import flyte
from flyte.io import File
import requests

# Environment for scraping tasks
env = flyte.TaskEnvironment(
    name="hts_scraper",
    resources=flyte.Resources(cpu=2, memory="4Gi"),
    image=flyte.Image.from_debian_base().with_pip_packages(
        "requests",
        "unionai-reuse>=0.1.9",
    ),
    secrets=[flyte.Secret(key="DATAWEB_API_TOKEN", as_env_var="DATAWEB_API_TOKEN")],
    reusable=flyte.ReusePolicy(
        replicas=(1, 2),
        idle_ttl=120,
        concurrency=5,
        scaledown_ttl=120,
    ),
)


@env.task()
async def fetch_hts_range_with_retry(
    chapter_start: str,
    chapter_end: str,
    max_retries: int = 3,
    base_delay: float = 1.0
) -> List[Dict[str, Any]]:
    """
    Fetch HTS data for a specific chapter range with retry logic and rate limiting.

    Args:
        chapter_start: Starting HTS chapter (e.g., "01")
        chapter_end: Ending HTS chapter (e.g., "02")
        max_retries: Maximum number of retry attempts
        base_delay: Base delay between retries (exponential backoff)

    Returns:
        List of HTS records with None values cleaned
    """
    token = os.environ["DATAWEB_API_TOKEN"]
    headers = {"Authorization": f"Bearer {token}"}

    # Format the range parameters (e.g., "0101" for chapter 01)
    from_code = f"{chapter_start}01"
    to_code = f"{chapter_end}99"

    for attempt in range(max_retries):
        try:
            print(f"Fetching HTS data from {from_code} to {to_code} (attempt {attempt + 1}/{max_retries})")

            response = requests.get(
                "https://hts.usitc.gov/reststop/exportList",
                headers=headers,
                params={
                    "format": "JSON",
                    "from": from_code,
                    "to": to_code,
                    "styles": "false"
                },
                timeout=30
            )

            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', base_delay * (2 ** attempt)))
                print(f"Rate limited. Waiting {retry_after} seconds before retry...")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()

            # Clean the data - replace None values to avoid pickling issues
            cleaned_data = []
            for record in data:
                cleaned_record = {
                    k: (v if v is not None else "")
                    for k, v in record.items()
                }
                cleaned_data.append(cleaned_record)

            print(f"Retrieved {len(cleaned_data)} HTS records for chapters {chapter_start}-{chapter_end}")
            return cleaned_data

        except requests.exceptions.RequestException as e:
            print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"All retry attempts exhausted for chapters {chapter_start}-{chapter_end}")
                raise

    # Should not reach here, but just in case
    raise Exception(f"Failed to fetch data for chapters {chapter_start}-{chapter_end}")


@env.task()
async def write_all_hts_data(
    all_batch_results: List[List[Dict[str, Any]]]
) -> File:
    """
    Write all HTS records to a single database file.

    Args:
        all_batch_results: List of batches, where each batch is a list of HTS records

    Returns:
        Flyte File reference to the completed database
    """
    db_path = "hts_data.db"
    print(f"Creating database at: {db_path}")

    # Remove existing file if it exists
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hts_schedule (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            htsno TEXT,
            indent TEXT,
            description TEXT,
            superior TEXT,
            general TEXT,
            special TEXT,
            other TEXT,
            quota_quantity TEXT,
            additional_duties TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create index on htsno for faster lookups
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_htsno ON hts_schedule(htsno)
    """)

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
                INSERT INTO hts_schedule (
                    htsno, indent, description, superior, general, special,
                    other, quota_quantity, additional_duties
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                (
                    record.get('htsno', ''),
                    str(record.get('indent', '')),
                    record.get('description', ''),
                    record.get('superior', ''),
                    record.get('general', ''),
                    record.get('special', ''),
                    record.get('other', ''),
                    record.get('quotaQuantity', ''),
                    record.get('additionalDuties', ''),
                )
                for record in sub_batch
            ])

        total_records += len(batch_records)

    conn.commit()
    row_count = cursor.execute("SELECT COUNT(*) FROM hts_schedule").fetchone()[0]
    conn.close()

    print(f"Database complete: {row_count} total records written to {db_path}")

    return await File.from_local(db_path)


@env.task()
async def scrape_hts_workflow(
    start_chapter: int = 1,
    end_chapter: int = 99,
    batch_size: int = 5,
    delay_between_batches: float = 2.0,
) -> File:
    """
    Main workflow to scrape entire HTS schedule and return database file.

    Args:
        start_chapter: Starting chapter number (1-99)
        end_chapter: Ending chapter number (1-99)
        batch_size: Number of chapters to fetch in each batch (lower = more conservative)
        delay_between_batches: Delay in seconds between batches to respect rate limits

    Returns:
        SQLite database file with all scraped HTS data
    """
    print(f"Starting HTS scrape from chapter {start_chapter} to {end_chapter}")
    print(f"Batch size: {batch_size}, Delay between batches: {delay_between_batches}s")

    all_batch_results = []
    total_records = 0
    chapters_processed = 0
    batch_count = 0

    # Process chapters in batches
    for chapter in range(start_chapter, end_chapter + 1, batch_size):
        batch_end = min(chapter + batch_size - 1, end_chapter)
        batch_count += 1

        print(f"\n=== Processing batch {batch_count}: chapters {chapter}-{batch_end} ===")

        # Fetch data for this batch in parallel
        fetch_tasks = []
        with flyte.group(f"fetch-chapters-{chapter}-{batch_end}"):
            for ch in range(chapter, batch_end + 1):
                chapter_str = f"{ch:02d}"  # Format as 2-digit string
                fetch_tasks.append(fetch_hts_range_with_retry(chapter_str, chapter_str))

            batch_results = await asyncio.gather(*fetch_tasks)

        # Flatten batch results
        batch_records = []
        for result in batch_results:
            batch_records.extend(result)

        batch_record_count = len(batch_records)
        total_records += batch_record_count

        # Collect batch results
        if batch_records:
            all_batch_results.append(batch_records)

        chapters_processed += (batch_end - chapter + 1)
        print(f"Batch {batch_count} complete: {batch_record_count} records")
        print(f"Progress: {chapters_processed}/{end_chapter - start_chapter + 1} chapters, {total_records} total records")

        # Add delay between batches to respect rate limits (except for last batch)
        if chapter + batch_size <= end_chapter:
            print(f"Waiting {delay_between_batches}s before next batch...")
            await asyncio.sleep(delay_between_batches)

    print(f"\n=== Fetching completed ===")
    print(f"Chapters processed: {chapters_processed}")
    print(f"Total records fetched: {total_records}")

    # Write all data to database in a single task
    print(f"\n=== Writing {total_records} records to database ===")
    with flyte.group("write-database"):
        db_file = await write_all_hts_data(all_batch_results)

    print(f"=== Workflow completed ===")
    return db_file


if __name__ == "__main__":
    flyte.init_from_config()

    # Run the full workflow for all 99 chapters
    run = flyte.with_runcontext(mode="local").run(scrape_hts_workflow, start_chapter=1, end_chapter=99)
    # run = flyte.run(scrape_hts_workflow, start_chapter=1, end_chapter=99)

    print(f"Run name: {run.name}")
    print(f"Run URL: {run.url}")
