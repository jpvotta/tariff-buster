import json
from pathlib import Path

import flyte
from flyte.io import File

env = flyte.TaskEnvironment(
    name="hts-downloader",
    resources=flyte.Resources(cpu=1, memory="1Gi"),
    image=flyte.Image.from_debian_base()
    .with_pip_packages("httpx")
    .with_source_file(Path("src/hts_archive.json"), "/root"),
)


@env.task()
async def download_hts():
    """Load the HTS archive and download the first revision."""
    import httpx

    # Load the archive file
    archive_file = await File.from_local("/root/hts_archive.json")

    async with archive_file.open() as fh:
        data = await fh.read()
        json_contents = str(data, "utf-8")

    revisions = json.loads(json_contents)
    print(f"Loaded {len(revisions)} HTS revisions from archive")

    # Get the first revision
    revision = revisions[0]
    print(f"Downloading: {revision['revision_name']}")
    print(f"URL: {revision['url']}")

    # Download it
    async with httpx.AsyncClient() as client:
        response = await client.get(revision['url'])
        response.raise_for_status()
        data = response.json()

    print(f"Successfully downloaded {revision['revision_name']}")
    print(f"Data has {len(data)} top-level keys")


if __name__ == "__main__":
    flyte.init_from_config()
    # run = flyte.run(download_hts)
    run = flyte.with_runcontext(mode="local").run(download_hts)
    print(run.name)
    
    print(run.url)
