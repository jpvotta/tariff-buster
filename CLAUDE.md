# Claude Flyte 2.0 Project Memory

- You are working inside a project that uses Flyte 2.0 as its execution fabric.  
- Default to implementing all non-trivial logic—pipelines, agents, services, async flows, data processing—using Flyte workflows, tasks, and apps.  
- When in doubt, build a Flyte workflow or Flyte app.  
- All code that you return should be tested. This means that if you write it, you should run it to validate that it works.

## Core Principles

- Use **Flyte 2.0** as the primary execution environment.
- Inspect SDK source in `./flyte-sdk/` and example patterns in `./flyte-sdk/examples/`.
- Runtime dependencies must be declared using **Flyte Images**, not installed locally.
- Assume:
  - Flyte installed via `uv pip install flyte`
  - Virtual environment at `./.venv`
  - Configuration at `./.flyte/config.yaml`

## Authoring Workflows & Tasks

### What Workflows Are

- A **task** is a Python function wrapped with `@env.task`.
- A **workflow** is **a task that calls other tasks**, typically asynchronous.

### What Tasks Can Contain

- Standard Python control flow  
- `try/except` error handling  
- Async logic (`async/await`)  
- Fanout via `asyncio.gather`  
- Parallel grouping via **`flyte.group`**

### Example Flyte 2.0 Workflow

```python
import asyncio
import flyte

env = flyte.TaskEnvironment(
    name="hello_v2",
    resources=flyte.Resources(cpu=1, memory="1Gi"),
    image=flyte.Image.from_debian_base().with_pip_packages("unionai-reuse>=0.1.9"),
    reusable=flyte.ReusePolicy(
        replicas=(1, 2),
        idle_ttl=60,
        concurrency=10,
        scaledown_ttl=60,
    ),
)

@env.task()
async def hello_worker(id: int) -> str:
    ctx = flyte.ctx()
    assert ctx is not None
    return f"hello, my id is: {id} and I am being run by Action: {ctx.action}"

@env.task()
async def hello_driver(ids: list[int] = [1, 2, 3]) -> list[str]:
    coros = []
    with flyte.group("fanout-group"):
        for id in ids:
            coros.append(hello_worker(id))
        vals = await asyncio.gather(*coros)
    return vals

if __name__ == "__main__":
    flyte.init_from_config()
    run = flyte.run(hello_driver)
    print(run.name)
    print(run.url)
```

## Flyte Apps

- Use an **AppEnvironment** for deployable services.
- For external invocation (curl, browser), set:

```python
requires_auth=False
```

- Apps must be **deployed** before invocation.
- Examples in `./flyte-sdk/examples/apps/`.

## Inspecting Workflows

Two levels of inspection:

### 1. CLI (Basic)

```bash
flyte get run <run_name>
flyte get action <run_name>
flyte get action <run_name> <action_name>
flyte get logs <run_name>
flyte get logs <run_name> <action_name>
```

### 2. Python REPL (default for getting failure reason)

```python
import asyncio
import flyte
import flyte.remote

flyte.init_from_config()

run_name = "rb5lsw4wmcvg8c59qsz9"
run = flyte.remote.Run.get(name=run_name)

actions = [a.name for a in flyte.remote.Action.listall(for_run_name=run_name)]

async def inspect():
    for a in actions:
        action_details = await flyte.remote.Action.get(
            run_name=run_name,
            name=a,
        ).details()
        print(action_details.to_json(indent=2))
        break

asyncio.run(inspect())
```

This reveals full metadata, errorInfo, container args, task template, resources, attempts, and cluster events.

## Logging for Output Validation

- Log intermediate values inside tasks.
- Validate via:

```bash
flyte get logs <run_name>
```

## Data I/O

Flyte automatically offloads data passed between tasks.

Use `File`, `Dir`, and `DataFrame` only when:

- passing local files/directories into Flyte  
- passing large tabular data by reference  

## Project Structure

```
./flyte-sdk/
./src/
./CLAUDE.md
./.flyte/config.yaml
./.venv/
```

## Additional Notes

### Workflow authoring guidance
- Default to using reusable environments as they dramatically speed up execution of successive runs. To use reusable environments, you must include the `unionai-reuse` python library. Size the reusable environment reasonably (concurrent processes share the replica's memory, so be mindful of OOMs)
- You can use GPUs if needed. See ./flyte-sdk/examples/ml for examples.

### Image Building Best Practices
For GPU or ML workloads (PyTorch, TensorFlow, etc.), use `Image.from_debian_base()` with pip packages:

```python
gpu_env = flyte.TaskEnvironment(
    name="ml_task",
    resources=flyte.Resources(gpu=1, memory="16Gi"),
    image=flyte.Image.from_debian_base().with_apt_packages(
        "ffmpeg",
    ).with_pip_packages(
        "torch",
        "transformers",
        "your-ml-library",
    ),
)
```
 - Avoid using Image.from_base() with pre-built images unless you know they include Flyte runtime.
 - Installing large packages (PyTorch, TensorFlow) can take 5-15 minutes. Images are cached after first build.

### Working with Files and Directories

`flyte.io.File` and `flyte.io.Dir` handle S3/remote storage automatically, but many libraries expect local file paths. You should download before processing:

  ```python
  @env.task()
  async def process_file(remote_file: flyte.io.File) -> str:
      """Process a file that's stored in object storage."""
      # Download to local disk first
      local_path = await remote_file.download()

      # Now use with any library expecting a file path
      result = some_library.process(local_path)
      return result
```

Common Issue: Passing remote_file.path directly to libraries like ffmpeg, PIL, or pandas will fail with "Protocol not found" errors.

### Working with multiple environments
A parent environment must depend on children.

If environment A has tasks that call tasks from environment B, then A must declare depends_on=[B]

```python
worker_env = flyte.TaskEnvironment(
    name="worker",
    resources=flyte.Resources(gpu=1),
    image=gpu_image,
)

@worker_env.task()
async def process_on_gpu(data: str) -> str:
    return f"processed {data}"

# Driver orchestrates workers (calls worker tasks)
driver_env = flyte.TaskEnvironment(
    name="driver",
    resources=flyte.Resources(cpu=2),
    image=cpu_image,
    depends_on=[worker_env],  # REQUIRED - driver calls worker tasks
)

@driver_env.task()
async def orchestrate(items: List[str]) -> List[str]:
    # This calls worker_env tasks, so driver needs depends_on=[worker_env]
    tasks = [process_on_gpu(item) for item in items]
    return await asyncio.gather(*tasks)
```

### Using secrets

Flyte has native support for secrets.

To check for the existence of a secret, you can run `flyte get secret`.

If you encounter the need for a secret, ask the user to enter a secret value. Once you have the secret value, run the following series of commands:
- `flyte create secret <secret-key>`. This will start the secret creation flow.
- After a few moments, paste the secret value when prompted.

You can inject secrets into tasks by specifying the secret in the environment like so:

```python
env = flyte.TaskEnvironment(
    name="secrets-example",
    secrets=[flyte.Secret(key="votta-openai-api-key", as_env_var="OPENAI_API_KEY")],
)
```

### Managing Dependencies

**Key principle:** Your local environment should contain a **superset** of dependencies used in Flyte tasks. This means:
- Install packages locally (in `.venv`) for development, testing, and IDE support
- Declare the same packages in `image.with_pip_packages()` for remote execution

**Correct Pattern:**
```python
import flyte
import requests  # ✅ Installed locally AND declared in image
import pandas as pd  # ✅ Installed locally AND declared in image

env = flyte.TaskEnvironment(
    name="my_task",
    image=flyte.Image.from_debian_base().with_pip_packages("requests", "pandas"),
)

@env.task()
async def fetch_data(url: str) -> dict:
    response = requests.get(url)
    df = pd.DataFrame(response.json())
    return df.to_dict()
```

**To install dependencies locally:**
```bash
uv pip install requests pandas
```

**Why this matters:**
- Module-level imports enable IDE features (autocomplete, type checking, go-to-definition)
- Catches import errors during development, not at runtime
- Standard Python practice - imports at the top are clearer

**Only use function-level imports when:**
- The package is ONLY needed remotely (e.g., GPU libraries you can't install locally)
- You're doing conditional imports based on runtime environment

### Data Serialization Best Practices

**Avoid None values with Any type:** Flyte uses pickling for `typing.Any` types and **cannot pickle None values**. This causes `AssertionError: Cannot pickle None Value.`

**Solutions:**

1. **Clean your data** - Replace None with appropriate defaults:
```python
@env.task()
async def fetch_api_data(url: str) -> List[Dict[str, Any]]:
    import requests

    data = requests.get(url).json()

    # Clean None values before returning
    cleaned = []
    for record in data:
        cleaned_record = {
            k: (v if v is not None else "")  # Replace None with empty string
            for k, v in record.items()
        }
        cleaned.append(cleaned_record)

    return cleaned
```

2. **Use specific types** - Avoid `Any` when possible:
```python
from dataclasses import dataclass

@dataclass
class Record:
    id: str
    value: str = ""  # Use default values instead of Optional[str]

@env.task()
async def fetch_data() -> List[Record]:  # Better than List[Dict[str, Any]]
    ...
```

**Common issue:** API responses often include `null`/`None` values in JSON. Always clean data before returning from tasks with `Any` type hints.

### File and Database Location

**Important:** Files created inside tasks (SQLite databases, output files, etc.) are created in the **remote Flyte container's filesystem**, not your local machine.

**Creating File objects for upload:**

You MUST use factory methods to create File objects - the constructor doesn't accept paths directly.

```python
from flyte.io import File

@env.task()
async def create_database() -> File:
    import sqlite3

    db_path = "/tmp/data.db"
    conn = sqlite3.connect(db_path)
    # ... populate database ...
    conn.close()

    # ✅ CORRECT: Use from_local() in async tasks
    return await File.from_local(db_path)

@env.task()
def create_database_sync() -> File:
    import sqlite3

    db_path = "/tmp/data.db"
    conn = sqlite3.connect(db_path)
    # ... populate database ...
    conn.close()

    # ✅ CORRECT: Use from_local_sync() in sync tasks
    return File.from_local_sync(db_path)
```

**Common mistake:**
```python
# ❌ WRONG: File() constructor doesn't accept paths
return File(db_path)  # Raises: BaseModel.__init__() takes 1 positional argument but 2 were given
```

**Downloading files:**

Use the `download()` method to get files locally:

```python
@env.task()
async def process_database(db_file: File) -> str:
    # Download from S3 to local path
    local_path = await db_file.download()

    # Now use with libraries that expect local paths
    import sqlite3
    conn = sqlite3.connect(local_path)
    # ... process database ...
    conn.close()

    return f"Processed database at {local_path}"
```

**Default behavior:** Without using `File` return types, files remain in the ephemeral container storage and are not accessible after the task completes.

### Managing Dependencies

Key principle: Your local environment should contain a superset of dependencies used in Flyte tasks. This means:
- Install packages locally (in .venv) for development, testing, and IDE support
- Declare the same packages in image.with_pip_packages() for remote execution

Correct Pattern:

```python
import flyte
import requests  # Correct: Installed locally AND declared in image
import pandas as pd  # Correct: Installed locally AND declared in image

env = flyte.TaskEnvironment(
    name="my_task",
    image=flyte.Image.from_debian_base().with_pip_packages("requests", "pandas"),
)

@env.task()
async def fetch_data(url: str) -> dict:
    response = requests.get(url)
    df = pd.DataFrame(response.json())
    return df.to_dict()
```

To install dependencies locally:
uv pip install requests pandas

Why this matters:
- Module-level imports enable IDE features (autocomplete, type checking, go-to-definition)
- Catches import errors during development, not at runtime
- Standard Python practice - imports at the top are clearer

Only use function-level imports when:
- The package is ONLY needed remotely (e.g., GPU libraries you can't install locally)
- You're doing conditional imports based on runtime environment

### Data Serialization Best Practices

Avoid None values with Any type: Flyte uses pickling for typing.Any types and cannot pickle None values. This causes
AssertionError: Cannot pickle None Value.

Solutions:

1. Clean your data - Replace None with appropriate defaults:

```python
@env.task()
async def fetch_api_data(url: str) -> List[Dict[str, Any]]:
    import requests

    data = requests.get(url).json()

    cleaned = []
    for record in data:
        cleaned_record = {
            k: (v if v is not None else "")  # Replace None with empty string
            for k, v in record.items()
        }
        cleaned.append(cleaned_record)

    return cleaned
```

2. Use specific types - Avoid Any when possible:

```python
from dataclasses import dataclass

@dataclass
class Record:
    id: str
    value: str = ""  # Use default values instead of Optional[str]

@env.task()
async def fetch_data() -> List[Record]:  # Better than List[Dict[str, Any]]
    ...
```

Common issue: API responses often include null/None values in JSON. Always clean data before returning from tasks with Any type
hints.

