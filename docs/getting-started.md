# Getting started

## Install

```bash
pip install rekep            # Arrow + Iceberg core
pip install "rekep[all]"     # + YAML, TOML writing, Jinja, xxhash, Airflow
```

Extras are additive and independent:

| Extra | Adds |
| --- | --- |
| `yaml` | reading and writing YAML (`pyyaml`) |
| `toml` | *writing* TOML (`tomli-w`; reading is stdlib) |
| `jinja` | Jinja templating in config files and CLI options |
| `fast` | ~2× parse throughput (`xxhash` line hashes) |
| `airflow` | DAG authoring — POSIX only, per Airflow itself |

## Parse a log

```python
from rekep.logs import LogFile

# local path, file:// URI, or any pyarrow.fs URL (s3://, gs://, hdfs://)
with LogFile.from_path("app-2026-08-14.txt.gz") as log:
    for batch in log.into_arrow_reader():
        ...                       # pyarrow.RecordBatch, bounded memory
```

Compression is inferred from the extension and decoded in Arrow's C++ layer;
`.gz`, `.zst`, `.bz2` and `.lz4` all work. The parsed columns are defined by
`rekep.models.Log` — override `LogFile.RECORD` to reshape them.

## Develop

```bash
cd python
uv sync              # env + every extra the tests use
uv run pytest
uv run ruff check
```

The repository's `AGENTS.md` documents the house patterns — object-oriented
first, `from_*`/`into_*` pairs, Arrow as the hub — and `python/benchmarks/`
holds the performance harness expected to run before and after touching a hot
path.
