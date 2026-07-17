# butler_transform

Tools for bulk export and modification of [Butler](https://github.com/lsst/daf_butler) databases.

## Organization of the code

Currently this package contains top-level scripts specific to a specific LSST
data release, and some semi-reusable library functions that will gradually
become more reusable in the future.

The main modules under the `lsst.butler_transform` namespace are:
- `releases` -- contains the top-level scripts, currently for the DP2 data
  release.
- `export` -- functions for bulk-exporting metadata from a Butler database to
  parquet files
- `importer` -- functions for bulk-loading exported parquet files back into a
   Butler database
- `transform` -- functions for modifying the exported parquet files and reading
   them in-place without a full Butler setup.
- `parquet` -- Low level functions for representing Butler data to Arrow tables,
   and writing these tables to parquet files.

## Setting up an environment for working with the code

This library is using [uv](https://docs.astral.sh/uv/) for package management.
To set it up, just run `pip install uv`.  `uv` will automatically create a
virtual environment and install other required libraries for you.

```
# Typechecking
uv run mypy
# Unit tests
uv run pytest

# Running the top-level DP2 data release scripts
uv run python -m lsst.butler_transform.releases.dp2.export
uv run python -m lsst.butler_transform.releases.dp2.import_dp2 dp2-export postgresql://...
```

## Format of the exported Butler data

Exported Butler data is written as multiple files in a single directory.  The
contents of this directory are:
- `manifest.json` -- Contains a full list of the parquet files included in the
export, as well as other metadata like the Butler dimension universe.
- `*.dimension.parquet` -- Butler "dimension records" containing the detailed metadata
that may be associated with datasets (e.g. visit metadata).
- `*.datasets.parquet` -- Butler dataset UUIDs and the "data IDs" associating them
with dimension record primary keys, as well as their run collections.
- `*.datastore.parquet` -- Butler datastore records containing file paths and
other metadata about the artifacts associated with datasets.
- `*.associations.parquet` -- Tracks membership of datasets in Butler TAGGED
and CHAINED collections.
- `collections.parquet` -- Butler collections associated with the exported datasets.


## Why this library is using anyio instead of stdlib asyncio

The vast majority of I/O in this library involves pushing synchronous blocking
library functions onto a thread pool.  The standard library `asyncio.to_thread`
has a major defect: when cancellation occurs, it detaches the running thread
and returns immediately.  This means that resources acquired outside the thread
cannot be safely used inside the thread, because the async code will continue
on and release the resource while the detached thread is still using it.
`anyio`'s `to_thread`, when combined with its implementation of task groups,
does not suffer from this problem: cancellation waits for the thread to
complete before continuing.

I also found myself re-inventing a wrapper around the standard library
`asyncio.Queue` which was a worse version of `anyio`'s
`MemoryObjectStream`s.

`anyio` is a thin wrapper around the standard library, and is used internally
by other libraries used widely in Rubin (notably FastAPI and httpx), so
hopefully it doesn't prove to be an overly burdensome dependency.
