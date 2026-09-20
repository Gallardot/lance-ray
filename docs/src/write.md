# Writing to Lance Dataset

## `write_lance`

```python
write_lance(
    ds, 
    uri=None, 
    *, 
    namespace=None, 
    table_id=None, 
    schema=None, 
    mode="create", 
    target_bases=None,
    **kwargs)
```

Write a Ray Dataset to Lance format.

**Parameters:**

- `ds`: Ray Dataset to write
- `uri`: Path to the destination Lance dataset (either uri OR namespace+table_id required)
- `namespace`: LanceNamespace instance for metadata catalog integration (requires table_id)
- `table_id`: Table identifier as list of strings (requires namespace)
- `schema`: Optional PyArrow schema
- `mode`: Write mode - "create", "append", or "overwrite"
- `target_bases`: Optional list of registered base names or base path URIs where new data files should be written. In `create` mode, entries must match `initial_bases`; in `append` and `overwrite` modes, entries must match bases already registered in the dataset manifest
- `min_rows_per_file`: Minimum rows per file (default: 1024 * 1024)
- `max_rows_per_file`: Maximum rows per file (default: 64 * 1024 * 1024)
- `data_storage_version`: Optional data storage version
- `storage_options`: Optional storage configuration dictionary
- `base_store_params`: Optional runtime storage options keyed by registered base path URI, used for BlobV2 references outside the dataset root
- `initial_bases`: Optional Lance `DatasetBasePath` objects to register when creating a new dataset
- `external_blob_mode`: Optional BlobV2 external URI handling mode. `"reference"` stores external references; `"ingest"` reads external bytes and writes them into Lance-managed storage
- `allow_external_blob_outside_bases`: Optional boolean to allow BlobV2 external references outside registered non-dataset-root base paths when `external_blob_mode="reference"`
- `ray_remote_args`: Optional kwargs for Ray remote tasks
- `concurrency`: Optional maximum number of concurrent Ray tasks

**Returns:** None

## Write retries and data integrity

`LanceDatasink`, used by the default `write_lance` path, allows up to ten fragment
write attempts for matching I/O errors. Direct calls to `write_fragment`, and
`LanceFragmentWriter` without `retry_params`, use one streaming attempt.
Providing `retry_params` without `max_attempts` also means a single attempt:
errors propagate without retrying, and no input is staged. Set `max_attempts`
explicitly above one to enable retries. `LanceDatasink` explicitly sets its
ten-attempt policy, so its default behavior is unchanged.

Each attempt creates a new input iterator, schema inference, converter and Arrow
reader. There is no additional IPC serialization or temporary-file staging.

**Behavior change for direct `write_fragment` calls:** enabling retries with a
one-shot `Iterator` (including a generator object or an empty iterator) raises
`TypeError` before consuming input. Pass a factory that creates a fresh stream,
or a replayable iterable such as a list or tuple:

```python
import pyarrow as pa
from lance_ray.fragment import write_fragment


def read_blocks():
    for start in range(0, 100, 10):
        yield pa.table({"id": range(start, start + 10)})


# A generator object is fine for a single streaming attempt.
fragments = write_fragment(read_blocks(), "single.lance")

# Pass the function itself so each retry gets a new generator.
fragments = write_fragment(
    read_blocks,
    "retry.lance",
    retry_params={
        "description": "write lance fragments",
        "match": ["LanceError(IO)"],
        "max_attempts": 3,
        "max_backoff_s": 8,
    },
)
```

Custom iterables are also accepted. The caller must ensure that **each iteration
or factory call produces complete, logically equivalent input**. Type checks
cannot prove that contract; `lambda: existing_generator` does not satisfy it.
Each attempt closes its reader and any factory/reiterable-created iterator that
supports `close()`. A directly supplied single-attempt iterator remains the
caller's responsibility.

`LanceDatasink` collects all blocks for one write call into a list of references
and creates a new iterator over that list for each attempt. Ray has already
resolved those blocks before calling the sink: retaining them does not copy
Arrow data or rerun upstream computation. It does keep all blocks alive until
the call finishes, may extend their memory lifetime, and consumes the entire
block iterable before starting the write. Concurrent writes retain separate
lists. A single-attempt sink passes its input through without collecting it.
`max_bytes_per_file` limits destination files, not the memory retained by a call.

`LanceFragmentWriter` converts the original batch to an Arrow table once, then
reruns its transform for each attempt. The transform may return a table or an
iterable of tables. It must produce logically equivalent output from the same
input table; callers must handle any side effects of repeated execution.

Before returning fragments for commit, the writer checks that their total row
count matches the input and raises `RuntimeError` on a mismatch. This is a row
count check, not a comparison of every row's contents. Replay and row counting
do not provide job-wide exactly-once semantics or remove uncommitted destination
files left by failed attempts.
