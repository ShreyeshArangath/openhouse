# PyIceberg Changes Required for Streaming Support

## Summary

PyIceberg's `ArrowScan` materializes entire files into memory before yielding batches. Two changes are required to enable true streaming:

1. Forward `batch_size` parameter to PyArrow's `Scanner.from_fragment()`
2. Remove `list()` materialization in `to_record_batches()`

Both changes are required. Fixing only one does not enable streaming.

## Change 1: Add batch_size Parameter to ArrowScan

**File**: `pyiceberg/io/pyarrow.py`

**Current** ([line 1724](https://github.com/apache/iceberg-python/blob/pyiceberg-0.11.0rc2/pyiceberg/io/pyarrow.py#L1724)):
```python
def __init__(
    self,
    table_metadata: TableMetadata,
    io: FileIO,
    row_filter: BooleanExpression,
    projected_schema: Schema,
    case_sensitive: bool = True,
    limit: int | None = None,
    name_mapping: NameMapping | None = None,
    downcast_ns_timestamp_to_us: bool = False,
):
```

**Needed**:
```python
def __init__(
    self,
    table_metadata: TableMetadata,
    io: FileIO,
    row_filter: BooleanExpression,
    projected_schema: Schema,
    case_sensitive: bool = True,
    limit: int | None = None,
    batch_size: int = 131072,  # NEW: match PyArrow default
    name_mapping: NameMapping | None = None,
    downcast_ns_timestamp_to_us: bool = False,
):
    # ... existing code ...
    self._batch_size = batch_size  # NEW: store for later use
```

**Current** ([line 1615-1622](https://github.com/apache/iceberg-python/blob/pyiceberg-0.11.0rc2/pyiceberg/io/pyarrow.py#L1615-L1622)):
```python
fragment_scanner = ds.Scanner.from_fragment(
    fragment=fragment,
    schema=physical_schema,
    filter=pyarrow_filter if not positional_deletes else None,
    columns=[col.name for col in file_project_schema.columns],
)
```

**Needed**:
```python
fragment_scanner = ds.Scanner.from_fragment(
    fragment=fragment,
    schema=physical_schema,
    filter=pyarrow_filter if not positional_deletes else None,
    columns=[col.name for col in file_project_schema.columns],
    batch_size=self._batch_size,  # NEW: forward batch_size
)
```

This is in the `_task_to_record_batches()` helper function. The `ArrowScan` instance would need to be passed down or `batch_size` would need to be threaded through the call chain.

## Change 2: Remove list() Materialization

**File**: `pyiceberg/io/pyarrow.py`

**Current** ([line 1782-1786](https://github.com/apache/iceberg-python/blob/pyiceberg-0.11.0rc2/pyiceberg/io/pyarrow.py#L1782-L1786)):
```python
def batches_for_task(task: FileScanTask) -> list[pa.RecordBatch]:
    # Materialize the iterator here to ensure execution happens within the executor.
    # Otherwise, the iterator would be lazily consumed later (in the main thread),
    # defeating the purpose of using executor.map.
    return list(self._record_batches_from_scan_tasks_and_deletes([task], deletes_per_file))
```

**Problem**: The `list()` call loads all batches into memory before returning.

**Needed**: Return an iterator instead of a list, but this requires rethinking the executor pattern.

### Option A: Keep Executor, Yield Batches Directly

```python
def to_record_batches(self, tasks: Iterable[FileScanTask]) -> Iterator[pa.RecordBatch]:
    deletes_per_file = _read_all_delete_files(self._io, tasks)
    total_row_count = 0

    # Process tasks sequentially and yield batches as they arrive
    for task in tasks:
        for batch in self._record_batches_from_scan_tasks_and_deletes([task], deletes_per_file):
            current_batch_size = len(batch)
            if self._limit is not None and total_row_count + current_batch_size >= self._limit:
                yield batch.slice(0, self._limit - total_row_count)
                return
            else:
                yield batch
                total_row_count += current_batch_size
```

This removes parallelism but enables streaming.

### Option B: Parallel + Streaming with Generator Queue

```python
from queue import Queue
from threading import Thread

def to_record_batches(self, tasks: Iterable[FileScanTask]) -> Iterator[pa.RecordBatch]:
    deletes_per_file = _read_all_delete_files(self._io, tasks)
    executor = ExecutorFactory.get_or_create()

    batch_queue: Queue = Queue(maxsize=16)  # Backpressure: limit queue size

    def process_task(task: FileScanTask):
        for batch in self._record_batches_from_scan_tasks_and_deletes([task], deletes_per_file):
            batch_queue.put(batch)

    def producer():
        futures = [executor.submit(process_task, task) for task in tasks]
        for future in futures:
            future.result()  # Wait for completion
        batch_queue.put(None)  # Sentinel

    Thread(target=producer, daemon=True).start()

    total_row_count = 0
    while True:
        batch = batch_queue.get()
        if batch is None:
            break

        current_batch_size = len(batch)
        if self._limit is not None and total_row_count + current_batch_size >= self._limit:
            yield batch.slice(0, self._limit - total_row_count)
            return
        else:
            yield batch
            total_row_count += current_batch_size
```

This maintains parallelism while enabling streaming via a bounded queue (provides backpressure).

## Change 3: Update Table.scan() API (Optional)

Allow users to specify `batch_size`:

**File**: `pyiceberg/table/__init__.py`

```python
def scan(
    self,
    row_filter: Union[str, BooleanExpression] = ALWAYS_TRUE,
    selected_fields: Tuple[str, ...] = ("*",),
    case_sensitive: bool = True,
    snapshot_id: Optional[int] = None,
    options: Properties = EMPTY_DICT,
    limit: Optional[int] = None,
    batch_size: int = 131072,  # NEW
) -> DataScan:
    # ... pass batch_size to ArrowScan constructor ...
```

## Testing

After changes, verify streaming behavior with:

```python
# Should stream with constant ~10 MB memory regardless of file size
pf = pq.ParquetFile("large_file.parquet")
for batch in pf.iter_batches(batch_size=100_000):
    process(batch)  # Memory stays flat

# After PyIceberg changes, ArrowScan should behave similarly
scan = ArrowScan(..., batch_size=100_000)
for batch in scan.to_record_batches(tasks):
    process(batch)  # Memory should stay flat
```

Run the experiments in `tests/test_arrowscan_memory.py` and `tests/test_ray_arrowscan_oom.py` to verify:
- Memory peak should equal ~2-3 batches (not full file)
- Ratio should be < 0.1x (not 1.0x)
- Ray worker should not OOM on 3.8 GB file with 1 GB limit

## References

- [ARROWSCAN_BATCH_SIZE.md](ARROWSCAN_BATCH_SIZE.md) - Experimental proof of current behavior
- [PyArrow Scanner API](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html)
- [PyIceberg Source](https://github.com/apache/iceberg-python/blob/pyiceberg-0.11.0rc2/pyiceberg/io/pyarrow.py)
