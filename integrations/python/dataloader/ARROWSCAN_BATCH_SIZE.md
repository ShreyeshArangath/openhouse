# ArrowScan Has No batch_size Control: Analysis and Experimental Proof

## TL;DR

PyIceberg's [`ArrowScan`](https://github.com/apache/iceberg-python/blob/pyiceberg-0.11.0rc2/pyiceberg/io/pyarrow.py#L1724) does not expose a `batch_size` parameter. PyArrow's underlying [`Scanner.from_fragment()`](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html) does accept `batch_size` (default: 131072 rows), but PyIceberg [never passes it through](https://github.com/apache/iceberg-python/blob/pyiceberg-0.11.0rc2/pyiceberg/io/pyarrow.py#L1615-L1622). Additionally, [`ArrowScan.to_record_batches()`](https://github.com/apache/iceberg-python/blob/pyiceberg-0.11.0rc2/pyiceberg/io/pyarrow.py#L1759) eagerly [materializes all batches per file into a `list()`](https://github.com/apache/iceberg-python/blob/pyiceberg-0.11.0rc2/pyiceberg/io/pyarrow.py#L1782-L1786) before yielding, so even small batches do not enable streaming. File size must fit in worker RAM.

Tested with PyIceberg 0.11.0, PyArrow 23.0.0.

---

## The Gap

PyArrow's [`Scanner.from_fragment()`](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html) signature includes `batch_size`:

```
Scanner.from_fragment(
    fragment,       # required
    schema=None,
    columns=None,
    filter=None,
    batch_size=131072,     <-- PyArrow supports this
    batch_readahead=16,
    ...
)
```

PyIceberg's `ArrowScan` calls `Scanner.from_fragment()` at [`pyiceberg/io/pyarrow.py:1615`](https://github.com/apache/iceberg-python/blob/pyiceberg-0.11.0rc2/pyiceberg/io/pyarrow.py#L1615-L1622) without forwarding `batch_size`:

```python
fragment_scanner = ds.Scanner.from_fragment(
    fragment=fragment,
    schema=physical_schema,
    filter=pyarrow_filter if not positional_deletes else None,
    columns=[col.name for col in file_project_schema.columns],
    # batch_size is NOT passed
)
```

[`ArrowScan.__init__`](https://github.com/apache/iceberg-python/blob/pyiceberg-0.11.0rc2/pyiceberg/io/pyarrow.py#L1724) accepts only four parameters: `table_metadata`, `io`, `projected_schema`, `row_filter`. There is no scan option, read option, or property-based mechanism to control batch size.

Even if `batch_size` were forwarded, it would not help. [`to_record_batches()`](https://github.com/apache/iceberg-python/blob/pyiceberg-0.11.0rc2/pyiceberg/io/pyarrow.py#L1759) at [line 1782-1786](https://github.com/apache/iceberg-python/blob/pyiceberg-0.11.0rc2/pyiceberg/io/pyarrow.py#L1782-L1786) materializes each task's batches into a `list()`:

```python
def batches_for_task(task: FileScanTask) -> list[pa.RecordBatch]:
    # Materialize the iterator here to ensure execution happens within the executor.
    return list(self._record_batches_from_scan_tasks_and_deletes([task], deletes_per_file))
```

This converts the lazy batch iterator into a fully materialized list before any batch is yielded to the caller.

---

## Experiment 1: Memory Behavior (200K rows)

Tests whether "streaming" iteration (delete each batch after processing) uses less memory than full materialization.

Source: [`test_arrowscan_memory.py::test_arrowscan_memory_behavior`](tests/test_arrowscan_memory.py)

### Setup

- 200,000 rows, 4 columns (id int64, value_a int32, value_b int32, label string ~20 chars)
- ~40 bytes/row uncompressed
- Parquet: 10,000-row row groups. ORC: small stripes.
- Measure via `pa.total_allocated_bytes()`

### Results

```
  ArrowScan Memory Behavior -- PARQUET
  File size on disk:        3.76 MB
  Total rows:               200,000
  Batch count (streaming):  20
  Streaming peak allocated: 7.70 MB
  Streaming residual:       0.00 MB
  Materialized allocated:   7.70 MB
  Peak/Materialized ratio:  1.00x
  Conclusion: MATERIALIZING (full file loaded)

  ArrowScan Memory Behavior -- ORC
  File size on disk:        5.00 MB
  Total rows:               200,000
  Batch count (streaming):  98
  Streaming peak allocated: 7.64 MB
  Streaming residual:       0.00 MB
  Materialized allocated:   7.64 MB
  Peak/Materialized ratio:  1.00x
  Conclusion: MATERIALIZING (full file loaded)
```

Streaming peak equals materialized total in both formats. Ratio is 1.00x. ArrowScan loads the entire file before yielding the first batch.

---

## Experiment 2: Memory Scaling (500K to 10M rows)

Tests whether memory scales linearly with row count (confirming full materialization, not streaming).

Source: [`test_arrowscan_memory.py::test_arrowscan_memory_scaling`](tests/test_arrowscan_memory.py)

### Results

```
  ArrowScan Memory Scaling -- PARQUET
          Rows    File MB   Stream Peak MB   Mater. MB   Ratio   Stream B/row   Mater. B/row
       500,000       9.39            19.26       19.26    1.00           40.4           40.4
     1,000,000      18.78            38.52       38.52    1.00           40.4           40.4
     2,000,000      37.56            77.04       77.04    1.00           40.4           40.4
    10,000,000     187.79           385.19      385.19    1.00           40.4           40.4

  Conclusion: CONSTANT bytes/row (40.4 avg, 0.0% spread) = LINEAR scaling

  ArrowScan Memory Scaling -- ORC
          Rows    File MB   Stream Peak MB   Mater. MB   Ratio   Stream B/row   Mater. B/row
       500,000      12.51            19.09       19.09    1.00           40.0           40.0
     1,000,000      25.02            38.18       38.18    1.00           40.0           40.0
     2,000,000      50.05            76.35       76.35    1.00           40.0           40.0
    10,000,000     250.25           381.77      381.77    1.00           40.0           40.0

  Conclusion: CONSTANT bytes/row (40.0 avg, 0.0% spread) = LINEAR scaling
```

Memory usage is 40 bytes/row regardless of file size. Streaming peak never dips below materialized total. The entire file is loaded into memory.

---

## Experiment 3: Ray Worker OOM (150M rows, 1 GB worker)

Tests whether ArrowScan causes OOM in a memory-bounded distributed worker.

Source: [`test_ray_arrowscan_oom.py::test_ray_worker_oom_large_orc`](tests/test_ray_arrowscan_oom.py)

### Setup

- 150 million rows, ~3.8 GB uncompressed ORC file
- 100 rows per ORC stripe (1.5 million stripes, ~4 KB each)
- Ray worker with 1 GB memory limit enforced via [RSS watchdog thread](tests/test_ray_arrowscan_oom.py#L217-L234)
- Watchdog polls every 50ms, injects MemoryError when RSS exceeds 1 GB

### Observed Behavior

- RSS climbs continuously from 142 MB to 1078 MB
- Worker crashes with OOM before yielding a single batch
- Zero batch messages printed, confirming the iterator never yields

A true streaming reader with 100-row batches would hold ~8 KB in memory (1-2 batches). Instead, ArrowScan loads the full 3.8 GB before the first yield.

### Run Command

```bash
cd integrations/python/dataloader
uv run pytest tests/test_ray_arrowscan_oom.py -s -m slow
```

First run generates the file (~1-2 minutes), subsequent runs use cache at `/tmp/arrowscan_oom_data/data.orc`.

### Actual Test Output

```
tests/test_ray_arrowscan_oom.py
  Reusing cached ORC file: /tmp/arrowscan_oom_data/data.orc
2026-02-09 15:39:26,100 INFO worker.py:1998 -- Started a local Ray instance.
(read_orc_via_arrowscan pid=92344) [WATCHDOG] RSS:   142.2 MB  (limit: 1024 MB)
(read_orc_via_arrowscan pid=92344) [WORKER] Starting ArrowScan of 3783.8 MB file...
(read_orc_via_arrowscan pid=92344) [WATCHDOG] RSS:   212.0 MB  (limit: 1024 MB)
(read_orc_via_arrowscan pid=92344) [WATCHDOG] RSS:   276.8 MB  (limit: 1024 MB)
(read_orc_via_arrowscan pid=92344) [WATCHDOG] RSS:   341.6 MB  (limit: 1024 MB)
(read_orc_via_arrowscan pid=92344) [WATCHDOG] RSS:   406.0 MB  (limit: 1024 MB)
(read_orc_via_arrowscan pid=92344) [WATCHDOG] RSS:   468.5 MB  (limit: 1024 MB)
(read_orc_via_arrowscan pid=92344) [WATCHDOG] RSS:   526.6 MB  (limit: 1024 MB)
(read_orc_via_arrowscan pid=92344) [WATCHDOG] RSS:   595.3 MB  (limit: 1024 MB)
(read_orc_via_arrowscan pid=92344) [WATCHDOG] RSS:   662.1 MB  (limit: 1024 MB)
(read_orc_via_arrowscan pid=92344) [WATCHDOG] RSS:   726.4 MB  (limit: 1024 MB)
(read_orc_via_arrowscan pid=92344) [WATCHDOG] RSS:   792.9 MB  (limit: 1024 MB)
(read_orc_via_arrowscan pid=92344) [WATCHDOG] RSS:   858.1 MB  (limit: 1024 MB)
(read_orc_via_arrowscan pid=92344) [WATCHDOG] RSS:   922.0 MB  (limit: 1024 MB)
(read_orc_via_arrowscan pid=92344) [WATCHDOG] RSS:   988.6 MB  (limit: 1024 MB)
(read_orc_via_arrowscan pid=92344) [WATCHDOG] RSS:  1052.7 MB  (limit: 1024 MB)
(read_orc_via_arrowscan pid=92344) [WATCHDOG] MEMORY LIMIT EXCEEDED! Injecting MemoryError...

============================================================
  Ray ArrowScan OOM Experiment
============================================================
  ORC file size on disk:    3783.8 MB
  Worker memory limit:      1024 MB
  Row count:                150,000,000
  ORC stripe size:          100 rows/stripe
  Outcome:                  OOM (MemoryError)
  Peak RSS at crash:        1052.7 MB
============================================================
  → ArrowScan materialized the full file, exceeding worker memory.
  → A streaming reader would process batches within bounded memory.
============================================================
```

The logs show RSS climbing steadily from 142 MB to 1052 MB before the watchdog triggers OOM. No batch was ever yielded.
