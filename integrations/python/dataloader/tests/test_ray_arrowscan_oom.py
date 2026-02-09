"""Experiment: Does ArrowScan's full-materialization behavior cause OOM in bounded workers?

## Background

The companion test (`test_arrowscan_memory.py`) proves that PyIceberg's `ArrowScan.to_record_batches()`
materializes entire files into memory rather than streaming lazily. This experiment validates that
behavior in a distributed compute scenario: does ArrowScan crash workers with bounded memory?

## Experiment Design

### Data Generation

We generate a large ORC file with these characteristics:

- **150 million rows**
- **~3.8 GB uncompressed** on disk
- **4 columns**: id (int64), value_a (int32), value_b (int32), label (string ~20 chars)
- **~40 bytes per row** uncompressed
- **100 rows per ORC stripe**
- **Uncompressed** to ensure disk size ≈ in-memory size
- **Incrementally written** in 2M-row batches to avoid OOM during generation
- **Cached** to `/tmp/arrowscan_oom_data/` for reuse across runs

The file is written incrementally to avoid OOM during generation, but contains ~1.5 million
tiny stripes (100 rows each, ~4 KB each) to test whether ArrowScan can stream per-stripe.

### ORC Stripe Size = Batch Size

**Important**: For ORC files, the stripe size IS the batch size. PyArrow's ORC reader yields
one RecordBatch per stripe. We set stripe size to 100 rows to create tiny batches (~4 KB each).

**ArrowScan has no batch_size parameter**. The batch size is determined by:
- **ORC files**: Stripe size (set when writing, cannot override at read time)
- **Parquet files**: PyArrow defaults to 64K rows (ArrowScan doesn't expose `batch_size` param)

With 100-row stripes, a true streaming reader would process 1.5M tiny batches incrementally,
never holding more than 1-2 batches in memory (~8 KB). Memory should stay flat around 150-200 MB.

### Ray Worker with Memory Watchdog

We run ArrowScan inside a Ray worker with a **1 GB memory limit**. Since Ray's local mode doesn't
enforce the `memory` annotation on `@ray.remote`, we implement a self-enforcing watchdog:

1. Background thread monitors worker RSS via `psutil` every 50ms
2. When RSS exceeds 1 GB, injects `MemoryError` into main thread via `ctypes.pythonapi.PyThreadState_SetAsyncExc`
3. Main thread catches `MemoryError` and returns crash status

This simulates a hard memory cap that production clusters enforce via cgroups/OOM killer.

### What We're Testing

**Hypothesis**: ArrowScan materializes all batches before yielding, regardless of stripe/batch size.

**Test**: Call `list(arrow_scan.to_record_batches([task]))` on a 3.8 GB file with 1.5M tiny
batches, inside a 1 GB worker.

**Expected behavior if ArrowScan streams**:
- Yields first 100-row batch immediately (~4 KB)
- RSS stays flat around 150-200 MB (only 1-2 batches in memory)
- Completes reading all 150M rows without OOM

**Expected behavior if ArrowScan materializes**:
- Loads all 1.5M batches into memory before yielding first batch
- RSS climbs steadily: 200 MB → 400 MB → 600 MB → 800 MB → 1000 MB
- Worker crashes with OOM before completing

## Observed Results

With 100-row batches (1.5 million tiny stripes):
- **Memory climbs continuously** from 142 MB to 1078 MB
- **Worker crashes with OOM** before printing a single batch
- **No batch messages** appear, confirming iterator never yields

This proves ArrowScan **eagerly materializes all batches** before the iterator yields anything,
regardless of batch size. Even with 1.5 million 4KB batches, it loads the entire 3.8 GB file
into memory first.

## Why This Causes OOM in Distributed Systems

In distributed compute frameworks (Ray, Spark, Dask), workers typically have bounded memory
(e.g., 4-16 GB). ArrowScan's eager materialization means:

1. **File size must fit in worker RAM** — A 10 GB Parquet file needs a 10+ GB worker, even with
   64K-row batches that should enable streaming processing
2. **No backpressure** — Can't pause reading while downstream is processing
3. **Memory is wasted** — All batches held in memory even if consumer processes one at a time

A true streaming reader would let workers process files larger than RAM by:
- Yielding batches incrementally (lazy iteration)
- Holding only 1-2 batches in memory
- Allowing consumer to dictate read pace

## Workarounds

1. **Split large files into smaller partitions** — Ensure each partition fits in worker RAM
2. **Use ArrowScan's `limit` parameter** — Bounds rows read, but still materializes all rows
   up to the limit (tested separately, not included here)
3. **Replace ArrowScan with direct PyArrow readers** — Use `pq.ParquetFile.iter_batches(batch_size=...)`
   or `orc.ORCFile.read_stripe(n)` for true streaming

## Usage

Regular test suite (excludes slow experiment):
```bash
make test
```

Run the OOM experiment:
```bash
uv run pytest tests/test_ray_arrowscan_oom.py -s -m slow
```

First run generates the file (~1-2 minutes), subsequent runs reuse cached file (~7 seconds).
Cached file is stored at `/tmp/arrowscan_oom_data/data.orc`.
"""

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.orc as orc
import pytest
import ray

# Configuration
DATA_DIR = Path("/tmp/arrowscan_oom_data")
NUM_ROWS = 150_000_000  # 150 million rows
STRIPE_ROWS = 100  # Rows per ORC stripe (= batch size)
BATCH_SIZE = 2_000_000  # Rows per write() call during generation (doesn't affect stripe size)
WORKER_MEMORY = 1 * 1024**3  # 1 GB


def _make_table(num_rows: int) -> pa.Table:
    """Generate an N-row, 4-column PyArrow table (~40 bytes/row uncompressed)."""
    rng = np.random.default_rng(42)
    ids = np.arange(num_rows, dtype=np.int64)
    value_a = rng.integers(0, 1_000_000, size=num_rows, dtype=np.int32)
    value_b = rng.integers(0, 1_000_000, size=num_rows, dtype=np.int32)

    pool_size = min(1000, num_rows)
    pool = np.array([bytes(rng.integers(97, 123, size=20, dtype=np.uint8)).decode() for _ in range(pool_size)])
    labels = np.tile(pool, (num_rows + pool_size - 1) // pool_size)[:num_rows]

    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "value_a": pa.array(value_a, type=pa.int32()),
            "value_b": pa.array(value_b, type=pa.int32()),
            "label": pa.array(labels, type=pa.string()),
        }
    )


def _generate_large_orc(file_path: Path, num_rows: int, stripe_rows: int) -> None:
    """Write a large ORC file incrementally, BATCH_SIZE rows at a time.

    Args:
        file_path: Output ORC file path
        num_rows: Total rows to generate
        stripe_rows: Rows per ORC stripe (ArrowScan yields batches per stripe).
                     Set to 0 for default 64MB stripes. ~40 bytes/row uncompressed.
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)
    # Calculate stripe size: 0 means use 64MB default, otherwise ~40 bytes/row
    stripe_size = 64 * 1024 * 1024 if stripe_rows == 0 else stripe_rows * 40
    writer = orc.ORCWriter(str(file_path), compression="uncompressed", stripe_size=stripe_size)

    total_batches = (num_rows + BATCH_SIZE - 1) // BATCH_SIZE
    for i, start in enumerate(range(0, num_rows, BATCH_SIZE)):
        batch_rows = min(BATCH_SIZE, num_rows - start)
        table = _make_table(num_rows=batch_rows)
        writer.write(table)
        del table
        if (i + 1) % 10 == 0:
            print(f"  Generated {i + 1}/{total_batches} batches ({(i + 1) * BATCH_SIZE:,} rows)")

    writer.close()
    print(f"  Wrote {file_path} ({file_path.stat().st_size / 1024 / 1024:.1f} MB)")


@ray.remote(memory=WORKER_MEMORY)
def read_orc_via_arrowscan(file_path: str, num_rows: int, memory_limit: int) -> dict:
    """Attempt to read an ORC file through ArrowScan in a memory-bounded Ray worker.

    A background thread monitors RSS and raises MemoryError when the limit is exceeded,
    simulating a hard memory cap that Ray local mode doesn't enforce.

    Returns dict with keys:
        - status: "oom" or "survived"
        - peak_rss_mb: Peak RSS reached
        - batch_count: Number of batches processed (if survived)
        - total_rows: Total rows read (if survived)
    """
    import ctypes
    import os
    import threading
    import time
    from pathlib import Path

    import psutil
    import pyarrow as pa
    from pyiceberg.expressions import AlwaysTrue
    from pyiceberg.io.pyarrow import ArrowScan, PyArrowFileIO
    from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.table import FileScanTask
    from pyiceberg.table.metadata import new_table_metadata
    from pyiceberg.table.sorting import SortOrder
    from pyiceberg.types import IntegerType, LongType, NestedField, StringType

    # --- Memory watchdog: raise MemoryError in main thread when RSS exceeds limit ---
    proc = psutil.Process()
    peak_rss = 0
    stop_event = threading.Event()

    def _watchdog():
        nonlocal peak_rss
        main_thread_id = threading.main_thread().ident
        while not stop_event.is_set():
            rss = proc.memory_info().rss
            peak_rss = max(peak_rss, rss)
            rss_mb = rss / 1024 / 1024
            limit_mb = memory_limit / 1024 / 1024
            print(f"[WATCHDOG] RSS: {rss_mb:>7.1f} MB  (limit: {limit_mb:.0f} MB)", flush=True)
            if rss > memory_limit:
                print("[WATCHDOG] MEMORY LIMIT EXCEEDED! Injecting MemoryError...", flush=True)
                # Inject MemoryError into the main thread
                ctypes.pythonapi.PyThreadState_SetAsyncExc(
                    ctypes.c_ulong(main_thread_id),
                    ctypes.py_object(MemoryError),
                )
                return
            time.sleep(0.05)  # 50ms polling

    watchdog = threading.Thread(target=_watchdog, daemon=True)
    watchdog.start()

    try:
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="value_a", field_type=IntegerType(), required=False),
            NestedField(field_id=3, name="value_b", field_type=IntegerType(), required=False),
            NestedField(field_id=4, name="label", field_type=StringType(), required=False),
        )

        name_mapping = (
            '[{"field-id": 1, "names": ["id"]},'
            ' {"field-id": 2, "names": ["value_a"]},'
            ' {"field-id": 3, "names": ["value_b"]},'
            ' {"field-id": 4, "names": ["label"]}]'
        )

        table_metadata = new_table_metadata(
            schema=schema,
            partition_spec=PartitionSpec(),
            sort_order=SortOrder(order_id=0),
            location=str(Path(file_path).parent),
            properties={"schema.name-mapping.default": name_mapping},
        )

        file_size = os.path.getsize(file_path)
        data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=file_path,
            file_format=FileFormat.ORC,
            partition={},
            record_count=num_rows,
            file_size_in_bytes=file_size,
        )
        data_file._spec_id = 0
        task = FileScanTask(data_file=data_file)

        arrow_scan = ArrowScan(
            table_metadata=table_metadata,
            io=PyArrowFileIO(),
            projected_schema=schema,
            row_filter=AlwaysTrue(),
        )

        # Materialize all batches via list() — this is where OOM should occur
        print(f"[WORKER] Starting ArrowScan of {file_size / 1024 / 1024:.1f} MB file...", flush=True)
        batch_count = 0
        total_rows = 0
        for batch in arrow_scan.to_record_batches([task]):
            batch_count += 1
            total_rows += batch.num_rows
            if batch_count <= 10 or batch_count % 100000 == 0:
                rss_mb = proc.memory_info().rss / 1024 / 1024
                print(f"[WORKER] Batch {batch_count}: {batch.num_rows} rows (RSS: {rss_mb:.1f} MB)", flush=True)
            del batch  # Release immediately (though ArrowScan materializes everything anyway)
        print(f"[WORKER] Complete! {batch_count:,} batches, {total_rows:,} rows", flush=True)
        allocated = pa.total_allocated_bytes()
        return {
            "status": "survived",
            "total_rows": total_rows,
            "batch_count": batch_count,
            "allocated_mb": allocated / 1024 / 1024,
            "peak_rss_mb": peak_rss / 1024 / 1024,
        }
    except MemoryError:
        return {
            "status": "oom",
            "peak_rss_mb": peak_rss / 1024 / 1024,
        }
    finally:
        stop_event.set()


@pytest.mark.slow
def test_ray_worker_oom_large_orc() -> None:
    """Run ArrowScan inside a memory-bounded Ray worker to observe OOM behavior."""
    orc_path = DATA_DIR / "data.orc"

    # --- Data generation (cached) ---
    if orc_path.exists():
        print(f"\n  Reusing cached ORC file: {orc_path}")
    else:
        stripe_desc = f"{STRIPE_ROWS} rows" if STRIPE_ROWS > 0 else "64MB"
        print(f"\n  Generating {NUM_ROWS:,} row ORC file (stripe size: {stripe_desc})...")
        _generate_large_orc(orc_path, NUM_ROWS, STRIPE_ROWS)

    file_size_bytes = orc_path.stat().st_size
    file_size_mb = file_size_bytes / 1024 / 1024

    # --- Ray cluster ---
    # Ray 2.53 auto-detects `uv run` ancestors and creates fresh venvs for
    # workers, which lack the ray package. Patch the detection to disable
    # this so workers reuse the current venv's Python directly.
    import ray._private.runtime_env.uv_runtime_env_hook as _uv_hook

    _uv_hook._get_uv_run_cmdline = lambda: None
    ray.init(num_cpus=2, object_store_memory=200 * 1024**2)

    try:
        ref = read_orc_via_arrowscan.remote(str(orc_path), NUM_ROWS, WORKER_MEMORY)
        result = ray.get(ref, timeout=600)

        # --- Report ---
        print(f"\n{'=' * 60}")
        print("  Ray ArrowScan OOM Experiment")
        print(f"{'=' * 60}")
        print(f"  ORC file size on disk:    {file_size_mb:.1f} MB")
        print(f"  Worker memory limit:      {WORKER_MEMORY / 1024 / 1024:.0f} MB")
        print(f"  Row count:                {NUM_ROWS:,}")
        stripe_desc = f"{STRIPE_ROWS} rows/stripe" if STRIPE_ROWS > 0 else "64MB stripes"
        print(f"  ORC stripe size:          {stripe_desc}")

        if result["status"] == "oom":
            print("  Outcome:                  OOM (MemoryError)")
            print(f"  Peak RSS at crash:        {result['peak_rss_mb']:.1f} MB")
            print(f"{'=' * 60}")
            print("  → ArrowScan materialized the full file, exceeding worker memory.")
            print("  → A streaming reader would process batches within bounded memory.")
        else:
            print("  Outcome:                  SURVIVED (no OOM)")
            print(f"  Batches processed:        {result.get('batch_count', 'N/A'):,}")
            print(f"  Rows read:                {result['total_rows']:,}")
            print(f"  Arrow allocated:          {result['allocated_mb']:.1f} MB")
            print(f"  Peak RSS:                 {result['peak_rss_mb']:.1f} MB")

        print(f"{'=' * 60}\n")

    except (ray.exceptions.WorkerCrashedError, ray.exceptions.OutOfMemoryError, ray.exceptions.RayTaskError) as exc:
        print(f"\n{'=' * 60}")
        print("  Ray ArrowScan OOM Experiment")
        print(f"{'=' * 60}")
        print(f"  ORC file size on disk:    {file_size_mb:.1f} MB")
        print(f"  Worker memory limit:      {WORKER_MEMORY / 1024 / 1024:.0f} MB")
        print(f"  Row count:                {NUM_ROWS:,}")
        stripe_desc = f"{STRIPE_ROWS} rows/stripe" if STRIPE_ROWS > 0 else "64MB stripes"
        print(f"  ORC stripe size:          {stripe_desc}")
        print("  Outcome:                  WORKER CRASHED")
        print(f"  Exception type:           {type(exc).__name__}")
        print(f"  Message:                  {exc}")
        print(f"{'=' * 60}")
        print("  → ArrowScan materialized the full file, exceeding worker memory.")
        print("  → A streaming reader would process batches within bounded memory.")
        print(f"{'=' * 60}\n")

    finally:
        ray.shutdown()
