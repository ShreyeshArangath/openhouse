"""Test ArrowScan OOM behavior in memory-bounded Ray workers.

Tests whether ArrowScan materializes entire files before yielding batches,
causing OOM in distributed workers with bounded memory.

Setup:
- 150M rows, 3.8 GB ORC file with 100-row stripes (1.5M tiny batches)
- Ray worker with 1 GB memory limit (enforced via RSS watchdog)
- If streaming: should process batches incrementally within 200 MB
- If materializing: should load full 3.8 GB and crash

See ARROWSCAN_BATCH_SIZE.md for detailed analysis.

Usage:
    uv run pytest tests/test_ray_arrowscan_oom.py -s -m slow
"""

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.orc as orc
import pytest
import ray

DATA_DIR = Path("/tmp/arrowscan_oom_data")
NUM_ROWS = 150_000_000
STRIPE_ROWS = 100
BATCH_SIZE = 2_000_000
WORKER_MEMORY = 1 * 1024**3


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
    """Write large ORC file incrementally to avoid OOM during generation."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
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
    """Read ORC via ArrowScan in memory-bounded worker with RSS watchdog."""
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
                ctypes.pythonapi.PyThreadState_SetAsyncExc(
                    ctypes.c_ulong(main_thread_id),
                    ctypes.py_object(MemoryError),
                )
                return
            time.sleep(0.05)

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

        print(f"[WORKER] Starting ArrowScan of {file_size / 1024 / 1024:.1f} MB file...", flush=True)
        batch_count = 0
        total_rows = 0
        for batch in arrow_scan.to_record_batches([task]):
            batch_count += 1
            total_rows += batch.num_rows
            if batch_count <= 10 or batch_count % 100000 == 0:
                rss_mb = proc.memory_info().rss / 1024 / 1024
                print(f"[WORKER] Batch {batch_count}: {batch.num_rows} rows (RSS: {rss_mb:.1f} MB)", flush=True)
            del batch
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
    """Test ArrowScan OOM behavior in memory-bounded Ray worker."""
    orc_path = DATA_DIR / "data.orc"

    if orc_path.exists():
        print(f"\n  Reusing cached ORC file: {orc_path}")
    else:
        stripe_desc = f"{STRIPE_ROWS} rows" if STRIPE_ROWS > 0 else "64MB"
        print(f"\n  Generating {NUM_ROWS:,} row ORC file (stripe size: {stripe_desc})...")
        _generate_large_orc(orc_path, NUM_ROWS, STRIPE_ROWS)

    file_size_bytes = orc_path.stat().st_size
    file_size_mb = file_size_bytes / 1024 / 1024

    # Patch uv detection to avoid fresh venv creation for workers
    import ray._private.runtime_env.uv_runtime_env_hook as _uv_hook

    _uv_hook._get_uv_run_cmdline = lambda: None
    ray.init(num_cpus=2, object_store_memory=200 * 1024**2)

    try:
        ref = read_orc_via_arrowscan.remote(str(orc_path), NUM_ROWS, WORKER_MEMORY)
        result = ray.get(ref, timeout=600)
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
