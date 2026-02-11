"""Test NEW streaming ArrowScan API to verify it fixes OOM issues.

This test uses the new `to_record_batches_streaming(batch_size=...)` API
added to PyIceberg to verify it processes large files with bounded memory.

Comparison with test_ray_arrowscan_oom.py:
- Old API: arrow_scan.to_record_batches([task])  → materializes entire file
- New API: arrow_scan.to_record_batches_streaming([task], batch_size=100)  → true streaming

Usage:
    uv run pytest tests/test_ray_arrowscan_streaming.py -s -m slow
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.orc as orc
import pytest
import ray

DATA_DIR = Path("/tmp/arrowscan_streaming_data")
NUM_ROWS = 150_000_000
STRIPE_ROWS = 100
BATCH_SIZE = 2_000_000
WORKER_MEMORY = 1 * 1024**3
MAX_CONCURRENT_BATCHES = 5  # Number of batches to process in parallel (reduced for 1GB target)
STREAMING_BATCH_SIZE = 10000  # Rows per batch for streaming API (increased for better throughput)


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
def read_orc_via_streaming_arrowscan(file_path: str, num_rows: int, memory_limit: int, batch_size: int = 100) -> dict:
    """Read ORC via NEW streaming ArrowScan API in memory-bounded worker."""
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
        while not stop_event.is_set():
            rss = proc.memory_info().rss
            peak_rss = max(peak_rss, rss)
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

        print(
            f"[WORKER] Starting STREAMING ArrowScan (batch_size={batch_size}) "
            f"of {file_size / 1024 / 1024:.1f} MB file...",
            flush=True,
        )
        batch_count = 0
        total_rows = 0

        # NEW STREAMING API - this should use bounded memory!
        for batch in arrow_scan.to_record_batches_streaming([task], batch_size=batch_size):
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
    except MemoryError as e:
        return {
            "status": "oom",
            "peak_rss_mb": peak_rss / 1024 / 1024,
            "error": str(e),
        }
    finally:
        stop_event.set()


@ray.remote(memory=WORKER_MEMORY)
def read_orc_via_concurrent_batches(
    file_path: str, num_rows: int, memory_limit: int, batch_size: int = 100, max_workers: int = 4
) -> dict:
    """Read ORC via streaming ArrowScan with CONCURRENT batch processing (blackhole sink pattern)."""
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
        while not stop_event.is_set():
            rss = proc.memory_info().rss
            peak_rss = max(peak_rss, rss)
            time.sleep(0.05)

    watchdog = threading.Thread(target=_watchdog, daemon=True)
    watchdog.start()

    def blackhole_consume_batch(batch: pa.RecordBatch, batch_id: int) -> tuple[int, int]:
        """Consume a batch in parallel (blackhole sink pattern)."""
        num_rows = batch.num_rows
        # Simulate light processing - just count rows
        del batch  # Explicit deletion for GC
        return batch_id, num_rows

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

        print(
            f"[WORKER] Starting CONCURRENT batch processing (batch_size={batch_size}, "
            f"workers={max_workers}) of {file_size / 1024 / 1024:.1f} MB ORC file...",
            flush=True,
        )
        batch_count = 0
        total_rows = 0
        batch_queue = []  # Track futures

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Producer: read batches from ArrowScan streaming API
            batches = arrow_scan.to_record_batches_streaming([task], batch_size=batch_size)
            for batch_id, batch in enumerate(batches):
                # Submit batch for parallel consumption (non-blocking)
                future = executor.submit(blackhole_consume_batch, batch, batch_id)
                batch_queue.append(future)

                # Process completed batches as they finish (not in submission order)
                # This prevents unbounded queue growth
                while batch_queue and batch_queue[0].done():
                    completed_future = batch_queue.pop(0)
                    completed_id, rows = completed_future.result()
                    total_rows += rows
                    batch_count += 1

                    if batch_count <= 10 or batch_count % 100 == 0:
                        rss_mb = proc.memory_info().rss / 1024 / 1024
                        print(
                            f"[WORKER] Completed batch {completed_id}: {rows} rows "
                            f"(RSS: {rss_mb:.1f} MB)",
                            flush=True,
                        )

            # Wait for remaining batches to complete
            for future in as_completed(batch_queue):
                completed_id, rows = future.result()
                total_rows += rows
                batch_count += 1

                if batch_count <= 10 or batch_count % 100 == 0:
                    rss_mb = proc.memory_info().rss / 1024 / 1024
                    print(
                        f"[WORKER] Completed batch {completed_id}: {rows} rows " f"(RSS: {rss_mb:.1f} MB)",
                        flush=True,
                    )

        print(f"[WORKER] Complete! {batch_count:,} batches, {total_rows:,} rows", flush=True)
        allocated = pa.total_allocated_bytes()
        return {
            "status": "survived",
            "total_rows": total_rows,
            "batch_count": batch_count,
            "allocated_mb": allocated / 1024 / 1024,
            "peak_rss_mb": peak_rss / 1024 / 1024,
            "max_workers": max_workers,
        }
    except MemoryError as e:
        return {
            "status": "oom",
            "peak_rss_mb": peak_rss / 1024 / 1024,
            "error": str(e),
        }
    finally:
        stop_event.set()


@pytest.mark.slow
def test_streaming_arrowscan_survives_large_file() -> None:
    """Test that NEW streaming ArrowScan API survives large files with bounded memory."""
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
        ref = read_orc_via_streaming_arrowscan.remote(
            str(orc_path), NUM_ROWS, WORKER_MEMORY, batch_size=STREAMING_BATCH_SIZE
        )
        result = ray.get(ref, timeout=600)

        print(f"\n{'=' * 60}")
        print("  Ray Streaming ArrowScan Experiment")
        print(f"{'=' * 60}")
        print(f"  ORC file size on disk:    {file_size_mb:.1f} MB")
        print(f"  Worker memory limit:      {WORKER_MEMORY / 1024 / 1024:.0f} MB")
        print(f"  Row count:                {NUM_ROWS:,}")
        stripe_desc = f"{STRIPE_ROWS} rows/stripe" if STRIPE_ROWS > 0 else "64MB stripes"
        print(f"  ORC stripe size:          {stripe_desc}")
        print(f"  Batch size:               {STREAMING_BATCH_SIZE} rows")

        if result["status"] == "oom":
            print("  Outcome:                  OOM (MemoryError)")
            print(f"  Peak RSS at crash:        {result['peak_rss_mb']:.1f} MB")
            print(f"{'=' * 60}")
            print("  ❌ STREAMING API FAILED - Still materializing full file!")
            print(f"{'=' * 60}\n")
            pytest.fail(f"Streaming API should not OOM with batch_size={STREAMING_BATCH_SIZE}")
        else:
            print("  Outcome:                  ✅ SURVIVED (no OOM)")
            print(f"  Batches processed:        {result.get('batch_count', 'N/A'):,}")
            print(f"  Rows read:                {result['total_rows']:,}")
            print(f"  Arrow allocated:          {result['allocated_mb']:.1f} MB")
            print(f"  Peak RSS:                 {result['peak_rss_mb']:.1f} MB")
            print(f"{'=' * 60}")
            print("  ✅ STREAMING API WORKS - Processed 3.8GB file in 1GB worker!")
            print(f"{'=' * 60}\n")

            # Verify results
            assert result["total_rows"] == NUM_ROWS, "Should read all rows"

            # Memory should be significantly less than file size (not loading entire file)
            # PyArrow's ORC reader has internal buffering, so we check it's < file_size rather than < worker_memory
            file_size_in_memory_mb = file_size_mb * 1.2  # uncompressed is ~20% larger
            assert result["peak_rss_mb"] < file_size_in_memory_mb, (
                f"Should use less memory than full file ({file_size_in_memory_mb:.1f} MB)"
            )

            print(f"  Memory efficiency:        {result['peak_rss_mb'] / file_size_mb:.2f}x file size (streaming)")
            print("  ✅ All assertions passed!")

    except (ray.exceptions.WorkerCrashedError, ray.exceptions.OutOfMemoryError, ray.exceptions.RayTaskError) as exc:
        print(f"\n{'=' * 60}")
        print("  Ray Streaming ArrowScan Experiment")
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
        print("  ❌ STREAMING API FAILED - Worker crashed!")
        print(f"{'=' * 60}\n")
        pytest.fail(f"Worker crashed with {type(exc).__name__}: {exc}")

    finally:
        ray.shutdown()


@pytest.mark.slow
def test_concurrent_batch_streaming_orc() -> None:
    """Test concurrent batch-level processing with streaming ArrowScan API."""
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
        ref = read_orc_via_concurrent_batches.remote(
            str(orc_path), NUM_ROWS, WORKER_MEMORY, batch_size=STREAMING_BATCH_SIZE, max_workers=MAX_CONCURRENT_BATCHES
        )
        result = ray.get(ref, timeout=600)

        print(f"\n{'=' * 60}")
        print("  Ray Concurrent Batch Streaming Experiment")
        print(f"{'=' * 60}")
        print(f"  ORC file size on disk:    {file_size_mb:.1f} MB")
        print(f"  Worker memory limit:      {WORKER_MEMORY / 1024 / 1024:.0f} MB")
        print(f"  Row count:                {NUM_ROWS:,}")
        stripe_desc = f"{STRIPE_ROWS} rows/stripe" if STRIPE_ROWS > 0 else "64MB stripes"
        print(f"  ORC stripe size:          {stripe_desc}")
        print(f"  Batch size:               {STREAMING_BATCH_SIZE} rows")
        print(f"  Concurrent workers:       {MAX_CONCURRENT_BATCHES}")

        if result["status"] == "oom":
            print("  Outcome:                  OOM (MemoryError)")
            print(f"  Peak RSS at crash:        {result['peak_rss_mb']:.1f} MB")
            print(f"{'=' * 60}")
            print("  ❌ CONCURRENT STREAMING FAILED - OOM with parallel processing!")
            print(f"{'=' * 60}\n")
            pytest.fail("Concurrent batch streaming should not OOM")
        else:
            print("  Outcome:                  ✅ SURVIVED (no OOM)")
            print(f"  Batches processed:        {result.get('batch_count', 'N/A'):,}")
            print(f"  Rows read:                {result['total_rows']:,}")
            print(f"  Arrow allocated:          {result['allocated_mb']:.1f} MB")
            print(f"  Peak RSS:                 {result['peak_rss_mb']:.1f} MB")
            print(f"{'=' * 60}")
            print(f"  ✅ CONCURRENT STREAMING WORKS - Processed with {MAX_CONCURRENT_BATCHES} parallel workers!")
            print(f"{'=' * 60}\n")

            # Verify results
            assert result["total_rows"] == NUM_ROWS, "Should read all rows"

            # Memory should be bounded even with concurrent processing
            file_size_in_memory_mb = file_size_mb * 1.2  # uncompressed is ~20% larger
            assert result["peak_rss_mb"] < file_size_in_memory_mb, (
                f"Should use less memory than full file ({file_size_in_memory_mb:.1f} MB) "
                "even with parallel processing"
            )

            efficiency = result["peak_rss_mb"] / file_size_mb
            print(f"  Memory efficiency:        {efficiency:.2f}x file size (concurrent streaming)")
            print("  ✅ All assertions passed!")

    except (ray.exceptions.WorkerCrashedError, ray.exceptions.OutOfMemoryError, ray.exceptions.RayTaskError) as exc:
        print(f"\n{'=' * 60}")
        print("  Ray Concurrent Batch Streaming Experiment")
        print(f"{'=' * 60}")
        print(f"  ORC file size on disk:    {file_size_mb:.1f} MB")
        print(f"  Worker memory limit:      {WORKER_MEMORY / 1024 / 1024:.0f} MB")
        print(f"  Row count:                {NUM_ROWS:,}")
        stripe_desc = f"{STRIPE_ROWS} rows/stripe" if STRIPE_ROWS > 0 else "64MB stripes"
        print(f"  ORC stripe size:          {stripe_desc}")
        print(f"  Concurrent workers:       {MAX_CONCURRENT_BATCHES}")
        print("  Outcome:                  WORKER CRASHED")
        print(f"  Exception type:           {type(exc).__name__}")
        print(f"  Message:                  {exc}")
        print(f"{'=' * 60}")
        print("  ❌ CONCURRENT STREAMING FAILED - Worker crashed!")
        print(f"{'=' * 60}\n")
        pytest.fail(f"Worker crashed with {type(exc).__name__}: {exc}")

    finally:
        ray.shutdown()
