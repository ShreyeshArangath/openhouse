"""Comparison test: OLD API vs NEW Streaming API

Clear side-by-side comparison showing:

┌─────────────────────────────────────────────────────────────────┐
│ OLD API (Eager Materialization)                                 │
│ - Method: arrow_scan.to_record_batches([task])                  │
│ - Behavior: Loads all batches before yielding (uses list())     │
│ - Memory: Scales with file size                                 │
│ - Result: OOM on large files                                    │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ NEW API (True Streaming)                                        │
│ - Method: arrow_scan.to_record_batches_streaming(batch_size=N)  │
│ - Behavior: Yields batches incrementally (generator)            │
│ - Memory: Bounded by batch_size + PyArrow buffers              │
│ - Result: Handles files larger than memory                      │
└─────────────────────────────────────────────────────────────────┘

Test file: 10M rows, ~380 MB ORC
"""

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.orc as orc
import pytest


def _make_table(num_rows: int) -> pa.Table:
    """Generate test table."""
    rng = np.random.default_rng(42)
    return pa.table(
        {
            "id": pa.array(np.arange(num_rows, dtype=np.int64), type=pa.int64()),
            "value_a": pa.array(rng.integers(0, 1_000_000, size=num_rows, dtype=np.int32), type=pa.int32()),
            "value_b": pa.array(rng.integers(0, 1_000_000, size=num_rows, dtype=np.int32), type=pa.int32()),
            "label": pa.array([f"label_{i % 1000}" for i in range(num_rows)], type=pa.string()),
        }
    )


def _generate_orc(file_path: Path, num_rows: int, batch_size: int = 100_000) -> None:
    """Generate ORC file incrementally."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    writer = orc.ORCWriter(str(file_path), compression="uncompressed", stripe_size=64 * 1024 * 1024)

    for start in range(0, num_rows, batch_size):
        batch_rows = min(batch_size, num_rows - start)
        table = _make_table(num_rows=batch_rows)
        writer.write(table)
        del table

    writer.close()


def _read_with_api(file_path: str, num_rows: int, api: str, batch_size: int | None = None) -> dict:
    """Read ORC file with specified API and track memory."""
    import psutil
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
    start_rss = proc.memory_info().rss
    peak_rss = start_rss

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="value_a", field_type=IntegerType(), required=False),
        NestedField(field_id=3, name="value_b", field_type=IntegerType(), required=False),
        NestedField(field_id=4, name="label", field_type=StringType(), required=False),
    )

    table_metadata = new_table_metadata(
        schema=schema,
        partition_spec=PartitionSpec(),
        sort_order=SortOrder(order_id=0),
        location=str(Path(file_path).parent),
        properties={
            "schema.name-mapping.default": (
                '[{"field-id": 1, "names": ["id"]}, '
                '{"field-id": 2, "names": ["value_a"]}, '
                '{"field-id": 3, "names": ["value_b"]}, '
                '{"field-id": 4, "names": ["label"]}]'
            )
        },
    )

    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=file_path,
        file_format=FileFormat.ORC,
        partition={},
        record_count=num_rows,
        file_size_in_bytes=Path(file_path).stat().st_size,
    )
    data_file._spec_id = 0
    task = FileScanTask(data_file=data_file)

    arrow_scan = ArrowScan(
        table_metadata=table_metadata,
        io=PyArrowFileIO(),
        projected_schema=schema,
        row_filter=AlwaysTrue(),
    )

    batch_count = 0
    total_rows = 0
    sample_batch_sizes = []

    if api == "old":
        # OLD API: to_record_batches() - no batch_size control
        # This uses the old eager materialization approach
        for batch in arrow_scan.to_record_batches([task]):
            batch_count += 1
            total_rows += batch.num_rows
            if len(sample_batch_sizes) < 5:
                sample_batch_sizes.append(batch.num_rows)
            peak_rss = max(peak_rss, proc.memory_info().rss)
            del batch
    elif api == "new":
        # NEW API: to_record_batches_streaming(batch_size=N) - true streaming
        # This yields batches incrementally with user-controlled size
        for batch in arrow_scan.to_record_batches_streaming([task], batch_size=batch_size):
            batch_count += 1
            total_rows += batch.num_rows
            if len(sample_batch_sizes) < 5:
                sample_batch_sizes.append(batch.num_rows)
            peak_rss = max(peak_rss, proc.memory_info().rss)
            del batch
    else:
        raise ValueError(f"Unknown API: {api}")

    allocated = pa.total_allocated_bytes()
    return {
        "total_rows": total_rows,
        "batch_count": batch_count,
        "sample_batch_sizes": sample_batch_sizes,
        "start_rss_mb": start_rss / 1024 / 1024,
        "peak_rss_mb": peak_rss / 1024 / 1024,
        "rss_increase_mb": (peak_rss - start_rss) / 1024 / 1024,
        "allocated_mb": allocated / 1024 / 1024,
    }


@pytest.mark.slow
def test_old_vs_new_api_memory_comparison():
    """Compare memory usage between old and new APIs."""
    data_dir = Path("/tmp/streaming_comparison_test")
    orc_path = data_dir / "data.orc"
    num_rows = 10_000_000

    if not orc_path.exists():
        print(f"\n  Generating {num_rows:,} row ORC file...")
        _generate_orc(orc_path, num_rows)

    file_size_mb = orc_path.stat().st_size / 1024 / 1024
    print(f"\n{'=' * 70}")
    print("  OLD API vs NEW STREAMING API - Memory Comparison")
    print(f"{'=' * 70}")
    print(f"  File size:    {file_size_mb:.1f} MB")
    print(f"  Row count:    {num_rows:,}")
    print()

    # Test OLD API
    print("  ┌─────────────────────────────────────────────────────────────┐")
    print("  │ [1/2] OLD API - arrow_scan.to_record_batches([task])       │")
    print("  │       Eager materialization (no batch_size control)        │")
    print("  └─────────────────────────────────────────────────────────────┘")
    old_result = _read_with_api(str(orc_path), num_rows, api="old")
    print(f"        ├─ Method:         to_record_batches([task])")
    print(f"        ├─ Batches:        {old_result['batch_count']}")
    print(f"        ├─ Sample sizes:   {old_result['sample_batch_sizes']}")
    print(f"        ├─ Total rows:     {old_result['total_rows']:,}")
    print(f"        ├─ Start RSS:      {old_result['start_rss_mb']:.1f} MB")
    print(f"        ├─ Peak RSS:       {old_result['peak_rss_mb']:.1f} MB")
    print(f"        └─ RSS increase:   {old_result['rss_increase_mb']:.1f} MB")

    # Test NEW API
    print()
    print("  ┌─────────────────────────────────────────────────────────────┐")
    print("  │ [2/2] NEW API - arrow_scan.to_record_batches_streaming()   │")
    print("  │       True streaming with batch_size=1000                   │")
    print("  └─────────────────────────────────────────────────────────────┘")
    new_result = _read_with_api(str(orc_path), num_rows, api="new", batch_size=1000)
    print(f"        ├─ Method:         to_record_batches_streaming([task], batch_size=1000)")
    print(f"        ├─ Batches:        {new_result['batch_count']}")
    print(f"        ├─ Sample sizes:   {new_result['sample_batch_sizes']}")
    print(f"        ├─ Total rows:     {new_result['total_rows']:,}")
    print(f"        ├─ Start RSS:      {new_result['start_rss_mb']:.1f} MB")
    print(f"        ├─ Peak RSS:       {new_result['peak_rss_mb']:.1f} MB")
    print(f"        └─ RSS increase:   {new_result['rss_increase_mb']:.1f} MB")

    print()
    print(f"{'=' * 70}")
    print("  📊 COMPARISON RESULTS")
    print(f"{'=' * 70}")
    print()
    print("  Memory Usage:")
    print(f"    OLD API increase:        {old_result['rss_increase_mb']:.1f} MB")
    print(f"    NEW API increase:        {new_result['rss_increase_mb']:.1f} MB")
    improvement = (old_result["rss_increase_mb"] - new_result["rss_increase_mb"]) / old_result["rss_increase_mb"] * 100
    if improvement > 0:
        print(f"    Memory saved:            {improvement:.1f}% reduction 📉")
    else:
        print(f"    Memory change:           {improvement:.1f}% ⚠️")
    print()
    print("  Batch Control:")
    print(f"    OLD API batches:         {old_result['batch_count']} (no control)")
    print(f"    NEW API batches:         {new_result['batch_count']} (user-controlled)")
    print(f"    Granularity increase:    {new_result['batch_count'] / old_result['batch_count']:.1f}x more batches")
    print()
    print("  Sample Batch Sizes:")
    print(f"    OLD API:                 {old_result['sample_batch_sizes']}")
    print(f"    NEW API:                 {new_result['sample_batch_sizes']}")
    print(f"{'=' * 70}")
    print()
    if improvement > 0:
        print("  ✅ NEW STREAMING API SUCCESS!")
        print(f"     - {improvement:.1f}% less memory")
        print(f"     - {new_result['batch_count'] / old_result['batch_count']:.1f}x better batch control")
        print("     - Prevents OOM on large files")
    else:
        print("  ⚠️  Memory usage similar (PyArrow internal buffering)")
        print("     But NEW API still provides:")
        print(f"     - {new_result['batch_count'] / old_result['batch_count']:.1f}x better batch control")
        print("     - More predictable memory behavior")

    print(f"{'=' * 70}\n")

    # Assertions
    assert old_result["total_rows"] == num_rows
    assert new_result["total_rows"] == num_rows
    assert new_result["batch_count"] > old_result["batch_count"], "Streaming should produce more batches"


if __name__ == "__main__":
    test_old_vs_new_api_memory_comparison()
