"""Quick smoke test for the new streaming API.

This test verifies the new API works correctly with a small file
before running the expensive OOM test.
"""

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.orc as orc
import pytest


def test_streaming_api_basic():
    """Test that the new streaming API works with a small file."""
    from pyiceberg.expressions import AlwaysTrue
    from pyiceberg.io.pyarrow import ArrowScan, PyArrowFileIO
    from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.table import FileScanTask
    from pyiceberg.table.metadata import new_table_metadata
    from pyiceberg.table.sorting import SortOrder
    from pyiceberg.types import IntegerType, LongType, NestedField, StringType

    # Create a small test file
    tmp_dir = Path("/tmp/streaming_quick_test")
    tmp_dir.mkdir(exist_ok=True)
    orc_path = tmp_dir / "test.orc"

    # Generate 1000 rows
    rng = np.random.default_rng(42)
    data = pa.table(
        {
            "id": pa.array(np.arange(1000, dtype=np.int64), type=pa.int64()),
            "value_a": pa.array(rng.integers(0, 100, size=1000, dtype=np.int32), type=pa.int32()),
            "value_b": pa.array(rng.integers(0, 100, size=1000, dtype=np.int32), type=pa.int32()),
            "label": pa.array([f"label_{i}" for i in range(1000)], type=pa.string()),
        }
    )

    # Write ORC with small stripes
    writer = orc.ORCWriter(str(orc_path), compression="uncompressed", stripe_size=100 * 40)
    writer.write(data)
    writer.close()

    # Set up Iceberg metadata
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
        location=str(tmp_dir),
        properties={"schema.name-mapping.default": name_mapping},
    )

    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=str(orc_path),
        file_format=FileFormat.ORC,
        partition={},
        record_count=1000,
        file_size_in_bytes=orc_path.stat().st_size,
    )
    data_file._spec_id = 0
    task = FileScanTask(data_file=data_file)

    arrow_scan = ArrowScan(
        table_metadata=table_metadata,
        io=PyArrowFileIO(),
        projected_schema=schema,
        row_filter=AlwaysTrue(),
    )

    # Test OLD API (should work)
    print("\n[TEST] Testing OLD API: to_record_batches([task])")
    old_batches = list(arrow_scan.to_record_batches([task]))
    old_total = sum(len(b) for b in old_batches)
    print(f"  → Read {old_total} rows in {len(old_batches)} batches")
    assert old_total == 1000, f"Expected 1000 rows, got {old_total}"

    # Test NEW STREAMING API
    print("\n[TEST] Testing NEW API: to_record_batches_streaming([task], batch_size=50)")
    streaming_batches = list(arrow_scan.to_record_batches_streaming([task], batch_size=50))
    streaming_total = sum(len(b) for b in streaming_batches)
    print(f"  → Read {streaming_total} rows in {len(streaming_batches)} batches")
    print(f"  → Batch sizes: {[len(b) for b in streaming_batches[:5]]}...")

    # Verify results
    assert streaming_total == 1000, f"Expected 1000 rows, got {streaming_total}"
    assert all(len(b) <= 50 for b in streaming_batches), "All batches should be <= 50 rows"
    assert len(streaming_batches) > len(old_batches), "Streaming should produce more smaller batches"

    print("\n✅ All tests passed! Streaming API works correctly.")


if __name__ == "__main__":
    test_streaming_api_basic()
