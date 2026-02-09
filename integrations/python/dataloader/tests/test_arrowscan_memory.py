"""Experiment: Does ArrowScan.to_record_batches() materialize entire files or stream lazily?

Generates test data, writes Parquet/ORC files, and compares peak memory between
streaming iteration (delete-as-you-go) and full materialization (list()).
"""

import os
from pathlib import Path
from typing import NamedTuple

import numpy as np
import pyarrow as pa
import pyarrow.orc as orc
import pyarrow.parquet as pq
import pytest
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.io.pyarrow import ArrowScan, PyArrowFileIO
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import FileScanTask
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import SortOrder
from pyiceberg.types import IntegerType, LongType, NestedField, StringType

NUM_ROWS = 200_000
ROW_GROUP_SIZE = 10_000  # Parquet row-group / ORC stripe target
SCALING_SIZES = [500_000, 1_000_000, 2_000_000, 10_000_000]


class _SizeResult(NamedTuple):
    num_rows: int
    file_size_mb: float
    streaming_peak_mb: float
    materialized_mb: float
    ratio: float
    streaming_bytes_per_row: float
    materialized_bytes_per_row: float


ICEBERG_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=True),
    NestedField(field_id=2, name="value_a", field_type=IntegerType(), required=False),
    NestedField(field_id=3, name="value_b", field_type=IntegerType(), required=False),
    NestedField(field_id=4, name="label", field_type=StringType(), required=False),
)


def _make_table(num_rows: int = NUM_ROWS) -> pa.Table:
    """Generate an N-row, 4-column PyArrow table using numpy for speed."""
    rng = np.random.default_rng(42)
    ids = np.arange(num_rows, dtype=np.int64)
    value_a = rng.integers(0, 1_000_000, size=num_rows, dtype=np.int32)
    value_b = rng.integers(0, 1_000_000, size=num_rows, dtype=np.int32)

    # Generate a pool of unique 20-char strings, then tile to fill num_rows.
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


def _write_file(tmp_path: Path, table: pa.Table, file_format: str) -> tuple[str, dict[str, str]]:
    """Write a Parquet or ORC file with small row groups/stripes.

    Returns (file_path_str, properties_dict) suitable for ArrowScan.
    """
    if file_format == "parquet":
        file_path = tmp_path / "data.parquet"
        # Add Iceberg field-id metadata to each field so ArrowScan can resolve columns.
        fields = []
        for i, field in enumerate(table.schema):
            metadata = {b"PARQUET:field_id": str(i + 1).encode()}
            fields.append(field.with_metadata(metadata))
        schema_with_ids = pa.schema(fields)
        table = table.cast(schema_with_ids)
        pq.write_table(table, str(file_path), row_group_size=ROW_GROUP_SIZE)
        return str(file_path), {}

    # ORC — use a name-mapping property so ArrowScan can resolve columns by name.
    file_path = tmp_path / "data.orc"
    orc.write_table(table, str(file_path), stripe_size=ROW_GROUP_SIZE * 50)  # bytes ≈ small stripes
    name_mapping = (
        '[{"field-id": 1, "names": ["id"]},'
        ' {"field-id": 2, "names": ["value_a"]},'
        ' {"field-id": 3, "names": ["value_b"]},'
        ' {"field-id": 4, "names": ["label"]}]'
    )
    return str(file_path), {"schema.name-mapping.default": name_mapping}


def _make_arrowscan_and_task(
    file_path: str, file_format: str, properties: dict[str, str], record_count: int = NUM_ROWS
) -> tuple[ArrowScan, FileScanTask]:
    """Construct an ArrowScan and FileScanTask for a local data file."""
    fmt = FileFormat.PARQUET if file_format == "parquet" else FileFormat.ORC

    table_metadata = new_table_metadata(
        schema=ICEBERG_SCHEMA,
        partition_spec=PartitionSpec(),
        sort_order=SortOrder(order_id=0),
        location=str(Path(file_path).parent),
        properties=properties,
    )

    file_size = os.path.getsize(file_path)
    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=file_path,
        file_format=fmt,
        partition={},
        record_count=record_count,
        file_size_in_bytes=file_size,
    )
    # spec_id is a property backed by _spec_id, which from_args does not populate.
    # ArrowScan reads task.file.spec_id to look up the partition spec.
    data_file._spec_id = 0
    task = FileScanTask(data_file=data_file)

    arrow_scan = ArrowScan(
        table_metadata=table_metadata,
        io=PyArrowFileIO(),
        projected_schema=ICEBERG_SCHEMA,
        row_filter=AlwaysTrue(),
    )
    return arrow_scan, task


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_arrowscan_memory_behavior(tmp_path: Path, file_format: str) -> None:
    """Compare memory usage: streaming vs materializing all batches at once."""
    table = _make_table()
    file_path, properties = _write_file(tmp_path, table, file_format)

    # --- Streaming run: iterate and delete each batch ---
    arrow_scan, task = _make_arrowscan_and_task(file_path, file_format, properties)
    pa.total_allocated_bytes()  # warm up allocator
    baseline = pa.total_allocated_bytes()
    streaming_peak = 0
    batch_count = 0
    total_rows = 0

    for batch in arrow_scan.to_record_batches([task]):
        current = pa.total_allocated_bytes() - baseline
        streaming_peak = max(streaming_peak, current)
        total_rows += batch.num_rows
        batch_count += 1
        del batch

    streaming_after = pa.total_allocated_bytes() - baseline

    # --- Materialized run: load everything into a list ---
    arrow_scan2, task2 = _make_arrowscan_and_task(file_path, file_format, properties)
    baseline2 = pa.total_allocated_bytes()
    all_batches = list(arrow_scan2.to_record_batches([task2]))
    materialized_total = pa.total_allocated_bytes() - baseline2
    materialized_rows = sum(b.num_rows for b in all_batches)

    # --- Report ---
    print(f"\n{'=' * 60}")
    print(f"  ArrowScan Memory Behavior — {file_format.upper()}")
    print(f"{'=' * 60}")
    print(f"  File size on disk:        {os.path.getsize(file_path) / 1024 / 1024:.2f} MB")
    print(f"  Total rows:               {total_rows:,}")
    print(f"  Batch count (streaming):  {batch_count}")
    print(f"  Streaming peak allocated: {streaming_peak / 1024 / 1024:.2f} MB")
    print(f"  Streaming residual:       {streaming_after / 1024 / 1024:.2f} MB")
    print(f"  Materialized allocated:   {materialized_total / 1024 / 1024:.2f} MB")
    print(f"  Materialized rows:        {materialized_rows:,}")
    if materialized_total > 0:
        ratio = streaming_peak / materialized_total
        print(f"  Peak/Materialized ratio:  {ratio:.2f}x")
        if ratio < 0.5:
            print("  → Conclusion: STREAMING (lazy batches, much less memory)")
        elif ratio < 0.9:
            print("  → Conclusion: PARTIAL STREAMING (some savings)")
        else:
            print("  → Conclusion: MATERIALIZING (full file loaded)")
    print(f"{'=' * 60}\n")

    # Sanity: we got all the rows back
    assert total_rows == NUM_ROWS
    assert materialized_rows == NUM_ROWS


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_arrowscan_memory_scaling(tmp_path: Path, file_format: str) -> None:
    """Verify that ArrowScan memory usage scales linearly with file size (constant bytes/row)."""
    results: list[_SizeResult] = []

    for num_rows in SCALING_SIZES:
        size_dir = tmp_path / f"size_{num_rows}"
        size_dir.mkdir()

        table = _make_table(num_rows=num_rows)
        file_path, properties = _write_file(size_dir, table, file_format)
        file_size_mb = os.path.getsize(file_path) / 1024 / 1024
        del table

        # --- Streaming run: iterate and delete each batch ---
        arrow_scan, task = _make_arrowscan_and_task(file_path, file_format, properties, record_count=num_rows)
        pa.total_allocated_bytes()  # warm up allocator
        baseline = pa.total_allocated_bytes()
        streaming_peak = 0

        for batch in arrow_scan.to_record_batches([task]):
            current = pa.total_allocated_bytes() - baseline
            streaming_peak = max(streaming_peak, current)
            del batch

        # --- Materialized run: load everything into a list ---
        arrow_scan2, task2 = _make_arrowscan_and_task(file_path, file_format, properties, record_count=num_rows)
        baseline2 = pa.total_allocated_bytes()
        all_batches = list(arrow_scan2.to_record_batches([task2]))
        materialized_total = pa.total_allocated_bytes() - baseline2

        streaming_peak_mb = streaming_peak / 1024 / 1024
        materialized_mb = materialized_total / 1024 / 1024
        ratio = streaming_peak / materialized_total if materialized_total > 0 else 0.0
        streaming_bpr = streaming_peak / num_rows if num_rows > 0 else 0.0
        materialized_bpr = materialized_total / num_rows if num_rows > 0 else 0.0

        results.append(
            _SizeResult(
                num_rows=num_rows,
                file_size_mb=file_size_mb,
                streaming_peak_mb=streaming_peak_mb,
                materialized_mb=materialized_mb,
                ratio=ratio,
                streaming_bytes_per_row=streaming_bpr,
                materialized_bytes_per_row=materialized_bpr,
            )
        )

        del all_batches

    # --- Summary table ---
    print(f"\n{'=' * 100}")
    print(f"  ArrowScan Memory Scaling — {file_format.upper()}")
    print(f"{'=' * 100}")
    print(
        f"  {'Rows':>12s}  {'File MB':>9s}  {'Stream Peak MB':>15s}  {'Mater. MB':>10s}"
        f"  {'Ratio':>6s}  {'Stream B/row':>13s}  {'Mater. B/row':>13s}"
    )
    for r in results:
        print(
            f"  {r.num_rows:>12,d}  {r.file_size_mb:>9.2f}  {r.streaming_peak_mb:>15.2f}  {r.materialized_mb:>10.2f}"
            f"  {r.ratio:>6.2f}  {r.streaming_bytes_per_row:>13.1f}  {r.materialized_bytes_per_row:>13.1f}"
        )

    # Assert constant bytes-per-row across sizes (within 30% tolerance).
    bpr_values = [r.materialized_bytes_per_row for r in results]
    bpr_min, bpr_max = min(bpr_values), max(bpr_values)
    bpr_mean = sum(bpr_values) / len(bpr_values)
    spread = (bpr_max - bpr_min) / bpr_mean if bpr_mean > 0 else 0.0

    if spread < 0.30:
        print(f"\n  → Conclusion: CONSTANT bytes/row ({bpr_mean:.1f} avg, {spread:.1%} spread) = LINEAR scaling")
        print("    ArrowScan materializes entire files. Memory management needed at a higher level.")
    else:
        print(f"\n  → Conclusion: bytes/row varies ({bpr_min:.1f}–{bpr_max:.1f}, {spread:.1%} spread)")
    print(f"{'=' * 100}\n")

    assert spread < 0.30, (
        f"Materialized bytes/row should be roughly constant across sizes, "
        f"but spread was {spread:.1%} (min={bpr_min:.1f}, max={bpr_max:.1f})"
    )
