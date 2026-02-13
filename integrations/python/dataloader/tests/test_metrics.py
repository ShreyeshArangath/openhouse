"""Tests for OpenTelemetry metrics instrumentation."""

import os
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from datafusion.context import SessionContext
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from pyiceberg.io import load_file_io
from pyiceberg.manifest import DataFile, FileFormat
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table import FileScanTask
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.types import LongType, NestedField, StringType

from openhouse.dataloader.data_loader_split import DataLoaderSplit, TableScanContext
from openhouse.dataloader.metrics import DataLoaderMetrics, get_metrics


def _make_split(tmp_path, table_name="test_db.test_table"):
    """Create a DataLoaderSplit backed by a small Parquet file."""
    iceberg_schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    )

    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["a", "b", "c"], type=pa.string()),
        }
    )

    file_path = str(tmp_path / "test.parquet")
    fields = [field.with_metadata({b"PARQUET:field_id": str(i + 1).encode()}) for i, field in enumerate(table.schema)]
    pq.write_table(table.cast(pa.schema(fields)), file_path)

    metadata = new_table_metadata(
        schema=iceberg_schema,
        partition_spec=UNPARTITIONED_PARTITION_SPEC,
        sort_order=UNSORTED_SORT_ORDER,
        location=str(tmp_path),
        properties={},
    )

    scan_context = TableScanContext(
        table_metadata=metadata,
        io=load_file_io(properties={}, location=file_path),
        projected_schema=iceberg_schema,
        table_name=table_name,
    )

    ctx = SessionContext()
    plan = ctx.sql("SELECT 1 as a").logical_plan()

    data_file = DataFile.from_args(
        file_path=file_path,
        file_format=FileFormat.PARQUET,
        record_count=table.num_rows,
        file_size_in_bytes=os.path.getsize(file_path),
    )
    data_file._spec_id = 0
    task = FileScanTask(data_file=data_file)

    return DataLoaderSplit(plan=plan, file_scan_task=task, scan_context=scan_context)


@pytest.fixture()
def metric_reader():
    """Provide an InMemoryMetricReader backed by a fresh MeterProvider.

    Patches ``get_metrics`` so DataLoaderSplit uses this provider instead of
    the global default.  No internal OTEL state is mutated.
    """
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    metrics = DataLoaderMetrics(meter_provider=provider)
    with patch("openhouse.dataloader.data_loader_split.get_metrics", return_value=metrics):
        yield reader
    provider.shutdown()


def _get_metric(reader, name):
    """Find a metric by name from the reader's collected data."""
    data = reader.get_metrics_data()
    for resource_metric in data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                if metric.name == name:
                    return metric
    return None


def _get_metric_value(reader, name):
    """Get the aggregated value of a sum or histogram metric."""
    metric = _get_metric(reader, name)
    if metric is None:
        return None
    for dp in metric.data.data_points:
        if hasattr(dp, "sum"):
            return dp.sum
        if hasattr(dp, "value"):
            return dp.value
    return None


def test_split_iteration_emits_metrics(tmp_path, metric_reader):
    """Iterating a split should emit all expected counter and histogram metrics."""
    split = _make_split(tmp_path)
    batches = list(split)

    total_rows = sum(b.num_rows for b in batches)
    assert total_rows == 3

    assert _get_metric_value(metric_reader, "split_iter_count") == 1
    assert _get_metric_value(metric_reader, "batch_count") >= 1
    assert _get_metric_value(metric_reader, "row_count") == 3
    assert _get_metric_value(metric_reader, "bytes_read") > 0

    duration_metric = _get_metric(metric_reader, "split_iter_duration")
    assert duration_metric is not None
    dp = duration_metric.data.data_points[0]
    assert dp.sum >= 0


def test_error_emits_error_count(tmp_path, metric_reader):
    """When ArrowScan raises, error_count should be incremented."""
    split = _make_split(tmp_path)

    with patch("openhouse.dataloader.data_loader_split.ArrowScan") as mock_cls:
        mock_cls.return_value.to_record_batches.side_effect = RuntimeError("test error")
        with pytest.raises(RuntimeError, match="test error"):
            list(split)

    assert _get_metric_value(metric_reader, "error_count") == 1
    assert _get_metric_value(metric_reader, "split_iter_count") == 1


def test_no_sdk_is_noop(tmp_path):
    """Without an SDK configured, iteration should work without error (no-op metrics)."""
    split = _make_split(tmp_path)
    batches = list(split)
    assert sum(b.num_rows for b in batches) == 3


def test_attributes_include_table_name(tmp_path, metric_reader):
    """All metrics should carry the 'table' attribute with the fully-qualified table name."""
    table_name = "my_db.my_table"
    split = _make_split(tmp_path, table_name=table_name)
    list(split)

    for metric_name in ["split_iter_count", "batch_count", "row_count", "bytes_read"]:
        metric = _get_metric(metric_reader, metric_name)
        assert metric is not None, f"Missing metric: {metric_name}"
        for dp in metric.data.data_points:
            attrs = dict(dp.attributes)
            assert attrs.get("table") == table_name, f"Wrong table attr on {metric_name}: {attrs}"


def test_get_metrics_returns_default_without_provider():
    """get_metrics() with no args returns the same default instance."""
    m1 = get_metrics()
    m2 = get_metrics()
    assert m1 is m2


def test_get_metrics_returns_new_instance_with_provider():
    """get_metrics(meter_provider=...) returns a fresh DataLoaderMetrics."""
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    m = get_metrics(meter_provider=provider)
    assert isinstance(m, DataLoaderMetrics)
    assert m is not get_metrics()
    provider.shutdown()
