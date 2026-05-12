"""Tests for the OpenTelemetry metrics emitted by the dataloader."""

from __future__ import annotations

import os
import pickle
from collections.abc import Iterator

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from opentelemetry import metrics as otel_metrics
from opentelemetry.metrics import Meter, get_meter
from opentelemetry.metrics import _internal as otel_metrics_internal
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from pyiceberg.io import load_file_io
from pyiceberg.manifest import DataFile, FileFormat
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table import FileScanTask
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.types import LongType, NestedField

from openhouse.dataloader._table_scan_context import TableScanContext
from openhouse.dataloader.data_loader import _retry
from openhouse.dataloader.data_loader_split import DataLoaderSplit
from openhouse.dataloader.metrics import METER_NAME, build_attributes, instruments
from openhouse.dataloader.table_identifier import TableIdentifier

# --- Meter / METER_NAME basics ---


def test_meter_name_is_stable():
    assert METER_NAME == "openhouse.dataloader"


def test_get_meter_with_meter_name_returns_a_meter():
    assert isinstance(get_meter(METER_NAME), Meter)


# --- build_attributes ---


def test_build_attributes_includes_database_table():
    table_id = TableIdentifier("db1", "tbl1")
    attrs = build_attributes(table_id, None)
    assert attrs == {"openhouse.database": "db1", "openhouse.table": "tbl1"}


def test_build_attributes_merges_caller_provided_attributes_verbatim():
    table_id = TableIdentifier("db1", "tbl1")
    attrs = build_attributes(table_id, {"tenant": "team-a", "env": "prod"})
    assert attrs["tenant"] == "team-a"
    assert attrs["env"] == "prod"


def test_build_attributes_caller_keys_override_builtins():
    table_id = TableIdentifier("db1", "tbl1")
    attrs = build_attributes(table_id, {"openhouse.table": "override"})
    assert attrs["openhouse.table"] == "override"


# --- InMemoryMetricReader harness ---


@pytest.fixture
def metrics_reader() -> Iterator[InMemoryMetricReader]:
    """Install an SDK MeterProvider with an InMemoryMetricReader for the test.

    Resets the one-shot ``_METER_PROVIDER_SET_ONCE`` guard and restores the
    prior MeterProvider on exit so other tests are not affected.
    """
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    once = otel_metrics_internal._METER_PROVIDER_SET_ONCE
    prior_provider = otel_metrics_internal._METER_PROVIDER
    prior_done = once._done
    once._done = False
    otel_metrics.set_meter_provider(provider)
    try:
        yield reader
    finally:
        otel_metrics_internal._METER_PROVIDER = prior_provider
        once._done = prior_done


def _data_points(reader: InMemoryMetricReader, metric_name: str) -> list:
    """Collect and return all data points for *metric_name* across scopes."""
    data = reader.get_metrics_data()
    points: list = []
    if data is None:
        return points
    for resource_metric in data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                if metric.name == metric_name:
                    points.extend(metric.data.data_points)
    return points


def _attrs(point) -> dict:
    return dict(point.attributes)


# --- _retry attempts / duration ---


def test_retry_emits_one_attempt_and_one_duration_on_success(metrics_reader):
    table_id = TableIdentifier("db", "tbl")
    attrs = build_attributes(table_id, None)
    result = _retry(
        lambda: "ok",
        label="load_table db.tbl",
        max_attempts=3,
        duration_histogram=instruments.load_table_duration,
        attempts_counter=instruments.load_table_attempts,
        attributes=attrs,
    )
    assert result == "ok"

    attempts = _data_points(metrics_reader, "openhouse.dataloader.load_table.attempts")
    assert len(attempts) == 1
    assert _attrs(attempts[0]) == attrs
    assert attempts[0].value == 1

    durations = _data_points(metrics_reader, "openhouse.dataloader.load_table.duration")
    assert len(durations) == 1
    assert _attrs(durations[0]) == attrs


def test_retry_counts_each_attempt_on_transient_then_success(metrics_reader):
    table_id = TableIdentifier("db", "tbl")
    attrs = build_attributes(table_id, {"tenant": "t1"})
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("transient")
        return "ok"

    result = _retry(
        fn,
        label="plan_files db.tbl",
        max_attempts=3,
        duration_histogram=instruments.plan_files_duration,
        attempts_counter=instruments.plan_files_attempts,
        attributes=attrs,
    )
    assert result == "ok"
    assert calls["n"] == 2

    attempts = _data_points(metrics_reader, "openhouse.dataloader.plan_files.attempts")
    assert len(attempts) == 1
    assert attempts[0].value == 2
    assert _attrs(attempts[0])["tenant"] == "t1"

    durations = _data_points(metrics_reader, "openhouse.dataloader.plan_files.duration")
    assert len(durations) == 1


def test_retry_permanent_failure_still_records_duration(metrics_reader):
    table_id = TableIdentifier("db", "tbl")
    attrs = build_attributes(table_id, None)

    class _NonTransient(Exception):
        pass

    def fn():
        raise _NonTransient("nope")

    with pytest.raises(_NonTransient):
        _retry(
            fn,
            label="load_table",
            max_attempts=3,
            duration_histogram=instruments.load_table_duration,
            attempts_counter=instruments.load_table_attempts,
            attributes=attrs,
        )

    attempts = _data_points(metrics_reader, "openhouse.dataloader.load_table.attempts")
    assert len(attempts) == 1
    assert attempts[0].value == 1

    durations = _data_points(metrics_reader, "openhouse.dataloader.load_table.duration")
    assert len(durations) == 1


# --- DataLoaderSplit instrumentation ---

_SPLIT_SCHEMA = Schema(NestedField(field_id=1, name="id", field_type=LongType(), required=False))
_SPLIT_TABLE_ID = TableIdentifier("db", "tbl")


def _make_split(tmp_path, metric_attributes: dict | None = None) -> DataLoaderSplit:
    file_path = str(tmp_path / "data.parquet")
    table = pa.table({"id": pa.array([1, 2, 3], type=pa.int64())})
    fields = [field.with_metadata({b"PARQUET:field_id": str(i + 1).encode()}) for i, field in enumerate(table.schema)]
    pq.write_table(table.cast(pa.schema(fields)), file_path)

    metadata = new_table_metadata(
        schema=_SPLIT_SCHEMA,
        partition_spec=UNPARTITIONED_PARTITION_SPEC,
        sort_order=UNSORTED_SORT_ORDER,
        location=str(tmp_path),
    )
    scan_context = TableScanContext(
        table_metadata=metadata,
        io=load_file_io(properties={}, location=file_path),
        projected_schema=_SPLIT_SCHEMA,
        table_id=_SPLIT_TABLE_ID,
        metric_attributes=metric_attributes or {},
    )
    data_file = DataFile.from_args(
        file_path=file_path,
        file_format=FileFormat.PARQUET,
        record_count=table.num_rows,
        file_size_in_bytes=os.path.getsize(file_path),
    )
    data_file._spec_id = 0
    task = FileScanTask(data_file=data_file)
    return DataLoaderSplit(file_scan_tasks=[task], scan_context=scan_context)


def test_split_emits_per_split_and_per_batch_metrics(tmp_path, metrics_reader):
    split = _make_split(tmp_path, metric_attributes={"tenant": "t1"})
    batches = list(split)
    assert sum(b.num_rows for b in batches) == 3

    expected_attrs = {
        "openhouse.database": "db",
        "openhouse.table": "tbl",
        "tenant": "t1",
    }

    split_duration = _data_points(metrics_reader, "openhouse.dataloader.split.duration")
    assert len(split_duration) == 1
    assert _attrs(split_duration[0]) == expected_attrs

    split_files = _data_points(metrics_reader, "openhouse.dataloader.split.files")
    assert len(split_files) == 1
    assert split_files[0].sum == 1

    split_rows = _data_points(metrics_reader, "openhouse.dataloader.split.rows")
    assert len(split_rows) == 1
    assert split_rows[0].sum == 3

    split_bytes = _data_points(metrics_reader, "openhouse.dataloader.split.bytes")
    assert len(split_bytes) == 1
    assert split_bytes[0].sum > 0

    split_batches = _data_points(metrics_reader, "openhouse.dataloader.split.batches")
    assert len(split_batches) == 1
    assert split_batches[0].sum >= 1

    batch_duration = _data_points(metrics_reader, "openhouse.dataloader.batch.duration")
    assert len(batch_duration) == 1
    assert _attrs(batch_duration[0]) == expected_attrs

    batch_rows = _data_points(metrics_reader, "openhouse.dataloader.batch.rows")
    assert len(batch_rows) == 1
    assert batch_rows[0].sum == 3


def test_batch_read_failure_bumps_error_counters(tmp_path, monkeypatch, metrics_reader):
    split = _make_split(tmp_path)

    class _ReaderError(Exception):
        pass

    def _fake_to_record_batches(self, scan_tasks, **kwargs):
        def _gen():
            raise _ReaderError("boom")
            yield  # pragma: no cover  -- makes this a generator

        return _gen()

    monkeypatch.setattr(
        "openhouse.dataloader.data_loader_split.ArrowScan.to_record_batches",
        _fake_to_record_batches,
    )

    with pytest.raises(_ReaderError):
        list(split)

    batch_errors = _data_points(metrics_reader, "openhouse.dataloader.batch.errors")
    assert len(batch_errors) == 1
    assert batch_errors[0].value == 1

    split_errors = _data_points(metrics_reader, "openhouse.dataloader.split.errors")
    assert len(split_errors) == 1
    assert split_errors[0].value == 1

    # split.duration is still recorded on failure
    split_duration = _data_points(metrics_reader, "openhouse.dataloader.split.duration")
    assert len(split_duration) == 1


# --- TableScanContext.metric_attributes ---


def test_table_scan_context_default_metric_attributes_is_empty(tmp_path):
    split = _make_split(tmp_path)
    assert dict(split._scan_context.metric_attributes) == {}


def test_table_scan_context_pickle_preserves_metric_attributes(tmp_path):
    split = _make_split(tmp_path, metric_attributes={"tenant": "t1"})
    restored = pickle.loads(pickle.dumps(split._scan_context))
    assert dict(restored.metric_attributes) == {"tenant": "t1"}
