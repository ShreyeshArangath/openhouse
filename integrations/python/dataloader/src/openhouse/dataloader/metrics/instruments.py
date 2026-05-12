"""OpenTelemetry instrument singletons for the dataloader.

All instruments are bound to a single ``Meter`` obtained via ``get_meter(METER_NAME)``.
Call sites import the module-level objects rather than constructing their own so the
instrument inventory stays in one place.

When no SDK is configured, ``opentelemetry-api`` returns no-op instruments and the
``record``/``add`` calls are cheap.
"""

from __future__ import annotations

from opentelemetry.metrics import get_meter

from openhouse.dataloader.metrics import METER_NAME

_meter = get_meter(METER_NAME)

# Retried catalog/metadata operations.  Duration is wall-clock across all attempts;
# attempts counts every try (success or failure).
load_table_duration = _meter.create_histogram(
    name="openhouse.dataloader.load_table.duration",
    unit="s",
    description="Wall-clock duration of the load_table call (across retries).",
)
load_table_attempts = _meter.create_counter(
    name="openhouse.dataloader.load_table.attempts",
    unit="1",
    description="Attempt count for the load_table call (each try, success or failure).",
)

plan_files_duration = _meter.create_histogram(
    name="openhouse.dataloader.plan_files.duration",
    unit="s",
    description="Wall-clock duration of the plan_files call (across retries).",
)
plan_files_attempts = _meter.create_counter(
    name="openhouse.dataloader.plan_files.attempts",
    unit="1",
    description="Attempt count for the plan_files call (each try, success or failure).",
)

# Per-split read.
split_duration = _meter.create_histogram(
    name="openhouse.dataloader.split.duration",
    unit="s",
    description="Wall-clock duration of a DataLoaderSplit iteration.",
)
split_files = _meter.create_histogram(
    name="openhouse.dataloader.split.files",
    unit="1",
    description="Number of files in a DataLoaderSplit.",
)
split_rows = _meter.create_histogram(
    name="openhouse.dataloader.split.rows",
    unit="1",
    description="Total rows yielded by a DataLoaderSplit.",
)
split_bytes = _meter.create_histogram(
    name="openhouse.dataloader.split.bytes",
    unit="By",
    description="Total decompressed bytes yielded by a DataLoaderSplit.",
)
split_batches = _meter.create_histogram(
    name="openhouse.dataloader.split.batches",
    unit="1",
    description="Total RecordBatches yielded by a DataLoaderSplit.",
)
split_errors = _meter.create_counter(
    name="openhouse.dataloader.split.errors",
    unit="1",
    description="Unhandled exceptions raised from a DataLoaderSplit iteration.",
)

# Per-batch read.
batch_duration = _meter.create_histogram(
    name="openhouse.dataloader.batch.duration",
    unit="s",
    description="Wall-clock duration of reading a single RecordBatch.",
)
batch_rows = _meter.create_histogram(
    name="openhouse.dataloader.batch.rows",
    unit="1",
    description="Rows in a single RecordBatch.",
)
batch_bytes = _meter.create_histogram(
    name="openhouse.dataloader.batch.bytes",
    unit="By",
    description="Decompressed bytes of a single RecordBatch.",
)
batch_errors = _meter.create_counter(
    name="openhouse.dataloader.batch.errors",
    unit="1",
    description="Unhandled exceptions raised while reading a RecordBatch.",
)
