"""Instruments for DataLoaderSplit iteration."""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any

from opentelemetry.metrics import Meter


class SplitMetrics:
    """OpenTelemetry instruments scoped to split-level iteration.

    Instruments are created lazily in ``__init__`` from a shared ``Meter``.
    This follows the pattern used by opentelemetry-python-contrib
    instrumentation libraries (FastAPI, SQLAlchemy, Django, etc.).
    """

    def __init__(self, meter: Meter) -> None:
        self.split_iter_count = meter.create_counter(
            name="split_iter_count",
            unit="1",
            description="Split iterations started",
        )
        self.split_iter_duration = meter.create_histogram(
            name="split_iter_duration",
            unit="ms",
            description="Wall-clock time to fully iterate a split",
        )
        self.batch_count = meter.create_counter(
            name="batch_count",
            unit="1",
            description="RecordBatches yielded",
        )
        self.row_count = meter.create_counter(
            name="row_count",
            unit="1",
            description="Rows read",
        )
        self.bytes_read = meter.create_counter(
            name="bytes_read",
            unit="By",
            description="Bytes read from data files",
        )
        self.error_count = meter.create_counter(
            name="error_count",
            unit="1",
            description="Errors during iteration",
        )

    def record_split_started(self, attributes: dict[str, Any]) -> None:
        self.split_iter_count.add(1, attributes)

    def record_batch(self, num_rows: int, attributes: dict[str, Any]) -> None:
        self.batch_count.add(1, attributes)
        self.row_count.add(num_rows, attributes)

    def record_bytes_read(self, size: int, attributes: dict[str, Any]) -> None:
        self.bytes_read.add(size, attributes)

    def record_split_duration(self, duration_ms: float, attributes: dict[str, Any]) -> None:
        self.split_iter_duration.record(duration_ms, attributes)

    def record_error(self, attributes: dict[str, Any]) -> None:
        self.error_count.add(1, attributes)

    @contextmanager
    def timed_split_iteration(self, attributes: dict[str, Any]):
        """Context manager that records split start, duration, and errors."""
        self.record_split_started(attributes)
        start = time.monotonic()
        try:
            yield
        except BaseException:
            self.record_error(attributes)
            raise
        finally:
            elapsed_ms = (time.monotonic() - start) * 1000
            self.record_split_duration(elapsed_ms, attributes)
