from __future__ import annotations

import hashlib
import logging
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from functools import cached_property
from itertools import chain
from types import MappingProxyType

from datafusion import SessionConfig
from datafusion.context import SessionContext
from pyarrow import RecordBatch
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.table import ArrivalOrder, FileScanTask

from openhouse.dataloader._jvm import apply_libhdfs_opts
from openhouse.dataloader._table_scan_context import TableScanContext
from openhouse.dataloader._timer import log_duration
from openhouse.dataloader.filters import _quote_identifier
from openhouse.dataloader.metrics import build_attributes, instruments
from openhouse.dataloader.table_identifier import TableIdentifier
from openhouse.dataloader.udf_registry import NoOpRegistry, UDFRegistry

logger = logging.getLogger(__name__)


def to_sql_identifier(table_id: TableIdentifier) -> str:
    """Return the quoted DataFusion SQL identifier, e.g. ``"db"."tbl"``."""
    return f"{_quote_identifier(table_id.database)}.{_quote_identifier(table_id.table)}"


def _create_transform_session(
    table_id: TableIdentifier,
    udf_registry: UDFRegistry,
    batch_size: int | None = None,
) -> SessionContext:
    """Create a DataFusion SessionContext for running split-level transforms.

    Returns a ready-to-query SessionContext where UDFs are registered and the
    target schema exists.
    """
    config = SessionConfig()
    if batch_size is not None:
        config = config.set("datafusion.execution.batch_size", str(batch_size))
    session = SessionContext(config)
    udf_registry.register_udfs(session)

    session.sql(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(table_id.database)}").collect()
    return session


def _bind_batch_table(session: SessionContext, table_id: TableIdentifier, batch: RecordBatch) -> None:
    """Bind a single batch to the table name used by transform SQL."""
    name = to_sql_identifier(table_id)
    session.deregister_table(name)
    session.register_record_batches(name, [[batch]])


class _TimedBatchIter:
    """Wraps a RecordBatch iterator to log and emit metrics for each ``next()`` call."""

    def __init__(
        self,
        inner: Iterator[RecordBatch],
        split_id: str,
        attributes: Mapping[str, str],
    ) -> None:
        self._inner = inner
        self._split_id = split_id
        self._attributes = attributes
        self._idx = 0
        self.total_rows = 0
        self.total_bytes = 0
        self.batch_count = 0

    def __iter__(self) -> _TimedBatchIter:
        return self

    def __next__(self) -> RecordBatch:
        start = time.monotonic()
        try:
            batch = next(self._inner)
        except StopIteration:
            raise
        except Exception:
            elapsed = time.monotonic() - start
            logger.warning("record_batch %s [%d] failed after %.3fs", self._split_id, self._idx, elapsed)
            instruments.batch_errors.add(1, self._attributes)
            raise
        elapsed = time.monotonic() - start
        logger.info("record_batch %s [%d] in %.3fs", self._split_id, self._idx, elapsed)
        rows = batch.num_rows
        nbytes = batch.nbytes
        instruments.batch_duration.record(elapsed, self._attributes)
        instruments.batch_rows.record(rows, self._attributes)
        instruments.batch_bytes.record(nbytes, self._attributes)
        self.total_rows += rows
        self.total_bytes += nbytes
        self.batch_count += 1
        self._idx += 1
        return batch


def _timed_transform(
    batches: Iterator[RecordBatch],
    split_id: str,
    session: SessionContext,
    apply_fn: Callable[[SessionContext, RecordBatch], Iterator[RecordBatch]],
) -> Iterator[RecordBatch]:
    """Apply a transform to each batch, logging the wall-clock time of each."""
    for idx, batch in enumerate(batches):
        with log_duration(logger, "transform_batch %s [%d]", split_id, idx):
            transformed = list(apply_fn(session, batch))
        yield from transformed


class DataLoaderSplit:
    """A data split that reads one or more files."""

    def __init__(
        self,
        file_scan_tasks: Sequence[FileScanTask],
        scan_context: TableScanContext,
        transform_sql: str | None = None,
        udf_registry: UDFRegistry | None = None,
        batch_size: int | None = None,
    ):
        self._file_scan_tasks = list(file_scan_tasks)
        if not self._file_scan_tasks:
            raise ValueError("file_scan_tasks must not be empty")
        self._scan_context = scan_context
        self._transform_sql = transform_sql
        self._udf_registry = udf_registry or NoOpRegistry()
        self._batch_size = batch_size

    @cached_property
    def id(self) -> str:
        """Unique ID for the split. This is stable across executions for a given
        snapshot and split size.
        """
        paths = sorted(t.file.file_path for t in self._file_scan_tasks)
        combined = "\0".join(paths)
        return hashlib.sha256(combined.encode("utf-8")).hexdigest()

    @property
    def table_properties(self) -> Mapping[str, str]:
        """Properties of the table being loaded"""
        return MappingProxyType(self._scan_context.table_metadata.properties)

    def __iter__(self) -> Iterator[RecordBatch]:
        """Reads the file scan tasks and yields Arrow RecordBatches.

        When the split contains multiple files, they are read concurrently.

        Yields:
            RecordBatch from the underlying file scan tasks
        """
        ctx = self._scan_context
        if ctx.worker_jvm_args is not None:
            apply_libhdfs_opts(ctx.worker_jvm_args)
        attributes = build_attributes(ctx.table_id, ctx.metric_attributes)
        split_start = time.monotonic()
        timed: _TimedBatchIter | None = None
        try:
            arrow_scan = ArrowScan(
                table_metadata=ctx.table_metadata,
                io=ctx.io,
                projected_schema=ctx.projected_schema,
                row_filter=ctx.row_filter,
            )

            split_id = self.id[:12]

            with log_duration(logger, "setup_scan %s", split_id):
                batches = arrow_scan.to_record_batches(
                    self._file_scan_tasks,
                    order=ArrivalOrder(concurrent_streams=len(self._file_scan_tasks), batch_size=self._batch_size),
                )

            timed = _TimedBatchIter(iter(batches), split_id, attributes)

            if self._transform_sql is None:
                yield from timed
            else:
                # Materialize the first batch before creating the transform session
                # so that the HDFS JVM starts (and picks up worker_jvm_args) before
                # any UDF registration code can trigger JNI.
                first = next(timed, None)
                if first is None:
                    return
                session = _create_transform_session(self._scan_context.table_id, self._udf_registry, self._batch_size)
                yield from _timed_transform(chain([first], timed), split_id, session, self._apply_transform)
        except BaseException:
            instruments.split_errors.add(1, attributes)
            raise
        finally:
            instruments.split_duration.record(time.monotonic() - split_start, attributes)
            instruments.split_files.record(len(self._file_scan_tasks), attributes)
            if timed is not None:
                instruments.split_rows.record(timed.total_rows, attributes)
                instruments.split_bytes.record(timed.total_bytes, attributes)
                instruments.split_batches.record(timed.batch_count, attributes)

    def _apply_transform(self, session: SessionContext, batch: RecordBatch) -> Iterator[RecordBatch]:
        """Execute the transform SQL against a single RecordBatch."""
        _bind_batch_table(session, self._scan_context.table_id, batch)
        df = session.sql(self._transform_sql)  # type: ignore[arg-type]  # caller guarantees not None
        yield from df.collect()
