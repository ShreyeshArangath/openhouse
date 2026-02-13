from __future__ import annotations

from collections.abc import Iterator, Mapping

from datafusion.plan import LogicalPlan
from pyarrow import RecordBatch
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.table import FileScanTask

from openhouse.dataloader._table_scan_context import TableScanContext
from openhouse.dataloader.metrics import get_metrics
from openhouse.dataloader.udf_registry import NoOpRegistry, UDFRegistry


class DataLoaderSplit:
    """A single data split"""

    def __init__(
        self,
        plan: LogicalPlan,
        file_scan_task: FileScanTask,
        scan_context: TableScanContext,
        udf_registry: UDFRegistry | None = None,
    ):
        self._plan = plan
        self._file_scan_task = file_scan_task
        self._udf_registry = udf_registry or NoOpRegistry()
        self._scan_context = scan_context

    @property
    def table_properties(self) -> Mapping[str, str]:
        """Properties of the table being loaded"""
        return self._scan_context.table_metadata.properties

    def __iter__(self) -> Iterator[RecordBatch]:
        """Reads the file scan task and yields Arrow RecordBatches.

        Uses PyIceberg's ArrowScan to handle format dispatch, schema resolution,
        delete files, and partition spec lookups.
        """
        ctx = self._scan_context
        attrs = {"table": ctx.table_name}
        split_metrics = get_metrics().split

        split_metrics.record_bytes_read(self._file_scan_task.file.file_size_in_bytes, attrs)

        arrow_scan = ArrowScan(
            table_metadata=ctx.table_metadata,
            io=ctx.io,
            projected_schema=ctx.projected_schema,
            row_filter=ctx.row_filter,
        )
        with split_metrics.timed_split_iteration(attrs):
            for batch in arrow_scan.to_record_batches([self._file_scan_task]):
                split_metrics.record_batch(batch.num_rows, attrs)
                yield batch
