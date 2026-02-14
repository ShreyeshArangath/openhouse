"""OpenTelemetry metrics instrumentation for the dataloader.

This package depends only on ``opentelemetry-api``, which provides a no-op
fallback when no SDK is configured.  The *application* (not this library)
is responsible for installing an SDK and configuring exporters.

Usage::

    # Default (global MeterProvider, no-op until SDK configured):
    from openhouse.dataloader.metrics import get_metrics
    m = get_metrics()

    # Custom provider (for testing or explicit wiring):
    m = get_metrics(meter_provider=my_provider)

    m.split.record_batch(100, {"table": "db.t"})

To add a new metric group (e.g. loader-level planning metrics), create a
new ``_loader.py`` module with its own instruments class, instantiate it
from a shared ``Meter`` in ``DataLoaderMetrics``, and expose it as an
attribute (e.g. ``m.loader``).
"""

from __future__ import annotations

from opentelemetry.metrics import MeterProvider, get_meter

from openhouse.dataloader.metrics._split import SplitMetrics

METER_NAME = "openhouse.dataloader"


class DataLoaderMetrics:
    """Top-level metrics container.

    Groups instrument classes by concern.  Currently only ``split``
    (split iteration metrics) is populated; future groups (e.g. ``loader``
    for scan-planning metrics) can be added without changing call sites.
    """

    def __init__(self, meter_provider: MeterProvider | None = None) -> None:
        self._meter = get_meter(METER_NAME, meter_provider=meter_provider)
        self.split = SplitMetrics(self._meter)


# Default instance using the global MeterProvider (no-op until SDK configured).
_default = DataLoaderMetrics()


def get_metrics(meter_provider: MeterProvider | None = None) -> DataLoaderMetrics:
    """Return a ``DataLoaderMetrics`` instance.

    With no arguments, returns the module-level default (global MeterProvider).
    Pass a ``meter_provider`` to get a dedicated instance — useful for testing
    or when the application manages multiple providers.
    """
    if meter_provider is not None:
        return DataLoaderMetrics(meter_provider=meter_provider)
    return _default
