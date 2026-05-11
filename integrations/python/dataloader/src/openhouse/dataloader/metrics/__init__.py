"""OpenTelemetry metrics infrastructure for the dataloader.

This package depends only on ``opentelemetry-api``, which provides a no-op
fallback when no SDK is configured.  The *application* (not this library)
is responsible for installing an SDK and configuring exporters.

Usage::

    # Default (global MeterProvider, no-op until SDK configured):
    from openhouse.dataloader.metrics import get_metrics
    m = get_metrics()

    # Custom provider (for testing or explicit wiring):
    m = get_metrics(meter_provider=my_provider)

To add a metric group, create a module (e.g. ``_split.py``) with an
instruments class built from a shared ``Meter``, instantiate it from
``DataLoaderMetrics.__init__``, and expose it as an attribute.
"""

from __future__ import annotations

from opentelemetry.metrics import Meter, MeterProvider, get_meter

METER_NAME = "openhouse.dataloader"


class DataLoaderMetrics:
    """Top-level metrics container.

    Holds a shared ``Meter`` from which metric-group classes can be built.
    No instrument groups are populated yet; add them (e.g.
    ``self.split = SplitMetrics(self._meter)``) as they are introduced.
    """

    def __init__(self, meter_provider: MeterProvider | None = None) -> None:
        self._meter: Meter = get_meter(METER_NAME, meter_provider=meter_provider)


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
