"""OpenTelemetry metrics infrastructure for the dataloader.

This package depends only on ``opentelemetry-api``, which provides a no-op
fallback when no SDK is configured.  The *application* (not this library)
is responsible for installing an SDK and configuring exporters.

External code that wants the dataloader's meter can call::

    from opentelemetry.metrics import get_meter
    from openhouse.dataloader.metrics import METER_NAME

    meter = get_meter(METER_NAME)

Internal emission inside the dataloader uses the module-level instruments
in :mod:`openhouse.dataloader.metrics.instruments` paired with the attribute
helper :func:`openhouse.dataloader.metrics.attributes.build_attributes`.
"""

METER_NAME = "OpenHouse.DataLoader"

from openhouse.dataloader.metrics.attributes import build_attributes  # noqa: E402

__all__ = ["METER_NAME", "build_attributes"]
