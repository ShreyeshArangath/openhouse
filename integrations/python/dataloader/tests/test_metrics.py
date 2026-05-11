"""Tests for the OpenTelemetry metrics infrastructure."""

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from openhouse.dataloader.metrics import DataLoaderMetrics, get_metrics


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
