"""Tests for the OpenTelemetry metrics infrastructure."""

from opentelemetry.metrics import Meter, get_meter

from openhouse.dataloader.metrics import METER_NAME


def test_meter_name_is_stable():
    assert METER_NAME == "openhouse.dataloader"


def test_get_meter_with_meter_name_returns_a_meter():
    assert isinstance(get_meter(METER_NAME), Meter)
