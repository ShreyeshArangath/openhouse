"""Helpers for building the common metric attribute dict."""

from __future__ import annotations

from collections.abc import Mapping

from openhouse.dataloader.table_identifier import TableIdentifier


def build_attributes(
    table_id: TableIdentifier,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return the metric attributes for *table_id*, merged with *extra*."""
    attrs: dict[str, str] = {
        "OpenHouse.Database": table_id.database,
        "OpenHouse.Table": table_id.table,
    }
    if extra:
        attrs.update(extra)
    return attrs
