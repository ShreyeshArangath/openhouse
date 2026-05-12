"""Helpers for building the common metric attribute dict."""

from __future__ import annotations

from collections.abc import Mapping

from openhouse.dataloader.table_identifier import TableIdentifier


def build_attributes(
    table_id: TableIdentifier,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return the standard metric attributes for a table read.

    Caller-provided ``extra`` attributes are merged in verbatim (no namespacing),
    so callers can supply exactly the dimensions they want on every emission —
    e.g. ``{"tenant": "team-a", "env": "prod"}``.  Caller keys override built-ins
    on collision.
    """
    attrs: dict[str, str] = {
        "openhouse.database": table_id.database,
        "openhouse.table": table_id.table,
    }
    if extra:
        attrs.update(extra)
    return attrs
