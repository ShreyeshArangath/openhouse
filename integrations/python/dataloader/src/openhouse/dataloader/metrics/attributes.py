"""Helpers for building the common metric attribute dict."""

from __future__ import annotations

from collections.abc import Mapping

from openhouse.dataloader.table_identifier import TableIdentifier


def build_attributes(
    table_id: TableIdentifier,
    execution_context: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return the standard metric attributes for a table read.

    Caller-provided ``execution_context`` keys are namespaced under
    ``openhouse.ctx.`` so they cannot collide with built-in attributes.
    ``openhouse.branch`` is omitted when the table identifier has no branch.
    """
    attrs: dict[str, str] = {
        "openhouse.database": table_id.database,
        "openhouse.table": table_id.table,
    }
    if table_id.branch is not None:
        attrs["openhouse.branch"] = table_id.branch
    if execution_context:
        for key, value in execution_context.items():
            attrs[f"openhouse.ctx.{key}"] = value
    return attrs
