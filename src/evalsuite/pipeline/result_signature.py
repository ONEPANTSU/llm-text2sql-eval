"""Stable signature of execution result for self-consistency aggregation (majority by result)."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from evalsuite.core.types import ExecResult


def _normalize_cell(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, float):
        return round(x, 10)
    if isinstance(x, (list, dict)):
        return json.dumps(x, sort_keys=True, default=str)
    return x


def result_signature(
    result: ExecResult,
    *,
    sort_rows: bool = True,
    max_rows: int | None = None,
    column_order_sensitive: bool = True,
) -> str | None:
    """
    Produce a stable hash from ExecResult for grouping (e.g. majority vote by result).
    If result is not ok, returns None.
    """
    if not result.ok or result.rows is None:
        return None
    rows = result.rows
    if max_rows is not None and len(rows) > max_rows:
        rows = rows[:max_rows]
    normalized = [[_normalize_cell(c) for c in row] for row in rows]
    if not column_order_sensitive:
        normalized = [sorted(row, key=lambda c: str(c)) for row in normalized]
    if sort_rows:
        normalized = sorted(normalized, key=lambda r: str(r))
    payload = json.dumps(normalized, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()
