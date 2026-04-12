from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from evalsuite.compare.utils import normalize_row


@dataclass
class ComparisonResult:
    match: bool
    reason: str | None = None


def _rows_equal(a: list[Any], b: list[Any], float_tol: float) -> bool:
    if len(a) != len(b):
        return False
    for left, right in zip(a, b):
        if left is None or right is None:
            if left != right:
                return False
            continue
        if isinstance(left, float) or isinstance(right, float):
            try:
                l = float(left)
                r = float(right)
            except (TypeError, ValueError):
                if left != right:
                    return False
                continue
            if abs(l - r) > float_tol:
                return False
        else:
            if left != right:
                return False
    return True


def compare_results(
    gold_rows: list[list[Any]],
    pred_rows: list[list[Any]],
    *,
    order_by: bool,
    float_tol: float,
    column_order_insensitive: bool = True,
    string_normalize: bool = True,
) -> ComparisonResult:
    gold_norm = [normalize_row(r, column_order_insensitive, string_normalize) for r in gold_rows]
    pred_norm = [normalize_row(r, column_order_insensitive, string_normalize) for r in pred_rows]

    if not order_by:
        gold_norm = sorted(gold_norm, key=lambda r: str(r))
        pred_norm = sorted(pred_norm, key=lambda r: str(r))

    if len(gold_norm) != len(pred_norm):
        return ComparisonResult(match=False, reason="row_count_mismatch")

    for idx, (g, p) in enumerate(zip(gold_norm, pred_norm)):
        if not _rows_equal(g, p, float_tol=float_tol):
            return ComparisonResult(match=False, reason=f"row_mismatch_{idx}")

    return ComparisonResult(match=True, reason=None)
