from __future__ import annotations

import math
from typing import Any


def normalize_value(val: Any, string_normalize: bool = True) -> Any:
    if val is None:
        return None
    if isinstance(val, float):
        if math.isnan(val):
            return None
        return val
    if isinstance(val, str) and string_normalize:
        return val.strip()
    return val


def normalize_row(row: list[Any], column_order_insensitive: bool, string_normalize: bool) -> list[Any]:
    normalized = [normalize_value(v, string_normalize=string_normalize) for v in row]
    if column_order_insensitive:
        return sorted(normalized, key=lambda x: str(x))
    return normalized
