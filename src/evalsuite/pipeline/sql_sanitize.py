"""Check predicted SQL for placeholders and strip markdown fences before execution."""

from __future__ import annotations

import re


def strip_sql_fences(sql: str) -> str:
    """Remove leading/trailing ```sql or ``` fences so executors receive plain SQL."""
    if not sql or not sql.strip():
        return (sql or "").strip()
    s = sql.strip()
    s = re.sub(r"^```[a-zA-Z0-9+-]*\s*", "", s)
    s = re.sub(r"\s*```\s*$", "", s)
    return s.strip()


# Patterns that indicate the model left a placeholder instead of a concrete value.
PLACEHOLDER_PATTERNS = [
    re.compile(r"<[^>]+>", re.IGNORECASE),  # <manufacturer_id>, <state>
    re.compile(r"YourState", re.IGNORECASE),
    re.compile(r"Replace\s+with", re.IGNORECASE),
    re.compile(r"TODO", re.IGNORECASE),
    re.compile(r"\?\?\?"),
]


def has_placeholders(sql: str) -> tuple[bool, str | None]:
    """
    Return (True, reason) if sql contains placeholder-like text; else (False, None).
    Use before executing pred_sql: if True, treat as pred_invalid_sql instead of running.
    """
    if not (sql or "").strip():
        return True, "empty_sql"
    for pat in PLACEHOLDER_PATTERNS:
        m = pat.search(sql)
        if m:
            return True, f"placeholder:{pat.pattern}"
    return False, None
