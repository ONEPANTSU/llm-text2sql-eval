"""SGR utilities: schema truncation for token/char limits."""

from __future__ import annotations


def truncate_schema(schema: str | None, max_chars: int = 12000) -> str:
    """
    Truncate schema string to at most max_chars. Prefer cutting at newline boundaries.
    Grounding/plan prompts use full schema but truncated; synthesis can use truncated + addendum.
    """
    if not schema or not schema.strip():
        return ""
    if len(schema) <= max_chars:
        return schema
    truncated = schema[:max_chars]
    last_nl = truncated.rfind("\n")
    if last_nl > max_chars // 2:
        return truncated[: last_nl + 1] + "\n[... schema truncated ...]"
    return truncated + "\n[... schema truncated ...]"
