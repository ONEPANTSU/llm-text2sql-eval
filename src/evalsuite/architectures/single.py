"""Single sampling: pick best by score or first candidate."""

from __future__ import annotations

from typing import Any

from evalsuite.core.types import CandidateResult


def select_single(
    candidates: list[CandidateResult],
    context: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
) -> tuple[CandidateResult, dict[str, Any]]:
    """
    Pick one candidate: best by score (if present), else first exec_ok, else first.
    Returns (selected, metadata) with metadata for logging.
    """
    if not candidates:
        raise ValueError("select_single requires at least one candidate")

    # Prefer by score (higher better), then exec_ok, then first
    def key(c: CandidateResult) -> tuple:
        score = c.score if c.score is not None else -1.0
        return (c.exec_ok, score, -len(c.sql), c.attempt_id)

    chosen = max(candidates, key=key)
    idx = next(i for i, c in enumerate(candidates) if c is chosen)
    meta = {
        "mode": "single",
        "selected_idx": idx,
        "total_candidates": len(candidates),
        "selected_attempt_id": chosen.attempt_id,
    }
    return chosen, meta
