"""
Eval architectures: plain, self_consistency, sgr, sql_factory, hybrid.
"""

from typing import Any

from evalsuite.architectures.hybrid import HybridParams, build_hybrid_params, run_hybrid
from evalsuite.architectures.self_consistency import select_self_consistency
from evalsuite.architectures.single import select_single
from evalsuite.architectures.sql_factory import run_sql_factory
from evalsuite.core.types import CandidateResult


def select(
    candidates: list[CandidateResult],
    mode: str,
    context: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
) -> tuple[CandidateResult, dict[str, Any]]:
    """
    Select one candidate from the list. Returns (selected_candidate, sampling_metadata).
    metadata is used for logging (e.g. groups, selected_signature, selected_idx).
    """
    if not candidates:
        raise ValueError("select() requires at least one candidate")
    config = config or {}
    if mode == "self_consistency":
        return select_self_consistency(candidates, context=context, config=config)
    return select_single(candidates, context=context, config=config)


__all__ = ["run_sql_factory", "HybridParams", "build_hybrid_params", "run_hybrid", "select"]
