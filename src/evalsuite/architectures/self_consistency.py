"""Self-consistency sampling: select one candidate by majority_result or best_score."""

from __future__ import annotations

from typing import Any

from evalsuite.core.types import CandidateResult
from evalsuite.pipeline.aggregation import aggregate


def select_self_consistency(
    candidates: list[CandidateResult],
    context: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
) -> tuple[CandidateResult, dict[str, Any]]:
    """
    Select one candidate: group by result_signature (or by score), choose by aggregation mode.
    config: sc_aggregation = majority_result | best_score (default majority_result).
    Returns (selected_candidate, metadata) with groups, selected_signature, selected_idx for logs.
    """
    if not candidates:
        raise ValueError("select_self_consistency requires at least one candidate")
    cfg = config or {}
    agg_mode = (cfg.get("sc_aggregation") or "majority_result").strip()
    if agg_mode == "best_score":
        agg_internal = "best_score"
    else:
        agg_internal = "majority_result"  # maps to hybrid in aggregate()

    sel_id, sel_sql, reason, votes = aggregate(candidates, agg_internal)
    chosen = next(c for c in candidates if c.attempt_id == sel_id)
    selected_idx = next(i for i, c in enumerate(candidates) if c.attempt_id == sel_id)

    # Build groups for logging: by result_signature (for majority_result)
    groups: list[dict[str, Any]] = []
    if agg_internal == "majority_result":
        exec_ok = [c for c in candidates if c.exec_ok]
        sig_to_list: dict[str, list[CandidateResult]] = {}
        for c in exec_ok:
            sig = c.result_signature or ""
            sig_to_list.setdefault(sig, []).append(c)
        for sig, lst in sig_to_list.items():
            best_in_group = min(lst, key=lambda c: (c.exec_time_ms or float("inf"), c.attempt_id))
            groups.append(
                {
                    "signature": sig[:16] + "..." if len(sig) > 16 else sig,
                    "count": len(lst),
                    "best_sql": (
                        best_in_group.sql[:200] + "..." if len(best_in_group.sql) > 200 else best_in_group.sql
                    ),
                }
            )
        groups.sort(key=lambda g: -g["count"])
    selected_sig = (
        (chosen.result_signature or "")[:16] + "..."
        if len(chosen.result_signature or "") > 16
        else (chosen.result_signature or "")
    )

    meta = {
        "mode": "self_consistency",
        "groups": groups,
        "selected_signature": selected_sig,
        "selected_idx": selected_idx,
        "selected_attempt_id": sel_id,
        "aggregation_reason": reason,
        "votes": votes,
        "total_candidates": len(candidates),
    }
    return chosen, meta
