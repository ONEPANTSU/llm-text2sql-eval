"""
Self-consistency aggregation: choose one candidate from K attempts.
Modes: majority_by_normalized_sql, best_by_preflight_then_exec, hybrid.
"""

from __future__ import annotations

import re
from typing import Any

from evalsuite.core.types import CandidateResult


def normalize_sql_for_aggregation(sql: str) -> str:
    """Whitespace normalize, lowercase keywords, strip trailing semicolon for vote grouping."""
    if not sql or not sql.strip():
        return ""
    s = sql.strip().rstrip(";").strip()
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s)
    # Optional: lowercase keywords for comparison (can be disabled if case matters)
    s = s.lower()
    return s


def aggregation_majority_by_normalized_sql(
    candidates: list[CandidateResult],
) -> tuple[int, str, str, dict[str, Any]]:
    """
    Select the most frequent normalized SQL. Tie-break: min exec_time_ms, then min len(sql), then min attempt_id.
    Returns (selected_attempt_id, selected_sql, reason, votes_dict).
    """
    if not candidates:
        raise ValueError("empty candidates")
    norm_to_cands: dict[str, list[CandidateResult]] = {}
    for c in candidates:
        n = normalize_sql_for_aggregation(c.sql)
        norm_to_cands.setdefault(n, []).append(c)
    by_sql = {n: len(lst) for n, lst in norm_to_cands.items()}
    best_norm = max(
        norm_to_cands.keys(), key=lambda n: (by_sql[n], -min(c.exec_time_ms or float("inf") for c in norm_to_cands[n]))
    )
    best_list = norm_to_cands[best_norm]
    # Tie-break: min exec_time_ms, then min len(sql), then min attempt_id
    best_list.sort(
        key=lambda c: (
            c.exec_time_ms if c.exec_time_ms is not None else float("inf"),
            len(c.sql),
            c.attempt_id,
        )
    )
    chosen = best_list[0]
    return (
        chosen.attempt_id,
        chosen.sql,
        "majority_by_normalized_sql",
        {"by_sql": by_sql},
    )


def aggregation_best_by_preflight_then_exec(
    candidates: list[CandidateResult],
) -> tuple[int, str, str, dict[str, Any]]:
    """
    Priority: exec_ok > preflight_ok > any. Tie-break: min exec_time_ms, len(sql), attempt_id.
    Returns (selected_attempt_id, selected_sql, reason, votes_dict).
    """
    if not candidates:
        raise ValueError("empty candidates")
    exec_ok = [c for c in candidates if c.exec_ok]
    preflight_ok = [c for c in candidates if c.preflight_ok and not c.exec_ok]
    rest = [c for c in candidates if not c.preflight_ok]

    def pick_one(lst: list[CandidateResult]) -> CandidateResult:
        lst_sorted = sorted(
            lst,
            key=lambda c: (
                c.exec_time_ms if c.exec_time_ms is not None else float("inf"),
                len(c.sql),
                c.attempt_id,
            ),
        )
        return lst_sorted[0]

    if exec_ok:
        chosen = pick_one(exec_ok)
        return chosen.attempt_id, chosen.sql, "best_by_preflight_then_exec: exec_ok", {}
    if preflight_ok:
        chosen = pick_one(preflight_ok)
        return chosen.attempt_id, chosen.sql, "best_by_preflight_then_exec: preflight_ok", {}
    chosen = pick_one(rest)
    return chosen.attempt_id, chosen.sql, "best_by_preflight_then_exec: fallback", {}


def aggregation_hybrid(
    candidates: list[CandidateResult],
) -> tuple[int, str, str, dict[str, Any]]:
    """
    If any exec_ok: majority by result_signature, then pick fastest in winning group.
    Else if any preflight_ok: majority by normalized SQL.
    Else: pick by error_type priority (parse_fail > bind_fail > runtime_fail), then attempt_id 0.
    Returns (selected_attempt_id, selected_sql, reason, votes_dict).
    """
    if not candidates:
        raise ValueError("empty candidates")

    exec_ok = [c for c in candidates if c.exec_ok]
    if exec_ok:
        sig_to_cands: dict[str, list[CandidateResult]] = {}
        for c in exec_ok:
            sig = c.result_signature or ""
            sig_to_cands.setdefault(sig, []).append(c)
        by_sig = {s: len(lst) for s, lst in sig_to_cands.items()}
        best_sig = max(sig_to_cands.keys(), key=lambda s: by_sig[s])
        group = sig_to_cands[best_sig]
        group.sort(key=lambda c: (c.exec_time_ms or float("inf"), c.attempt_id))
        chosen = group[0]
        return (
            chosen.attempt_id,
            chosen.sql,
            "hybrid: majority signature among exec_ok",
            {"by_signature": by_sig},
        )

    preflight_ok = [c for c in candidates if c.preflight_ok]
    if preflight_ok:
        idx, sql, _, votes = aggregation_majority_by_normalized_sql(preflight_ok)
        return idx, sql, "hybrid: majority_by_normalized_sql (preflight_ok)", votes

    # Fallback: prefer parse_fail (easier to fix) over bind/runtime; then attempt_id 0
    error_priority = {"pred_parse_fail": 0, "pred_bind_fail": 1, "pred_runtime_fail": 2, "pred_exec_fail": 3}
    rest_sorted = sorted(
        candidates,
        key=lambda c: (
            error_priority.get(c.preflight_error_type or "", 99),
            c.attempt_id,
        ),
    )
    chosen = rest_sorted[0]
    return (
        chosen.attempt_id,
        chosen.sql,
        "hybrid: fallback by error_type priority",
        {},
    )


def aggregation_best_score(
    candidates: list[CandidateResult],
) -> tuple[int, str, str, dict[str, Any]]:
    """
    Pick candidate with highest score. Tie-break: exec_ok, then exec_time_ms, attempt_id.
    Returns (selected_attempt_id, selected_sql, reason, votes_dict).
    """
    if not candidates:
        raise ValueError("empty candidates")
    sorted_cands = sorted(
        candidates,
        key=lambda c: (
            c.score if c.score is not None else -1.0,
            c.exec_ok,
            -(c.exec_time_ms or 0),
            -c.attempt_id,
        ),
        reverse=True,
    )
    chosen = sorted_cands[0]
    return (
        chosen.attempt_id,
        chosen.sql,
        "best_score",
        {"best_score": chosen.score},
    )


def aggregate(
    candidates: list[CandidateResult],
    mode: str,
) -> tuple[int, str, str, dict[str, Any]]:
    """
    Dispatch to the requested aggregation mode.
    Returns (selected_attempt_id, selected_sql, aggregation_reason, votes).
    """
    if mode == "majority_by_normalized_sql":
        return aggregation_majority_by_normalized_sql(candidates)
    if mode == "best_by_preflight_then_exec":
        return aggregation_best_by_preflight_then_exec(candidates)
    if mode == "hybrid" or mode == "majority_result":
        return aggregation_hybrid(candidates)
    if mode == "best_score":
        return aggregation_best_score(candidates)
    raise ValueError(f"unknown aggregation mode: {mode}")
