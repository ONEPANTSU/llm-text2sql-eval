"""
SQL-Factory: multi-agent exploration/exploitation, quality-gated SQL generation.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from evalsuite.architectures.plain import _run_preflight_exec
from evalsuite.architectures.similarity import (
    similarity_max as _similarity_max,
)
from evalsuite.architectures.similarity import (
    similarity_max_token_only as _similarity_max_token_only,
)
from evalsuite.architectures.similarity import (
    token_jaccard as _token_jaccard,
)
from evalsuite.core.types import CandidateResult, ExecResult
from evalsuite.pipeline.preflight import extract_tables_from_sql
from evalsuite.pipeline.result_signature import result_signature
from evalsuite.pipeline.sql_sanitize import strip_sql_fences

# -------- Params & Candidate --------


@dataclass
class SqlFactoryParams:
    max_rounds: int = 3
    warmup_rounds: int = 1
    gen_batch: int = 2
    exp_batch: int = 2
    target_pool_size: int = 5
    stop_on_saturation: bool = True
    saturation_patience: int = 2
    expansion_ratio_target: float = 0.7
    sim_threshold: float = 0.85
    k_neighbors: int = 5
    weights: dict[str, float] = field(default_factory=lambda: {"tok": 0.6, "ast": 0.3, "emb": 0.1})
    models: dict[str, str | None] = field(default_factory=dict)
    sampling: dict[str, dict[str, float]] = field(default_factory=dict)
    generation_timeout_per_attempt: int = 15
    max_workers: int = 5
    parallelism: str = "parallel"
    time_budget_per_task_sec: int | None = 30  # stop rounds when elapsed >= this (None = no limit)
    no_progress_patience: int = 2  # stop if added_to_pool == 0 for this many rounds in a row
    best_score_stagnation_rounds: int = 2  # stop if pool >= target and best_score unchanged
    scoring: dict[str, Any] = field(
        default_factory=lambda: {
            "bonus_tables_cap": 6,
            "bonus_per_table": 0.05,
            "similarity_penalty": 0.5,
            "complexity_bonus": True,
            "complexity_bonus_cap": 0.2,
            "tables_over_penalty_threshold": 6,
            "tables_over_penalty": 0.05,
        }
    )


@dataclass
class SqlFactoryCandidate:
    sql: str
    phase: str  # "gen" | "exp"
    round_idx: int
    exec_ok: bool
    error_type: str | None = None
    error_message: str | None = None
    tables_used: set[str] = field(default_factory=set)
    similarity_max: float = 0.0
    score: float = 0.0
    meta: dict[str, Any] = field(default_factory=dict)
    result_signature: str | None = None  # for self_consistency sampling


def build_sql_factory_params(config_params: dict[str, Any]) -> SqlFactoryParams:
    p = config_params
    return SqlFactoryParams(
        max_rounds=int(p.get("max_rounds", 3)),
        warmup_rounds=int(p.get("warmup_rounds", 1)),
        gen_batch=int(p.get("gen_batch", 2)),
        exp_batch=int(p.get("exp_batch", 2)),
        target_pool_size=int(p.get("target_pool_size", 5)),
        stop_on_saturation=bool(p.get("stop_on_saturation", True)),
        saturation_patience=int(p.get("saturation_patience", 2)),
        expansion_ratio_target=float(p.get("expansion_ratio_target", 0.7)),
        sim_threshold=float(p.get("sim_threshold", 0.85)),
        k_neighbors=int(p.get("k_neighbors", 5)),
        weights=dict(p.get("weights") or {"tok": 0.6, "ast": 0.3, "emb": 0.1}),
        models=dict(p.get("models") or {}),
        sampling=dict(p.get("sampling") or {}),
        generation_timeout_per_attempt=int(p.get("generation_timeout_per_attempt", 15)),
        max_workers=int(p.get("max_workers", 5)),
        parallelism=p.get("parallelism", "parallel"),
        time_budget_per_task_sec=p.get("time_budget_per_task_sec"),
        no_progress_patience=int(p.get("no_progress_patience", 2)),
        best_score_stagnation_rounds=int(p.get("best_score_stagnation_rounds", 2)),
        scoring=dict(p.get("scoring") or {}),
    )


# -------- Table selection: 0.7*relevance(question) + 0.3*underused --------


def _tokenize_for_relevance(text: str) -> set[str]:
    """Simple tokenization: lowercase, split on non-alphanumeric, keep words."""
    if not text:
        return set()
    tokens = set(re.findall(r"[a-zA-Z0-9_]+", text.lower()))
    return {t for t in tokens if len(t) > 1 or t.isdigit()}


def table_selection_agent(
    pool: list[SqlFactoryCandidate],
    all_table_names: list[str],
    question: str = "",
    schema: str | None = None,
    relevance_weight: float = 0.7,
) -> list[str]:
    """Priority tables: relevance_weight * relevance(question) + (1 - relevance_weight) * underused."""
    if not all_table_names:
        return list(all_table_names)
    q_tokens = _tokenize_for_relevance(question)
    # Build table -> set of tokens (table name + columns from schema if available)
    table_tokens: dict[str, set[str]] = {}
    for t in all_table_names:
        table_tokens[t] = _tokenize_for_relevance(t)
    if schema and len(schema) < 12000:
        for m in re.finditer(r"-?\s*([a-zA-Z0-9_]+)\s*\(([^)]*)\)", schema):
            tbl, cols = m.group(1), m.group(2)
            if tbl in table_tokens:
                table_tokens[tbl] = table_tokens[tbl] | _tokenize_for_relevance(cols)
    # Relevance: Jaccard or overlap with question
    max_overlap = 1
    relevance: dict[str, float] = {}
    for t in all_table_names:
        overlap = len(q_tokens & table_tokens[t]) / max(len(q_tokens | table_tokens[t]), 1)
        relevance[t] = overlap
        if overlap > max_overlap:
            max_overlap = overlap
    if max_overlap > 0:
        for t in relevance:
            relevance[t] /= max_overlap
    # Underused: tables used least in pool (normalize to 0..1)
    counts: dict[str, int] = {t: 0 for t in all_table_names}
    for c in pool:
        for t in c.tables_used:
            if t in counts:
                counts[t] += 1
    max_count = max(counts.values()) or 1
    underused: dict[str, float] = {t: 1.0 - (counts[t] / max_count) for t in all_table_names}
    # Combined: 0.7 * relevance + 0.3 * underused
    combined = {
        t: relevance_weight * relevance.get(t, 0) + (1.0 - relevance_weight) * underused[t] for t in all_table_names
    }
    sorted_tables = sorted(combined.keys(), key=lambda t: -combined[t])
    return sorted_tables[: max(5, len(all_table_names) // 2)]


# -------- Scoring --------


def _has_join(sql: str) -> bool:
    return " join " in sql.lower() or " inner join" in sql.lower() or " left join" in sql.lower()


def _has_cte(sql: str) -> bool:
    return sql.strip().lower().startswith("with ")


def _has_window(sql: str) -> bool:
    return " over " in sql.lower() and "(" in sql


def _has_nested_select(sql: str) -> bool:
    return bool(re.search(r"\(\s*SELECT\b", sql, re.IGNORECASE))


def compute_complexity_bonus(sql: str, cap: float = 0.2) -> float:
    """+0.1 per: JOIN, WITH (CTE), OVER (window), nested SELECT; total capped at cap (default 0.2)."""
    bonus = 0.0
    if _has_join(sql):
        bonus += 0.1
    if _has_cte(sql):
        bonus += 0.1
    if _has_window(sql):
        bonus += 0.1
    if _has_nested_select(sql):
        bonus += 0.1
    return min(bonus, cap)


def compute_score(
    candidate: SqlFactoryCandidate,
    scoring: dict[str, Any],
) -> float:
    """score = base + tables_bonus + complexity_bonus - sim_penalty - tables_over_penalty."""
    base = 1.0
    cap = int(scoring.get("bonus_tables_cap", 6))
    per_table = float(scoring.get("bonus_per_table", 0.05))
    tables_bonus = per_table * min(len(candidate.tables_used), cap)
    sim_penalty = float(scoring.get("similarity_penalty", 0.5)) * candidate.similarity_max
    complexity = 0.0
    if scoring.get("complexity_bonus", True):
        complexity_cap = float(scoring.get("complexity_bonus_cap", 0.2))
        complexity = compute_complexity_bonus(candidate.sql, cap=complexity_cap)
    tables_over = int(scoring.get("tables_over_penalty_threshold", 6))
    tables_over_penalty = 0.0
    if len(candidate.tables_used) > tables_over:
        tables_over_penalty = float(scoring.get("tables_over_penalty", 0.05)) * (
            len(candidate.tables_used) - tables_over
        )
    return base + tables_bonus + complexity - sim_penalty - tables_over_penalty


# -------- Critical: preflight + similarity -> accept/reject --------


def _preflight_one(
    sql: str,
    db_path: str,
    dialect: str,
    timeout_sec: int | None,
) -> tuple[str, bool, str | None, str | None, set[str], float, ExecResult]:
    """Run preflight+exec; returns (sql, exec_ok, error_type, error_message, tables_used, exec_time_ms, result)."""
    sql = strip_sql_fences(sql)
    if not sql or not sql.strip():
        return sql, False, "exec_fail", "empty_sql", set(), 0.0, ExecResult(ok=False, rows=None, error="empty_sql")
    result, err_type, exec_time_ms = _run_preflight_exec(db_path, sql, dialect, timeout_sec=timeout_sec)
    tables_used = set(extract_tables_from_sql(sql))
    err_msg = result.error if result is not None and hasattr(result, "error") else None
    return (sql, result.ok, err_type, err_msg, tables_used, exec_time_ms, result)


# -------- Management: phase for round (heuristics) --------


def management_agent(
    round_idx: int,
    pool: list[SqlFactoryCandidate],
    recent_added_sim_max: list[float],
    params: SqlFactoryParams,
    rounds_with_no_add: int = 0,
    best_score_so_far: float = 0.0,
    rounds_since_best_improved: int = 0,
) -> tuple[str, bool]:
    """
    Returns (phase: "exploration" | "exploitation", should_stop: bool).
    Stop if: no progress for no_progress_patience rounds; or pool full and best_score stagnant.
    """
    if round_idx >= params.max_rounds:
        return "exploitation", True
    if rounds_with_no_add >= params.no_progress_patience:
        return "exploitation", True
    if len(pool) >= params.target_pool_size and rounds_since_best_improved >= params.best_score_stagnation_rounds:
        return "exploitation", True
    if round_idx < params.warmup_rounds:
        return "exploration", False
    if params.stop_on_saturation and len(pool) >= params.target_pool_size and recent_added_sim_max:
        if len(recent_added_sim_max) >= params.saturation_patience:
            avg_sim = sum(recent_added_sim_max[-params.saturation_patience :]) / params.saturation_patience
            if avg_sim >= params.sim_threshold * 0.9:
                return "exploitation", True
    if recent_added_sim_max and len(recent_added_sim_max) >= 2:
        if sum(recent_added_sim_max[-2:]) / 2 >= params.sim_threshold * 0.9:
            return "exploration", False
    return "exploitation", False


# -------- Generation agent: gen_batch SQLs --------


def _gen_one(
    question: str,
    schema: str | None,
    model: Any,
    sampling_kw: dict[str, float],
    base_seed: int,
    index: int,
) -> str:
    """Single generation attempt for parallel execution."""
    try:
        raw = model.generate_sql(
            question=question,
            schema=schema,
            messages=None,
            temperature=sampling_kw.get("temperature", 0.8),
            top_p=sampling_kw.get("top_p", 0.9),
            seed=base_seed + index,
        )
        return strip_sql_fences(raw or "")
    except Exception:
        return ""


def generation_agent(
    question: str,
    schema: str | None,
    priority_tables: list[str],
    batch_size: int,
    model: Any,
    sampling_kw: dict[str, float],
    timeout_sec: int,
    base_seed: int = 42,
    parallelism: str = "serial",
    max_workers: int = 3,
) -> list[str]:
    """Generate batch_size SQL candidates. Uses model.generate_sql with schema + optional table hint."""
    table_hint = ""
    if priority_tables and schema:
        table_hint = f"\nConsider using tables: {', '.join(priority_tables[:10])}."
        schema = f"{schema}{table_hint}"
    if parallelism == "parallel" and batch_size > 1:
        workers = min(max_workers, batch_size)
        sqls = []
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [
                ex.submit(_gen_one, question, schema, model, sampling_kw, base_seed, i) for i in range(batch_size)
            ]
            for f in futures:
                try:
                    s = f.result(timeout=timeout_sec)
                    if s:
                        sqls.append(s)
                except (FuturesTimeoutError, TimeoutError, Exception):
                    pass
        return sqls
    sqls: list[str] = []
    for i in range(batch_size):
        s = _gen_one(question, schema, model, sampling_kw, base_seed, i)
        if s:
            sqls.append(s)
    return sqls


# -------- Seed selection: diversity-aware (top-N by score, then pick K least similar) --------


def seed_selection_agent(
    pool: list[SqlFactoryCandidate],
    priority_tables: list[str],
    num_seeds: int = 3,
    candidate_pool_size: int = 5,
    w_tok: float = 0.6,
    w_ast: float = 0.3,
    w_emb: float = 0.1,
) -> list[SqlFactoryCandidate]:
    """Top candidate_pool_size by score, then pick num_seeds by diversity (token-only for speed)."""
    if not pool or num_seeds <= 0:
        return []
    sorted_pool = sorted(pool, key=lambda c: (c.score, -len(c.tables_used)), reverse=True)
    candidates = sorted_pool[: min(candidate_pool_size, len(sorted_pool))]
    if len(candidates) <= num_seeds:
        return candidates
    seeds: list[SqlFactoryCandidate] = [candidates[0]]
    for _ in range(num_seeds - 1):
        best_idx = -1
        best_max_sim = 2.0
        for i, c in enumerate(candidates):
            if c in seeds:
                continue
            max_sim_to_seeds = max(_token_jaccard(c.sql, s.sql) for s in seeds)
            if max_sim_to_seeds < best_max_sim:
                best_max_sim = max_sim_to_seeds
                best_idx = i
        if best_idx >= 0:
            seeds.append(candidates[best_idx])
    return seeds[:num_seeds]


# -------- Expansion agent: exp_batch variations from seeds --------


def _exp_one(
    seed_sql: str,
    question: str,
    schema: str | None,
    model: Any,
    sampling_kw: dict[str, float],
    base_seed: int,
    index: int,
) -> str:
    """Single expansion attempt for parallel execution."""
    try:
        user = f"Question: {question}\n\nReference SQL (vary this):\n{seed_sql}\n\nProvide a variation."
        if schema:
            user = f"{schema}\n\n{user}"
        raw = model.generate_sql(
            question=user,
            schema=None,
            messages=None,
            temperature=sampling_kw.get("temperature", 0.8),
            top_p=sampling_kw.get("top_p", 0.9),
            seed=base_seed + index,
        )
        s = strip_sql_fences(raw or "")
        return s if s != seed_sql else ""
    except Exception:
        return ""


def expansion_agent(
    seeds: list[SqlFactoryCandidate],
    question: str,
    schema: str | None,
    batch_size: int,
    model: Any,
    sampling_kw: dict[str, float],
    timeout_sec: int,
    base_seed: int = 100,
    parallelism: str = "serial",
    max_workers: int = 3,
) -> list[str]:
    """Generate batch_size variations: rewrite subquery<->CTE, filters, join order, aliases."""
    if not seeds:
        return []
    seed_sqls = [s.sql for s in seeds[:3]]
    if parallelism == "parallel" and batch_size > 1:
        workers = min(max_workers, batch_size)
        sqls = []
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [
                ex.submit(
                    _exp_one,
                    seed_sqls[i % len(seed_sqls)],
                    question,
                    schema,
                    model,
                    sampling_kw,
                    base_seed,
                    i,
                )
                for i in range(batch_size)
            ]
            for f in futures:
                try:
                    s = f.result(timeout=timeout_sec)
                    if s:
                        sqls.append(s)
                except (FuturesTimeoutError, TimeoutError, Exception):
                    pass
        return sqls
    sqls: list[str] = []
    for i in range(batch_size):
        s = _exp_one(
            seed_sqls[i % len(seed_sqls)],
            question,
            schema,
            model,
            sampling_kw,
            base_seed,
            i,
        )
        if s:
            sqls.append(s)
    return sqls


# -------- Main loop --------


def run_sql_factory(
    *,
    task_id: str,
    get_context: Callable[[], dict[str, Any]],
    model: Any,
    db_path: str,
    dialect: str,
    params: SqlFactoryParams,
    sql_execution_timeout_sec: int | None = None,
    all_table_names: list[str] | None = None,
    run_dir: Path | None = None,
) -> tuple[list, dict[str, Any], bool]:
    """
    Multi-round exploration/exploitation; quality gate + similarity filter.
    Returns (candidates, artifact_dict, task_timeout_hit).
    """
    ctx = get_context()
    question = ctx.get("question", "")
    schema = ctx.get("schema")
    messages = ctx.get("messages")
    table_names = list(all_table_names or [])
    if not table_names and schema:
        for m in re.finditer(r"-?\s*([a-zA-Z0-9_]+)\s*\(", schema):
            table_names.append(m.group(1))

    pool: list[SqlFactoryCandidate] = []
    rounds_log: list[dict[str, Any]] = []
    recent_added_sim: list[float] = []
    reject_exec_fail = 0
    reject_similarity = 0
    exploration_rounds = 0
    exploitation_rounds = 0
    timeout_sec = params.generation_timeout_per_attempt
    exec_timeout = sql_execution_timeout_sec
    sampling_gen = (params.sampling or {}).get("generation") or {"temperature": 0.8, "top_p": 0.9}
    sampling_exp = (params.sampling or {}).get("expansion") or {"temperature": 0.8, "top_p": 0.9}
    task_timeout_hit = False
    start_wall = time.perf_counter()
    rounds_with_no_add = 0
    best_score_so_far = 0.0
    rounds_since_best_improved = 0
    exec_fail_breakdown: dict[str, int] = {}
    error_message_counts: dict[tuple[str, str], int] = {}  # (err_type, msg_head) -> count
    preflight_times_ms: list[float] = []

    for round_idx in range(params.max_rounds):
        if params.time_budget_per_task_sec is not None:
            if (time.perf_counter() - start_wall) >= params.time_budget_per_task_sec:
                task_timeout_hit = True
                break
        phase, should_stop = management_agent(
            round_idx,
            pool,
            recent_added_sim,
            params,
            rounds_with_no_add=rounds_with_no_add,
            best_score_so_far=best_score_so_far,
            rounds_since_best_improved=rounds_since_best_improved,
        )
        if should_stop:
            break
        if phase == "exploration":
            exploration_rounds += 1
        else:
            exploitation_rounds += 1

        round_log: dict[str, Any] = {
            "round": round_idx,
            "phase": phase,
            "generated": 0,
            "preflight_ok": 0,
            "reject_similarity": 0,
            "pool_size_before": len(pool),
        }
        candidates_to_check: list[tuple[str, str]] = []  # (sql, phase)

        if phase == "exploration":
            priority = table_selection_agent(pool, table_names, question=question, schema=schema, relevance_weight=0.7)
            batch = generation_agent(
                question=question,
                schema=schema,
                priority_tables=priority,
                batch_size=params.gen_batch,
                model=model,
                sampling_kw=sampling_gen,
                timeout_sec=timeout_sec,
                base_seed=42 + round_idx * 100,
                parallelism=params.parallelism,
                max_workers=params.max_workers,
            )
            for sql in batch:
                candidates_to_check.append((sql, "gen"))
        else:
            w = params.weights
            seeds = seed_selection_agent(
                pool,
                table_names,
                num_seeds=3,
                candidate_pool_size=5,
                w_tok=w.get("tok", 0.6),
                w_ast=w.get("ast", 0.3),
                w_emb=w.get("emb", 0.1),
            )
            batch = expansion_agent(
                seeds=seeds,
                question=question,
                schema=schema,
                batch_size=params.exp_batch,
                model=model,
                sampling_kw=sampling_exp,
                timeout_sec=timeout_sec,
                base_seed=200 + round_idx * 100,
                parallelism=params.parallelism,
                max_workers=params.max_workers,
            )
            for sql in batch:
                candidates_to_check.append((sql, "exp"))

        round_log["generated"] = len(candidates_to_check)
        added_this_round = 0
        rej_sim_round = 0
        rej_exec_round = 0
        pool_at_round_start = list(pool)
        w = params.weights

        # Parallel preflight (I/O)
        if params.parallelism == "parallel" and len(candidates_to_check) > 1:
            workers = min(params.max_workers, len(candidates_to_check))

            def _preflight_task(item):
                sql, _ = item
                return _preflight_one(sql, db_path, dialect, exec_timeout)

            with ThreadPoolExecutor(max_workers=workers) as ex:
                preflight_results = list(ex.map(_preflight_task, candidates_to_check))
        else:
            preflight_results = [_preflight_one(sql, db_path, dialect, exec_timeout) for sql, _ in candidates_to_check]

        # Collect exec_fail breakdown and build valid list (exec_ok); use token-only sim for sort (fast)
        valid_for_round: list[
            tuple[str, str, set[str], float, float, Any]
        ] = []  # (sql, ph, tables_used, exec_time_ms, sim_token, result)
        pool_sqls_start = [c.sql for c in pool_at_round_start]
        for (sql, ph), (_, exec_ok, err_type, err_msg, tables_used, exec_time_ms, result) in zip(
            candidates_to_check, preflight_results
        ):
            preflight_times_ms.append(exec_time_ms)
            if not exec_ok:
                reject_exec_fail += 1
                rej_exec_round += 1
                err_key = err_type or "pred_exec_fail"
                exec_fail_breakdown[err_key] = exec_fail_breakdown.get(err_key, 0) + 1
                msg_head = (err_msg or "")[:80] if err_msg else ""
                error_message_counts[(err_key, msg_head)] = error_message_counts.get((err_key, msg_head), 0) + 1
                continue
            sim_token = _similarity_max_token_only(sql, pool_sqls_start)
            valid_for_round.append((sql, ph, tables_used, exec_time_ms, sim_token, result))

        # Sort by least token-similar first, then add one-by-one with full similarity (hybrid) to current pool
        valid_for_round.sort(key=lambda x: x[4])  # asc token sim
        for sql, ph, tables_used, exec_time_ms, _sim_token, result in valid_for_round:
            pool_sqls_now = [c.sql for c in pool]
            sim_max = _similarity_max(
                sql,
                pool_sqls_now,
                k_neighbors=params.k_neighbors,
                w_tok=w.get("tok", 0.6),
                w_ast=w.get("ast", 0.3),
                w_emb=w.get("emb", 0.1),
            )
            if sim_max >= params.sim_threshold:
                reject_similarity += 1
                rej_sim_round += 1
                continue
            sig = result_signature(result, sort_rows=True) if result and result.ok and result.rows else None
            c = SqlFactoryCandidate(
                sql=sql,
                phase=ph,
                round_idx=round_idx,
                exec_ok=True,
                error_type=None,
                error_message=None,
                tables_used=tables_used,
                similarity_max=sim_max,
                score=0.0,
                meta={"exec_time_ms": exec_time_ms},
                result_signature=sig,
            )
            c.score = compute_score(c, params.scoring)
            pool.append(c)
            recent_added_sim.append(c.similarity_max)
            added_this_round += 1

        if added_this_round == 0:
            rounds_with_no_add += 1
        else:
            rounds_with_no_add = 0
        round_best = max((c.score for c in pool), default=0.0)
        if round_best > best_score_so_far:
            best_score_so_far = round_best
            rounds_since_best_improved = 0
        else:
            rounds_since_best_improved += 1

        round_log["preflight_ok"] = added_this_round
        round_log["reject_similarity"] = rej_sim_round
        round_log["pool_size_after"] = len(pool)
        top3 = sorted(pool, key=lambda x: x.score, reverse=True)[:3]
        round_log["top3"] = [
            {
                "sql": (c.sql[:500] + "..." if len(c.sql) > 500 else c.sql),
                "score": c.score,
                "similarity_max": c.similarity_max,
                "tables_used": list(c.tables_used),
            }
            for c in top3
        ]
        rounds_log.append(round_log)

    # Convert pool to list[CandidateResult] for sampling layer; keep best for artifact
    def pool_to_candidates(p: list[SqlFactoryCandidate]) -> list[CandidateResult]:
        out: list[CandidateResult] = []
        for i, c in enumerate(p):
            out.append(
                CandidateResult(
                    attempt_id=i,
                    raw_text=c.sql,
                    sql=c.sql,
                    preflight_ok=c.exec_ok,
                    preflight_error_type=c.error_type,
                    exec_ok=c.exec_ok,
                    exec_error=c.error_message,
                    exec_time_ms=c.meta.get("exec_time_ms"),
                    result_signature=c.result_signature,
                    gen_params={"score": c.score, "phase": c.phase, "round_idx": c.round_idx},
                    score=c.score,
                )
            )
        return out

    fallback_used = False
    if pool:
        best = max(pool, key=lambda c: c.score)
        final_sql = best.sql
        final_meta = {
            "score": best.score,
            "similarity_max": best.similarity_max,
            "tables_used": list(best.tables_used),
            "phase": best.phase,
            "round_idx": best.round_idx,
        }
        candidates_out = pool_to_candidates(pool)
    else:
        # Fallback: single-shot; do NOT apply similarity gate (return as-is with fallback_used=True)
        raw_fb = ""
        try:
            raw_fb = model.generate_sql(question=question, schema=schema, messages=messages)
            final_sql = strip_sql_fences(raw_fb or "")
            if final_sql:
                preflight_sql, exec_ok, _et, _em, _tu, _et_ms, res = _preflight_one(
                    final_sql, db_path, dialect, exec_timeout
                )
                if exec_ok:
                    final_sql = preflight_sql
                    sig = result_signature(res, sort_rows=True) if res and res.ok and res.rows else None
                else:
                    sig = None
            else:
                sig = None
            fallback_used = True
        except Exception:
            final_sql = ""
            sig = None
        final_meta = {"fallback": True, "fallback_used": True}
        candidates_out = [
            CandidateResult(
                attempt_id=0,
                raw_text=raw_fb or "",
                sql=final_sql,
                preflight_ok=bool(final_sql),
                preflight_error_type=None if final_sql else "no_sql",
                exec_ok=bool(final_sql),
                exec_error=None,
                exec_time_ms=None,
                result_signature=sig,
                gen_params={"fallback_used": True},
            )
        ]

    # Top error messages by (type, msg_head), take top 5 overall
    top_errors = sorted(
        [{"error_type": k[0], "message_head": k[1], "count": v} for k, v in error_message_counts.items()],
        key=lambda x: -x["count"],
    )[:5]
    avg_preflight_ms = sum(preflight_times_ms) / len(preflight_times_ms) if preflight_times_ms else 0.0

    effective_params = {
        "max_rounds": params.max_rounds,
        "warmup_rounds": params.warmup_rounds,
        "gen_batch": params.gen_batch,
        "exp_batch": params.exp_batch,
        "target_pool_size": params.target_pool_size,
        "sim_threshold": params.sim_threshold,
        "stop_on_saturation": params.stop_on_saturation,
        "saturation_patience": params.saturation_patience,
        "no_progress_patience": params.no_progress_patience,
    }
    artifact = {
        "task_id": task_id,
        "params": effective_params,
        "rounds": rounds_log,
        "final": {
            "sql": (final_sql[:500] + "..." if len(final_sql) > 500 else final_sql),
            "meta": {**final_meta, "fallback_used": fallback_used},
        },
        "summary": {
            "avg_rounds": len(rounds_log),
            "avg_pool_size": len(pool),
            "reject_exec_fail_rate": reject_exec_fail / max(1, reject_exec_fail + reject_similarity + len(pool)),
            "reject_similarity_rate": reject_similarity / max(1, reject_exec_fail + reject_similarity + len(pool)),
            "exploration_vs_exploitation_ratio": exploration_rounds / max(1, exploitation_rounds),
            "exec_fail_breakdown": exec_fail_breakdown,
            "top_error_messages": top_errors,
            "avg_preflight_ms": round(avg_preflight_ms, 2),
        },
    }
    if run_dir:
        raw_dir = run_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        art_path = raw_dir / f"sql_factory_{task_id}.json"
        try:
            art_path.write_text(json.dumps(artifact, indent=2, default=str), encoding="utf-8")
        except Exception:
            pass

    return candidates_out, artifact, task_timeout_hit
