"""
Eval architectures: plain (default) and self_consistency.
Self-consistency: K independent generations per example + aggregation to final SQL.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field
from typing import Any

from evalsuite.adapters.db.duckdb import preflight_and_execute_db
from evalsuite.adapters.db.sqlite import execute_sql as sqlite_execute
from evalsuite.core.types import CandidateResult, ExecResult
from evalsuite.pipeline.aggregation import aggregate
from evalsuite.pipeline.result_signature import result_signature
from evalsuite.pipeline.sql_sanitize import strip_sql_fences


@dataclass
class ArchitectureConfig:
    name: str = "plain"
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class SelfConsistencyParams:
    num_samples: int = 5
    temperature: float = 0.7
    top_p: float = 0.9
    seed_strategy: str = "per_attempt"  # fixed | per_attempt | random
    base_seed: int = 42
    parallelism: str = "sequential"  # sequential | parallel
    max_workers: int = 3
    aggregation_mode: str = "hybrid"
    generation_timeout_per_attempt: int | None = None
    execution_timeout_per_candidate: int | None = None
    overall_timeout_per_example: int | None = None
    result_signature_sort_rows: bool = True
    result_signature_max_rows: int | None = None


def _run_preflight_exec(
    db_path: str,
    sql: str,
    dialect: str,
    timeout_sec: int | None = None,
) -> tuple[ExecResult, str | None, float]:
    """Returns (ExecResult, error_type, exec_time_ms). For sqlite, preflight_ok is inferred from exec (no separate EXPLAIN)."""
    t0 = time.perf_counter()
    if dialect == "duckdb":
        result, err_type = preflight_and_execute_db(db_path, sql, timeout_sec=timeout_sec)
        exec_time_ms = (time.perf_counter() - t0) * 1000
        return result, err_type, exec_time_ms
    # sqlite: no separate preflight, just execute
    result = sqlite_execute(db_path, sql)
    exec_time_ms = (time.perf_counter() - t0) * 1000
    err_type = "pred_exec_fail" if not result.ok else None
    return result, err_type, exec_time_ms


def run_self_consistency(
    *,
    task_id: str,
    get_context: Callable[[], dict[str, Any]],  # question, schema?, messages?
    model: Any,  # ModelAdapter with generate_sql(..., temperature=, top_p=, seed=)
    db_path: str,
    dialect: str,
    params: SelfConsistencyParams,
    sql_execution_timeout_sec: int | None = None,
) -> tuple[str, list[dict[str, Any]], dict[str, Any], bool]:
    """
    Generate K candidates, run preflight/exec on each, aggregate to one SQL.
    get_context() returns dict with keys: question, schema (optional), messages (optional).
    Returns (selected_sql, candidates_as_dicts, aggregation_dict, task_timeout_hit).
    """
    ctx = get_context()
    question = ctx.get("question", "")
    schema = ctx.get("schema")
    messages = ctx.get("messages")

    num_samples = params.num_samples
    exec_timeout = params.execution_timeout_per_candidate or sql_execution_timeout_sec
    overall_timeout = params.overall_timeout_per_example
    start_wall = time.perf_counter()
    task_timeout_hit = False

    def gen_one(attempt_id: int) -> tuple[int, str, str, dict]:
        if overall_timeout and (time.perf_counter() - start_wall) > overall_timeout:
            return attempt_id, "", "", {"timeout": True}
        seed = None
        if params.seed_strategy == "per_attempt":
            seed = params.base_seed + attempt_id
        elif params.seed_strategy == "fixed":
            seed = params.base_seed
        try:
            raw = model.generate_sql(
                question=question,
                schema=schema,
                messages=messages,
                temperature=params.temperature,
                top_p=params.top_p,
                seed=seed,
            )
        except Exception as e:
            return attempt_id, "", "", {"gen_error": str(e)}
        sql = strip_sql_fences(raw)
        return attempt_id, raw, sql, {"temperature": params.temperature, "top_p": params.top_p, "seed": seed}

    if params.parallelism == "parallel" and num_samples > 1:
        candidates_raw: list[tuple[int, str, str, dict]] = []
        with ThreadPoolExecutor(max_workers=min(params.max_workers, num_samples)) as ex:
            futures = [ex.submit(gen_one, i) for i in range(num_samples)]
            for f in futures:
                try:
                    wait = params.generation_timeout_per_attempt or 120
                    candidates_raw.append(f.result(timeout=wait))
                except FuturesTimeoutError:
                    candidates_raw.append((len(candidates_raw), "", "", {"timeout": True}))
                except Exception as e:
                    candidates_raw.append((len(candidates_raw), "", "", {"gen_error": str(e)}))
    else:
        candidates_raw = [gen_one(i) for i in range(num_samples)]

    candidates: list[CandidateResult] = []
    for attempt_id, raw_text, sql, gen_params in candidates_raw:
        if task_timeout_hit:
            break
        if overall_timeout and (time.perf_counter() - start_wall) > overall_timeout:
            task_timeout_hit = True
            break
        preflight_ok = True
        preflight_error_type: str | None = None
        exec_ok = False
        exec_error: str | None = None
        exec_time_ms: float | None = None
        sig: str | None = None
        if not sql and gen_params.get("timeout"):
            preflight_ok = False
            preflight_error_type = "gen_timeout"
        elif not sql and gen_params.get("gen_error"):
            preflight_ok = False
            preflight_error_type = "pred_generation_fail"
        elif sql:
            result, err_type, et_ms = _run_preflight_exec(db_path, sql, dialect, timeout_sec=exec_timeout)
            exec_time_ms = et_ms
            exec_ok = result.ok
            exec_error = result.error
            preflight_error_type = err_type
            # DuckDB: preflight_ok = EXPLAIN passed (parse/bind ok); if only runtime failed, preflight_ok still True
            if dialect == "duckdb":
                preflight_ok = result.ok or (err_type == "pred_runtime_fail")
            else:
                preflight_ok = result.ok
            if result.ok and result.rows is not None:
                sig = result_signature(
                    result,
                    sort_rows=params.result_signature_sort_rows,
                    max_rows=params.result_signature_max_rows,
                )
        candidates.append(
            CandidateResult(
                attempt_id=attempt_id,
                raw_text=raw_text,
                sql=sql or "",
                preflight_ok=preflight_ok,
                preflight_error_type=preflight_error_type,
                exec_ok=exec_ok,
                exec_error=exec_error,
                exec_time_ms=exec_time_ms,
                result_signature=sig,
                gen_params=gen_params,
            )
        )

    if not candidates:
        return "", [], {"aggregation_reason": "no_candidates", "votes": {}}, task_timeout_hit

    sel_id, sel_sql, reason, votes = aggregate(candidates, params.aggregation_mode)
    agg_dict = {
        "selected_attempt_id": sel_id,
        "selected_sql": sel_sql,
        "aggregation_reason": reason,
        "votes": votes,
    }
    candidates_dicts = [
        {
            "attempt_id": c.attempt_id,
            "raw_text": c.raw_text,
            "sql": c.sql,
            "preflight_ok": c.preflight_ok,
            "preflight_error_type": c.preflight_error_type,
            "exec_ok": c.exec_ok,
            "exec_error": c.exec_error,
            "exec_time_ms": c.exec_time_ms,
            "result_signature": c.result_signature,
            "gen_params": c.gen_params,
        }
        for c in candidates
    ]
    return sel_sql, candidates_dicts, agg_dict, task_timeout_hit


def aggregate_candidates_from_sql_list(
    sql_list: list[tuple[int, str]],
    db_path: str,
    dialect: str,
    params: SelfConsistencyParams,
    sql_execution_timeout_sec: int | None = None,
    strip_sql_fn: Any | None = None,
) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    """
    Given a list of (attempt_id, sql) from e.g. K toolchain runs, run preflight/exec on each,
    build CandidateResults, aggregate. Returns (selected_sql, candidates_dicts, aggregation_dict).
    """
    if strip_sql_fn is None:
        strip_sql_fn = strip_sql_fences
    exec_timeout = params.execution_timeout_per_candidate or sql_execution_timeout_sec
    candidates: list[CandidateResult] = []
    for attempt_id, sql in sql_list:
        sql = strip_sql_fn(sql) if sql else ""
        preflight_ok = True
        preflight_error_type: str | None = None
        exec_ok = False
        exec_error: str | None = None
        exec_time_ms: float | None = None
        sig: str | None = None
        if not sql:
            preflight_ok = False
            preflight_error_type = "no_sql"
        elif sql:
            result, err_type, et_ms = _run_preflight_exec(db_path, sql, dialect, timeout_sec=exec_timeout)
            exec_time_ms = et_ms
            exec_ok = result.ok
            exec_error = result.error
            preflight_error_type = err_type
            if dialect == "duckdb":
                preflight_ok = result.ok or (err_type == "pred_runtime_fail")
            else:
                preflight_ok = result.ok
            if result.ok and result.rows is not None:
                sig = result_signature(
                    result,
                    sort_rows=params.result_signature_sort_rows,
                    max_rows=params.result_signature_max_rows,
                )
        candidates.append(
            CandidateResult(
                attempt_id=attempt_id,
                raw_text="",
                sql=sql or "",
                preflight_ok=preflight_ok,
                preflight_error_type=preflight_error_type,
                exec_ok=exec_ok,
                exec_error=exec_error,
                exec_time_ms=exec_time_ms,
                result_signature=sig,
                gen_params={"source": "toolchain"},
            )
        )
    if not candidates:
        return "", [], {"aggregation_reason": "no_candidates", "votes": {}}
    sel_id, sel_sql, reason, votes = aggregate(candidates, params.aggregation_mode)
    agg_dict = {
        "selected_attempt_id": sel_id,
        "selected_sql": sel_sql,
        "aggregation_reason": reason,
        "votes": votes,
    }
    candidates_dicts = [
        {
            "attempt_id": c.attempt_id,
            "raw_text": c.raw_text,
            "sql": c.sql,
            "preflight_ok": c.preflight_ok,
            "preflight_error_type": c.preflight_error_type,
            "exec_ok": c.exec_ok,
            "exec_error": c.exec_error,
            "exec_time_ms": c.exec_time_ms,
            "result_signature": c.result_signature,
            "gen_params": c.gen_params,
        }
        for c in candidates
    ]
    return sel_sql, candidates_dicts, agg_dict


def get_architecture_config(raw_config: dict[str, Any]) -> ArchitectureConfig:
    """Read architecture from config.raw or default."""
    arch = raw_config.get("architecture") or {}
    name = (arch.get("name") or "plain").lower().strip()
    # New model: "plain" -> single_shot
    if name == "plain":
        name = "plain"
    params = arch.get("params") or {}
    if name == "self_consistency":
        p = params.get("self_consistency") or params
        return ArchitectureConfig(
            name="self_consistency",
            params={
                "num_samples": int(p.get("num_samples", 5)),
                "temperature": float(p.get("temperature", 0.7)),
                "top_p": float(p.get("top_p", 0.9)),
                "seed_strategy": p.get("seed_strategy", "per_attempt"),
                "base_seed": int(p.get("base_seed", 42)),
                "parallelism": p.get("parallelism", "sequential"),
                "max_workers": int(p.get("max_workers", 3)),
                "aggregation_mode": (p.get("aggregation") or {}).get("mode") or p.get("aggregation_mode", "hybrid"),
                "generation_timeout_per_attempt": p.get("generation_timeout_per_attempt"),
                "execution_timeout_per_candidate": p.get("execution_timeout_per_candidate"),
                "overall_timeout_per_example": p.get("overall_timeout_per_example"),
                "result_signature_max_rows": p.get("result_signature_max_rows"),
            },
        )
    if name == "sql_factory":
        p = params
        scoring = p.get("scoring") or {}
        weights = p.get("weights") or {}
        models = p.get("models") or {}
        sampling = p.get("sampling") or {}
        return ArchitectureConfig(
            name="sql_factory",
            params={
                "max_rounds": int(p.get("max_rounds", 3)),
                "warmup_rounds": int(p.get("warmup_rounds", 1)),
                "gen_batch": int(p.get("gen_batch", 2)),
                "exp_batch": int(p.get("exp_batch", 2)),
                "target_pool_size": int(p.get("target_pool_size", 5)),
                "stop_on_saturation": bool(p.get("stop_on_saturation", True)),
                "saturation_patience": int(p.get("saturation_patience", 2)),
                "expansion_ratio_target": float(p.get("expansion_ratio_target", 0.7)),
                "sim_threshold": float(p.get("sim_threshold", 0.85)),
                "k_neighbors": int(p.get("k_neighbors", 5)),
                "weights": {
                    "tok": float(weights.get("tok", 0.6)),
                    "ast": float(weights.get("ast", 0.3)),
                    "emb": float(weights.get("emb", 0.1)),
                },
                "models": {
                    "generation": models.get("generation"),
                    "expansion": models.get("expansion"),
                    "management": models.get("management"),
                },
                "sampling": {
                    "generation": sampling.get("generation") or {"temperature": 0.8, "top_p": 0.9},
                    "expansion": sampling.get("expansion") or {"temperature": 0.8, "top_p": 0.9},
                    "management": sampling.get("management") or {"temperature": 0.2, "top_p": 1.0},
                },
                "generation_timeout_per_attempt": int(p.get("generation_timeout_per_attempt", 15)),
                "max_workers": int(p.get("max_workers", 5)),
                "parallelism": p.get("parallelism", "parallel"),
                "time_budget_per_task_sec": p.get("time_budget_per_task_sec", 30),
                "no_progress_patience": int(p.get("no_progress_patience", 2)),
                "best_score_stagnation_rounds": int(p.get("best_score_stagnation_rounds", 2)),
                "scoring": {
                    "bonus_tables_cap": int(scoring.get("bonus_tables_cap", 6)),
                    "bonus_per_table": float(scoring.get("bonus_per_table", 0.05)),
                    "similarity_penalty": float(scoring.get("similarity_penalty", 0.5)),
                    "complexity_bonus": bool(scoring.get("complexity_bonus", True)),
                    "complexity_bonus_cap": float(scoring.get("complexity_bonus_cap", 0.2)),
                    "tables_over_penalty_threshold": int(scoring.get("tables_over_penalty_threshold", 6)),
                    "tables_over_penalty": float(scoring.get("tables_over_penalty", 0.05)),
                },
            },
        )
    if name == "hybrid":
        return ArchitectureConfig(
            name="hybrid",
            params={
                "sgr_grounding": bool(params.get("sgr_grounding", False)),
                "initial_candidates": int(params.get("initial_candidates", 5)),
                "temperature": float(params.get("temperature", 0.7)),
                "top_p": float(params.get("top_p", 0.9)),
                "parallelism": params.get("parallelism", "parallel"),
                "max_workers": int(params.get("max_workers", 5)),
                "generation_timeout": int(params.get("generation_timeout", 30)),
                "expansion_enabled": bool(params.get("expansion_enabled", True)),
                "expansion_seeds": int(params.get("expansion_seeds", 2)),
                "expansion_per_seed": int(params.get("expansion_per_seed", 2)),
                "expansion_sim_threshold": float(params.get("expansion_sim_threshold", 0.85)),
                "expansion_timeout": int(params.get("expansion_timeout", 15)),
                "aggregation_mode": params.get("aggregation_mode", "hybrid"),
                "execution_timeout": int(params.get("execution_timeout", 30)),
            },
        )
    if name == "sgr":
        return ArchitectureConfig(name="sgr", params=dict(params))
    return ArchitectureConfig(name="plain", params={})


def build_self_consistency_params(config_params: dict[str, Any]) -> SelfConsistencyParams:
    return SelfConsistencyParams(
        num_samples=config_params.get("num_samples", 5),
        temperature=config_params.get("temperature", 0.7),
        top_p=config_params.get("top_p", 0.9),
        seed_strategy=config_params.get("seed_strategy", "per_attempt"),
        base_seed=config_params.get("base_seed", 42),
        parallelism=config_params.get("parallelism", "sequential"),
        max_workers=config_params.get("max_workers", 3),
        aggregation_mode=config_params.get("aggregation_mode", "hybrid"),
        generation_timeout_per_attempt=config_params.get("generation_timeout_per_attempt"),
        execution_timeout_per_candidate=config_params.get("execution_timeout_per_candidate"),
        overall_timeout_per_example=config_params.get("overall_timeout_per_example"),
        result_signature_max_rows=config_params.get("result_signature_max_rows"),
    )
