"""
Hybrid architecture: self-consistency + optional SGR grounding + optional expansion.

Variant A (expansion_enabled=False): SC + optional SGR grounding
Variant B (expansion_enabled=True): SC + expansion + optional SGR grounding

Phase 2: generate K independent SQL candidates in parallel/sequential,
run preflight/exec on each, build CandidateResults.

Phase 3: seed expansion — pick best initial candidates as seeds, generate M
variations per seed, filter by Jaccard similarity, merge into candidate pool.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from evalsuite.architectures.plain import _run_preflight_exec
from evalsuite.architectures.similarity import token_jaccard
from evalsuite.core.types import CandidateResult
from evalsuite.pipeline.aggregation import aggregate
from evalsuite.pipeline.result_signature import result_signature
from evalsuite.pipeline.sql_sanitize import has_placeholders, strip_sql_fences
from evalsuite.pipeline.utils import edit_distance

# ---------------------------------------------------------------------------
# Schema validation: post-generation fix of table/column names
# ---------------------------------------------------------------------------

_TABLE_RE = re.compile(r"\b(?:FROM|JOIN)\s+(\w+)", re.IGNORECASE)
_QUALIFIED_RE = re.compile(r"\b(\w+)\.(\w+)\b")


def _fuzzy_match(
    name: str,
    candidates: list[str],
    max_distance: int = 2,
) -> str | None:
    """Find best fuzzy match for *name* among *candidates* within *max_distance*."""
    best: str | None = None
    best_dist = max_distance + 1
    for c in candidates:
        d = edit_distance(name, c, case_sensitive=False)
        if d == 0:
            return None  # exact match — nothing to fix
        if d <= max_distance and d < best_dist:
            best = c
            best_dist = d
    return best


def validate_sql_schema(
    sql: str,
    schema_info: dict[str, list[str]] | None,
) -> tuple[str, int]:
    """Validate and fix table/column names in *sql* against *schema_info*.

    *schema_info* maps ``table_name -> [column_names]``.

    Returns ``(corrected_sql, fix_count)``.
    """
    if not sql or not schema_info:
        return sql, 0

    fix_count = 0
    corrected = sql

    # Build case-insensitive lookup for tables
    table_names = list(schema_info.keys())
    table_lower: dict[str, str] = {t.lower(): t for t in table_names}

    # --- Fix table names in FROM/JOIN ---
    tables_in_sql = _TABLE_RE.findall(corrected)
    for tbl in dict.fromkeys(tables_in_sql):  # unique, order-preserving
        if tbl.lower() in table_lower:
            continue  # already correct
        match = _fuzzy_match(tbl, table_names, max_distance=2)
        if match:
            pattern = re.compile(r"\b" + re.escape(tbl) + r"\b")
            corrected = pattern.sub(match, corrected)
            fix_count += 1

    # --- Fix qualified column refs (table.column) ---
    qualified_refs = _QUALIFIED_RE.findall(corrected)
    for tbl, col in dict.fromkeys(qualified_refs):
        real_table = table_lower.get(tbl.lower())
        if not real_table or real_table not in schema_info:
            continue
        cols = schema_info[real_table]
        col_lower = {c.lower(): c for c in cols}
        if col.lower() in col_lower:
            continue  # already correct
        match = _fuzzy_match(col, cols, max_distance=2)
        if match:
            old = f"{tbl}.{col}"
            new = f"{tbl}.{match}"
            corrected = corrected.replace(old, new)
            fix_count += 1

    return corrected, fix_count


@dataclass
class HybridParams:
    """Parameters for hybrid architecture.

    Supports two variants controlled by ``expansion_enabled``:
    - Variant A (False): SC + optional SGR grounding
    - Variant B (True):  SC + expansion + optional SGR grounding
    """

    # Phase 1: SGR enrichment
    sgr_grounding: bool = False

    # Phase 2: Initial candidates
    initial_candidates: int = 5
    temperature: float = 0.7
    top_p: float = 0.9
    parallelism: str = "parallel"  # parallel | sequential
    max_workers: int = 5
    generation_timeout: int = 30

    # Phase 3: Expansion (Variant B only)
    expansion_enabled: bool = True
    expansion_seeds: int = 2
    expansion_per_seed: int = 2
    expansion_sim_threshold: float = 0.85
    expansion_timeout: int = 15

    # Phase 4: Aggregation
    aggregation_mode: str = "hybrid"
    execution_timeout: int = 30


def build_hybrid_params(config_params: dict) -> HybridParams:
    """Build ``HybridParams`` from a raw config dict (e.g. architecture.params)."""
    p = config_params
    return HybridParams(
        sgr_grounding=bool(p.get("sgr_grounding", False)),
        initial_candidates=int(p.get("initial_candidates", 5)),
        temperature=float(p.get("temperature", 0.7)),
        top_p=float(p.get("top_p", 0.9)),
        parallelism=p.get("parallelism", "parallel"),
        max_workers=int(p.get("max_workers", 5)),
        generation_timeout=int(p.get("generation_timeout", 30)),
        expansion_enabled=bool(p.get("expansion_enabled", True)),
        expansion_seeds=int(p.get("expansion_seeds", 2)),
        expansion_per_seed=int(p.get("expansion_per_seed", 2)),
        expansion_sim_threshold=float(p.get("expansion_sim_threshold", 0.85)),
        expansion_timeout=int(p.get("expansion_timeout", 15)),
        aggregation_mode=p.get("aggregation_mode", "hybrid"),
        execution_timeout=int(p.get("execution_timeout", 30)),
    )


# ---------------------------------------------------------------------------
# Phase 2: generate K independent candidates
# ---------------------------------------------------------------------------


def _generate_one_candidate(
    attempt_id: int,
    model: Any,
    question: str,
    schema: str | None,
    messages: list[dict[str, Any]] | None,
    params: HybridParams,
    base_seed: int,
) -> tuple[int, str, str, dict[str, Any]]:
    """Generate a single SQL candidate.

    Returns ``(attempt_id, raw_text, cleaned_sql, gen_params)``.
    ``cleaned_sql`` is empty on failure or placeholder detection.
    """
    seed = base_seed + attempt_id
    gen_params: dict[str, Any] = {
        "temperature": params.temperature,
        "top_p": params.top_p,
        "seed": seed,
        "attempt_id": attempt_id,
    }
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
        gen_params["gen_error"] = str(e)
        return attempt_id, "", "", gen_params

    sql = strip_sql_fences(raw)

    # Check for placeholders
    has_ph, ph_reason = has_placeholders(sql)
    if has_ph:
        gen_params["placeholder"] = ph_reason
        return attempt_id, raw, "", gen_params

    return attempt_id, raw, sql, gen_params


def _build_candidate_result(
    attempt_id: int,
    raw_text: str,
    sql: str,
    gen_params: dict[str, Any],
    db_path: str,
    dialect: str,
    exec_timeout: int | None,
) -> CandidateResult:
    """Build a CandidateResult by running preflight/exec on *sql*."""
    preflight_ok = True
    preflight_error_type: str | None = None
    exec_ok = False
    exec_error: str | None = None
    exec_time_ms: float | None = None
    sig: str | None = None

    if not sql:
        # generation or placeholder failure
        preflight_ok = False
        if gen_params.get("gen_error"):
            preflight_error_type = "pred_generation_fail"
        elif gen_params.get("placeholder"):
            preflight_error_type = "pred_invalid_sql"
        else:
            preflight_error_type = "no_sql"
    else:
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
            sig = result_signature(result, sort_rows=True)

    return CandidateResult(
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


# ---------------------------------------------------------------------------
# Phase 3: seed expansion (Variant B)
# ---------------------------------------------------------------------------


def _select_seeds(
    candidates: list[CandidateResult],
    num_seeds: int,
    sim_threshold: float = 0.85,
) -> list[CandidateResult]:
    """Pick up to *num_seeds* diverse seeds from initial candidates.

    Selection priority: exec_ok first, then preflight_ok.
    Deduplication: greedily reject candidates whose token Jaccard to any
    already-selected seed is >= *sim_threshold*.
    """
    if not candidates or num_seeds <= 0:
        return []

    # Sort: exec_ok first, then preflight_ok, then by exec_time (fast first)
    ranked = sorted(
        candidates,
        key=lambda c: (
            not c.exec_ok,
            not c.preflight_ok,
            c.exec_time_ms if c.exec_time_ms is not None else float("inf"),
            c.attempt_id,
        ),
    )

    seeds: list[CandidateResult] = []
    for c in ranked:
        if not c.sql:
            continue
        # Check Jaccard against already-selected seeds
        too_similar = any(token_jaccard(c.sql, s.sql) >= sim_threshold for s in seeds)
        if not too_similar:
            seeds.append(c)
        if len(seeds) >= num_seeds:
            break
    return seeds


def _expand_one(
    seed_sql: str,
    question: str,
    schema: str | None,
    model: Any,
    temperature: float,
    top_p: float,
    base_seed: int,
    index: int,
) -> str:
    """Generate a single expansion variation from *seed_sql*.

    Returns cleaned SQL or empty string on failure.
    """
    try:
        user = (
            f"Question: {question}\n\n"
            f"Reference SQL (vary this):\n{seed_sql}\n\n"
            f"Provide a different but equivalent SQL query."
        )
        if schema:
            user = f"{schema}\n\n{user}"
        raw = model.generate_sql(
            question=user,
            schema=None,
            messages=None,
            temperature=temperature,
            top_p=top_p,
            seed=base_seed + index,
        )
        sql = strip_sql_fences(raw or "")
        # Reject if identical to seed
        if sql == seed_sql:
            return ""
        # Reject placeholders
        has_ph, _ = has_placeholders(sql)
        if has_ph:
            return ""
        return sql
    except Exception:
        return ""


def _run_expansion(
    seeds: list[CandidateResult],
    question: str,
    schema: str | None,
    model: Any,
    params: HybridParams,
    db_path: str,
    dialect: str,
    exec_timeout: int | None,
    next_attempt_id: int,
    base_seed: int = 200,
) -> tuple[list[CandidateResult], dict[str, Any]]:
    """Run Phase 3 expansion on selected seeds.

    For each seed, generate *expansion_per_seed* variations. Filter by
    Jaccard similarity threshold. Build CandidateResult for each accepted
    variation.

    Returns ``(expansion_candidates, expansion_artifact)``.
    """
    if not seeds:
        return [], {"seeds_used": 0, "variations_requested": 0, "variations_generated": 0, "variations_accepted": 0}

    per_seed = params.expansion_per_seed
    threshold = params.expansion_sim_threshold
    total_requested = len(seeds) * per_seed

    # Collect all expansion SQL strings (parallel or sequential)
    expansion_tasks: list[tuple[str, int]] = []  # (seed_sql, global_index)
    for si, seed in enumerate(seeds):
        for vi in range(per_seed):
            global_idx = si * per_seed + vi
            expansion_tasks.append((seed.sql, global_idx))

    raw_sqls: list[tuple[str, str]] = []  # (seed_sql, expanded_sql)

    if params.parallelism == "parallel" and len(expansion_tasks) > 1:
        workers = min(params.max_workers, len(expansion_tasks))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [
                ex.submit(
                    _expand_one,
                    seed_sql,
                    question,
                    schema,
                    model,
                    params.temperature,
                    params.top_p,
                    base_seed,
                    gidx,
                )
                for seed_sql, gidx in expansion_tasks
            ]
            for (seed_sql, _gidx), f in zip(expansion_tasks, futures):
                try:
                    sql = f.result(timeout=params.expansion_timeout)
                    if sql:
                        raw_sqls.append((seed_sql, sql))
                except (FuturesTimeoutError, Exception):
                    pass
    else:
        for seed_sql, gidx in expansion_tasks:
            try:
                sql = _expand_one(
                    seed_sql,
                    question,
                    schema,
                    model,
                    params.temperature,
                    params.top_p,
                    base_seed,
                    gidx,
                )
                if sql:
                    raw_sqls.append((seed_sql, sql))
            except Exception:
                pass

    # Filter by similarity threshold: reject if too similar to the seed
    accepted: list[str] = []
    for seed_sql, exp_sql in raw_sqls:
        sim = token_jaccard(exp_sql, seed_sql)
        if sim < threshold:
            accepted.append(exp_sql)

    # Build CandidateResult for each accepted expansion
    expansion_candidates: list[CandidateResult] = []
    for i, sql in enumerate(accepted):
        aid = next_attempt_id + i
        gen_params: dict[str, Any] = {
            "source": "expansion",
            "attempt_id": aid,
        }
        c = _build_candidate_result(
            attempt_id=aid,
            raw_text=sql,
            sql=sql,
            gen_params=gen_params,
            db_path=db_path,
            dialect=dialect,
            exec_timeout=exec_timeout,
        )
        expansion_candidates.append(c)

    artifact: dict[str, Any] = {
        "seeds_used": len(seeds),
        "seed_attempt_ids": [s.attempt_id for s in seeds],
        "variations_requested": total_requested,
        "variations_generated": len(raw_sqls),
        "variations_accepted": len(accepted),
    }
    return expansion_candidates, artifact


# ---------------------------------------------------------------------------
# Diagnostic Retry: when all candidates fail with the same error
# ---------------------------------------------------------------------------


def _classify_error(c: CandidateResult) -> str | None:
    """Return a canonical error key for a failed candidate.

    Uses ``exec_error`` text if available, otherwise ``preflight_error_type``.
    Returns ``None`` for successful candidates.
    """
    if c.exec_ok:
        return None
    if c.exec_error:
        return c.exec_error.strip()
    if c.preflight_error_type:
        return c.preflight_error_type
    return "unknown_error"


def _should_diagnostic_retry(candidates: list[CandidateResult]) -> tuple[bool, str | None]:
    """Decide whether diagnostic retry should run.

    Returns ``(should_retry, common_error)``.
    Retry runs only when:
    - No candidate has ``exec_ok``
    - All failed candidates share the same error key
    """
    if not candidates:
        return False, None
    if any(c.exec_ok for c in candidates):
        return False, None

    errors = [_classify_error(c) for c in candidates]
    non_none = [e for e in errors if e is not None]
    if not non_none:
        return False, None

    unique = set(non_none)
    if len(unique) == 1:
        return True, non_none[0]
    return False, None


def _generate_retry_candidates(
    *,
    model: Any,
    question: str,
    schema: str | None,
    failed_sql: str,
    error_text: str,
    params: HybridParams,
    base_seed: int,
    db_path: str,
    dialect: str,
    exec_timeout: int | None,
    next_attempt_id: int,
    retry_count: int = 2,
) -> tuple[list[CandidateResult], dict[str, Any]]:
    """Generate 1-2 retry candidates with the error diagnosis in the prompt.

    Returns ``(retry_candidates, retry_artifact)``.
    """
    retry_prompt = (
        f"Question: {question}\n\n"
        f"The following SQL was generated but failed:\n```sql\n{failed_sql}\n```\n\n"
        f"Error: {error_text}\n\n"
        f"Fix this SQL to resolve the error. Return ONLY the corrected SQL query."
    )
    if schema:
        retry_prompt = f"{schema}\n\n{retry_prompt}"

    retry_candidates: list[CandidateResult] = []
    retry_temperature = 0.3  # more deterministic

    for i in range(retry_count):
        aid = next_attempt_id + i
        seed = base_seed + aid
        gen_params: dict[str, Any] = {
            "source": "diagnostic_retry",
            "temperature": retry_temperature,
            "seed": seed,
            "attempt_id": aid,
        }
        try:
            raw = model.generate_sql(
                question=retry_prompt,
                schema=None,  # schema already in prompt
                messages=None,
                temperature=retry_temperature,
                top_p=params.top_p,
                seed=seed,
            )
        except Exception as e:
            gen_params["gen_error"] = str(e)
            retry_candidates.append(
                _build_candidate_result(
                    attempt_id=aid,
                    raw_text="",
                    sql="",
                    gen_params=gen_params,
                    db_path=db_path,
                    dialect=dialect,
                    exec_timeout=exec_timeout,
                )
            )
            continue

        sql = strip_sql_fences(raw or "")
        has_ph, ph_reason = has_placeholders(sql)
        if has_ph:
            gen_params["placeholder"] = ph_reason
            sql = ""

        retry_candidates.append(
            _build_candidate_result(
                attempt_id=aid,
                raw_text=raw or "",
                sql=sql,
                gen_params=gen_params,
                db_path=db_path,
                dialect=dialect,
                exec_timeout=exec_timeout,
            )
        )

    artifact: dict[str, Any] = {
        "triggered": True,
        "common_error": error_text,
        "retry_count": retry_count,
        "retry_candidates_generated": len(retry_candidates),
        "retry_exec_ok": sum(1 for c in retry_candidates if c.exec_ok),
    }
    return retry_candidates, artifact


def _write_artifact(artifact: dict[str, Any], task_id: str, run_dir: Path | None) -> None:
    """Write hybrid artifact JSON to ``runs/<run_id>/raw/hybrid_<task_id>.json``."""
    if not run_dir:
        return
    raw_dir = run_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    art_path = raw_dir / f"hybrid_{task_id}.json"
    try:
        art_path.write_text(json.dumps(artifact, indent=2, default=str), encoding="utf-8")
    except Exception:
        pass


def run_hybrid(
    *,
    task_id: str,
    get_context: Callable[[], dict[str, Any]],
    model: Any,
    db_path: str,
    dialect: str,
    params: HybridParams,
    sql_execution_timeout_sec: int | None = None,
    base_seed: int = 42,
    run_dir: Path | None = None,
    schema_info: dict[str, list[str]] | None = None,
) -> tuple[str, list[CandidateResult], dict[str, Any]]:
    """Run hybrid pipeline: Phase 2 (generation) + optional Phase 3 (expansion) + aggregation.

    *get_context* returns ``{"question": str, "schema": str | None, "messages": ...}``.
    *schema_info* maps ``table_name -> [column_names]`` for post-generation
    schema validation (fuzzy-fix wrong identifiers).

    Returns ``(selected_sql, all_candidates, artifact)`` where *artifact* is a dict
    suitable for embedding in the task result's ``extra``.
    """
    pipeline_start = time.perf_counter()
    ctx = get_context()
    question = ctx.get("question", "")
    schema = ctx.get("schema")
    messages = ctx.get("messages")

    k = params.initial_candidates
    exec_timeout = params.execution_timeout or sql_execution_timeout_sec

    # --- Generation phase ---
    gen_start = time.perf_counter()

    def _gen(i: int) -> tuple[int, str, str, dict[str, Any]]:
        return _generate_one_candidate(
            attempt_id=i,
            model=model,
            question=question,
            schema=schema,
            messages=messages,
            params=params,
            base_seed=base_seed,
        )

    raw_results: list[tuple[int, str, str, dict[str, Any]]] = []

    if params.parallelism == "parallel" and k > 1:
        workers = min(params.max_workers, k)
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(_gen, i) for i in range(k)]
            for idx, f in enumerate(futures):
                try:
                    raw_results.append(f.result(timeout=params.generation_timeout))
                except FuturesTimeoutError:
                    raw_results.append((idx, "", "", {"gen_error": "timeout"}))
                except Exception as e:
                    raw_results.append((idx, "", "", {"gen_error": str(e)}))
    else:
        for i in range(k):
            try:
                raw_results.append(_gen(i))
            except Exception as e:
                raw_results.append((i, "", "", {"gen_error": str(e)}))

    gen_time_ms = (time.perf_counter() - gen_start) * 1000

    # --- Schema validation + Preflight + execution phase ---
    candidates_fixed_count = 0
    exec_start = time.perf_counter()
    candidates: list[CandidateResult] = []
    for attempt_id, raw_text, sql, gen_params in raw_results:
        if sql and schema_info:
            fixed_sql, n_fixes = validate_sql_schema(sql, schema_info)
            if n_fixes > 0:
                candidates_fixed_count += 1
                gen_params["schema_fixes"] = n_fixes
                sql = fixed_sql
        c = _build_candidate_result(
            attempt_id=attempt_id,
            raw_text=raw_text,
            sql=sql,
            gen_params=gen_params,
            db_path=db_path,
            dialect=dialect,
            exec_timeout=exec_timeout,
        )
        candidates.append(c)
    exec_time_ms = (time.perf_counter() - exec_start) * 1000

    # --- Diagnostic Retry ---
    retry_artifact: dict[str, Any] | None = None
    retry_candidates: list[CandidateResult] = []

    should_retry, common_error = _should_diagnostic_retry(candidates)
    if should_retry and common_error is not None:
        # Pick the first candidate with SQL as the example for the retry prompt
        example_sql = next((c.sql for c in candidates if c.sql), "")
        next_aid = max(c.attempt_id for c in candidates) + 1
        retry_candidates, retry_artifact = _generate_retry_candidates(
            model=model,
            question=question,
            schema=schema,
            failed_sql=example_sql,
            error_text=common_error,
            params=params,
            base_seed=base_seed + k + 50,
            db_path=db_path,
            dialect=dialect,
            exec_timeout=exec_timeout,
            next_attempt_id=next_aid,
        )
    else:
        retry_artifact = {"triggered": False}

    # --- Phase 3: Expansion (Variant B) with Smart Early Stop ---
    expansion_artifact: dict[str, Any] | None = None
    expansion_candidates: list[CandidateResult] = []
    expansion_skipped_reason: str | None = None

    # Include retry candidates in the pool for seed selection
    candidates_for_expansion = candidates + retry_candidates

    if params.expansion_enabled and candidates_for_expansion:
        # Smart Early Stop checks
        exec_ok_candidates = [c for c in candidates_for_expansion if c.exec_ok]

        if not exec_ok_candidates:
            # (a) No exec_ok → skip expansion (diagnostic retry already ran)
            expansion_skipped_reason = "no_exec_ok"
            expansion_artifact = {
                "seeds_used": 0,
                "seed_attempt_ids": [],
                "variations_requested": 0,
                "variations_generated": 0,
                "variations_accepted": 0,
                "expansion_time_ms": 0.0,
                "expansion_skipped_reason": expansion_skipped_reason,
            }
        elif len(set(c.result_signature for c in exec_ok_candidates)) == 1:
            # (b) All exec_ok share one signature → consensus, skip expansion
            expansion_skipped_reason = "consensus"
            expansion_artifact = {
                "seeds_used": 0,
                "seed_attempt_ids": [],
                "variations_requested": 0,
                "variations_generated": 0,
                "variations_accepted": 0,
                "expansion_time_ms": 0.0,
                "expansion_skipped_reason": expansion_skipped_reason,
            }
        else:
            # Multiple distinct signatures → expansion needed
            exp_start = time.perf_counter()
            seeds = _select_seeds(
                candidates_for_expansion,
                num_seeds=params.expansion_seeds,
                sim_threshold=params.expansion_sim_threshold,
            )
            if seeds:
                next_aid = max(c.attempt_id for c in candidates_for_expansion) + 1
                expansion_candidates, expansion_artifact = _run_expansion(
                    seeds=seeds,
                    question=question,
                    schema=schema,
                    model=model,
                    params=params,
                    db_path=db_path,
                    dialect=dialect,
                    exec_timeout=exec_timeout,
                    next_attempt_id=next_aid,
                    base_seed=base_seed + k + 100,
                )
                expansion_artifact["expansion_time_ms"] = (time.perf_counter() - exp_start) * 1000
                expansion_artifact["expansion_skipped_reason"] = None
            else:
                expansion_artifact = {
                    "seeds_used": 0,
                    "seed_attempt_ids": [],
                    "variations_requested": 0,
                    "variations_generated": 0,
                    "variations_accepted": 0,
                    "expansion_time_ms": 0.0,
                    "note": "no valid seeds for expansion",
                    "expansion_skipped_reason": None,
                }

    # Merge initial + retry + expansion candidates for aggregation
    all_candidates = candidates + retry_candidates + expansion_candidates

    # --- Aggregation phase ---
    if not all_candidates:
        empty_artifact: dict[str, Any] = {
            "task_id": task_id,
            "variant": "B" if params.expansion_enabled else "A",
            "params": asdict(params),
            "initial_candidates_requested": k,
            "initial_candidates_generated": 0,
            "initial_candidates": [],
            "candidates_fixed_count": 0,
            "generation_time_ms": gen_time_ms,
            "execution_time_ms": exec_time_ms,
            "latency_ms": (time.perf_counter() - pipeline_start) * 1000,
            "aggregation": {"aggregation_reason": "no_candidates", "votes": {}, "total_pool_size": 0},
        }
        _write_artifact(empty_artifact, task_id, run_dir)
        return "", [], empty_artifact

    # --- Fix Fallback: if no candidate has exec_ok or preflight_ok, return empty SQL ---
    has_any_ok = any(c.exec_ok or c.preflight_ok for c in all_candidates)
    if not has_any_ok:
        sel_id = all_candidates[0].attempt_id
        sel_sql = ""
        reason = "all_candidates_failed"
        votes: dict[str, Any] = {}
    else:
        sel_id, sel_sql, reason, votes = aggregate(all_candidates, params.aggregation_mode)

    # Build artifact
    def _candidate_dict(c: CandidateResult) -> dict[str, Any]:
        return {
            "attempt_id": c.attempt_id,
            "sql": c.sql,
            "preflight_ok": c.preflight_ok,
            "preflight_error_type": c.preflight_error_type,
            "exec_ok": c.exec_ok,
            "exec_error": c.exec_error,
            "exec_time_ms": c.exec_time_ms,
            "result_signature": c.result_signature,
            "gen_params": c.gen_params,
        }

    artifact: dict[str, Any] = {
        "task_id": task_id,
        "variant": "B" if params.expansion_enabled else "A",
        "params": asdict(params),
        "initial_candidates_requested": k,
        "initial_candidates_generated": len(candidates),
        "candidates_fixed_count": candidates_fixed_count,
        "generation_time_ms": gen_time_ms,
        "execution_time_ms": exec_time_ms,
        "initial_candidates": [_candidate_dict(c) for c in candidates],
        "aggregation": {
            "selected_attempt_id": sel_id,
            "selected_sql": sel_sql,
            "aggregation_reason": reason,
            "votes": votes,
            "total_pool_size": len(all_candidates),
        },
    }

    if retry_artifact is not None:
        artifact["diagnostic_retry"] = retry_artifact
        if retry_candidates:
            artifact["diagnostic_retry"]["candidates"] = [_candidate_dict(c) for c in retry_candidates]

    if expansion_artifact is not None:
        artifact["expansion"] = expansion_artifact
        artifact["expansion"]["candidates"] = [_candidate_dict(c) for c in expansion_candidates]

    artifact["expansion_skipped_reason"] = expansion_skipped_reason
    artifact["latency_ms"] = (time.perf_counter() - pipeline_start) * 1000
    _write_artifact(artifact, task_id, run_dir)

    return sel_sql, all_candidates, artifact
