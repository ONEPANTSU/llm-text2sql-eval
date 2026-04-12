"""
SGR standalone: Grounding → Plan → K SQL candidates → Preflight+Execute → optional Repair.
Returns list[CandidateResult] with result_signature, score, exec_ok; artifact with grounding/plan/repair_attempts.
Parallel synthesis and parallel preflight to target ~40s per task.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from evalsuite.architectures.plain import _run_preflight_exec
from evalsuite.architectures.sgr.layer import run_sgr_grounding_and_plan
from evalsuite.architectures.sgr.prompts import (
    REPAIR_PROMPT,
    REPAIR_SYSTEM,
    SQL_SYNTHESIS_PROMPT,
    SQL_SYNTHESIS_SYSTEM,
)
from evalsuite.architectures.sgr.schema import SGRGrounding, grounding_to_dict, plan_to_dict
from evalsuite.architectures.sgr.utils import truncate_schema
from evalsuite.core.types import CandidateResult, ExecResult
from evalsuite.pipeline.preflight import extract_tables_from_sql
from evalsuite.pipeline.result_signature import result_signature
from evalsuite.pipeline.sql_sanitize import strip_sql_fences

log = logging.getLogger(__name__)

MAX_SCHEMA_CHARS = 12000
SQL_LENGTH_PENALTY_THRESHOLD = 2000
PENALTY_TABLE_OUTSIDE_GROUNDING = 0.2
PENALTY_LONG_SQL = 0.1
REPAIR_ERROR_TYPES = frozenset({"pred_bind_fail", "pred_runtime_fail"})


def _score_candidate(
    exec_ok: bool,
    sql: str,
    grounding: SGRGrounding,
) -> float:
    """v1: exec_ok -> 1.0; penalties for tables outside grounding and long SQL."""
    allowed_tables: set[str] = {x for x in (grounding.tables or []) if x}
    allowed_lower = {x.lower() for x in allowed_tables}
    if not exec_ok:
        return 0.0
    base = 1.0
    try:
        used = extract_tables_from_sql(sql)
    except Exception:
        used = set()
    for t in used:
        if t and t.lower() not in allowed_lower:
            base -= PENALTY_TABLE_OUTSIDE_GROUNDING
    if len(sql) > SQL_LENGTH_PENALTY_THRESHOLD:
        base -= PENALTY_LONG_SQL
    return max(0.0, base)


def _format_columns_per_table(grounding: SGRGrounding) -> str:
    if not grounding.columns:
        return "—"
    return "; ".join(f"{t}: {', '.join(c or [])}" for t, c in grounding.columns.items())


def _format_joins(grounding: SGRGrounding) -> str:
    if not grounding.joins:
        return "—"
    return "; ".join(f"{j.left_table}.{j.left_column}={j.right_table}.{j.right_column}" for j in grounding.joins)


def _format_filters(plan: Any) -> str:
    if not getattr(plan, "filters", None):
        return "—"
    parts = []
    for f in plan.filters:
        ref = getattr(f, "column_ref", "") or getattr(f, "raw", "")
        op = getattr(f, "operator", "")
        val = getattr(f, "value_hint", "")
        parts.append(f"{ref} {op} {val}".strip())
    return "; ".join(parts) if parts else "—"


def _format_plan_summary(plan: Any) -> str:
    parts = []
    if getattr(plan, "aggregations", None):
        parts.append("aggregations: " + ", ".join(f"{a.function}({a.column_ref})" for a in plan.aggregations))
    if getattr(plan, "group_by", None):
        parts.append("group_by: " + ", ".join(plan.group_by))
    if getattr(plan, "order_by", None):
        parts.append("order_by: " + ", ".join(f"{o.column_ref} {o.direction}" for o in plan.order_by))
    if getattr(plan, "limit", None) is not None:
        parts.append(f"limit: {plan.limit}")
    if getattr(plan, "distinct", False):
        parts.append("distinct: true")
    return "; ".join(parts) if parts else "—"


def run_sgr_standalone(
    *,
    task_id: str,
    get_context: Callable[[], dict[str, Any]],
    model: Any,
    db_path: str,
    dialect: str,
    params: dict[str, Any],
    sql_execution_timeout_sec: int | None = None,
    num_candidates: int = 6,
) -> tuple[list[CandidateResult], dict[str, Any], bool]:
    """
    Full SGR pipeline: grounding+plan → K SQL candidates → preflight/exec → optional repair → score.
    Returns (candidates, artifact, task_timeout_hit).
    """
    ctx = get_context()
    question = ctx.get("question", "")
    schema_raw = ctx.get("schema")
    schema_truncated = truncate_schema(schema_raw, max_chars=MAX_SCHEMA_CHARS)
    artifact: dict[str, Any] = {
        "task_id": task_id,
        "sgr": {"grounding": {}, "plan": {}, "repair_attempts": []},
    }
    task_timeout_hit = False

    try:
        sgr_ctx = run_sgr_grounding_and_plan(question=question, schema=schema_raw, model=model)
    except Exception as e:
        log.warning("SGR grounding/plan failed for %s: %s", task_id, e)
        from evalsuite.architectures.sgr.schema import SGRContext, SGRPlan

        sgr_ctx = SGRContext(grounding=SGRGrounding(), plan=SGRPlan(), prompt_addendum="")
        artifact["sgr"]["grounding"] = {}
        artifact["sgr"]["plan"] = {}
        artifact["sgr"]["generation_fail"] = str(e)
        # Still produce one fallback candidate via plain generate_sql
        try:
            raw = model.generate_sql(question=question, schema=schema_raw, messages=None)
        except Exception:
            raw = ""
        sql = strip_sql_fences(raw or "")
        result, err_type, et_ms = _run_preflight_exec(db_path, sql, dialect, timeout_sec=sql_execution_timeout_sec)
        sig = result_signature(result, sort_rows=True, max_rows=None) if result and result.ok and result.rows else None
        score = _score_candidate(result.ok if result else False, sql, sgr_ctx.grounding)
        candidates = [
            CandidateResult(
                attempt_id=0,
                raw_text=raw or "",
                sql=sql,
                preflight_ok=(result.ok if result else False)
                or (err_type == "pred_runtime_fail" and dialect == "duckdb"),
                preflight_error_type=err_type,
                exec_ok=result.ok if result else False,
                exec_error=result.error if result else None,
                exec_time_ms=et_ms,
                result_signature=sig,
                gen_params={"source": "sgr_standalone_fallback"},
                score=score,
            )
        ]
        return candidates, artifact, task_timeout_hit

    artifact["sgr"]["grounding"] = grounding_to_dict(sgr_ctx.grounding)
    artifact["sgr"]["plan"] = plan_to_dict(sgr_ctx.plan)

    allowed_tables_str = ", ".join(sgr_ctx.grounding.tables or [])
    columns_per_table_str = _format_columns_per_table(sgr_ctx.grounding)
    joins_str = _format_joins(sgr_ctx.grounding)
    filters_str = _format_filters(sgr_ctx.plan)
    plan_summary_str = _format_plan_summary(sgr_ctx.plan)

    # Keep synthesis prompt shorter for faster API response (~8k schema + addendum)
    synthesis_schema = truncate_schema(
        (schema_truncated or "") + "\n\n" + (sgr_ctx.prompt_addendum or ""),
        max_chars=8000,
    )
    synthesis_user = SQL_SYNTHESIS_PROMPT.format(
        question=question,
        schema=synthesis_schema,
        tables=allowed_tables_str or "—",
        columns_per_table=columns_per_table_str,
        joins_text=joins_str,
        filters_text=filters_str,
        plan_summary=plan_summary_str,
    )
    messages_synthesis = [
        {"role": "system", "content": SQL_SYNTHESIS_SYSTEM},
        {"role": "user", "content": synthesis_user},
    ]

    k = max(1, int(params.get("num_candidates", num_candidates) or 6))
    temperature = 0.3 if k > 1 else 0.0
    max_workers = min(k, int(params.get("parallel_synthesis_workers", 6)))

    def _gen_one(attempt_id: int) -> tuple[int, str, str]:
        try:
            raw = model.generate_sql(
                question=question,
                schema=None,
                messages=messages_synthesis,
                temperature=temperature,
            )
        except Exception as e:
            log.warning("SGR synthesis attempt %s failed: %s", attempt_id + 1, e)
            return (attempt_id, "", "")
        return (attempt_id, raw or "", strip_sql_fences(raw or ""))

    def _preflight_one(
        item: tuple[int, str, str],
    ) -> tuple[int, str, str, ExecResult | None, str | None, float | None, str | None]:
        attempt_id, raw, sql = item
        if not sql:
            return (attempt_id, raw, sql, None, "no_sql", None, None)
        result, err_type, et_ms = _run_preflight_exec(db_path, sql, dialect, timeout_sec=sql_execution_timeout_sec)
        err_msg = result.error if result else None
        return (attempt_id, raw, sql, result, err_type, et_ms, err_msg)

    # Parallel synthesis
    synthesis_results: list[tuple[int, str, str]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_gen_one, i) for i in range(k)]
        for f in as_completed(futures):
            synthesis_results.append(f.result())
    synthesis_results.sort(key=lambda x: x[0])

    # Parallel preflight+exec
    preflight_results: list[tuple[int, str, str, ExecResult | None, str | None, float | None, str | None]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_preflight_one, item) for item in synthesis_results]
        for f in as_completed(futures):
            preflight_results.append(f.result())
    preflight_results.sort(key=lambda x: x[0])

    # Sequential repair for failed (bind/runtime) and build candidates
    repair_attempts = artifact["sgr"].get("repair_attempts", [])
    candidates: list[CandidateResult] = []
    for attempt_id, raw, sql, result, err_type, et_ms, exec_error in preflight_results:
        exec_ok = result.ok if result else False
        preflight_ok = result.ok if result else False
        if dialect == "duckdb" and err_type == "pred_runtime_fail":
            preflight_ok = True
        sig = result_signature(result, sort_rows=True, max_rows=None) if result and result.ok and result.rows else None
        error_message_short = (exec_error or "")[:500]

        if not exec_ok and err_type in REPAIR_ERROR_TYPES:
            repair_prompt = REPAIR_PROMPT.format(
                question=question,
                schema=synthesis_schema[:8000],
                tables=allowed_tables_str,
                columns_per_table=columns_per_table_str,
                previous_sql=sql[:2000],
                error_type=err_type or "unknown",
                error_message=error_message_short,
            )
            repair_messages = [
                {"role": "system", "content": REPAIR_SYSTEM},
                {"role": "user", "content": repair_prompt},
            ]
            try:
                repaired_raw = model.generate_sql(
                    question=question, schema=None, messages=repair_messages, temperature=0.0
                )
                repaired_sql = strip_sql_fences(repaired_raw or "")
                if repaired_sql:
                    result_rep, err_type_rep, _ = _run_preflight_exec(
                        db_path, repaired_sql, dialect, timeout_sec=sql_execution_timeout_sec
                    )
                    repair_ok = result_rep.ok if result_rep else False
                    repair_attempts.append(
                        {
                            "error_type": err_type,
                            "error_message": error_message_short[:200],
                            "old_sql": sql[:300],
                            "new_sql": repaired_sql[:300],
                            "success": repair_ok,
                        }
                    )
                    if repair_ok:
                        sql = repaired_sql
                        result = result_rep
                        err_type = err_type_rep
                        exec_ok = True
                        exec_error = result.error if result else None
                        sig = (
                            result_signature(result, sort_rows=True, max_rows=None) if result and result.rows else None
                        )
            except Exception as repair_ex:
                log.warning("SGR repair failed: %s", repair_ex)
                repair_attempts.append(
                    {
                        "error_type": err_type,
                        "error_message": error_message_short[:200],
                        "old_sql": sql[:300],
                        "new_sql": "",
                        "success": False,
                        "repair_error": str(repair_ex)[:200],
                    }
                )
        artifact["sgr"]["repair_attempts"] = repair_attempts

        score = _score_candidate(exec_ok, sql, sgr_ctx.grounding)
        candidates.append(
            CandidateResult(
                attempt_id=attempt_id,
                raw_text=raw or "",
                sql=sql,
                preflight_ok=preflight_ok,
                preflight_error_type=err_type,
                exec_ok=exec_ok,
                exec_error=exec_error,
                exec_time_ms=et_ms,
                result_signature=sig,
                gen_params={"source": "sgr_standalone"},
                score=score,
            )
        )

    artifact["sgr"]["repair_attempts"] = artifact["sgr"].get("repair_attempts", [])
    return candidates, artifact, task_timeout_hit
