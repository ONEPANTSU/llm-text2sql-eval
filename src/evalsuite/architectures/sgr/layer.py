"""
SGR as reasoning layer: run SGR Grounding + Plan (real LLM calls), then call base architecture with enriched context.
"""

from __future__ import annotations

import logging
from typing import Any

from evalsuite.architectures.sgr.prompts import (
    GROUNDING_PROMPT,
    GROUNDING_SYSTEM,
    PLAN_PROMPT,
    PLAN_SYSTEM,
)
from evalsuite.architectures.sgr.schema import (
    SGRContext,
    SGRGrounding,
    SGRPlan,
)
from evalsuite.architectures.sgr.utils import truncate_schema

log = logging.getLogger(__name__)

MAX_SCHEMA_CHARS = 12000
MAX_ADDENDUM_CHARS = 2000


def build_constraints(grounding: SGRGrounding, plan: SGRPlan) -> str:
    """
    Build a short directive addendum (~1500-2000 chars) for the base architecture:
    allowed tables, columns per table, joins, filters, aggregations/group_by/order_by/limit.
    """
    lines: list[str] = ["[SGR constraints — use ONLY the following]"]
    lines.append("Allowed tables: " + ", ".join(grounding.tables or ["(none)"]))
    if grounding.columns:
        lines.append("Allowed columns per table:")
        for t, cols in grounding.columns.items():
            lines.append(f"  {t}: " + ", ".join(cols or []))
    if grounding.joins:
        lines.append("Required joins:")
        for j in grounding.joins:
            lines.append(f"  {j.left_table}.{j.left_column} = {j.right_table}.{j.right_column}")
    if plan.filters:
        lines.append("Required filters:")
        for f in plan.filters:
            ref = getattr(f, "column_ref", "") or (f.raw or "")
            op = getattr(f, "operator", "")
            val = getattr(f, "value_hint", "")
            lines.append(f"  {ref} {op} {val}".strip())
    if plan.aggregations:
        lines.append(
            "Aggregations: "
            + ", ".join(
                f"{a.function}({a.column_ref})" + (f" AS {a.alias}" if a.alias else "") for a in plan.aggregations
            )
        )
    if plan.group_by:
        lines.append("Group by: " + ", ".join(plan.group_by))
    if plan.order_by:
        lines.append("Order by: " + ", ".join(f"{o.column_ref} {o.direction}" for o in plan.order_by))
    if plan.limit is not None:
        lines.append(f"Limit: {plan.limit}")
    if plan.distinct:
        lines.append("Distinct: true")
    text = "\n".join(lines)
    if len(text) > MAX_ADDENDUM_CHARS:
        text = text[: MAX_ADDENDUM_CHARS - 20] + "\n... [truncated]"
    return text


def run_sgr_grounding_and_plan(
    question: str,
    schema: str | None,
    model: Any,
    config: dict[str, Any] | None = None,
) -> SGRContext:
    """
    Call LLM for grounding and plan; validate with Pydantic; build prompt addendum.
    On validation failure after retries, raises; caller can set pred_generation_fail.
    """
    schema_truncated = truncate_schema(schema, max_chars=MAX_SCHEMA_CHARS)
    grounding = model.generate_structured(
        GROUNDING_PROMPT.format(question=question, schema=schema_truncated or "(no schema)"),
        SGRGrounding,
        system_prompt=GROUNDING_SYSTEM,
        temperature=0.0,
        max_retries=2,
    )
    # Pass grounding as JSON string for the plan prompt
    grounding_str = str(grounding.model_dump()) if grounding else "{}"
    plan = model.generate_structured(
        PLAN_PROMPT.format(
            question=question,
            schema=schema_truncated or "(no schema)",
            grounding=grounding_str,
        ),
        SGRPlan,
        system_prompt=PLAN_SYSTEM,
        temperature=0.0,
        max_retries=2,
    )
    prompt_addendum = build_constraints(grounding, plan)
    return SGRContext(grounding=grounding, plan=plan, prompt_addendum=prompt_addendum)
