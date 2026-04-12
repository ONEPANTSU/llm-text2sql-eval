"""SGR (Schema-Guided Reasoning) data structures — Pydantic models for strict validation."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# -------- Join edge --------


class JoinEdge(BaseModel):
    """Required join: left_table.left_column = right_table.right_column."""

    left_table: str = ""
    left_column: str = ""
    right_table: str = ""
    right_column: str = ""


# -------- Grounding --------


class SGRGrounding(BaseModel):
    """Grounding: tables and columns from schema that are relevant to the question."""

    tables: list[str] = Field(default_factory=list, min_length=0)
    columns: dict[str, list[str]] = Field(default_factory=dict)  # table -> [col]
    joins: list[JoinEdge] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    confidence: float | None = Field(None, ge=0.0, le=1.0)

    model_config = ConfigDict(extra="allow")


# -------- Plan: condition, aggregation, order, CTE --------


class Condition(BaseModel):
    """Filter condition: column ref and operator/value."""

    column_ref: str = ""  # table.column or alias.column
    operator: str | None = None  # =, !=, >, <, IN, LIKE, ...
    value_hint: str | int | float | None = None  # literal or placeholder (LLM may return int/float)
    raw: str | None = None

    model_config = ConfigDict(extra="allow")


class Aggregation(BaseModel):
    """Aggregation: function and column."""

    function: str = ""  # COUNT, SUM, AVG, MIN, MAX
    column_ref: str = ""
    alias: str | None = None

    model_config = ConfigDict(extra="allow")


class OrderSpec(BaseModel):
    """ORDER BY spec."""

    column_ref: str = ""
    direction: str = "ASC"  # ASC | DESC

    model_config = ConfigDict(extra="allow")


class CTEPlan(BaseModel):
    """CTE in plan."""

    name: str = ""
    description: str | None = None

    model_config = ConfigDict(extra="allow")


class SGRPlan(BaseModel):
    """Logical plan: select list, filters, aggregations, group/order/limit."""

    select: list[str] = Field(default_factory=list)  # column refs
    filters: list[Condition] = Field(default_factory=list)
    aggregations: list[Aggregation] = Field(default_factory=list)
    group_by: list[str] = Field(default_factory=list)
    order_by: list[OrderSpec] = Field(default_factory=list)
    limit: int | None = None
    distinct: bool = False
    ctes: list[CTEPlan] = Field(default_factory=list)

    model_config = ConfigDict(extra="allow")


# -------- Context (grounding + plan + addendum) --------


class SGRContext(BaseModel):
    """Enriched context after SGR grounding + plan."""

    grounding: SGRGrounding = Field(default_factory=SGRGrounding)
    plan: SGRPlan = Field(default_factory=SGRPlan)
    prompt_addendum: str = ""

    model_config = ConfigDict(arbitrary_types_allowed=False)


# -------- Legacy dataclass-style access (for code that uses .__dict__) --------


def grounding_to_dict(g: SGRGrounding) -> dict[str, Any]:
    return g.model_dump()


def plan_to_dict(p: SGRPlan) -> dict[str, Any]:
    return p.model_dump()
