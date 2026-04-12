from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class TaskSpec:
    task_id: str
    question: str
    gold_sql: str
    db_path: str
    bench: str
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecResult:
    ok: bool
    rows: list[list[Any]] | None
    error: str | None = None


@dataclass
class TaskResult:
    task_id: str
    bench: str
    gold_sql: str
    pred_sql: str
    prompt: str
    gold: ExecResult
    pred: ExecResult
    match: bool
    status: str = "ok"  # ok | skip | gold_fail | pred_fail
    error_message: str | None = None
    error_type: str | None = None
    latency_ms: float | None = None
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    extra: dict[str, Any] = field(default_factory=dict)
    # Self-consistency: final_pred is pred_sql; candidates and aggregation live in extra["candidates"], extra["aggregation"]


# -------- Self-consistency / architecture --------


@dataclass
class CandidateResult:
    """One attempt result: raw text, normalized SQL, preflight/exec outcome, signature for aggregation."""

    attempt_id: int
    raw_text: str
    sql: str
    preflight_ok: bool
    preflight_error_type: str | None = None  # pred_parse_fail | pred_bind_fail | pred_runtime_fail | ...
    exec_ok: bool = False
    exec_error: str | None = None
    exec_time_ms: float | None = None
    result_signature: str | None = None  # hash for grouping by result
    gen_params: dict[str, Any] = field(default_factory=dict)
    score: float | None = None  # optional score for best_score sampling


@dataclass
class AggregationResult:
    selected_attempt_id: int
    selected_sql: str
    aggregation_reason: str
    votes: dict[str, Any] = field(default_factory=dict)  # by_signature, by_sql, etc.


@dataclass
class BenchSummary:
    bench: str
    total: int
    executed: int
    skipped: int
    gold_failed: int
    pred_failed: int
    ex_correct: int
    compared: int


@dataclass
class GenerationRunConfig:
    """Two-layer generation: architecture (candidate generator) + reasoning (optional) + sampling (selector)."""

    architecture: str = "plain"  # plain | sql_factory | sgr
    reasoning: str = "none"  # none | sgr (only when architecture != sgr)
    sampling: str = "single"  # single | self_consistency
    sc_samples: int | None = None  # default by architecture: plain=8, sgr=6, sql_factory=use pool
    sc_aggregation: str = "majority_result"  # majority_result | best_score


# -------- Schema context / constraints --------


@dataclass
class ColumnInfo:
    name: str
    type: str | None = None


@dataclass
class FKInfo:
    src: str
    ref_table: str
    ref_col: str


@dataclass
class TableInfo:
    name: str
    columns: list[ColumnInfo]
    fks: list[FKInfo] = field(default_factory=list)


@dataclass
class SchemaContext:
    dialect: str  # "sqlite" | "duckdb"
    tables: list[TableInfo]


@dataclass
class DialectConstraints:
    dialect: str
    allowed_statements: list[str] = field(default_factory=lambda: ["SELECT", "WITH"])
    forbidden_tokens: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    omit_best_effort_instruction: bool = False  # If True, use strict "do NOT substitute" instead of "best effort"
