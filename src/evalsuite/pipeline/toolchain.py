from __future__ import annotations

import codecs
import json
import re
import time
from dataclasses import dataclass
from typing import Any

from evalsuite.adapters.models.base import ModelAdapter
from evalsuite.core.types import DialectConstraints


@dataclass
class ToolCall:
    name: str
    args: dict[str, Any]
    ok: bool
    result: Any = None
    error: str | None = None


# TOOL_CALL(list_tables) or TOOL_CALL(describe_table, "table_name")
_TOOL_CALL_PAREN = re.compile(r"tool_call\s*\(\s*(\w+)\s*(?:,\s*[\"']?(\w+)[\"']?\s*)?\)", re.IGNORECASE)


def parse_tool_call(text: str) -> tuple[str, dict[str, Any]] | None:
    """
    Extract tool call even if model adds prose before/after.
    Expected minimal format somewhere in the reply:
      TOOL_CALL: list_tables   or   TOOL_CALL list_tables   or   TOOL_CALL(list_tables)
      args: {...}
    """
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    name: str | None = None
    args: dict[str, Any] = {}

    for idx, line in enumerate(lines):
        low = line.lower()
        if "tool_call:" in low:
            name = line.split(":", 1)[1].strip()
        elif low.startswith("tool_call ") and low.split(maxsplit=1)[0] == "tool_call":
            # "TOOL_CALL list_tables" (no colon)
            parts = line.split(maxsplit=1)
            name = parts[1].strip() if len(parts) > 1 else ""
        else:
            # "TOOL_CALL(list_tables)" or "TOOL_CALL(describe_table, table_name)"
            m = _TOOL_CALL_PAREN.search(line)
            if m:
                name = m.group(1).strip()
                if m.lastindex >= 2 and m.group(2):
                    args = {"table": m.group(2).strip()}
            else:
                continue
        if not name:
            continue
        # Look ahead for args on the same or following lines (overwrite if we had paren args).
        candidate_lines = [line] + lines[idx + 1 :]
        for cand in candidate_lines:
            if cand.lower().startswith("args:"):
                try:
                    args = json.loads(cand.split(":", 1)[1].strip() or "{}")
                except json.JSONDecodeError:
                    args = {}
                break
        break

    if not name:
        return None
    return name, args


def _strip_code_fences(text: str) -> str:
    from evalsuite.pipeline.sql_sanitize import strip_sql_fences

    return strip_sql_fences(text)


def extract_sql(text: str) -> str:
    """
    Try to salvage executable SQL from model output with chatter.
    Heuristics:
      - JSON block with "sql" key: {"sql": "SELECT ..."}
      - SQL:\n... or SQL: ...
      - markdown fences
      - first SELECT/WITH keyword onward
    """
    if not text or not text.strip():
        return ""
    cleaned = text.strip()
    # JSON block with "sql" key (allow escaped content inside string)
    sql_json = re.search(r'["\']sql["\']\s*:\s*"((?:[^"\\]|\\.)*)"', cleaned)
    if sql_json:
        return codecs.decode(sql_json.group(1), "unicode_escape").strip()
    # Try full JSON object {"sql": "..."}
    try:
        obj = json.loads(cleaned)
        if isinstance(obj, dict) and "sql" in obj and obj["sql"]:
            return str(obj["sql"]).strip()
    except (json.JSONDecodeError, TypeError):
        pass
    # SQL:\n... or SQL: ...
    sql_label = re.search(r"\bSQL\s*:\s*\n?(.*)", cleaned, re.IGNORECASE | re.DOTALL)
    if sql_label:
        candidate = sql_label.group(1).strip()
        candidate = _strip_code_fences(candidate)
        if re.search(r"\b(SELECT|WITH)\b", candidate, re.IGNORECASE):
            return candidate
    cleaned = _strip_code_fences(cleaned)
    match = re.search(r"\b(SELECT|WITH)\b", cleaned, flags=re.IGNORECASE)
    if match:
        result = cleaned[match.start() :].strip()
        return _strip_code_fences(result)
    # Do not return prose or TOOL_CALL-only output as SQL
    return ""


class SchemaToolsExecutor:
    """Contract for database schema introspection (used by toolchain mode).

    To add a new database type, implement list_tables() and describe_table().

    Example (in your benchmark file):
        class MyDbTools(SchemaToolsExecutor):
            def __init__(self, db_path: Path):
                self.db_path = db_path

            def list_tables(self) -> list[str]:
                # Return list of table names
                ...

            def describe_table(self, table: str) -> dict:
                # Return {"columns": [{"name": ..., "type": ...}], "fks": [...]}
                ...
    """

    def list_tables(self) -> list[str]:
        """Return all table names in the database."""
        raise NotImplementedError

    def describe_table(self, table: str) -> dict[str, Any]:
        """Return table schema: {"columns": [{"name": str, "type": str}], "fks": [...]}."" """
        raise NotImplementedError

    def dialect_info(self) -> dict[str, Any]:
        """Return dialect metadata (optional). E.g. {"dialect": "sqlite"}."""
        return {}


# Canonical TPC-DS fact tables (for exploration soft-guard)
TPCDS_FACT_TABLES = frozenset(
    {
        "store_sales",
        "store_returns",
        "web_sales",
        "web_returns",
        "catalog_sales",
        "catalog_returns",
    }
)

# TPC-DS table names for "mentioned in task" heuristic (allow describe for join/dimension tables from prompt)
TPCDS_TABLE_NAMES = frozenset(
    {
        "store_sales",
        "store_returns",
        "web_sales",
        "web_returns",
        "catalog_sales",
        "catalog_returns",
        "date_dim",
        "store",
        "customer",
        "customer_address",
        "customer_demographics",
        "household_demographics",
        "item",
        "promotion",
        "call_center",
        "catalog_page",
        "web_site",
        "warehouse",
        "inventory",
        "reason",
        "income_band",
        "ship_mode",
    }
)


def _tables_mentioned_in_question(question: str) -> frozenset[str]:
    """Extract TPC-DS table names mentioned in the task text (word-boundary, case-insensitive)."""
    if not question:
        return frozenset()
    lower = question.lower()
    return frozenset(t for t in TPCDS_TABLE_NAMES if re.search(r"\b" + re.escape(t) + r"\b", lower))


def run_toolchain(
    *,
    model: ModelAdapter,
    question: str,
    tools: SchemaToolsExecutor,
    constraints: DialectConstraints,
    max_steps: int,
    timeout_sec: int,
    allow_sample_values: int = 0,
    max_describe: int = 6,
    max_list_tables: int = 1,
    max_describe_per_table: int = 1,
    max_tool_only_streak: int = 4,
    max_tool_calls: int = 10,
    max_describe_core_bonus: int = 3,
    temperature: float | None = None,
    top_p: float | None = None,
    seed: int | None = None,
) -> tuple[str | None, list[ToolCall], list[dict[str, str]], str | None, bool, list[str]]:
    """
    Iterate model-tool loop to collect schema, return SQL string or None.
    Budget: list_tables max_list_tables (1), describe_table max_describe (6) total, max_describe_per_table (1) per table,
    max_tool_calls (10) total; after max_tool_only_streak (4) consecutive tool calls, require SQL.
    Tables mentioned in question get a small bonus (up to max_describe_core_bonus extra describes).
    Returns: (sql_result, tool_calls, messages, fail_reason, toolchain_schema_exploration_detected, inspected_tables).
    """
    start = time.time()
    tool_calls: list[ToolCall] = []
    messages: list[dict[str, str]] = []
    describe_count = 0
    list_tables_count = 0
    tool_only_streak = 0
    described_tables: dict[str, int] = {}
    inspected_tables: list[str] = []
    question_mentioned_tables = _tables_mentioned_in_question(question)
    _no_more_tools_msg = "TOOL_RESULT: error: Output FINAL SQL only. No more tool calls."

    sys_lines = [
        "You are a SQL generation engine for benchmark evaluation.",
        "",
        "Your goal is to reproduce the canonical SQL query implied by the question,",
        "not to produce an approximate or alternative solution.",
        "",
        "Process:",
        "1. First, identify the correct fact table(s) and required dimensions.",
        "2. Use tools only to inspect tables that are necessary for the query.",
        "3. After you have sufficient schema information, output the FINAL SQL.",
        "",
        "Rules:",
        "- Do NOT explore the entire schema.",
        "- Prefer canonical TPC-DS fact tables and join paths.",
        "- Do NOT invent alternative interpretations, tables, or aggregation levels.",
        "- Be conservative and literal when interpreting the question.",
        "- Return SQL only. No explanations. No markdown.",
        "",
        "Use TOOL_CALL format exactly and as the FIRST non-empty line.",
        "Allowed tool names only: list_tables, describe_table, dialect_info. Do not invent other tool names.",
        "Example: TOOL_CALL: list_tables",
        "        args: {}",
        'For describe_table use args: {"table": "table_name"}.',
        "After schema inspection, output FINAL SQL only.",
        f"Dialect: {constraints.dialect}.",
        f"Allowed statements: {', '.join(constraints.allowed_statements)}.",
        "",
        "Tool responses will be provided as user messages prefixed with TOOL_RESULT.",
    ]
    if constraints.forbidden_tokens:
        sys_lines.append(f"Forbidden tokens: {', '.join(constraints.forbidden_tokens)}")
    if allow_sample_values == 0:
        sys_lines.append("Do NOT request data/sample rows. Metadata only.")
    sys_prompt = "\n".join(sys_lines)
    messages.append({"role": "system", "content": sys_prompt})
    messages.append({"role": "user", "content": question})

    sql_result: str | None = None
    fail_reason: str | None = None
    exploration_detected = False
    gen_kwargs: dict[str, Any] = {}
    if temperature is not None:
        gen_kwargs["temperature"] = temperature
    if top_p is not None:
        gen_kwargs["top_p"] = top_p
    if seed is not None:
        gen_kwargs["seed"] = seed

    for step in range(max_steps):
        if time.time() - start > timeout_sec:
            fail_reason = "toolchain_no_sql"
            break
        reply = model.generate_sql(question="", schema=None, messages=messages, **gen_kwargs)
        parsed = parse_tool_call(reply)
        messages.append({"role": "assistant", "content": reply})
        if parsed:
            name, args = parsed
            if name not in {"list_tables", "describe_table", "dialect_info"}:
                err_msg = f'unknown tool "{name}". Allowed tools: list_tables, describe_table, dialect_info.'
                tool_calls.append(ToolCall(name=name, args=args, ok=False, error="tool_call_invalid_format"))
                messages.append({"role": "user", "content": f"TOOL_RESULT: error: {err_msg}"})
                continue
            # Rule 2: tool-only streak — after N consecutive tool calls, require SQL
            if name in {"list_tables", "describe_table"} and tool_only_streak >= max_tool_only_streak:
                tool_calls.append(ToolCall(name=name, args=args, ok=False, error="tool_only_streak"))
                messages.append({"role": "user", "content": _no_more_tools_msg})
                continue
            # Total tool-call budget
            if len(tool_calls) >= max_tool_calls:
                tool_calls.append(ToolCall(name=name, args=args, ok=False, error="tool_calls_overuse"))
                messages.append({"role": "user", "content": _no_more_tools_msg})
                continue
            # Rule 1: list_tables max once
            if name == "list_tables" and list_tables_count >= max_list_tables:
                tool_calls.append(ToolCall(name=name, args=args, ok=False, error="list_tables_already_called"))
                messages.append({"role": "user", "content": _no_more_tools_msg})
                continue
            # Rule 1: describe_table per-table max once; total budget with core-table bonus
            if name == "describe_table":
                tbl = None
                if isinstance(args, dict):
                    for k in ("table", "table_name", "name"):
                        if k in args:
                            tbl = args.get(k)
                            break
                tbl = (tbl or "").strip()
                if tbl and described_tables.get(tbl, 0) >= max_describe_per_table:
                    tool_calls.append(ToolCall(name=name, args=args, ok=False, error="table_already_described"))
                    messages.append({"role": "user", "content": _no_more_tools_msg})
                    continue
                # Total describe budget: allow up to max_describe, or core tables up to max_describe + max_describe_core_bonus
                over_limit = describe_count >= max_describe
                core_allowed = (
                    tbl in question_mentioned_tables
                    and described_tables.get(tbl, 0) == 0
                    and describe_count < max_describe + max_describe_core_bonus
                )
                if over_limit and not core_allowed:
                    tool_calls.append(ToolCall(name=name, args=args, ok=False, error="toolchain_overuse"))
                    messages.append({"role": "user", "content": _no_more_tools_msg})
                    continue
            try:
                if name == "list_tables":
                    result = tools.list_tables()
                    list_tables_count += 1
                    tool_only_streak += 1
                elif name == "describe_table":
                    tbl = None
                    if isinstance(args, dict):
                        for k in ("table", "table_name", "name"):
                            if k in args:
                                tbl = args.get(k)
                                break
                    tbl = (tbl or "").strip()
                    result = tools.describe_table(tbl)
                    describe_count += 1
                    described_tables[tbl] = described_tables.get(tbl, 0) + 1
                    tool_only_streak += 1
                    if tbl:
                        inspected_tables.append(tbl)
                    if describe_count > max_describe + max_describe_core_bonus:
                        fail_reason = "toolchain_overuse"
                        break
                else:
                    result = tools.dialect_info()
                tool_calls.append(ToolCall(name=name, args=args, ok=True, result=result))
                messages.append({"role": "user", "content": f"TOOL_RESULT:\n{json.dumps(result)}"})
            except Exception as exc:  # pragma: no cover
                tool_calls.append(ToolCall(name=name, args=args, ok=False, error=str(exc)))
                messages.append({"role": "user", "content": f"TOOL_RESULT: error: {exc}"})
            continue
        else:
            sql_result = extract_sql(reply)
            break
    else:
        if sql_result is None and fail_reason is None:
            fail_reason = "toolchain_no_sql"

    # Retry once on empty SQL: if we had tool calls but no SQL, ask explicitly for SQL only
    if (not sql_result or not sql_result.strip()) and tool_calls and fail_reason != "toolchain_overuse":
        messages.append(
            {
                "role": "user",
                "content": "You have received schema information. Output ONLY the final SQL query. No tool calls. No explanations.",
            }
        )
        if time.time() - start <= timeout_sec:
            reply = model.generate_sql(question="", schema=None, messages=messages, **gen_kwargs)
            sql_result = extract_sql(reply)
            if sql_result and sql_result.strip():
                fail_reason = None

    # Soft guard: schema exploration (diagnostic only)
    if describe_count >= 6 or len(inspected_tables) >= 8:
        fact_inspected = sum(1 for t in inspected_tables if t in TPCDS_FACT_TABLES)
        if fact_inspected >= 6 or len(inspected_tables) >= 10:
            exploration_detected = True

    return sql_result, tool_calls, messages, fail_reason, exploration_detected, inspected_tables
