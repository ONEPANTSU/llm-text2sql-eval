from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_log = logging.getLogger(__name__)


def _ensure_sample_duckdb(db_path: Path) -> Path:
    """Create a lightweight DuckDB with demo_sales and employees tables for smoke tests."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        return db_path
    try:
        import duckdb

        con = duckdb.connect(str(db_path))
        try:
            con.execute(
                """
                CREATE TABLE demo_sales (
                    id INTEGER,
                    category VARCHAR,
                    amount DOUBLE
                );
                INSERT INTO demo_sales VALUES
                  (1, 'A', 10.0), (2, 'B', 20.0), (3, 'A', 30.0),
                  (4, 'C', 40.0), (5, 'B', 15.0), (6, 'C', 25.0);
                CREATE TABLE employees (
                    id INTEGER,
                    name VARCHAR,
                    dept VARCHAR,
                    salary DOUBLE
                );
                INSERT INTO employees VALUES
                  (1, 'Alice', 'eng', 120000),
                  (2, 'Bob', 'eng', 110000),
                  (3, 'Carol', 'hr', 90000),
                  (4, 'Dave', 'hr', 85000),
                  (5, 'Eve', 'eng', 130000);
                """
            )
        finally:
            con.close()
    except Exception as exc:  # pragma: no cover
        _log.warning("Could not create smoke DuckDB at %s: %s", db_path, exc)
    return db_path


def _default_tasks(config: Config | None) -> list[TaskSpec]:
    """Return lightweight DuckDB smoke tasks (excluded from summary)."""
    if config is not None:
        db_path = Path(config.datasets.tpcds_duckdb)
    else:
        db_path = Path("data") / "tpcds" / "duckdb.db"

    safe_db = _ensure_sample_duckdb(db_path)

    sqls = [
        ("What is the total sales amount?", "SELECT sum(amount) FROM demo_sales;"),
        (
            "Which is the top category by total sales?",
            "SELECT category, sum(amount) as total FROM demo_sales GROUP BY category ORDER BY total DESC;",
        ),
        ("How many demo_sales rows are there?", "SELECT count(*) FROM demo_sales;"),
        ("What is the average amount per category?", "SELECT category, avg(amount) FROM demo_sales GROUP BY category;"),
        ("List distinct categories.", "SELECT DISTINCT category FROM demo_sales ORDER BY category;"),
        ("What is the max sale amount?", "SELECT max(amount) FROM demo_sales;"),
        ("What is the min sale amount?", "SELECT min(amount) FROM demo_sales;"),
        (
            "Sum per category ordered by category.",
            "SELECT category, sum(amount) FROM demo_sales GROUP BY category ORDER BY category;",
        ),
        (
            "Avg per category ordered desc by avg.",
            "SELECT category, avg(amount) FROM demo_sales GROUP BY category ORDER BY avg(amount) DESC, category ASC;",
        ),
        ("Rows per category.", "SELECT category, count(*) FROM demo_sales GROUP BY category ORDER BY category;"),
        ("Top 2 rows by amount.", "SELECT * FROM demo_sales ORDER BY amount DESC LIMIT 2;"),
        ("Bottom 2 rows by amount.", "SELECT * FROM demo_sales ORDER BY amount ASC LIMIT 2;"),
        (
            "Median-ish via percentile of amount.",
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY amount) FROM demo_sales;",
        ),
        ("Sales where amount > 15.", "SELECT * FROM demo_sales WHERE amount > 15 ORDER BY amount DESC;"),
        ("Sales where amount < 25.", "SELECT * FROM demo_sales WHERE amount < 25 ORDER BY amount ASC;"),
        (
            "Share of total per category.",
            "SELECT category, sum(amount)/(SELECT sum(amount) FROM demo_sales) AS share FROM demo_sales GROUP BY category ORDER BY share DESC;",
        ),
        ("Difference between max and min amount.", "SELECT max(amount) - min(amount) FROM demo_sales;"),
        ("Standard deviation of amount.", "SELECT stddev_samp(amount) FROM demo_sales;"),
        ("Variance of amount.", "SELECT var_samp(amount) FROM demo_sales;"),
        ("Unique amounts count.", "SELECT COUNT(DISTINCT amount) FROM demo_sales;"),
        ("How many employees are there?", "SELECT COUNT(*) FROM employees;"),
        ("Departments and headcount.", "SELECT dept, COUNT(*) AS c FROM employees GROUP BY dept ORDER BY c DESC;"),
        ("Average salary per department.", "SELECT dept, AVG(salary) FROM employees GROUP BY dept ORDER BY dept;"),
        ("Total salary payout.", "SELECT SUM(salary) FROM employees;"),
        ("Max salary overall.", "SELECT MAX(salary) FROM employees;"),
        ("Min salary overall.", "SELECT MIN(salary) FROM employees;"),
    ]

    tasks: list[TaskSpec] = []
    for idx, (q, sql) in enumerate(sqls, start=1):
        tasks.append(
            TaskSpec(
                task_id=f"tpcds_smoke_{idx}",
                question=q,
                gold_sql=sql,
                db_path=str(safe_db),
                bench="tpcds",
                meta={"smoke": True},
            )
        )
    return tasks


from evalsuite.adapters.db.duckdb import (
    execute_sql,
    preflight_and_execute_db,
)
from evalsuite.adapters.models.base import ModelAdapter
from evalsuite.benchmarks.base import Benchmark
from evalsuite.core.config import Config
from evalsuite.core.types import BenchSummary, DialectConstraints, ExecResult, SchemaContext, TaskResult, TaskSpec
from evalsuite.pipeline.preflight import (
    build_alias_map,
    extract_cte_names,
    extract_partial_metrics,
    try_ambiguous_patch,
    try_candidate_binding_patch,
    try_prefix_patch,
    validate_schema_refs,
)
from evalsuite.pipeline.schema_extract import schema_from_duckdb
from evalsuite.pipeline.sql_sanitize import has_placeholders
from evalsuite.pipeline.toolchain import SchemaToolsExecutor


def _nl_data_root(config: Config | None) -> Path:
    """
    Locate NL dataset root (same directory as duckdb.db).

    Layout (relative to repo root):
      data/tpcds/duckdb.db
      data/tpcds/tasks.jsonl
      data/tpcds/schema.json
      data/tpcds/queries/q01.sql ... q99.sql
    """
    if config is None:
        return Path("data") / "tpcds"
    # config.datasets.tpcds_duckdb -> .../data/tpcds/duckdb.db
    return Path(config.datasets.tpcds_duckdb).parent


def _load_tasks(config: Config | None, bench_name: str = "tpcds") -> list[TaskSpec]:
    """
    Load NL tasks from data/tpcds/tasks.jsonl.

    Each line:
      {
        "id": "tpcds_q07",
        "question": "...",
        "sql_file": "q07.sql",
        "difficulty": "medium",
        "tags": [...]
      }
    """
    root = _nl_data_root(config)
    tasks_path = root / "tasks.jsonl"
    if not tasks_path.exists():
        return []

    # Gold SQL lives under data/tpcds/queries/qXX.sql
    if config is None:
        db_path = Path("data/tpcds/duckdb.db")
        queries_root = Path("data/tpcds/queries")
    else:
        db_path = config.datasets.tpcds_duckdb
        queries_root = Path(config.datasets.tpcds_duckdb).parent / "queries"

    out: list[TaskSpec] = []
    with tasks_path.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            sql_file = item.get("sql_file")
            question = item.get("question", "").strip()
            qid = item.get("id") or sql_file
            if not sql_file or not question:
                continue
            sql_path = queries_root / sql_file
            if not sql_path.exists():
                # Dataset entry refers to a query file that is not present locally.
                continue
            gold_sql = sql_path.read_text()
            meta: dict[str, Any] = {
                "question_id": qid,
                "difficulty": item.get("difficulty"),
                "tags": item.get("tags") or [],
            }
            out.append(
                TaskSpec(
                    task_id=qid,
                    question=question,
                    gold_sql=gold_sql,
                    db_path=str(db_path),
                    bench=bench_name,
                    meta=meta,
                )
            )
    return out


@dataclass
class TPCDSNLBenchmark(Benchmark):
    """
    Semi-synthetic NL->SQL benchmark based on TPC-DS specification.

    - Uses NL questions from data/tpcds/tasks.jsonl
    - Gold SQL is loaded from data/tpcds/queries/qXX.sql
    - Requires a populated DuckDB TPC-DS database (data/tpcds/duckdb.db)
    - Requires context_mode != "none" (full_schema or toolchain)
    """

    name: str = "tpcds"
    config: Config | None = None
    model: ModelAdapter | None = None
    float_tol: float = 1e-4
    column_order_insensitive: bool = True
    string_normalize: bool = True
    context_mode: str = "none"
    schema_max_tables: int = 50
    schema_max_cols_per_table: int = 30
    schema_format: str = "compact"

    def discover_tasks(self) -> list[TaskSpec]:
        tasks = _load_tasks(self.config, bench_name=self.name)
        if not tasks:
            _log.warning("No real TPC-DS NL data found — using smoke tasks (results will be excluded from summary)")
            return _default_tasks(self.config)
        return tasks

    # ------------------------------------------------------------------
    # Hook implementations for _run_task_common
    # ------------------------------------------------------------------

    def _get_dialect(self) -> str:
        return "duckdb"

    def _get_constraints(self) -> DialectConstraints:
        return DialectConstraints(
            dialect="duckdb",
            allowed_statements=["SELECT", "WITH"],
            forbidden_tokens=[],
            notes=[
                "This is a TPC-DS data warehouse with a star schema.",
                "Each question corresponds to a specific canonical analytical query.",
                "Prefer canonical TPC-DS fact tables (store_sales, store_returns, web_sales, web_returns, catalog_sales, catalog_returns).",
                "Returns-related questions usually use *_returns tables.",
                "Time filters are typically applied via joins to date_dim.",
                "Do not change aggregation level or business meaning of the query.",
            ],
            omit_best_effort_instruction=True,
        )

    def _get_schema_context(self, db_path: Path) -> SchemaContext:
        return schema_from_duckdb(db_path, max_tables=self.schema_max_tables, max_cols=self.schema_max_cols_per_table)

    def _get_tool_executor(self, db_path: Path) -> SchemaToolsExecutor:
        class DuckdbTools(SchemaToolsExecutor):
            def __init__(self, path: Path):
                self.path = path

            def list_tables(self) -> list[str]:
                import duckdb

                con = duckdb.connect(str(self.path), read_only=True)
                try:
                    rows = con.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' ORDER BY table_name"
                    ).fetchall()
                    return [r[0] for r in rows]
                finally:
                    con.close()

            def describe_table(self, table: str) -> dict:
                import duckdb

                if not table:
                    return {}
                con = duckdb.connect(str(self.path), read_only=True)
                try:
                    cols = con.execute(
                        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name=? ORDER BY ordinal_position",
                        [table],
                    ).fetchall()
                    return {"columns": [{"name": c, "type": t} for c, t in cols], "fks": []}
                finally:
                    con.close()

            def dialect_info(self) -> dict[str, Any]:
                return {"dialect": "duckdb"}

        return DuckdbTools(db_path)

    def _execute_sql(self, db_path: str, sql: str) -> ExecResult:
        timeout_sec = getattr(self, "sql_execution_timeout_sec", None)
        return execute_sql(db_path, sql, timeout_sec=timeout_sec)

    def ensure_db_copy(self, db_path: Path) -> None:
        """Create a copy of the DuckDB file once (avoids lock conflicts)."""
        if getattr(self, "_tpcds_db_copy_path", None):
            return
        if not db_path.exists():
            return
        print("[evalsuite] TPC-DS: preparing DB copy (may take a few minutes for large DBs)...", flush=True)
        _log.info("TPC-DS: preparing DB copy (may take a few minutes for large DBs)...")
        try:
            fd, self._tpcds_db_copy_path = tempfile.mkstemp(suffix=".duckdb.db")
            os.close(fd)
            shutil.copy2(db_path, self._tpcds_db_copy_path)
        except Exception:
            self._tpcds_db_copy_path = None

    def _resolve_db_path(self, task: TaskSpec) -> str:
        """Use a copy of the DuckDB file to avoid lock conflicts."""
        db_path = Path(task.db_path)
        self.ensure_db_copy(db_path)
        return str(getattr(self, "_tpcds_db_copy_path", None) or db_path)

    def _should_skip(self, task: TaskSpec) -> TaskResult | None:
        """Skip when DB is missing or context_mode is 'none' (TPC-DS requires schema)."""
        prompt = task.question
        db_path_obj = Path(task.db_path) if task.db_path else None

        if not task.db_path or (db_path_obj is not None and not db_path_obj.exists()):
            return TaskResult(
                task_id=task.task_id,
                bench=self.name,
                gold_sql=task.gold_sql,
                pred_sql="",
                prompt=prompt,
                gold=ExecResult(ok=False, rows=None, error="missing_db"),
                pred=ExecResult(ok=False, rows=None, error="missing_db"),
                match=False,
                status="skip",
                error_message="DB not found",
                error_type="missing_db",
                latency_ms=0,
                extra={
                    "question": task.question,
                    "db_path": task.db_path,
                    "context_mode": self.context_mode,
                    "candidates_count": 0,
                },
            )

        # Smoke tasks bypass the context_mode restriction so they can run without schema context.
        is_smoke = bool(task.meta and task.meta.get("smoke"))
        if self.context_mode == "none" and not is_smoke:
            return TaskResult(
                task_id=task.task_id,
                bench=self.name,
                gold_sql=task.gold_sql,
                pred_sql="",
                prompt=prompt,
                gold=ExecResult(ok=True, rows=None, error=None),
                pred=ExecResult(ok=False, rows=None, error="context_required"),
                match=False,
                status="skip",
                error_message="tpcds requires context_mode != none",
                error_type="context_required",
                latency_ms=0,
                extra={
                    "question": task.question,
                    "db_path": task.db_path,
                    "context_mode": self.context_mode,
                    "candidates_count": 0,
                },
            )

        return None

    def _post_execute(
        self,
        task: TaskSpec,
        pred_sql: str,
        pred_exec: ExecResult,
        err_type: str | None,
        extra: dict[str, Any],
    ) -> tuple[str, ExecResult, str | None, dict[str, Any]]:
        """TPC-DS post-execute: placeholder check, preflight/EXPLAIN,
        auto-patching (ambiguous/candidate_binding/prefix), schema validation,
        and enriched extra metadata (status_v2, partial metrics)."""
        db_path = self._resolve_db_path(task)
        timeout_sec = getattr(self, "sql_execution_timeout_sec", None)

        # --- Placeholder check ---
        placeholder_ok, placeholder_reason = has_placeholders(pred_sql)
        if placeholder_ok:
            pred_exec = ExecResult(ok=False, rows=None, error=f"pred_invalid_sql:{placeholder_reason}")
            err_type = "pred_invalid_sql"
            extra.update(
                {
                    "auto_patch_applied": False,
                    "auto_patch_type": None,
                    "auto_patch_from": None,
                    "auto_patch_to": None,
                    "autofix_success": None,
                    "autofix_failed": None,
                    "missing_tables": [],
                    "missing_columns": [],
                    "schema_warn": False,
                }
            )
            extra.update(extract_partial_metrics(task.gold_sql, pred_sql))
            return pred_sql, pred_exec, err_type, extra

        # --- Re-execute via preflight (EXPLAIN + execute) for structured error types ---
        pred_exec, err_type = preflight_and_execute_db(db_path, pred_sql, timeout_sec=timeout_sec)

        # --- Schema validation (diagnostic only, never blocks) ---
        schema_warn = False
        schema_cache: dict[str, list[str]] = {}
        list_tables_for_validation: list[str] | None = None

        # Build schema_cache from tool_calls in extra if available
        tool_calls_info = extra.get("tool_calls", [])
        for tc in tool_calls_info:
            if isinstance(tc, dict):
                if tc.get("name") == "list_tables" and isinstance(tc.get("result"), list):
                    list_tables_for_validation = tc["result"]
                if tc.get("name") == "describe_table" and isinstance(tc.get("result"), dict):
                    tbl = (tc.get("args", {}).get("table") or tc.get("args", {}).get("table_name") or "").strip()
                    if tbl:
                        cols = [
                            c.get("name", c) if isinstance(c, dict) else str(c) for c in tc["result"].get("columns", [])
                        ]
                        schema_cache[tbl] = cols

        if schema_cache:
            cte_names = extract_cte_names(pred_sql)
            alias_map = build_alias_map(pred_sql, cte_names)
            schema_ok, missing_tables, missing_columns = validate_schema_refs(
                pred_sql, schema_cache, list_tables_for_validation, cte_names=cte_names, alias_map=alias_map
            )
            schema_warn = not schema_ok
        else:
            missing_tables = []
            missing_columns = []

        # --- Auto-patch on bind failures ---
        auto_patch_applied = False
        auto_patch_type_val: str | None = None
        auto_patch_from_val: str | None = None
        auto_patch_to_val: str | None = None
        autofix_success_flag: bool | None = None
        autofix_failed_flag: bool | None = None

        if err_type == "pred_bind_fail" and pred_exec.error:
            err_msg = pred_exec.error or ""
            patched_sql = None
            if "Ambiguous reference" in err_msg and schema_cache:
                patched_sql, applied = try_ambiguous_patch(pred_sql, err_msg, schema_cache)
                if applied and patched_sql:
                    auto_patch_type_val = "ambiguous"
                    auto_patch_from_val = auto_patch_to_val = None
            if patched_sql is None:
                patched_sql, applied, ptype, pfrom, pto = try_candidate_binding_patch(pred_sql, err_msg)
                if applied and patched_sql:
                    auto_patch_type_val = ptype or "candidate_binding"
                    auto_patch_from_val, auto_patch_to_val = pfrom, pto
            if patched_sql is None:
                patched_sql, applied, pfrom, pto = try_prefix_patch(pred_sql, err_msg)
                if applied and patched_sql:
                    auto_patch_type_val = "prefix"
                    auto_patch_from_val, auto_patch_to_val = pfrom, pto
            if patched_sql is not None and auto_patch_type_val is not None:
                auto_patch_applied = True
                original_sql = pred_sql
                pred_sql = patched_sql
                pred_exec, err_type = preflight_and_execute_db(db_path, pred_sql, timeout_sec=timeout_sec)
                if err_type == "pred_bind_fail":
                    pred_sql = original_sql
                    autofix_failed_flag = True
                else:
                    autofix_success_flag = True

        extra.update(
            {
                "schema_warn": schema_warn,
                "auto_patch_applied": auto_patch_applied,
                "auto_patch_type": auto_patch_type_val,
                "auto_patch_from": auto_patch_from_val,
                "auto_patch_to": auto_patch_to_val,
                "autofix_success": autofix_success_flag,
                "autofix_failed": autofix_failed_flag,
                "missing_tables": missing_tables,
                "missing_columns": missing_columns,
            }
        )
        extra.update(extract_partial_metrics(task.gold_sql, pred_sql))

        return pred_sql, pred_exec, err_type, extra

    def run_task(self, task: TaskSpec) -> TaskResult:
        return self._run_task_common(task)

    def summarize(self, results: Iterable[TaskResult]) -> BenchSummary:
        # Clean up temp DB copy if we created one
        copy_path = getattr(self, "_tpcds_db_copy_path", None)
        if copy_path and os.path.isfile(copy_path):
            try:
                os.unlink(copy_path)
            except Exception:
                pass
            self._tpcds_db_copy_path = None
        return super().summarize(results)
