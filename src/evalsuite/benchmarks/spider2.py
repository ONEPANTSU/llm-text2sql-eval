from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from evalsuite.adapters.db.sqlite import execute_sql
from evalsuite.adapters.models.base import ModelAdapter
from evalsuite.benchmarks.base import Benchmark
from evalsuite.core.config import Config
from evalsuite.core.types import DialectConstraints, ExecResult, SchemaContext, TaskResult, TaskSpec
from evalsuite.pipeline.schema_extract import schema_from_sqlite
from evalsuite.pipeline.toolchain import SchemaToolsExecutor

_log = logging.getLogger(__name__)


def _ensure_sample_db(db_root: Path) -> Path:
    """Create a tiny SQLite DB so default tasks always have something to query."""
    db_root.mkdir(parents=True, exist_ok=True)
    db_path = db_root / "spider2_sample.db"
    if db_path.exists():
        return db_path

    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            DROP TABLE IF EXISTS employees;
            CREATE TABLE employees (
                id INTEGER PRIMARY KEY,
                name TEXT,
                dept TEXT,
                salary INTEGER
            );
            INSERT INTO employees (id, name, dept, salary) VALUES
            (1, 'Ada', 'eng', 120000),
            (2, 'Lin', 'ops', 95000),
            (3, 'Ravi', 'eng', 105000),
            (4, 'Mia', 'sales', 99000),
            (5, 'Jo', 'eng', 87000);
            """
        )
        conn.commit()
    finally:
        conn.close()
    return db_path


def _default_tasks(db_root: Path) -> list[TaskSpec]:
    db_path = _ensure_sample_db(db_root)
    sqls = [
        ("Count tables in database", "SELECT COUNT(*) FROM sqlite_master WHERE type='table';"),
        ("Return any table name", "SELECT name FROM sqlite_master WHERE type='table' LIMIT 1;"),
        ("How many employees are stored?", "SELECT COUNT(*) FROM employees;"),
        (
            "List departments and headcount sorted by size.",
            "SELECT dept, COUNT(*) AS c FROM employees GROUP BY dept ORDER BY c DESC;",
        ),
        ("Average salary per department.", "SELECT dept, AVG(salary) FROM employees GROUP BY dept ORDER BY dept;"),
        ("Total salary payout.", "SELECT SUM(salary) FROM employees;"),
        ("Max salary overall.", "SELECT MAX(salary) FROM employees;"),
        ("Min salary overall.", "SELECT MIN(salary) FROM employees;"),
        ("Employees in eng department.", "SELECT COUNT(*) FROM employees WHERE dept='eng';"),
        ("Average salary in eng.", "SELECT AVG(salary) FROM employees WHERE dept='eng';"),
        ("List employees ordered by salary desc.", "SELECT name, salary FROM employees ORDER BY salary DESC;"),
        ("Top 2 salaries.", "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 2;"),
        ("Bottom 2 salaries.", "SELECT name, salary FROM employees ORDER BY salary ASC LIMIT 2;"),
        ("Distinct departments.", "SELECT DISTINCT dept FROM employees ORDER BY dept;"),
        ("Salary sum per department.", "SELECT dept, SUM(salary) FROM employees GROUP BY dept ORDER BY dept;"),
        (
            "Eng vs non-eng headcount.",
            "SELECT CASE WHEN dept='eng' THEN 'eng' ELSE 'other' END AS grp, COUNT(*) FROM employees GROUP BY grp;",
        ),
        ("Salary range (max - min).", "SELECT MAX(salary) - MIN(salary) FROM employees;"),
        ("Employees with salary above 100k.", "SELECT name FROM employees WHERE salary > 100000 ORDER BY salary DESC;"),
        ("Employees with salary below 95k.", "SELECT name FROM employees WHERE salary < 95000 ORDER BY salary ASC;"),
        (
            "Dept with highest average salary.",
            "SELECT dept FROM employees GROUP BY dept ORDER BY AVG(salary) DESC LIMIT 1;",
        ),
    ]
    tasks: list[TaskSpec] = []
    for idx, (q, sql) in enumerate(sqls, start=1):
        tasks.append(
            TaskSpec(
                task_id=f"spider2_{idx}",
                question=q,
                gold_sql=sql,
                db_path=str(db_path),
                bench="spider2",
                meta={"smoke": True},
            )
        )
    return tasks


def _load_full_tasks(config: Config | None) -> list[TaskSpec]:
    """
    Load Spider2 tasks from tasks.jsonl if available.
    Expected default location: data/spider2/tasks.jsonl (as produced by bootstrap).
    """
    root = config.datasets.spider2_root if config else Path("data/spider2")
    jsonl_path = Path(root) / "tasks.jsonl"
    tasks: list[TaskSpec] = []
    if not jsonl_path.exists():
        return tasks
    db_root = Path(root) / "sqlite_dbs"
    with jsonl_path.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            item = json.loads(line)
            db_id = item.get("db_id") or item.get("database_id") or "unknown_db"
            db_path = db_root / f"{db_id}.db"
            if not db_path.exists():
                # also accept .sqlite
                alt = db_root / f"{db_id}.sqlite"
                if alt.exists():
                    db_path = alt
                else:
                    continue
            tasks.append(
                TaskSpec(
                    task_id=f"spider2_{item.get('question_id', len(tasks))}",
                    question=item.get("question", ""),
                    gold_sql=item.get("gold_sql") or item.get("SQL") or "",
                    db_path=str(db_path),
                    bench="spider2",
                    meta={"db_id": db_id},
                )
            )
    return tasks


@dataclass
class Spider2Benchmark(Benchmark):
    name: str = "spider2"
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
        root = self.config.datasets.spider2_root if self.config else Path("data/spider2")
        full = _load_full_tasks(self.config)
        if full:
            return full
        _log.warning("No real Spider2 data found — using smoke tasks (results will be excluded from summary)")
        return _default_tasks(root)

    # ------------------------------------------------------------------
    # Hook implementations for _run_task_common
    # ------------------------------------------------------------------

    def _get_dialect(self) -> str:
        return "sqlite"

    def _get_constraints(self) -> DialectConstraints:
        return DialectConstraints(
            dialect="sqlite",
            allowed_statements=["SELECT", "WITH"],
            forbidden_tokens=["information_schema", "pg_catalog", "describe", "desc"],
            notes=["Use only provided tables/columns."],
        )

    def _get_schema_context(self, db_path: Path) -> SchemaContext:
        return schema_from_sqlite(db_path, max_tables=self.schema_max_tables, max_cols=self.schema_max_cols_per_table)

    def _get_tool_executor(self, db_path: Path) -> SchemaToolsExecutor:
        benchmark = self

        class SqliteTools(SchemaToolsExecutor):
            def __init__(self, path: Path):
                self.path = path

            def list_tables(self) -> list[str]:
                conn = sqlite3.connect(self.path)
                try:
                    cur = conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
                    )
                    return [r[0] for r in cur.fetchall()]
                finally:
                    conn.close()

            def describe_table(self, table: str) -> dict:
                if not table:
                    return {}
                conn = sqlite3.connect(self.path)
                try:
                    cols = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
                    fks = conn.execute(f"PRAGMA foreign_key_list('{table}')").fetchall()
                    return {
                        "columns": [{"name": c[1], "type": c[2]} for c in cols],
                        "fks": [{"src": r[3], "ref_table": r[2], "ref_col": r[4]} for r in fks],
                    }
                finally:
                    conn.close()

            def dialect_info(self) -> dict[str, Any]:
                return {"dialect": "sqlite", "notes": benchmark._get_constraints().notes}

        return SqliteTools(db_path)

    def _execute_sql(self, db_path: str, sql: str) -> ExecResult:
        return execute_sql(db_path, sql)

    def run_task(self, task: TaskSpec) -> TaskResult:
        return self._run_task_common(task)
