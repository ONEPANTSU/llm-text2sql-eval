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


def _ensure_sample_db(db_path: Path) -> Path:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        return db_path

    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            DROP TABLE IF EXISTS sample;
            DROP TABLE IF EXISTS metrics;
            CREATE TABLE sample (id INTEGER PRIMARY KEY, value TEXT);
            CREATE TABLE metrics (name TEXT, v INTEGER);
            INSERT INTO sample (id, value) VALUES
              (1, 'alpha'), (2, 'beta'), (3, 'gamma');
            INSERT INTO metrics (name, v) VALUES
              ('foo', 10), ('bar', 20), ('baz', 30);
            """
        )
        conn.commit()
    finally:
        conn.close()
    return db_path


def _default_tasks(db_path: Path) -> list[TaskSpec]:
    safe_db = _ensure_sample_db(db_path)
    # Build a richer set of lightweight smoke tasks (20) so --limit can slice more than 5.
    sqls = [
        (
            "In this SQLite database, how many tables are present?",
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table';",
        ),
        ("What is the SQLite version?", "SELECT sqlite_version();"),
        ("In SQLite, how many rows are in the sample table?", "SELECT COUNT(*) FROM sample;"),
        ("List all values from sample ordered alphabetically.", "SELECT value FROM sample ORDER BY value ASC;"),
        ("What is the average v in metrics table?", "SELECT AVG(v) FROM metrics;"),
        ("Sum of v in metrics?", "SELECT SUM(v) FROM metrics;"),
        ("Max v in metrics?", "SELECT MAX(v) FROM metrics;"),
        ("Min v in metrics?", "SELECT MIN(v) FROM metrics;"),
        ("Count distinct values in sample.", "SELECT COUNT(DISTINCT value) FROM sample;"),
        ("Does sample contain 'alpha'?", "SELECT COUNT(*) FROM sample WHERE value='alpha';"),
        ("List metrics greater than 10.", "SELECT name, v FROM metrics WHERE v > 10 ORDER BY v DESC;"),
        ("List metrics less than 25.", "SELECT name, v FROM metrics WHERE v < 25 ORDER BY v ASC;"),
        (
            "Average v by first letter of name.",
            "SELECT substr(name,1,1) AS prefix, AVG(v) FROM metrics GROUP BY prefix;",
        ),
        ("How many metrics entries?", "SELECT COUNT(*) FROM metrics;"),
        ("Top metric by v.", "SELECT name, v FROM metrics ORDER BY v DESC LIMIT 1;"),
        ("Bottom metric by v.", "SELECT name, v FROM metrics ORDER BY v ASC LIMIT 1;"),
        ("Values from sample with id > 1.", "SELECT value FROM sample WHERE id > 1 ORDER BY id;"),
        ("Values from sample with id < 3.", "SELECT value FROM sample WHERE id < 3 ORDER BY id;"),
        ("Sample ids reversed.", "SELECT id FROM sample ORDER BY id DESC;"),
        (
            "Join sample to metrics on id parity.",
            "SELECT s.id, s.value, m.name FROM sample s LEFT JOIN metrics m ON (s.id % 2) = (m.v % 2) ORDER BY s.id;",
        ),
    ]
    tasks: list[TaskSpec] = []
    for idx, (q, sql) in enumerate(sqls, start=1):
        tasks.append(
            TaskSpec(
                task_id=f"bird_{idx}",
                question=q,
                gold_sql=sql,
                db_path=str(safe_db),
                bench="bird_sqlite",
                meta={"smoke": True},
            )
        )
    return tasks


def _find_db_path(base: Path, db_id: str | None) -> str:
    if not db_id:
        return ""
    candidates = [
        base / "dev_databases" / f"{db_id}.sqlite",
        base / "dev_databases" / f"{db_id}.db",
        base / "dev_databases" / db_id / f"{db_id}.sqlite",
        base / "dev_databases" / db_id / f"{db_id}.db",
    ]
    folder = base / "dev_databases" / db_id
    if folder.exists():
        for ext in ("*.sqlite", "*.db"):
            candidates.extend(folder.glob(ext))
    for path in candidates:
        if path.exists():
            return str(path)
    return ""


def _maybe_extract_dev_dbs(base: Path) -> None:
    """
    Ensure the dev SQLite databases are available locally.
    If only a zip exists, extract it so _find_db_path can locate files.
    """
    target_dir = base / "dev_databases"
    if list(target_dir.glob("*.sqlite")) or list(target_dir.glob("*.db")):
        return
    zip_path = base / "dev_databases.zip"
    if not zip_path.exists():
        return
    target_dir.mkdir(parents=True, exist_ok=True)
    import zipfile

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(target_dir)


def _load_full_tasks(config: Config | None) -> list[TaskSpec]:
    """
    Load all BIRD tasks from dev.json if available.
    Falls back to default tasks when dataset is missing.
    """
    base = Path(config.datasets.bird_root) if config else Path("data/bird")
    json_path = base / "dev.json"
    if not json_path.exists():
        alt = Path("data") / "raw" / "bird" / "dev_20240627" / "dev.json"
        if not alt.exists():
            return []
        json_path = alt
        base = json_path.parent
    _maybe_extract_dev_dbs(base)
    tasks: list[TaskSpec] = []
    with json_path.open("r") as f:
        data = json.load(f)
    for item in data:
        db_id = item.get("db_id")
        db_path = _find_db_path(base, db_id)
        if not db_path:
            # Skip tasks when the backing SQLite file is unavailable.
            continue
        tasks.append(
            TaskSpec(
                task_id=f"bird_{item.get('question_id')}",
                question=item.get("question", ""),
                gold_sql=item.get("SQL", ""),
                db_path=db_path,
                bench="bird_sqlite",
                meta={"db_id": db_id, "difficulty": item.get("difficulty")},
            )
        )
    return tasks


@dataclass
class BirdSQLiteBenchmark(Benchmark):
    name: str = "bird_sqlite"
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
        db_path = (
            Path(self.config.datasets.bird_root) / "bird_sqlite.db" if self.config else Path("data/bird_sqlite.db")
        )
        full = _load_full_tasks(self.config)
        if full:
            return full
        _log.warning("No real BIRD data found — using smoke tasks (results will be excluded from summary)")
        return _default_tasks(db_path)

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
