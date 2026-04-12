from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import TYPE_CHECKING

import duckdb

from evalsuite.core.types import ExecResult
from evalsuite.pipeline.preflight import preflight_explain

if TYPE_CHECKING:
    pass  # DuckDB connection type for execute_sql_on_connection


def execute_sql_on_connection(con: duckdb.DuckDBPyConnection, sql: str) -> ExecResult:
    """Run SQL on an existing connection. Caller owns the connection."""
    try:
        rows: list = con.execute(sql).fetchall()
        return ExecResult(ok=True, rows=rows)
    except Exception as exc:
        return ExecResult(ok=False, rows=None, error=str(exc))


def preflight_and_execute(
    con: duckdb.DuckDBPyConnection,
    sql: str,
) -> tuple[ExecResult, str | None]:
    """
    Run EXPLAIN then execute. Returns (ExecResult, error_type).
    error_type is None if ok; else pred_parse_fail | pred_bind_fail | pred_runtime_fail.
    """
    ok, err_type, err_msg = preflight_explain(con, sql)
    if not ok:
        return ExecResult(ok=False, rows=None, error=err_msg or ""), err_type or "pred_exec_fail"
    try:
        rows: list = con.execute(sql).fetchall()
        return ExecResult(ok=True, rows=rows), None
    except Exception as exc:
        from evalsuite.pipeline.preflight import classify_duckdb_error

        return ExecResult(ok=False, rows=None, error=str(exc)), classify_duckdb_error(str(exc))


def _run_query(db_path: str, sql: str) -> list:
    con = duckdb.connect(db_path, read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def execute_sql(db_path: str, sql: str, timeout_sec: int | None = None) -> ExecResult:
    try:
        if timeout_sec is None or timeout_sec <= 0:
            rows: list = _run_query(db_path, sql)
            return ExecResult(ok=True, rows=rows)
        with ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(_run_query, db_path, sql)
            rows = future.result(timeout=timeout_sec)
        return ExecResult(ok=True, rows=rows)
    except FuturesTimeoutError:
        return ExecResult(ok=False, rows=None, error=f"execution timeout ({timeout_sec}s)")
    except Exception as exc:  # pragma: no cover - exercised in smoke tests
        return ExecResult(ok=False, rows=None, error=str(exc))


def preflight_and_execute_db(db_path: str, sql: str, timeout_sec: int | None = None) -> tuple[ExecResult, str | None]:
    """
    Open DB, run EXPLAIN then execute. Returns (ExecResult, error_type).
    error_type is None if ok; else pred_parse_fail | pred_bind_fail | pred_runtime_fail.
    """
    con = duckdb.connect(db_path, read_only=True)
    try:
        result, err_type = preflight_and_execute(con, sql)
        return result, err_type
    finally:
        con.close()
