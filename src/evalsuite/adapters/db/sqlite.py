from __future__ import annotations

import sqlite3

from evalsuite.core.types import ExecResult


def execute_sql(db_path: str, sql: str) -> ExecResult:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows: list = cur.fetchall()
        return ExecResult(ok=True, rows=rows)
    except Exception as exc:  # pragma: no cover - exercised in smoke tests
        return ExecResult(ok=False, rows=None, error=str(exc))
    finally:
        conn.close()
