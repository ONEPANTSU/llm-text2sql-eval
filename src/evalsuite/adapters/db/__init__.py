"""Database adapters for SQL execution.

Contract: every adapter must expose execute_sql(db_path, sql, ...) -> ExecResult.

To add a new database:
1. Create evalsuite/adapters/db/my_db.py
2. Implement execute_sql():

    from evalsuite.core.types import ExecResult

    def execute_sql(db_path: str, sql: str, timeout_sec: int | None = None) -> ExecResult:
        '''Execute SQL against the database.

        Args:
            db_path: Path to database file.
            sql: SQL query to execute.
            timeout_sec: Optional execution timeout.

        Returns:
            ExecResult(ok=True, rows=[...]) on success.
            ExecResult(ok=False, rows=None, error="...") on failure.
        '''
        ...

3. Use in your benchmark's _execute_sql() method.

See sqlite.py (simplest) or duckdb.py (with preflight/EXPLAIN).
"""
