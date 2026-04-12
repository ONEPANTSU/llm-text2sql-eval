from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb

from evalsuite.core.types import ColumnInfo, FKInfo, SchemaContext, TableInfo


def _sqlite_tables(conn: sqlite3.Connection) -> list[str]:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%';")
    return [r[0] for r in cur.fetchall()]


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> list[ColumnInfo]:
    cur = conn.execute(f"PRAGMA table_info('{table}')")
    cols = []
    for _, name, col_type, *_ in cur.fetchall():
        cols.append(ColumnInfo(name=name, type=col_type))
    return cols


def _sqlite_fks(conn: sqlite3.Connection, table: str) -> list[FKInfo]:
    cur = conn.execute(f"PRAGMA foreign_key_list('{table}')")
    fks = []
    for _, _, ref_table, from_col, ref_col, *_ in cur.fetchall():
        fks.append(FKInfo(src=from_col, ref_table=ref_table, ref_col=ref_col))
    return fks


def schema_from_sqlite(db_path: Path, max_tables: int | None = None, max_cols: int | None = None) -> SchemaContext:
    conn = sqlite3.connect(str(db_path))
    try:
        table_names = _sqlite_tables(conn)
        tables: list[TableInfo] = []
        for t in table_names[: max_tables or len(table_names)]:
            cols = _sqlite_columns(conn, t)
            fks = _sqlite_fks(conn, t)
            tables.append(
                TableInfo(
                    name=t,
                    columns=cols[: max_cols or len(cols)],
                    fks=fks,
                )
            )
    finally:
        conn.close()
    return SchemaContext(dialect="sqlite", tables=tables)


def schema_from_duckdb_conn(
    con: duckdb.DuckDBPyConnection,
    max_tables: int | None = None,
    max_cols: int | None = None,
) -> SchemaContext:
    """Build schema from an existing DuckDB connection (caller owns lifecycle)."""
    rows = con.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type='BASE TABLE'
        ORDER BY table_schema, table_name
        """
    ).fetchall()
    tables: list[TableInfo] = []
    for schema_name, table_name in rows[: max_tables or len(rows)]:
        cols_rows = con.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema=? AND table_name=?
            ORDER BY ordinal_position
            """,
            [schema_name, table_name],
        ).fetchall()
        cols = [ColumnInfo(name=c, type=t) for c, t in cols_rows][: max_cols or len(cols_rows)]
        tables.append(TableInfo(name=table_name, columns=cols, fks=[]))
    return SchemaContext(dialect="duckdb", tables=tables)


def schema_from_duckdb(db_path: Path, max_tables: int | None = None, max_cols: int | None = None) -> SchemaContext:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return schema_from_duckdb_conn(con, max_tables=max_tables, max_cols=max_cols)
    finally:
        con.close()
