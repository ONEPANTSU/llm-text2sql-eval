from __future__ import annotations

import json

from evalsuite.core.types import DialectConstraints, SchemaContext


def _limit_list(items: list, limit: int) -> list:
    if limit and limit > 0 and len(items) > limit:
        return items[:limit]
    return items


def compact_schema_prompt(
    schema: SchemaContext, max_tables: int, max_cols: int, constraints: DialectConstraints | None
) -> str:
    tables = _limit_list(schema.tables, max_tables)
    lines: list[str] = [f"DIALECT: {schema.dialect}"]
    lines.append("SCHEMA:")
    for t in tables:
        cols = _limit_list(t.columns, max_cols)
        col_names = ", ".join([c.name for c in cols])
        lines.append(f"- {t.name}({col_names})")
    fk_lines = []
    for t in tables:
        for fk in t.fks:
            fk_lines.append(f"- {t.name}.{fk.src} -> {fk.ref_table}.{fk.ref_col}")
    if fk_lines:
        lines.append("FK:")
        lines.extend(fk_lines)
    if constraints:
        lines.append("RULES:")
        if constraints.allowed_statements:
            lines.append(f"- Allowed statements: {', '.join(constraints.allowed_statements)}")
        if constraints.forbidden_tokens:
            lines.append(f"- Forbidden: {', '.join(constraints.forbidden_tokens)}")
        if constraints.notes:
            for n in constraints.notes:
                lines.append(f"- {n}")
    lines.append("- Return SQL only. No markdown.")
    if constraints and getattr(constraints, "omit_best_effort_instruction", False):
        lines.append("- If required columns are not found, do NOT substitute with other tables or columns.")
    else:
        lines.append("- If a needed column is missing, use best effort with available schema.")
    return "\n".join(lines)


def ddl_schema_prompt(
    schema: SchemaContext, max_tables: int, max_cols: int, constraints: DialectConstraints | None
) -> str:
    tables = _limit_list(schema.tables, max_tables)
    parts: list[str] = [f"-- DIALECT: {schema.dialect}"]
    for t in tables:
        cols = _limit_list(t.columns, max_cols)
        col_defs = ",\n  ".join([f"{c.name} {c.type or ''}".strip() for c in cols])
        parts.append(f"CREATE TABLE {t.name} (\n  {col_defs}\n);")
    if constraints:
        parts.append(f"-- Allowed: {', '.join(constraints.allowed_statements)}")
        if constraints.forbidden_tokens:
            parts.append(f"-- Forbidden: {', '.join(constraints.forbidden_tokens)}")
        for n in constraints.notes:
            parts.append(f"-- {n}")
    parts.append("-- Return SQL only.")
    if constraints and getattr(constraints, "omit_best_effort_instruction", False):
        parts.append("-- If required columns are not found, do NOT substitute with other tables or columns.")
    else:
        parts.append("-- If a needed column is missing, use best effort with available schema.")
    return "\n".join(parts)


def json_schema_prompt(
    schema: SchemaContext, max_tables: int, max_cols: int, constraints: DialectConstraints | None
) -> str:
    tables = []
    for t in _limit_list(schema.tables, max_tables):
        tables.append(
            {
                "name": t.name,
                "columns": [c.name for c in _limit_list(t.columns, max_cols)],
                "fks": [{"src": fk.src, "ref_table": fk.ref_table, "ref_col": fk.ref_col} for fk in t.fks],
            }
        )
    payload = {
        "dialect": schema.dialect,
        "tables": tables,
    }
    if constraints:
        payload["rules"] = {
            "allowed_statements": constraints.allowed_statements,
            "forbidden_tokens": constraints.forbidden_tokens,
            "notes": constraints.notes,
        }
    return json.dumps(payload, indent=2)


def build_schema_prompt(
    schema: SchemaContext, fmt: str, max_tables: int, max_cols: int, constraints: DialectConstraints | None
) -> str:
    fmt = (fmt or "compact").lower()
    if fmt == "ddl":
        return ddl_schema_prompt(schema, max_tables, max_cols, constraints)
    if fmt == "json":
        return json_schema_prompt(schema, max_tables, max_cols, constraints)
    return compact_schema_prompt(schema, max_tables, max_cols, constraints)
