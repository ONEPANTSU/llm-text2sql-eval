"""
Baseline v1.1: Preflight SQL check, error classification, ambiguous-column fix,
schema validation, and partial metrics extraction.
"""

from __future__ import annotations

import re
from typing import Any

from evalsuite.pipeline.utils import edit_distance

# --- Error classification ---


def classify_duckdb_error(error_message: str) -> str:
    """
    Classify DuckDB exception into: pred_parse_fail, pred_bind_fail, pred_runtime_fail.
    """
    if not error_message:
        return "pred_exec_fail"
    msg = error_message.strip()
    if "Parser Error" in msg or "parser error" in msg.lower() or "syntax error" in msg.lower():
        return "pred_parse_fail"
    if "Binder Error" in msg or "binder error" in msg.lower():
        return "pred_bind_fail"
    # Timeout, OOM, division by zero, etc.
    return "pred_runtime_fail"


def preflight_explain(con, sql: str) -> tuple[bool, str | None, str | None]:
    """
    Run EXPLAIN on SQL. Returns (ok, error_type, error_message).
    If ok, error_type and error_message are None.
    """
    try:
        con.execute("EXPLAIN " + sql)
        return True, None, None
    except Exception as exc:
        err = str(exc)
        return False, classify_duckdb_error(err), err


# --- Ambiguous column auto-patch ---

_AMBIGUOUS_PATTERN = re.compile(
    r'Ambiguous reference to column name\s+"([^"]+)"\s*\(use:\s*"([^"]+)"\s*or\s*"([^"]+)"',
    re.IGNORECASE,
)


def _extract_ambiguous_suggestion(error_message: str) -> tuple[str, list[str]] | None:
    """Extract (column_name, [alias.col, ...]) from DuckDB ambiguous error."""
    m = _AMBIGUOUS_PATTERN.search(error_message)
    if not m:
        return None
    col = m.group(1)
    opt1 = m.group(2).strip()
    opt2 = m.group(3).strip()
    return (col, [opt1, opt2])


def _pick_preferred_alias(column: str, options: list[str], schema_cache: dict[str, list[str]]) -> str:
    """
    Prefer alias from FROM-table that has this column in schema_cache; else first option.
    options are e.g. ["sr.customer_sk", "c.customer_sk"]; schema_cache is table_name -> [col1, col2].
    """
    col_lower = column.lower()
    for opt in options:
        if "." in opt:
            alias, c = opt.split(".", 1)
            alias = alias.strip().lower()
            c = c.strip().lower()
            if c != col_lower:
                continue
            for tbl, cols in schema_cache.items():
                if tbl.lower() == alias or alias in (tbl.lower(),):
                    if any(col_lower == cl.lower() for cl in cols):
                        return opt
            for tbl, cols in schema_cache.items():
                if any(col_lower == cl.lower() for cl in cols):
                    # Prefer option that matches this table's alias
                    if alias in tbl.lower() or tbl.lower().startswith(alias):
                        return opt
    return options[0] if options else column


def try_ambiguous_patch(
    sql: str,
    error_message: str,
    schema_cache: dict[str, list[str]],
) -> tuple[str | None, bool]:
    """
    One attempt to fix Ambiguous column by replacing bare column with alias.col.
    Returns (patched_sql, applied). Only replace unqualified col (not already x.col).
    """
    extracted = _extract_ambiguous_suggestion(error_message)
    if not extracted:
        return None, False
    col, options = extracted
    preferred = _pick_preferred_alias(col, options, schema_cache)
    col_esc = re.escape(col)
    # Replace bare col (not preceded by dot or word char) with preferred
    pattern = r"(?<![.\w])" + r"\b" + col_esc + r"\b" + r"(?!\w)"
    new_sql = re.sub(pattern, preferred, sql)
    if new_sql == sql:
        return None, False
    return new_sql, True


# --- Candidate binding / wrong-prefix auto-fix (v1.2) ---

# Table "X" does not have a column named "Y". Candidate bindings: "Z1", "Z2"
_TABLE_COL_PATTERN = re.compile(
    r'Table\s+"([^"]+)"\s+does not have a column named\s+"([^"]+)"[^.]*\.?\s*(?:Candidate bindings?:\s*)?([^.\n]*)',
    re.IGNORECASE,
)
# Referenced column "X" not found. Candidate bindings: "Y"
_REF_COL_PATTERN = re.compile(
    r'Referenced column\s+"([^"]+)"\s+not found[^.]*\.?\s*(?:Candidate bindings?:\s*)?([^.\n]*)',
    re.IGNORECASE,
)


def _parse_candidate_bindings(s: str) -> list[str]:
    """Parse "cs_bill_customer_sk", "cr_returned_date_sk" or similar from error text."""
    if not s or not s.strip():
        return []
    candidates = re.findall(r'"([^"]+)"', s)
    return [c.strip() for c in candidates if c.strip()]


def try_candidate_binding_patch(
    sql: str,
    error_message: str,
) -> tuple[str | None, bool, str | None, str | None, str | None]:
    """
    One attempt to fix Binder Error using Candidate bindings.
    Returns (patched_sql, applied, patch_type, from_col, to_col).
    Priority: alias_prefix + "_" + bad_column, then minimal edit distance, or single candidate.
    """
    err = error_message or ""
    # Pattern 1: Table "cs" does not have a column named "bill_customer_sk". Candidate bindings: "cs_bill_customer_sk"
    m = _TABLE_COL_PATTERN.search(err)
    if m:
        alias, bad_col, cand_text = m.group(1), m.group(2), m.group(3)
        candidates = _parse_candidate_bindings(cand_text)
        if not candidates:
            return None, False, None, None, None
        # Prefer alias + "_" + bad_column (e.g. cs_bill_customer_sk)
        preferred = f"{alias}_{bad_col}"
        if preferred in candidates:
            best = preferred
        else:
            with_prefix = [c for c in candidates if c.lower().startswith(alias.lower() + "_")]
            if with_prefix:
                best = min(
                    with_prefix,
                    key=lambda c: edit_distance(bad_col, c.split("_", 1)[-1] if "_" in c else c, case_sensitive=False),
                )
            else:
                best = min(candidates, key=lambda c: edit_distance(bad_col, c, case_sensitive=False))
        from_ref = f"{alias}.{bad_col}"
        to_ref = best if "." in best else f"{alias}.{best}"
        new_sql = sql.replace(from_ref, to_ref)
        if new_sql == sql:
            pattern_bare = r"\b" + re.escape(bad_col) + r"\b"
            new_sql = re.sub(pattern_bare, best, sql, count=1)
        if new_sql == sql:
            return None, False, None, None, None
        return new_sql, True, "candidate_binding", from_ref, to_ref

    # Pattern 2: Referenced column "sr_returned_date_sk" not found. Candidate bindings: "cr_returned_date_sk"
    m2 = _REF_COL_PATTERN.search(err)
    if m2:
        bad_col, cand_text = m2.group(1), m2.group(2)
        candidates = _parse_candidate_bindings(cand_text)
        if not candidates:
            return None, False, None, None, None
        best = min(candidates, key=lambda c: edit_distance(bad_col, c, case_sensitive=False))
        pattern = r"\b" + re.escape(bad_col) + r"\b"
        new_sql = re.sub(pattern, best, sql)
        if new_sql == sql:
            return None, False, None, None, None
        return new_sql, True, "candidate_binding", bad_col, best

    return None, False, None, None, None


# TPC-DS fact table prefixes: store_returns (sr_), catalog_returns (cr_), web_sales (ws_), catalog_sales (cs_), etc.
_FACT_PREFIXES = ("sr_", "cr_", "ws_", "cs_", "ss_", "wr_")


def try_prefix_patch(
    sql: str,
    error_message: str,
) -> tuple[str | None, bool, str | None, str | None]:
    """
    When bad_column and candidate differ only by fact-table prefix (sr_ vs cr_ vs ws_), replace all.
    Returns (patched_sql, applied, from_col, to_col).
    """
    m = _REF_COL_PATTERN.search(error_message or "")
    if not m:
        m = re.search(
            r'column named\s+"([^"]+)"[^.]*\.?\s*Candidate bindings?:\s*([^.\n]*)', error_message or "", re.IGNORECASE
        )
        if not m:
            return None, False, None, None
        bad_col, cand_text = m.group(1), m.group(2)
    else:
        bad_col, cand_text = m.group(1), m.group(2)
    candidates = _parse_candidate_bindings(cand_text)
    if not candidates:
        return None, False, None, None
    # Check if bad and best candidate differ only by prefix (e.g. sr_ vs cr_)
    best = min(candidates, key=lambda c: edit_distance(bad_col, c, case_sensitive=False))
    for pre in _FACT_PREFIXES:
        if bad_col.lower().startswith(pre) and best.lower().startswith(pre):
            continue
        suffix_bad = bad_col[len(pre) :] if bad_col.lower().startswith(pre) else None
        for pre2 in _FACT_PREFIXES:
            if pre2 == pre:
                continue
            if best.lower().startswith(pre2) and suffix_bad and best[len(pre2) :].lower() == suffix_bad.lower():
                pattern = r"\b" + re.escape(bad_col) + r"\b"
                new_sql = re.sub(pattern, best, sql)
                if new_sql != sql:
                    return new_sql, True, bad_col, best
    # Same check: bad_col and best differ only by one prefix
    for pre in _FACT_PREFIXES:
        if not bad_col.lower().startswith(pre):
            continue
        suffix = bad_col[len(pre) :]
        for pre2 in _FACT_PREFIXES:
            if pre2 == pre:
                continue
            candidate = pre2 + suffix
            if candidate.lower() == best.lower() or best.lower() == candidate.lower():
                pattern = r"\b" + re.escape(bad_col) + r"\b"
                new_sql = re.sub(pattern, best, sql)
                if new_sql != sql:
                    return new_sql, True, bad_col, best
    return None, False, None, None


# --- Schema validation (diagnostic only; never blocks execution) ---

_SQL_KEYWORDS = frozenset(
    {
        "select",
        "from",
        "where",
        "group",
        "order",
        "by",
        "on",
        "and",
        "or",
        "as",
        "left",
        "right",
        "inner",
        "outer",
        "cross",
        "join",
        "having",
        "limit",
        "offset",
        "asc",
        "desc",
        "nulls",
        "first",
        "last",
        "with",
        "union",
        "all",
        "case",
        "when",
        "then",
        "else",
        "end",
        "cast",
        "between",
        "in",
        "is",
        "not",
        "like",
        "over",
    }
)


def extract_cte_names(sql: str) -> set[str]:
    """Extract CTE names from WITH cte_name AS (...). These are allowed relations, not tables."""
    if not sql or not sql.strip():
        return set()
    ctes: set[str] = set()
    # WITH cte1 AS (...), cte2 AS (...) SELECT ...
    rest = sql.strip()
    if not rest.upper().startswith("WITH "):
        return ctes
    rest = rest[5:].lstrip()
    depth = 0
    i = 0
    while i < len(rest):
        if rest[i] == "(":
            depth += 1
            i += 1
            continue
        if rest[i] == ")":
            depth -= 1
            i += 1
            continue
        if depth == 0 and rest[i : i + 3].upper() == "AS ":
            # Back up to get identifier before AS
            j = i - 1
            while j >= 0 and rest[j] in " \t\n\r":
                j -= 1
            k = j + 1
            while j >= 0 and (rest[j].isalnum() or rest[j] == "_"):
                j -= 1
            name = rest[j + 1 : k].strip()
            if name and name.lower() not in _SQL_KEYWORDS:
                ctes.add(name)
            i += 3
            continue
        i += 1
    # Simpler: WITH word AS
    for m in re.finditer(r"\bWITH\s+([a-zA-Z0-9_]+)\s+AS\b", sql, re.IGNORECASE):
        ctes.add(m.group(1))
    for m in re.finditer(r",\s*([a-zA-Z0-9_]+)\s+AS\s+(?:\()", sql, re.IGNORECASE):
        ctes.add(m.group(1))
    return ctes


def build_alias_map(sql: str, cte_names: set[str] | None = None) -> dict[str, str]:
    """
    Build alias -> source (table or CTE name) from FROM/JOIN.
    FROM store_returns srt -> srt: store_returns; WITH ctr AS (...) FROM ctr -> ctr: ctr (CTE).
    """
    if not sql or not sql.strip():
        return {}
    cte_names = cte_names or set()
    sql_clean = re.sub(r"--[^\n]*", " ", sql)
    sql_clean = re.sub(r"/\*.*?\*/", " ", sql_clean, flags=re.DOTALL)
    alias_map: dict[str, str] = {}
    # FROM table [alias] / JOIN table [alias]
    from_join = re.findall(
        r"\b(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)(?:\s+(?:AS\s+)?([a-zA-Z0-9_]+))?(?=\s*\)|\s*,|\s+JOIN|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+LIMIT|$)",
        sql_clean,
        re.IGNORECASE,
    )
    for m in from_join:
        source = m[0]
        alias = m[1].strip() if m[1] else source
        if source.lower() not in _SQL_KEYWORDS:
            alias_map[alias] = source
    return alias_map


def extract_tables_from_sql(sql: str, cte_names: set[str] | None = None) -> set[str]:
    """Extract table/relation names from FROM and JOIN (simple regex, top-level)."""
    if not sql or not sql.strip():
        return set()
    cte_names = cte_names or set()
    sql_clean = re.sub(r"--[^\n]*", " ", sql)
    sql_clean = re.sub(r"/\*.*?\*/", " ", sql_clean, flags=re.DOTALL)
    tables = set()
    from_join = re.findall(
        r"\b(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)(?:\s+(?:AS\s+)?([a-zA-Z0-9_]+))?(?:\s*,\s*|\s+|$)",
        sql_clean,
        re.IGNORECASE,
    )
    for m in from_join:
        tables.add(m[0])
        if m[1]:
            tables.add(m[1])
    return {t for t in tables if t.lower() not in _SQL_KEYWORDS}


def extract_referenced_columns_qualified(sql: str) -> list[tuple[str, str]]:
    """
    Extract alias.col references (alias, col). Only alias.column form.
    Exclude: numeric col (1, 1.2), SQL keywords, common aggregate-like names.
    """
    refs = re.findall(r"\b([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\b", sql)
    out: list[tuple[str, str]] = []
    for a, c in refs:
        if a.lower() in _SQL_KEYWORDS:
            continue
        # Skip numeric "column" (e.g. 1.0, 100)
        if c.replace(".", "").isdigit():
            continue
        if c.lower() in _SQL_KEYWORDS:
            continue
        # Skip obvious aggregate/expression aliases (avg_*, sum_*, count_*, etc.)
        if re.match(r"^(avg|sum|min|max|count|agg)\d*_", c.lower()):
            continue
        out.append((a, c))
    return out


def validate_schema_refs(
    sql: str,
    schema_cache: dict[str, list[str]],
    list_tables: list[str] | None = None,
    cte_names: set[str] | None = None,
    alias_map: dict[str, str] | None = None,
) -> tuple[bool, list[str], list[str]]:
    """
    Diagnostic only: check referenced tables and alias.col. Never used as hard-gate.
    Returns (ok, missing_tables, missing_columns). Execution always proceeds (EXPLAIN is source of truth).
    """
    cte_names = cte_names or extract_cte_names(sql)
    alias_map = alias_map or build_alias_map(sql, cte_names)
    ref_tables = extract_tables_from_sql(sql, cte_names)
    known = set(k.lower() for k in (list_tables or []) + list(schema_cache))
    known.update(c.lower() for c in cte_names)
    missing_tables = []
    for t in ref_tables:
        if t.lower() in known:
            continue
        if t in cte_names or t.lower() in {c.lower() for c in cte_names}:
            continue
        if len(t) <= 3 and "_" not in t:
            continue
        missing_tables.append(t)
    missing_columns = []
    for alias, col in extract_referenced_columns_qualified(sql):
        source = alias_map.get(alias) or alias_map.get(alias.lower())
        if not source:
            continue
        if source in cte_names or source.lower() in {c.lower() for c in cte_names}:
            continue
        base_table = source
        cols = schema_cache.get(base_table) or schema_cache.get(base_table.lower())
        if not cols:
            for k, v in schema_cache.items():
                if k.lower() == base_table.lower():
                    cols = v
                    break
        if cols and not any(c.lower() == col.lower() for c in cols):
            missing_columns.append(f"{alias}.{col}")
        elif not cols and schema_cache:
            missing_columns.append(f"{alias}.{col}")
    ok = len(missing_tables) == 0 and len(missing_columns) == 0
    return ok, missing_tables, missing_columns


# --- Partial metrics ---


def _normalize_filter_token(s: str) -> str:
    return s.strip().lower().replace(" ", "")


def extract_filters(sql: str) -> list[str]:
    """Extract filter conditions: col=literal, col IN (...), col BETWEEN a AND b."""
    if not sql:
        return []
    tokens = []
    # col = literal (including 'TN', 2000, etc.)
    for m in re.finditer(r"(\w+)\s*=\s*([^\s,\)]+|'[^']*'|\"[^\"]*\")", sql, re.IGNORECASE):
        tokens.append(_normalize_filter_token(f"{m.group(1)}={m.group(2)}"))
    # col IN (...)
    for m in re.finditer(r"(\w+)\s+IN\s*\([^)]+\)", sql, re.IGNORECASE):
        tokens.append(_normalize_filter_token(m.group(0)))
    # col BETWEEN a AND b
    for m in re.finditer(r"(\w+)\s+BETWEEN\s+.+?\s+AND\s+\S+", sql, re.IGNORECASE):
        tokens.append(_normalize_filter_token(m.group(0)))
    return tokens


def extract_group_by_columns(sql: str) -> list[str]:
    """Extract GROUP BY column list (first occurrence, top-level)."""
    if not sql:
        return []
    m = re.search(r"\bGROUP\s+BY\s+(.+?)(?=\bORDER\b|\bHAVING\b|\bLIMIT\b|$)", sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return []
    part = m.group(1).strip()
    # Split by comma, respect parentheses
    cols = []
    depth = 0
    cur = []
    for c in part:
        if c == "(":
            depth += 1
            cur.append(c)
        elif c == ")":
            depth -= 1
            cur.append(c)
        elif c == "," and depth == 0:
            cols.append("".join(cur).strip())
            cur = []
        else:
            cur.append(c)
    if cur:
        cols.append("".join(cur).strip())
    return [c.strip().lower() for c in cols if c.strip()]


def compute_filter_f1(gold_filters: list[str], pred_filters: list[str]) -> float:
    """F1 based on exact token match."""
    gset = set(gold_filters)
    pset = set(pred_filters)
    if not gset and not pset:
        return 1.0
    if not gset or not pset:
        return 0.0
    inter = len(gset & pset)
    prec = inter / len(pset) if pset else 0.0
    rec = inter / len(gset) if gset else 0.0
    if prec + rec == 0:
        return 0.0
    return 2 * prec * rec / (prec + rec)


def extract_partial_metrics(gold_sql: str, pred_sql: str) -> dict[str, Any]:
    """
    Compute table coverage, filter F1, group-by match.
    Returns dict: gold_tables, pred_tables, table_jaccard, gold_filters, pred_filters, filter_f1,
                 gold_group_by_cols, pred_group_by_cols, group_by_match.
    """
    gold_tables = set(extract_tables_from_sql(gold_sql or ""))
    pred_tables = set(extract_tables_from_sql(pred_sql or ""))
    inter = len(gold_tables & pred_tables)
    union = len(gold_tables | pred_tables)
    table_jaccard = (inter / union) if union else 1.0

    gold_filters = extract_filters(gold_sql or "")
    pred_filters = extract_filters(pred_sql or "")
    filter_f1 = compute_filter_f1(gold_filters, pred_filters)

    gold_gb = extract_group_by_columns(gold_sql or "")
    pred_gb = extract_group_by_columns(pred_sql or "")
    group_by_match = set(gold_gb) == set(pred_gb)

    return {
        "gold_tables": sorted(gold_tables),
        "pred_tables": sorted(pred_tables),
        "table_jaccard": round(table_jaccard, 4),
        "gold_filters": gold_filters,
        "pred_filters": pred_filters,
        "filter_f1": round(filter_f1, 4),
        "gold_group_by_cols": gold_gb,
        "pred_group_by_cols": pred_gb,
        "group_by_match": group_by_match,
    }
