"""
SQL similarity: hybrid token + AST (optional embedding).
Used by sql_factory to reject duplicates (similarity_max >= threshold).
"""

from __future__ import annotations

import re

from evalsuite.pipeline.utils import edit_distance

try:
    import sqlglot
except ImportError:
    sqlglot = None  # type: ignore


def normalize_sql_for_tokens(sql: str) -> str:
    """Lowercase, collapse whitespace, strip."""
    if not sql or not sql.strip():
        return ""
    s = sql.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s.strip()


_SQL_TOKEN_RE = re.compile(
    r"\b(?:select|from|where|group|order|by|having|limit|offset|join|left|right|inner|outer|"
    r"on|and|or|as|with|union|all|case|when|then|else|end|over|asc|desc|nulls|first|last)\b"
    r"|[a-zA-Z_][a-zA-Z0-9_]*"
    r"|\d+(?:\.\d+)?"
    r"|[=<>!]+|[,;()*.]",
    re.IGNORECASE,
)


def tokenize_sql(sql: str) -> list[str]:
    """Tokenize normalized SQL (keywords, identifiers, numbers, operators)."""
    norm = normalize_sql_for_tokens(sql)
    if not norm:
        return []
    tokens: list[str] = []
    for m in _SQL_TOKEN_RE.finditer(norm):
        t = m.group(0).strip()
        if t:
            tokens.append(t.lower())
    return tokens


def token_set(sql: str) -> set:
    """Bag-of-tokens as set for Jaccard."""
    return set(tokenize_sql(sql))


def token_jaccard(a: str, b: str) -> float:
    """Jaccard similarity on token sets. [0, 1]."""
    sa, sb = token_set(a), token_set(b)
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / union if union else 0.0


def ast_node_sequence(sql: str) -> list[str]:
    """
    Structural fingerprint: sequence of node types (depth-first).
    Returns empty list if parse fails (AST similarity will be 0).
    """
    if not sql or not sql.strip():
        return []
    if sqlglot is None:
        return []
    try:
        parsed = sqlglot.parse_one(sql)
    except Exception:
        return []

    out: list[str] = []
    for node in parsed.walk():
        out.append(node.__class__.__name__)
    return out


def ast_similarity(a: str, b: str) -> float:
    """
    1 - normalized edit distance between AST node sequences.
    If either AST fails to parse, returns 0.0.
    """
    sa, sb = ast_node_sequence(a), ast_node_sequence(b)
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    d = edit_distance(sa, sb)
    max_len = max(len(sa), len(sb))
    norm_dist = d / max_len if max_len else 0.0
    return 1.0 - norm_dist


def hybrid_similarity(
    a: str,
    b: str,
    w_tok: float = 0.6,
    w_ast: float = 0.3,
    w_emb: float = 0.1,
    emb_sim: float | None = None,
) -> float:
    """
    Sim = w_tok * token_sim + w_ast * ast_sim + w_emb * emb_sim.
    If w_emb > 0 and emb_sim is None, use 0.0 for embedding component.
    """
    tok = token_jaccard(a, b)
    ast = ast_similarity(a, b)
    emb = (emb_sim if emb_sim is not None else 0.0) if w_emb > 0 else 0.0
    return w_tok * tok + w_ast * ast + w_emb * emb


def similarity_max(
    candidate_sql: str,
    pool_sqls: list[str],
    k_neighbors: int = 5,
    w_tok: float = 0.6,
    w_ast: float = 0.3,
    w_emb: float = 0.1,
) -> float:
    """
    Max similarity between candidate and up to k_neighbors in pool.
    If pool is smaller than k, use all. No embedding in MVP.
    """
    if not pool_sqls:
        return 0.0
    sims = [hybrid_similarity(candidate_sql, p, w_tok=w_tok, w_ast=w_ast, w_emb=w_emb) for p in pool_sqls]
    k = min(k_neighbors, len(sims))
    if k <= 0:
        return 0.0
    sims.sort(reverse=True)
    return sims[0] if sims else 0.0


def similarity_max_token_only(candidate_sql: str, pool_sqls: list[str]) -> float:
    """Max token Jaccard between candidate and pool (fast, no AST). For ordering/sort only."""
    if not pool_sqls:
        return 0.0
    return max(token_jaccard(candidate_sql, p) for p in pool_sqls)
