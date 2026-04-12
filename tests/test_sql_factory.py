"""
Unit tests for sql_factory: similarity (token, AST), scoring, sim_threshold filtering.
"""

from __future__ import annotations

from evalsuite.architectures.similarity import (
    ast_node_sequence,
    ast_similarity,
    hybrid_similarity,
    normalize_sql_for_tokens,
    similarity_max,
    token_jaccard,
    token_set,
)
from evalsuite.architectures.sql_factory import (
    SqlFactoryCandidate,
    _has_nested_select,
    build_sql_factory_params,
    compute_complexity_bonus,
    compute_score,
)

# -------- Token similarity --------


def test_token_normalize():
    s = "  SELECT   *  FROM  t  "
    assert "select * from t" in normalize_sql_for_tokens(s).lower()


def test_token_set():
    a = "SELECT a FROM t"
    b = "SELECT a FROM t"
    assert token_set(a) == token_set(b)
    c = "SELECT b FROM t"
    assert token_set(a) != token_set(c)


def test_token_jaccard_identical():
    sql = "SELECT id, name FROM users WHERE active = 1"
    assert token_jaccard(sql, sql) == 1.0


def test_token_jaccard_different():
    a = "SELECT a FROM x"
    b = "SELECT b FROM y"
    assert 0 <= token_jaccard(a, b) <= 1.0
    assert token_jaccard(a, b) < 1.0


def test_token_jaccard_overlap():
    a = "SELECT a, b FROM t"
    b = "SELECT a, c FROM t"
    j = token_jaccard(a, b)
    assert j > 0 and j < 1.0


# -------- AST similarity --------


def test_ast_node_sequence_parse_ok():
    seq = ast_node_sequence("SELECT 1 FROM t")
    assert isinstance(seq, list)
    # Should have some node types (Select, Column, Table, etc.)
    assert len(seq) >= 2


def test_ast_node_sequence_invalid_returns_empty():
    seq = ast_node_sequence("SELEC T FRO M t")  # invalid
    assert seq == [] or len(seq) == 0


def test_ast_similarity_identical():
    sql = "SELECT id FROM users"
    assert ast_similarity(sql, sql) == 1.0


def test_ast_similarity_different_structure():
    a = "SELECT 1"
    b = "SELECT a FROM t"
    sim = ast_similarity(a, b)
    assert 0 <= sim <= 1.0


# -------- Hybrid & similarity_max --------


def test_hybrid_similarity_weights():
    a = "SELECT x FROM t"
    b = "SELECT x FROM t"
    assert hybrid_similarity(a, b, w_tok=0.6, w_ast=0.3, w_emb=0.1) >= 0.89


def test_similarity_max_empty_pool():
    assert similarity_max("SELECT 1", [], k_neighbors=5) == 0.0


def test_similarity_max_with_pool():
    pool = ["SELECT a FROM t", "SELECT b FROM u"]
    sim = similarity_max("SELECT a FROM t", pool, k_neighbors=5)
    assert sim >= 0.0 and sim <= 1.0
    # Same as first in pool should give high similarity
    sim_same = similarity_max("SELECT a FROM t", pool, k_neighbors=5)
    assert sim_same >= 0.5


# -------- Scoring --------


def test_complexity_bonus_join():
    sql = "SELECT * FROM a JOIN b ON a.id = b.id"
    assert compute_complexity_bonus(sql) >= 0.1


def test_complexity_bonus_cte():
    sql = "WITH x AS (SELECT 1) SELECT * FROM x"
    assert compute_complexity_bonus(sql) >= 0.1


def test_compute_score_basic():
    c = SqlFactoryCandidate(
        sql="SELECT * FROM t",
        phase="gen",
        round_idx=0,
        exec_ok=True,
        tables_used={"t"},
        similarity_max=0.2,
        score=0.0,
    )
    sc = {"bonus_tables_cap": 6, "bonus_per_table": 0.05, "similarity_penalty": 0.5, "complexity_bonus": False}
    s = compute_score(c, sc)
    assert abs(s - (1.0 + 0.05 * 1 - 0.5 * 0.2)) < 1e-6
    assert s > 0.5


def test_build_sql_factory_params_defaults():
    params = build_sql_factory_params({})
    assert params.max_rounds == 3
    assert params.gen_batch == 2
    assert params.exp_batch == 2
    assert params.sim_threshold == 0.85
    assert params.weights.get("tok") == 0.6
    assert params.generation_timeout_per_attempt == 15


# -------- _has_nested_select --------


def test_has_nested_select_simple_count():
    """SELECT COUNT(*) FROM t — no nested SELECT, must return False."""
    assert _has_nested_select("SELECT COUNT(*) FROM t") is False


def test_has_nested_select_with_leading_whitespace():
    """Leading whitespace + function parens must NOT be a false positive."""
    assert _has_nested_select("  SELECT COUNT(*) FROM t") is False


def test_has_nested_select_subquery():
    """SELECT * FROM (SELECT id FROM t) — true nested SELECT."""
    assert _has_nested_select("SELECT * FROM (SELECT id FROM t)") is True


def test_has_nested_select_where_in_subquery():
    """WHERE x IN (SELECT ...) is a nested SELECT."""
    assert _has_nested_select("SELECT * FROM t WHERE x IN (SELECT id FROM u)") is True


def test_has_nested_select_coalesce():
    """COALESCE(a, 0) with no subquery must return False."""
    assert _has_nested_select("SELECT COALESCE(a, 0) FROM t") is False


def test_has_nested_select_exists():
    """WHERE EXISTS (SELECT 1 FROM ...) is a nested SELECT."""
    assert _has_nested_select("SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u)") is True


def test_has_nested_select_case_insensitive():
    """Subquery detection must be case-insensitive."""
    assert _has_nested_select("select * from (select id from t)") is True
    assert _has_nested_select("SELECT * FROM (select id FROM t)") is True


def test_has_nested_select_with_spaces_after_paren():
    """Space after opening paren before SELECT."""
    assert _has_nested_select("SELECT * FROM ( SELECT id FROM t )") is True
