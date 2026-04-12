"""Unit tests for result_signature: row sorting, column order, normalization."""

from __future__ import annotations

from evalsuite.core.types import ExecResult
from evalsuite.pipeline.result_signature import result_signature


def _ok(rows):
    return ExecResult(ok=True, rows=rows)


def _fail():
    return ExecResult(ok=False, rows=None, error="fail")


# -------- Row sorting --------


def test_same_rows_different_order_same_signature():
    """Core invariant: row order must not affect signature when sort_rows=True."""
    r1 = _ok([[1, 2], [3, 4]])
    r2 = _ok([[3, 4], [1, 2]])
    sig1 = result_signature(r1, sort_rows=True)
    sig2 = result_signature(r2, sort_rows=True)
    assert sig1 is not None
    assert sig1 == sig2


def test_different_rows_different_signature():
    r1 = _ok([[1, 2], [3, 4]])
    r2 = _ok([[1, 2], [5, 6]])
    sig1 = result_signature(r1, sort_rows=True)
    sig2 = result_signature(r2, sort_rows=True)
    assert sig1 != sig2


def test_sort_rows_false_preserves_order():
    """When sort_rows=False, different row order gives different signatures."""
    r1 = _ok([[1, 2], [3, 4]])
    r2 = _ok([[3, 4], [1, 2]])
    sig1 = result_signature(r1, sort_rows=False)
    sig2 = result_signature(r2, sort_rows=False)
    assert sig1 != sig2


# -------- Column order sensitivity --------


def test_column_order_sensitive_true():
    """Default: column order matters."""
    r1 = _ok([[1, 2]])
    r2 = _ok([[2, 1]])
    sig1 = result_signature(r1, column_order_sensitive=True)
    sig2 = result_signature(r2, column_order_sensitive=True)
    assert sig1 != sig2


def test_column_order_sensitive_false():
    """When column_order_sensitive=False, column order doesn't matter."""
    r1 = _ok([[1, 2]])
    r2 = _ok([[2, 1]])
    sig1 = result_signature(r1, column_order_sensitive=False)
    sig2 = result_signature(r2, column_order_sensitive=False)
    assert sig1 == sig2


def test_row_sort_with_column_insensitive():
    """Both sort_rows=True and column_order_sensitive=False: rows and columns both normalized."""
    r1 = _ok([[3, 4], [1, 2]])
    r2 = _ok([[2, 1], [4, 3]])
    sig1 = result_signature(r1, sort_rows=True, column_order_sensitive=False)
    sig2 = result_signature(r2, sort_rows=True, column_order_sensitive=False)
    assert sig1 == sig2


# -------- Edge cases --------


def test_not_ok_returns_none():
    assert result_signature(_fail()) is None


def test_none_rows_returns_none():
    r = ExecResult(ok=True, rows=None)
    assert result_signature(r) is None


def test_empty_rows():
    sig = result_signature(_ok([]))
    assert sig is not None


def test_max_rows_truncates():
    r = _ok([[i] for i in range(100)])
    sig_full = result_signature(r, max_rows=None)
    sig_10 = result_signature(r, max_rows=10)
    assert sig_full != sig_10


def test_float_normalization():
    """Floats with tiny differences should produce same signature after rounding."""
    r1 = _ok([[1.00000000001]])
    r2 = _ok([[1.00000000002]])
    sig1 = result_signature(r1)
    sig2 = result_signature(r2)
    assert sig1 == sig2


def test_none_cell():
    r1 = _ok([[None, 1]])
    r2 = _ok([[None, 1]])
    sig1 = result_signature(r1)
    sig2 = result_signature(r2)
    assert sig1 == sig2
