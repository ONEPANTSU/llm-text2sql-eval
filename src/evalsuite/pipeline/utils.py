"""Shared utility functions."""

from __future__ import annotations

from collections.abc import Sequence


def edit_distance(a: Sequence, b: Sequence, *, case_sensitive: bool = True) -> int:
    """Levenshtein distance between two sequences.

    When *case_sensitive* is ``False`` elements are compared after
    ``.lower()`` — useful for SQL identifier matching.
    """
    n, m = len(a), len(b)
    if n == 0:
        return m
    if m == 0:
        return n
    dp = [[0] * (m + 1) for _ in range(n + 1)]
    for i in range(n + 1):
        dp[i][0] = i
    for j in range(m + 1):
        dp[0][j] = j
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            ai, bj = a[i - 1], b[j - 1]
            if not case_sensitive:
                ai, bj = ai.lower(), bj.lower()
            cost = 0 if ai == bj else 1
            dp[i][j] = min(
                dp[i - 1][j] + 1,
                dp[i][j - 1] + 1,
                dp[i - 1][j - 1] + cost,
            )
    return dp[n][m]
