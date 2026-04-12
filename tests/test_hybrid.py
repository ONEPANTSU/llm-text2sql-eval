"""Unit tests for hybrid architecture: Phase 1 (SGR grounding) + Phase 2 (generation) + Phase 3 (expansion)."""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import Any
from unittest.mock import MagicMock, patch

from evalsuite.architectures.hybrid import (
    HybridParams,
    _expand_one,
    _generate_one_candidate,
    _generate_retry_candidates,
    _select_seeds,
    _should_diagnostic_retry,
    build_hybrid_params,
    run_hybrid,
    validate_sql_schema,
)
from evalsuite.core.types import CandidateResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class MockModel:
    """Model adapter that returns deterministic SQL based on seed.

    Default SQL is ``SELECT <seed>`` which executes on any SQLite DB
    (no tables needed).
    """

    def __init__(
        self,
        responses: dict[int, str] | None = None,
        fail_on: set | None = None,
    ):
        self.responses = responses or {}
        self.fail_on = fail_on or set()
        self.calls: list[dict[str, Any]] = []

    def generate_sql(
        self,
        question: str,
        schema: str | None = None,
        messages: Any = None,
        *,
        temperature: float | None = None,
        top_p: float | None = None,
        seed: int | None = None,
        **kwargs: Any,
    ) -> str:
        self.calls.append(
            {
                "question": question,
                "schema": schema,
                "temperature": temperature,
                "top_p": top_p,
                "seed": seed,
            }
        )
        if seed in self.fail_on:
            raise RuntimeError(f"generation failed for seed {seed}")
        if seed in self.responses:
            return self.responses[seed]
        # Tableless SELECT — works on any SQLite DB including :memory:
        return f"SELECT {seed}"


def _default_params(**overrides: Any) -> HybridParams:
    defaults = {
        "initial_candidates": 3,
        "temperature": 0.7,
        "top_p": 0.9,
        "parallelism": "sequential",
        "max_workers": 3,
        "generation_timeout": 10,
        "aggregation_mode": "hybrid",
        "execution_timeout": 10,
        "expansion_enabled": False,
    }
    defaults.update(overrides)
    return HybridParams(**defaults)


def _make_test_db() -> str:
    """Create a temporary SQLite DB with a simple table for testing."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    conn.execute("INSERT INTO t VALUES (1, 'alice')")
    conn.execute("INSERT INTO t VALUES (2, 'bob')")
    conn.commit()
    conn.close()
    return path


# ---------------------------------------------------------------------------
# build_hybrid_params
# ---------------------------------------------------------------------------


def test_build_hybrid_params_defaults():
    p = build_hybrid_params({})
    assert p.initial_candidates == 5
    assert p.temperature == 0.7
    assert p.parallelism == "parallel"
    assert p.aggregation_mode == "hybrid"
    assert p.expansion_enabled is True


def test_build_hybrid_params_overrides():
    p = build_hybrid_params(
        {
            "initial_candidates": 3,
            "temperature": 0.5,
            "parallelism": "sequential",
            "expansion_enabled": False,
        }
    )
    assert p.initial_candidates == 3
    assert p.temperature == 0.5
    assert p.parallelism == "sequential"
    assert p.expansion_enabled is False


# ---------------------------------------------------------------------------
# _generate_one_candidate
# ---------------------------------------------------------------------------


def test_generate_one_candidate_success():
    model = MockModel(responses={42: "SELECT 1"})
    params = _default_params()
    attempt_id, raw, sql, gen_params = _generate_one_candidate(
        attempt_id=0,
        model=model,
        question="q",
        schema=None,
        messages=None,
        params=params,
        base_seed=42,
    )
    assert attempt_id == 0
    assert sql == "SELECT 1"
    assert "gen_error" not in gen_params
    assert gen_params["seed"] == 42


def test_generate_one_candidate_with_fences():
    model = MockModel(responses={42: "```sql\nSELECT 1\n```"})
    params = _default_params()
    _, _, sql, gen_params = _generate_one_candidate(
        attempt_id=0,
        model=model,
        question="q",
        schema=None,
        messages=None,
        params=params,
        base_seed=42,
    )
    assert sql == "SELECT 1"
    assert "gen_error" not in gen_params


def test_generate_one_candidate_failure():
    model = MockModel(fail_on={42})
    params = _default_params()
    attempt_id, raw, sql, gen_params = _generate_one_candidate(
        attempt_id=0,
        model=model,
        question="q",
        schema=None,
        messages=None,
        params=params,
        base_seed=42,
    )
    assert sql == ""
    assert "gen_error" in gen_params


def test_generate_one_candidate_placeholder():
    model = MockModel(responses={42: "SELECT <column> FROM <table>"})
    params = _default_params()
    _, raw, sql, gen_params = _generate_one_candidate(
        attempt_id=0,
        model=model,
        question="q",
        schema=None,
        messages=None,
        params=params,
        base_seed=42,
    )
    assert sql == ""  # placeholder detected
    assert "placeholder" in gen_params


def test_generate_seeds_per_attempt():
    """Each attempt_id gets base_seed + attempt_id as seed."""
    model = MockModel()
    params = _default_params()
    for i in range(3):
        _, _, _, gen_params = _generate_one_candidate(
            attempt_id=i,
            model=model,
            question="q",
            schema=None,
            messages=None,
            params=params,
            base_seed=42,
        )
        assert gen_params["seed"] == 42 + i


# ---------------------------------------------------------------------------
# run_hybrid (Phase 2 end-to-end)
# ---------------------------------------------------------------------------


def test_run_hybrid_returns_k_candidates():
    """run_hybrid with mock adapter should return K CandidateResult objects."""
    k = 5
    db_path = _make_test_db()
    try:
        model = MockModel()
        params = _default_params(initial_candidates=k)

        def get_ctx():
            return {"question": "How many users?", "schema": "CREATE TABLE t (id INT)"}

        sel_sql, candidates, artifact = run_hybrid(
            task_id="test-001",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
        )
        assert len(candidates) == k
        assert all(isinstance(c, CandidateResult) for c in candidates)
        assert artifact["initial_candidates_requested"] == k
        assert artifact["initial_candidates_generated"] == k
    finally:
        os.unlink(db_path)


def test_run_hybrid_attempt_ids():
    """Each candidate should have attempt_id from 0 to K-1."""
    k = 4
    db_path = _make_test_db()
    try:
        model = MockModel()
        params = _default_params(initial_candidates=k)

        def get_ctx():
            return {"question": "q"}

        _, candidates, _ = run_hybrid(
            task_id="test-002",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
        )
        ids = sorted(c.attempt_id for c in candidates)
        assert ids == list(range(k))
    finally:
        os.unlink(db_path)


def test_run_hybrid_parallel_mode():
    """Parallel mode should also produce K candidates."""
    k = 3
    db_path = _make_test_db()
    try:
        model = MockModel()
        params = _default_params(initial_candidates=k, parallelism="parallel", max_workers=3)

        def get_ctx():
            return {"question": "q"}

        _, candidates, artifact = run_hybrid(
            task_id="test-003",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
        )
        assert len(candidates) == k
        assert artifact["initial_candidates_generated"] == k
    finally:
        os.unlink(db_path)


def test_run_hybrid_aggregation_majority_signature():
    """Identical SQL from all candidates should aggregate via majority signature."""
    k = 3
    db_path = _make_test_db()
    try:
        # All seeds return the same SQL — tableless, always executes OK
        model = MockModel(responses={42: "SELECT 1", 43: "SELECT 1", 44: "SELECT 1"})
        params = _default_params(initial_candidates=k)

        def get_ctx():
            return {"question": "q"}

        sel_sql, candidates, artifact = run_hybrid(
            task_id="test-004",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
        )
        assert sel_sql == "SELECT 1"
        # All should have exec_ok=True (tableless SELECT works)
        assert all(c.exec_ok for c in candidates)
        # All should have the same result_signature
        sigs = [c.result_signature for c in candidates]
        assert len(set(sigs)) == 1
        assert sigs[0] is not None

        agg = artifact["aggregation"]
        assert agg["selected_sql"] == "SELECT 1"
        assert "majority" in agg["aggregation_reason"].lower() or "signature" in agg["aggregation_reason"].lower()
    finally:
        os.unlink(db_path)


def test_run_hybrid_with_failures():
    """Even with some failed generations, run_hybrid should still return valid candidates."""
    k = 3
    db_path = _make_test_db()
    try:
        model = MockModel(
            responses={42: "SELECT 1", 43: "SELECT 2"},
            fail_on={44},  # seed 44 = base_seed(42) + attempt_id(2)
        )
        params = _default_params(initial_candidates=k)

        def get_ctx():
            return {"question": "q"}

        sel_sql, candidates, artifact = run_hybrid(
            task_id="test-005",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
        )
        assert len(candidates) == k  # all K slots present, even failures
        failed = [c for c in candidates if not c.preflight_ok]
        assert len(failed) == 1  # one failed
        assert sel_sql  # should still select from successful candidates
    finally:
        os.unlink(db_path)


def test_run_hybrid_all_fail():
    """If all candidates fail, run_hybrid should still return a result (fallback)."""
    k = 2
    db_path = _make_test_db()
    try:
        model = MockModel(fail_on={42, 43})
        params = _default_params(initial_candidates=k)

        def get_ctx():
            return {"question": "q"}

        sel_sql, candidates, artifact = run_hybrid(
            task_id="test-006",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
        )
        assert len(candidates) == k
        # All failed — aggregation should still work (fallback)
        assert "aggregation" in artifact
    finally:
        os.unlink(db_path)


def test_run_hybrid_artifact_structure():
    """Artifact dict should have required keys including params and latency_ms."""
    db_path = _make_test_db()
    try:
        model = MockModel()
        params = _default_params(initial_candidates=2)

        def get_ctx():
            return {"question": "q"}

        _, _, artifact = run_hybrid(
            task_id="test-007",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
        )
        assert artifact["variant"] == "A"  # expansion_enabled=False
        assert artifact["task_id"] == "test-007"
        assert "initial_candidates_requested" in artifact
        assert "initial_candidates_generated" in artifact
        assert "generation_time_ms" in artifact
        assert "execution_time_ms" in artifact
        assert "initial_candidates" in artifact
        assert "aggregation" in artifact
        assert "params" in artifact
        assert "latency_ms" in artifact
        assert artifact["generation_time_ms"] >= 0
        assert artifact["execution_time_ms"] >= 0
        assert artifact["latency_ms"] >= 0
        # params should be a dict with all HybridParams fields
        assert artifact["params"]["initial_candidates"] == 2
        assert artifact["params"]["expansion_enabled"] is False
    finally:
        os.unlink(db_path)


def test_run_hybrid_model_receives_schema():
    """Schema from get_context should be passed to model.generate_sql."""
    db_path = _make_test_db()
    try:
        model = MockModel()
        params = _default_params(initial_candidates=1)

        def get_ctx():
            return {"question": "How many?", "schema": "CREATE TABLE t (id INT)"}

        run_hybrid(
            task_id="test-008",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
        )
        assert len(model.calls) == 1
        assert model.calls[0]["schema"] == "CREATE TABLE t (id INT)"
        assert model.calls[0]["question"] == "How many?"
    finally:
        os.unlink(db_path)


def test_run_hybrid_empty_candidates_zero():
    """When initial_candidates=0, should return empty."""
    db_path = _make_test_db()
    try:
        model = MockModel()
        params = _default_params(initial_candidates=0)

        def get_ctx():
            return {"question": "q"}

        sel_sql, candidates, artifact = run_hybrid(
            task_id="test-009",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
        )
        assert sel_sql == ""
        assert len(candidates) == 0
        assert artifact["aggregation"]["aggregation_reason"] == "no_candidates"
    finally:
        os.unlink(db_path)


def test_run_hybrid_candidates_have_result_signatures():
    """Successful candidates should have non-None result signatures."""
    k = 3
    db_path = _make_test_db()
    try:
        # Different SQL but all valid — produces different signatures
        model = MockModel(responses={42: "SELECT 1", 43: "SELECT 2", 44: "SELECT 1"})
        params = _default_params(initial_candidates=k)

        def get_ctx():
            return {"question": "q"}

        _, candidates, _ = run_hybrid(
            task_id="test-010",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
        )
        for c in candidates:
            assert c.exec_ok is True
            assert c.result_signature is not None
        # SELECT 1 and SELECT 1 should have same signature
        assert candidates[0].result_signature == candidates[2].result_signature
        # SELECT 1 and SELECT 2 should have different signatures
        assert candidates[0].result_signature != candidates[1].result_signature
    finally:
        os.unlink(db_path)


# ---------------------------------------------------------------------------
# _select_seeds
# ---------------------------------------------------------------------------


def test_select_seeds_picks_exec_ok_first():
    """Seeds should prefer exec_ok candidates over preflight-only."""
    c0 = CandidateResult(attempt_id=0, raw_text="", sql="SELECT 1", preflight_ok=True, exec_ok=False, exec_time_ms=10.0)
    c1 = CandidateResult(attempt_id=1, raw_text="", sql="SELECT 2", preflight_ok=True, exec_ok=True, exec_time_ms=5.0)
    c2 = CandidateResult(attempt_id=2, raw_text="", sql="SELECT 3", preflight_ok=True, exec_ok=True, exec_time_ms=8.0)
    seeds = _select_seeds([c0, c1, c2], num_seeds=2)
    # exec_ok candidates should come first
    assert len(seeds) == 2
    assert seeds[0].attempt_id == 1  # exec_ok, faster
    assert seeds[1].attempt_id == 2  # exec_ok, slower


def test_select_seeds_dedup_by_jaccard():
    """Near-identical SQL should be deduplicated."""
    # Same SQL → Jaccard = 1.0 → second should be rejected
    c0 = CandidateResult(
        attempt_id=0, raw_text="", sql="SELECT id FROM users", preflight_ok=True, exec_ok=True, exec_time_ms=5.0
    )
    c1 = CandidateResult(
        attempt_id=1, raw_text="", sql="SELECT id FROM users", preflight_ok=True, exec_ok=True, exec_time_ms=6.0
    )
    c2 = CandidateResult(
        attempt_id=2, raw_text="", sql="SELECT name FROM orders", preflight_ok=True, exec_ok=True, exec_time_ms=7.0
    )
    seeds = _select_seeds([c0, c1, c2], num_seeds=3, sim_threshold=0.85)
    # c0 and c1 are identical, so only one should be kept
    assert len(seeds) == 2
    assert seeds[0].attempt_id == 0
    assert seeds[1].attempt_id == 2


def test_select_seeds_skips_empty_sql():
    """Candidates with no SQL should be skipped."""
    c0 = CandidateResult(attempt_id=0, raw_text="", sql="", preflight_ok=False, exec_ok=False)
    c1 = CandidateResult(attempt_id=1, raw_text="", sql="SELECT 1", preflight_ok=True, exec_ok=True, exec_time_ms=5.0)
    seeds = _select_seeds([c0, c1], num_seeds=2)
    assert len(seeds) == 1
    assert seeds[0].attempt_id == 1


def test_select_seeds_empty_input():
    seeds = _select_seeds([], num_seeds=3)
    assert seeds == []


def test_select_seeds_zero_requested():
    c0 = CandidateResult(attempt_id=0, raw_text="", sql="SELECT 1", preflight_ok=True, exec_ok=True)
    assert _select_seeds([c0], num_seeds=0) == []


# ---------------------------------------------------------------------------
# _expand_one
# ---------------------------------------------------------------------------


def test_expand_one_success():
    """Expansion should return a different SQL from the seed."""
    model = MockModel(responses={200: "SELECT COUNT(*) FROM t"})
    result = _expand_one(
        seed_sql="SELECT 1",
        question="How many?",
        schema="CREATE TABLE t (id INT)",
        model=model,
        temperature=0.8,
        top_p=0.9,
        base_seed=200,
        index=0,
    )
    assert result == "SELECT COUNT(*) FROM t"


def test_expand_one_rejects_identical():
    """If expansion returns same SQL as seed, should return empty."""
    model = MockModel(responses={200: "SELECT 1"})
    result = _expand_one(
        seed_sql="SELECT 1",
        question="q",
        schema=None,
        model=model,
        temperature=0.8,
        top_p=0.9,
        base_seed=200,
        index=0,
    )
    assert result == ""


def test_expand_one_rejects_placeholder():
    """Expansion with placeholders should be rejected."""
    model = MockModel(responses={200: "SELECT <column> FROM <table>"})
    result = _expand_one(
        seed_sql="SELECT 1",
        question="q",
        schema=None,
        model=model,
        temperature=0.8,
        top_p=0.9,
        base_seed=200,
        index=0,
    )
    assert result == ""


def test_expand_one_handles_failure():
    """Model failure should return empty string."""
    model = MockModel(fail_on={200})
    result = _expand_one(
        seed_sql="SELECT 1",
        question="q",
        schema=None,
        model=model,
        temperature=0.8,
        top_p=0.9,
        base_seed=200,
        index=0,
    )
    assert result == ""


# ---------------------------------------------------------------------------
# run_hybrid with expansion (Phase 3)
# ---------------------------------------------------------------------------


def test_run_hybrid_expansion_enabled():
    """With expansion_enabled=True, expansion candidates should be in the pool."""
    k = 3
    db_path = _make_test_db()
    try:
        # Initial: seeds 42,43,44 → SELECT 42, SELECT 43, SELECT 44
        # Expansion: different SQL for expansion seeds
        responses = {
            42: "SELECT 42",
            43: "SELECT 43",
            44: "SELECT 44",
            # Expansion responses: base_seed + k + 100 + index
            # base_seed=42, k=3 → expansion base_seed=145
            145: "SELECT 100",
            146: "SELECT 101",
            147: "SELECT 102",
            148: "SELECT 103",
        }
        model = MockModel(responses=responses)
        params = _default_params(
            initial_candidates=k,
            expansion_enabled=True,
            expansion_seeds=2,
            expansion_per_seed=2,
            expansion_sim_threshold=0.99,  # high threshold → accept most variations
        )

        def get_ctx():
            return {"question": "q"}

        sel_sql, all_candidates, artifact = run_hybrid(
            task_id="test-exp-001",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
        )
        # Should have initial + expansion candidates
        assert len(all_candidates) > k
        assert artifact["variant"] == "B"
        assert "expansion" in artifact
        assert artifact["expansion"]["seeds_used"] > 0
        assert artifact["expansion"]["variations_generated"] > 0
        assert artifact["aggregation"]["total_pool_size"] == len(all_candidates)
    finally:
        os.unlink(db_path)


def test_run_hybrid_expansion_disabled():
    """With expansion_enabled=False (Variant A), no expansion should happen."""
    k = 3
    db_path = _make_test_db()
    try:
        model = MockModel()
        params = _default_params(
            initial_candidates=k,
            expansion_enabled=False,
        )

        def get_ctx():
            return {"question": "q"}

        sel_sql, all_candidates, artifact = run_hybrid(
            task_id="test-exp-002",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
        )
        # Only initial candidates
        assert len(all_candidates) == k
        assert artifact["variant"] == "A"
        assert "expansion" not in artifact
    finally:
        os.unlink(db_path)


def test_run_hybrid_expansion_high_sim_filters():
    """Expansions too similar to seed (Jaccard >= threshold) should be filtered."""
    k = 2
    db_path = _make_test_db()
    try:
        # Initial candidates produce "SELECT 1" and "SELECT 2"
        # Expansion returns nearly-identical SQL
        responses = {
            42: "SELECT 1",
            43: "SELECT 2",
            # Expansion: base=42+2+100=144, returns same as seed
            144: "SELECT 1",  # identical to seed → rejected as identical
            145: "SELECT 2",  # identical to seed → rejected as identical
        }
        model = MockModel(responses=responses)
        params = _default_params(
            initial_candidates=k,
            expansion_enabled=True,
            expansion_seeds=2,
            expansion_per_seed=1,
            expansion_sim_threshold=0.5,  # very low threshold → filter aggressively
        )

        def get_ctx():
            return {"question": "q"}

        _, all_candidates, artifact = run_hybrid(
            task_id="test-exp-003",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
        )
        # Identical SQL is rejected by _expand_one (sql == seed_sql check),
        # so variations_generated should be 0
        exp = artifact.get("expansion", {})
        assert exp.get("variations_accepted", 0) == 0
        # Pool should be just initial candidates
        assert len(all_candidates) == k
    finally:
        os.unlink(db_path)


def test_run_hybrid_expansion_all_seeds_fail_preflight():
    """If all initial candidates fail, no seeds → expansion skipped."""
    k = 2
    db_path = _make_test_db()
    try:
        model = MockModel(fail_on={42, 43})
        params = _default_params(
            initial_candidates=k,
            expansion_enabled=True,
            expansion_seeds=2,
            expansion_per_seed=2,
        )

        def get_ctx():
            return {"question": "q"}

        _, all_candidates, artifact = run_hybrid(
            task_id="test-exp-004",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
        )
        assert len(all_candidates) == k  # only initial (all failed)
        exp = artifact.get("expansion", {})
        assert exp.get("seeds_used", 0) == 0
    finally:
        os.unlink(db_path)


# ---------------------------------------------------------------------------
# Phase 4: execute + aggregate (majority vote) — TASK-029
# ---------------------------------------------------------------------------


def test_phase4_majority_signature_picks_fastest():
    """5 candidates, 3 with same signature → fastest from winning group selected."""
    from evalsuite.pipeline.aggregation import aggregation_hybrid

    # 3 candidates produce result_signature "sig_A" (majority), 2 produce "sig_B"
    c0 = CandidateResult(
        attempt_id=0,
        raw_text="",
        sql="SELECT id FROM t",
        preflight_ok=True,
        exec_ok=True,
        exec_time_ms=50.0,
        result_signature="sig_A",
    )
    c1 = CandidateResult(
        attempt_id=1,
        raw_text="",
        sql="SELECT id FROM t ORDER BY id",
        preflight_ok=True,
        exec_ok=True,
        exec_time_ms=30.0,
        result_signature="sig_A",
    )
    c2 = CandidateResult(
        attempt_id=2,
        raw_text="",
        sql="SELECT name FROM t",
        preflight_ok=True,
        exec_ok=True,
        exec_time_ms=20.0,
        result_signature="sig_B",
    )
    c3 = CandidateResult(
        attempt_id=3,
        raw_text="",
        sql="SELECT id FROM t LIMIT 10",
        preflight_ok=True,
        exec_ok=True,
        exec_time_ms=10.0,
        result_signature="sig_A",
    )
    c4 = CandidateResult(
        attempt_id=4,
        raw_text="",
        sql="SELECT name FROM t LIMIT 5",
        preflight_ok=True,
        exec_ok=True,
        exec_time_ms=15.0,
        result_signature="sig_B",
    )

    sel_id, sel_sql, reason, votes = aggregation_hybrid([c0, c1, c2, c3, c4])

    # sig_A has 3 votes (majority)
    assert votes["by_signature"]["sig_A"] == 3
    assert votes["by_signature"]["sig_B"] == 2
    # From the winning group (sig_A: c0=50ms, c1=30ms, c3=10ms), fastest is c3
    assert sel_id == 3
    assert sel_sql == "SELECT id FROM t LIMIT 10"
    assert "majority" in reason.lower() or "signature" in reason.lower()


def test_phase4_no_exec_ok_fallback_preflight():
    """No exec_ok candidates → fallback to majority by normalized SQL among preflight_ok."""
    from evalsuite.pipeline.aggregation import aggregation_hybrid

    c0 = CandidateResult(
        attempt_id=0,
        raw_text="",
        sql="SELECT 1",
        preflight_ok=True,
        exec_ok=False,
        preflight_error_type="pred_runtime_fail",
    )
    c1 = CandidateResult(
        attempt_id=1,
        raw_text="",
        sql="SELECT 1",
        preflight_ok=True,
        exec_ok=False,
        preflight_error_type="pred_runtime_fail",
    )
    c2 = CandidateResult(
        attempt_id=2,
        raw_text="",
        sql="SELECT 2",
        preflight_ok=True,
        exec_ok=False,
        preflight_error_type="pred_runtime_fail",
    )

    sel_id, sel_sql, reason, votes = aggregation_hybrid([c0, c1, c2])

    # "SELECT 1" appears twice (majority) among preflight_ok
    assert sel_sql.strip().lower() in ("select 1",)
    assert sel_id in (0, 1)  # one of the SELECT 1 candidates
    assert "preflight_ok" in reason


def test_phase4_no_preflight_ok_fallback_attempt0():
    """No preflight_ok candidates → fallback picks attempt_id=0."""
    from evalsuite.pipeline.aggregation import aggregation_hybrid

    c0 = CandidateResult(
        attempt_id=0,
        raw_text="",
        sql="SELECT bad_syntax",
        preflight_ok=False,
        exec_ok=False,
        preflight_error_type="pred_parse_fail",
    )
    c1 = CandidateResult(
        attempt_id=1,
        raw_text="",
        sql="SELECT also_bad",
        preflight_ok=False,
        exec_ok=False,
        preflight_error_type="pred_parse_fail",
    )
    c2 = CandidateResult(
        attempt_id=2,
        raw_text="",
        sql="SELECT nope",
        preflight_ok=False,
        exec_ok=False,
        preflight_error_type="pred_parse_fail",
    )

    sel_id, sel_sql, reason, votes = aggregation_hybrid([c0, c1, c2])

    # All same error_type (pred_parse_fail), so sorted by attempt_id → picks 0
    assert sel_id == 0
    assert sel_sql == "SELECT bad_syntax"
    assert "fallback" in reason


def test_phase4_end_to_end_majority_via_run_hybrid():
    """End-to-end: run_hybrid with 5 candidates, 3 producing same result → majority wins."""
    k = 5
    db_path = _make_test_db()
    try:
        # Seeds 42-46: three produce "SELECT 1" (same result), two produce different results
        responses = {
            42: "SELECT 1",
            43: "SELECT 2",
            44: "SELECT 1",
            45: "SELECT 3",
            46: "SELECT 1",
        }
        model = MockModel(responses=responses)
        params = _default_params(initial_candidates=k)

        def get_ctx():
            return {"question": "q"}

        sel_sql, candidates, artifact = run_hybrid(
            task_id="test-p4-001",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
        )

        # All candidates should execute OK (tableless SELECT works on any SQLite DB)
        assert all(c.exec_ok for c in candidates)

        # "SELECT 1" appears 3 times → its signature is majority
        sig_1_candidates = [c for c in candidates if c.sql == "SELECT 1"]
        assert len(sig_1_candidates) == 3

        # Selected SQL should be "SELECT 1" (majority signature group)
        assert sel_sql == "SELECT 1"
        agg = artifact["aggregation"]
        assert "majority" in agg["aggregation_reason"].lower() or "signature" in agg["aggregation_reason"].lower()

        # From the winning group, fastest execution time should be selected
        winning_times = [c.exec_time_ms for c in sig_1_candidates]
        selected_candidate = next(c for c in candidates if c.attempt_id == agg["selected_attempt_id"])
        assert selected_candidate.exec_time_ms == min(winning_times)
    finally:
        os.unlink(db_path)


# ---------------------------------------------------------------------------
# Artifact file writing — TASK-030
# ---------------------------------------------------------------------------


def test_run_hybrid_writes_artifact_to_run_dir():
    """run_hybrid should write hybrid_<task_id>.json into run_dir/raw/."""
    import json
    from pathlib import Path

    db_path = _make_test_db()
    run_dir = tempfile.mkdtemp()
    try:
        model = MockModel()
        params = _default_params(initial_candidates=2)

        def get_ctx():
            return {"question": "q", "schema": "CREATE TABLE t (id INT)"}

        sel_sql, candidates, artifact = run_hybrid(
            task_id="art-001",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
            run_dir=Path(run_dir),
        )

        art_path = Path(run_dir) / "raw" / "hybrid_art-001.json"
        assert art_path.exists(), f"Artifact file not found at {art_path}"

        written = json.loads(art_path.read_text(encoding="utf-8"))
        assert written["task_id"] == "art-001"
        assert written["variant"] == "A"
        assert "params" in written
        assert written["params"]["initial_candidates"] == 2
        assert "latency_ms" in written
        assert written["latency_ms"] >= 0
        assert "initial_candidates" in written
        assert "aggregation" in written
        assert written["aggregation"]["selected_sql"]  # non-empty
    finally:
        os.unlink(db_path)
        import shutil

        shutil.rmtree(run_dir, ignore_errors=True)


def test_run_hybrid_no_run_dir_no_file():
    """When run_dir is None, no artifact file should be written."""
    db_path = _make_test_db()
    try:
        model = MockModel()
        params = _default_params(initial_candidates=2)

        def get_ctx():
            return {"question": "q"}

        sel_sql, candidates, artifact = run_hybrid(
            task_id="art-002",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
            run_dir=None,
        )
        # Should complete without errors even without run_dir
        assert artifact["task_id"] == "art-002"
        assert "params" in artifact
        assert "latency_ms" in artifact
    finally:
        os.unlink(db_path)


# ---------------------------------------------------------------------------
# Phase 1: SGR grounding — TASK-027
# ---------------------------------------------------------------------------


class _ConcreteBenchmark:
    """Minimal concrete benchmark for testing _dispatch_hybrid."""

    def __new__(cls, db_path: str, model: Any) -> Any:
        from evalsuite.benchmarks.base import Benchmark

        class _TestBench(Benchmark):
            name = "test_bench"

            def __init__(self, _db_path: str, _model: Any):
                self._test_db = _db_path
                self.model = _model
                self.context_mode = "full_schema"
                self.schema_format = "compact"
                self.schema_max_tables = 50
                self.schema_max_cols_per_table = 30
                self.sql_execution_timeout_sec = None
                self.run_dir = None

            def discover_tasks(self):
                return []

            def run_task(self, task):
                return self._run_task_common(task)

            def _get_dialect(self):
                return "sqlite"

            def _get_constraints(self):
                from evalsuite.core.types import DialectConstraints

                return DialectConstraints(dialect="sqlite")

            def _get_schema_context(self, db_path):
                from evalsuite.pipeline.schema_extract import schema_from_sqlite

                return schema_from_sqlite(str(db_path))

            def _get_tool_executor(self, db_path):
                raise NotImplementedError

            def _execute_sql(self, db_path, sql):
                from evalsuite.adapters.db.sqlite import execute_sql

                return execute_sql(db_path, sql)

        return _TestBench(_db_path=db_path, _model=model)


def test_sgr_grounding_true_enriches_schema():
    """With sgr_grounding=True, SGR output should enrich the schema passed to candidates."""
    from pathlib import Path

    from evalsuite.architectures.sgr.schema import SGRContext, SGRGrounding, SGRPlan
    from evalsuite.core.types import TaskSpec

    db_path = _make_test_db()
    try:
        model = MockModel()
        bench = _ConcreteBenchmark(db_path, model)

        # Configure hybrid architecture
        arch_cfg = MagicMock()
        arch_cfg.name = "hybrid"
        arch_cfg.params = {
            "sgr_grounding": True,
            "initial_candidates": 2,
            "parallelism": "sequential",
            "expansion_enabled": False,
        }
        bench.architecture_config = arch_cfg

        task = TaskSpec(
            task_id="sgr-001",
            question="How many users?",
            gold_sql="SELECT COUNT(*) FROM t",
            db_path=db_path,
            bench="test_bench",
            meta={},
        )

        sgr_ctx = SGRContext(
            grounding=SGRGrounding(tables=["t"], columns={"t": ["id", "name"]}),
            plan=SGRPlan(select=["COUNT(*)"]),
            prompt_addendum="\n[SGR constraints — use ONLY the following]\nAllowed tables: t",
        )

        with patch(
            "evalsuite.architectures.sgr.layer.run_sgr_grounding_and_plan",
            return_value=sgr_ctx,
        ) as mock_sgr:
            extra: dict[str, Any] = {"question": task.question}
            sel_sql, result_extra = bench._dispatch_hybrid(
                task,
                Path(db_path),
                db_path,
                "sqlite",
                bench._get_constraints(),
                extra,
            )

            # SGR was called
            mock_sgr.assert_called_once()
            # Schema enriched — model should receive SGR addendum
            assert len(model.calls) == 2  # 2 initial candidates
            for call in model.calls:
                assert "[SGR constraints" in (call["schema"] or "")
            # Extra has reasoning.sgr with grounding
            assert "reasoning" in result_extra
            assert "grounding" in result_extra["reasoning"]["sgr"]
            assert result_extra["reasoning"]["sgr"]["grounding"]["tables"] == ["t"]
    finally:
        os.unlink(db_path)


def test_sgr_grounding_false_no_sgr_call():
    """With sgr_grounding=False, SGR should not be called."""
    from pathlib import Path

    from evalsuite.core.types import TaskSpec

    db_path = _make_test_db()
    try:
        model = MockModel()
        bench = _ConcreteBenchmark(db_path, model)

        arch_cfg = MagicMock()
        arch_cfg.name = "hybrid"
        arch_cfg.params = {
            "sgr_grounding": False,
            "initial_candidates": 2,
            "parallelism": "sequential",
            "expansion_enabled": False,
        }
        bench.architecture_config = arch_cfg

        task = TaskSpec(
            task_id="sgr-002",
            question="How many users?",
            gold_sql="SELECT COUNT(*) FROM t",
            db_path=db_path,
            bench="test_bench",
            meta={},
        )

        with patch(
            "evalsuite.architectures.sgr.layer.run_sgr_grounding_and_plan",
        ) as mock_sgr:
            extra: dict[str, Any] = {"question": task.question}
            sel_sql, result_extra = bench._dispatch_hybrid(
                task,
                Path(db_path),
                db_path,
                "sqlite",
                bench._get_constraints(),
                extra,
            )

            # SGR was NOT called
            mock_sgr.assert_not_called()
            # No reasoning section in extra
            assert "reasoning" not in result_extra
    finally:
        os.unlink(db_path)


def test_sgr_grounding_error_graceful_fallback():
    """If SGR throws, pipeline continues without enrichment (graceful fallback)."""
    from pathlib import Path

    from evalsuite.core.types import TaskSpec

    db_path = _make_test_db()
    try:
        model = MockModel()
        bench = _ConcreteBenchmark(db_path, model)

        arch_cfg = MagicMock()
        arch_cfg.name = "hybrid"
        arch_cfg.params = {
            "sgr_grounding": True,
            "initial_candidates": 2,
            "parallelism": "sequential",
            "expansion_enabled": False,
        }
        bench.architecture_config = arch_cfg

        task = TaskSpec(
            task_id="sgr-003",
            question="How many users?",
            gold_sql="SELECT COUNT(*) FROM t",
            db_path=db_path,
            bench="test_bench",
            meta={},
        )

        with patch(
            "evalsuite.architectures.sgr.layer.run_sgr_grounding_and_plan",
            side_effect=RuntimeError("SGR service unavailable"),
        ) as mock_sgr:
            extra: dict[str, Any] = {"question": task.question}
            sel_sql, result_extra = bench._dispatch_hybrid(
                task,
                Path(db_path),
                db_path,
                "sqlite",
                bench._get_constraints(),
                extra,
            )

            # SGR was called but failed
            mock_sgr.assert_called_once()
            # Pipeline still produced SQL (from candidates without SGR enrichment)
            assert sel_sql  # non-empty SQL selected
            # Error recorded in extra
            assert "reasoning" in result_extra
            assert "error" in result_extra["reasoning"]["sgr"]
            assert "SGR service unavailable" in result_extra["reasoning"]["sgr"]["error"]
            # Schema was NOT enriched — model calls should have schema without SGR addendum
            for call in model.calls:
                schema = call["schema"] or ""
                assert "[SGR constraints" not in schema
    finally:
        os.unlink(db_path)


# ---------------------------------------------------------------------------
# Diagnostic Retry — TASK-035
# ---------------------------------------------------------------------------


def test_diagnostic_retry_same_error_triggers():
    """5 candidates with identical error → retry triggers, retry candidates in pool."""
    k = 5
    db_path = _make_test_db()
    try:
        # All candidates produce SQL that references a non-existent table
        responses = {42 + i: "SELECT * FROM nonexistent_table" for i in range(k)}
        model = MockModel(responses=responses)
        params = _default_params(initial_candidates=k, expansion_enabled=False)

        def get_ctx():
            return {"question": "How many?", "schema": "CREATE TABLE t (id INT)"}

        _, all_candidates, artifact = run_hybrid(
            task_id="retry-001",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
        )

        # Diagnostic retry should have triggered
        assert "diagnostic_retry" in artifact
        assert artifact["diagnostic_retry"]["triggered"] is True
        assert artifact["diagnostic_retry"]["common_error"]
        assert artifact["diagnostic_retry"]["retry_candidates_generated"] == 2
        # All candidates = initial + retry
        assert len(all_candidates) == k + 2
        # Retry candidates should have source=diagnostic_retry
        retry_cands = [c for c in all_candidates if c.gen_params.get("source") == "diagnostic_retry"]
        assert len(retry_cands) == 2
    finally:
        os.unlink(db_path)


def test_diagnostic_retry_different_errors_no_trigger():
    """5 candidates with different errors → retry does NOT trigger."""
    k = 3
    db_path = _make_test_db()
    try:
        # Each candidate references a different non-existent table → different error messages
        responses = {
            42: "SELECT * FROM table_a",
            43: "SELECT * FROM table_b",
            44: "SELECT * FROM table_c",
        }
        model = MockModel(responses=responses)
        params = _default_params(initial_candidates=k, expansion_enabled=False)

        def get_ctx():
            return {"question": "q"}

        _, all_candidates, artifact = run_hybrid(
            task_id="retry-002",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
        )

        # Retry should NOT trigger (different errors)
        assert artifact["diagnostic_retry"]["triggered"] is False
        assert len(all_candidates) == k  # no retry candidates added
    finally:
        os.unlink(db_path)


def test_diagnostic_retry_some_exec_ok_no_trigger():
    """3 exec_ok + 2 exec_fail → retry does NOT trigger."""
    k = 5
    db_path = _make_test_db()
    try:
        responses = {
            42: "SELECT 1",  # exec_ok
            43: "SELECT 2",  # exec_ok
            44: "SELECT 3",  # exec_ok
            45: "SELECT * FROM nonexistent",  # exec_fail
            46: "SELECT * FROM nonexistent",  # exec_fail
        }
        model = MockModel(responses=responses)
        params = _default_params(initial_candidates=k, expansion_enabled=False)

        def get_ctx():
            return {"question": "q"}

        _, all_candidates, artifact = run_hybrid(
            task_id="retry-003",
            get_context=get_ctx,
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
        )

        assert artifact["diagnostic_retry"]["triggered"] is False
        assert len(all_candidates) == k
    finally:
        os.unlink(db_path)


def test_should_diagnostic_retry_unit():
    """Unit test for _should_diagnostic_retry decision logic."""
    # All same error → trigger
    cands_same = [
        CandidateResult(
            attempt_id=i,
            raw_text="",
            sql=f"SELECT {i}",
            preflight_ok=False,
            exec_ok=False,
            exec_error="no such table: foo",
        )
        for i in range(5)
    ]
    should, err = _should_diagnostic_retry(cands_same)
    assert should is True
    assert err == "no such table: foo"

    # Mixed errors → no trigger
    cands_mixed = [
        CandidateResult(
            attempt_id=0,
            raw_text="",
            sql="SELECT 1",
            preflight_ok=False,
            exec_ok=False,
            exec_error="no such table: foo",
        ),
        CandidateResult(
            attempt_id=1, raw_text="", sql="SELECT 2", preflight_ok=False, exec_ok=False, exec_error="syntax error"
        ),
    ]
    should, _ = _should_diagnostic_retry(cands_mixed)
    assert should is False

    # Has exec_ok → no trigger
    cands_ok = [
        CandidateResult(attempt_id=0, raw_text="", sql="SELECT 1", preflight_ok=True, exec_ok=True),
        CandidateResult(
            attempt_id=1, raw_text="", sql="SELECT 2", preflight_ok=False, exec_ok=False, exec_error="error"
        ),
    ]
    should, _ = _should_diagnostic_retry(cands_ok)
    assert should is False

    # Empty list → no trigger
    should, _ = _should_diagnostic_retry([])
    assert should is False


def test_diagnostic_retry_retry_prompt_includes_error():
    """Retry candidates receive a prompt that includes the error text."""
    db_path = _make_test_db()
    try:
        model = MockModel()  # will return SELECT <seed> by default
        params = _default_params()

        retry_cands, retry_art = _generate_retry_candidates(
            model=model,
            question="How many users?",
            schema="CREATE TABLE t (id INT)",
            failed_sql="SELECT * FROM nonexistent",
            error_text="no such table: nonexistent",
            params=params,
            base_seed=300,
            db_path=db_path,
            dialect="sqlite",
            exec_timeout=10,
            next_attempt_id=10,
            retry_count=2,
        )

        assert len(retry_cands) == 2
        assert retry_art["triggered"] is True
        assert retry_art["common_error"] == "no such table: nonexistent"

        # The model was called with the error text in the question
        for call in model.calls:
            assert "no such table: nonexistent" in call["question"]
            assert "Fix this SQL" in call["question"]

        # All retry candidates have source=diagnostic_retry
        for c in retry_cands:
            assert c.gen_params["source"] == "diagnostic_retry"
            assert c.gen_params["temperature"] == 0.3
    finally:
        os.unlink(db_path)


# ---------------------------------------------------------------------------
# Smart Early Stop (TASK-036)
# ---------------------------------------------------------------------------


def test_smart_early_stop_no_exec_ok_skips_expansion():
    """When all candidates fail (no exec_ok), expansion is skipped (reason: no_exec_ok)."""
    db_path = _make_test_db()
    try:
        # All candidates will fail: SELECT from nonexistent table
        responses = {42 + i: "SELECT * FROM nonexistent_table" for i in range(5)}
        model = MockModel(responses=responses)
        params = _default_params(
            initial_candidates=5,
            expansion_enabled=True,
            expansion_seeds=2,
            expansion_per_seed=2,
        )
        _, candidates, artifact = run_hybrid(
            task_id="test_no_exec_ok",
            get_context=lambda: {"question": "q", "schema": None, "messages": None},
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
            base_seed=42,
        )
        # Expansion should be skipped
        assert artifact.get("expansion_skipped_reason") == "no_exec_ok"
        assert artifact["expansion"]["expansion_skipped_reason"] == "no_exec_ok"
        assert artifact["expansion"]["variations_generated"] == 0
        # No expansion candidates should exist
        assert artifact["expansion"]["candidates"] == []
    finally:
        os.unlink(db_path)


def test_smart_early_stop_consensus_skips_expansion():
    """When all exec_ok candidates share one signature, expansion is skipped (reason: consensus)."""
    db_path = _make_test_db()
    try:
        # All candidates return the same SQL → same result_signature
        responses = {42 + i: "SELECT 1" for i in range(5)}
        model = MockModel(responses=responses)
        params = _default_params(
            initial_candidates=5,
            expansion_enabled=True,
            expansion_seeds=2,
            expansion_per_seed=2,
        )
        _, candidates, artifact = run_hybrid(
            task_id="test_consensus",
            get_context=lambda: {"question": "q", "schema": None, "messages": None},
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
            base_seed=42,
        )
        # Expansion should be skipped due to consensus
        assert artifact.get("expansion_skipped_reason") == "consensus"
        assert artifact["expansion"]["expansion_skipped_reason"] == "consensus"
        assert artifact["expansion"]["variations_generated"] == 0
        assert artifact["expansion"]["candidates"] == []
        # All candidates are exec_ok
        assert all(c.exec_ok for c in candidates)
    finally:
        os.unlink(db_path)


def test_smart_early_stop_mixed_signatures_runs_expansion():
    """When exec_ok candidates have different signatures, expansion runs normally."""
    db_path = _make_test_db()
    try:
        # Different SQL → different result_signatures
        responses = {
            42: "SELECT 1",
            43: "SELECT 2",
            44: "SELECT 1",
        }
        model = MockModel(responses=responses)
        params = _default_params(
            initial_candidates=3,
            expansion_enabled=True,
            expansion_seeds=2,
            expansion_per_seed=1,
        )
        _, candidates, artifact = run_hybrid(
            task_id="test_mixed",
            get_context=lambda: {"question": "q", "schema": None, "messages": None},
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
            base_seed=42,
        )
        # Expansion should run (not skipped)
        assert artifact.get("expansion_skipped_reason") is None
        # Expansion artifact should exist with actual attempt
        assert "expansion" in artifact
    finally:
        os.unlink(db_path)


def test_smart_early_stop_no_exec_ok_triggers_diagnostic_retry():
    """When all candidates fail with same error, diagnostic retry runs AND expansion is skipped."""
    db_path = _make_test_db()
    try:
        # All fail with same error
        responses = {42 + i: "SELECT * FROM nonexistent_table" for i in range(3)}
        # Retry candidates also fail (same table)
        for i in range(2):
            responses[42 + 3 + 50 + 3 + i] = "SELECT * FROM also_nonexistent"
        model = MockModel(responses=responses)
        params = _default_params(
            initial_candidates=3,
            expansion_enabled=True,
            expansion_seeds=2,
            expansion_per_seed=2,
        )
        _, candidates, artifact = run_hybrid(
            task_id="test_retry_and_skip",
            get_context=lambda: {"question": "q", "schema": None, "messages": None},
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
            base_seed=42,
        )
        # Diagnostic retry should have triggered
        assert artifact.get("diagnostic_retry", {}).get("triggered") is True
        # Expansion should be skipped (no exec_ok even after retry)
        assert artifact.get("expansion_skipped_reason") == "no_exec_ok"
    finally:
        os.unlink(db_path)


def test_smart_early_stop_artifact_has_expansion_skipped_reason():
    """Artifact always contains expansion_skipped_reason (null when expansion ran)."""
    db_path = _make_test_db()
    try:
        # Variant A (expansion disabled) — expansion_skipped_reason should be null
        model = MockModel()
        params = _default_params(initial_candidates=2, expansion_enabled=False)
        _, _, artifact = run_hybrid(
            task_id="test_artifact_a",
            get_context=lambda: {"question": "q", "schema": None, "messages": None},
            model=model,
            db_path=db_path,
            dialect="sqlite",
            params=params,
            base_seed=42,
        )
        assert "expansion_skipped_reason" in artifact
        assert artifact["expansion_skipped_reason"] is None
    finally:
        os.unlink(db_path)


# ---------------------------------------------------------------------------
# Schema Validation Tests (TASK-037)
# ---------------------------------------------------------------------------


class TestValidateSqlSchema:
    """Unit tests for validate_sql_schema()."""

    SCHEMA = {
        "users": ["user_id", "username", "email", "creation_date"],
        "orders": ["order_id", "user_id", "total_amount", "order_date"],
        "products": ["product_id", "name", "price", "category"],
    }

    def test_correct_names_no_changes(self):
        """SQL with correct table/column names is returned unchanged."""
        sql = "SELECT u.user_id, u.username FROM users u JOIN orders o ON u.user_id = o.user_id"
        result, fixes = validate_sql_schema(sql, self.SCHEMA)
        assert result == sql
        assert fixes == 0

    def test_fix_table_name_typo(self):
        """Table name with edit distance <= 2 is auto-corrected."""
        sql = "SELECT * FROM usrs"
        result, fixes = validate_sql_schema(sql, self.SCHEMA)
        assert "users" in result
        assert "usrs" not in result
        assert fixes == 1

    def test_fix_column_name_typo(self):
        """Qualified column ref with typo (CreationDate -> creation_date) is fixed."""
        sql = "SELECT u.CreationDate FROM users u"
        # edit_distance("CreationDate", "creation_date") = 2 (case-insensitive: _ insertions)
        # Actually case insensitive: "creationdate" vs "creation_date" = distance 1 (one insertion)
        result, fixes = validate_sql_schema(sql, self.SCHEMA)
        assert "u.creation_date" in result
        assert fixes == 1

    def test_no_fix_large_distance(self):
        """Column name too different (distance > 2) is left unchanged."""
        sql = "SELECT u.totally_wrong_col FROM users u"
        result, fixes = validate_sql_schema(sql, self.SCHEMA)
        assert "u.totally_wrong_col" in result
        assert fixes == 0

    def test_empty_sql(self):
        """Empty SQL returns empty unchanged."""
        result, fixes = validate_sql_schema("", self.SCHEMA)
        assert result == ""
        assert fixes == 0

    def test_no_schema_info(self):
        """No schema_info returns SQL unchanged."""
        sql = "SELECT * FROM usrs"
        result, fixes = validate_sql_schema(sql, None)
        assert result == sql
        assert fixes == 0

    def test_fix_table_in_join(self):
        """Table name in JOIN clause is also fixed."""
        sql = "SELECT * FROM users u JOIN ordrs o ON u.user_id = o.user_id"
        result, fixes = validate_sql_schema(sql, self.SCHEMA)
        assert "orders" in result
        assert "ordrs" not in result
        assert fixes == 1

    def test_multiple_fixes(self):
        """Multiple table and column fixes in one SQL."""
        sql = "SELECT u.emal FROM usrs u"
        result, fixes = validate_sql_schema(sql, self.SCHEMA)
        assert "users" in result
        assert "u.email" in result
        assert fixes == 2


class TestSchemaValidationIntegration:
    """Integration: schema_info flows through run_hybrid and candidates_fixed_count appears in artifact."""

    def test_candidates_fixed_count_in_artifact(self):
        """run_hybrid with schema_info tracks candidates_fixed_count."""
        db_path = _make_test_db()
        try:
            # Model returns SQL with typo that schema validation can fix
            # "usrs" -> "users" (distance=1)
            model = MockModel(responses={42: "SELECT * FROM usrs", 43: "SELECT * FROM users"})
            params = HybridParams(
                initial_candidates=2,
                expansion_enabled=False,
                parallelism="sequential",
            )
            schema_info = {"users": ["id", "name"]}
            _, _, artifact = run_hybrid(
                task_id="test_fix",
                get_context=lambda: {"question": "List users"},
                model=model,
                db_path=db_path,
                dialect="sqlite",
                params=params,
                base_seed=42,
                schema_info=schema_info,
            )
            assert "candidates_fixed_count" in artifact
            # First candidate has typo "usrs" -> should be fixed
            assert artifact["candidates_fixed_count"] >= 1
        finally:
            os.unlink(db_path)

    def test_no_schema_info_zero_fixes(self):
        """Without schema_info, candidates_fixed_count is 0."""
        db_path = _make_test_db()
        try:
            model = MockModel()
            params = HybridParams(
                initial_candidates=2,
                expansion_enabled=False,
                parallelism="sequential",
            )
            _, _, artifact = run_hybrid(
                task_id="test_no_fix",
                get_context=lambda: {"question": "test"},
                model=model,
                db_path=db_path,
                dialect="sqlite",
                params=params,
                base_seed=42,
            )
            assert artifact["candidates_fixed_count"] == 0
        finally:
            os.unlink(db_path)


class TestFixFallback:
    """Tests for TASK-039: return empty SQL when all candidates fail."""

    def test_all_candidates_fail_returns_empty_sql(self):
        """When no candidate has exec_ok or preflight_ok, return empty SQL."""
        db_path = _make_test_db()
        try:
            # All candidates produce invalid SQL that fails preflight
            model = MockModel(
                responses={
                    42: "INVALID SQL SYNTAX !!!",
                    43: "ALSO INVALID !!!",
                    44: "STILL INVALID !!!",
                }
            )
            params = _default_params(initial_candidates=3)
            sql, candidates, artifact = run_hybrid(
                task_id="test_fallback",
                get_context=lambda: {"question": "test"},
                model=model,
                db_path=db_path,
                dialect="sqlite",
                params=params,
                base_seed=42,
            )
            assert sql == ""
            assert artifact["aggregation"]["aggregation_reason"] == "all_candidates_failed"
        finally:
            os.unlink(db_path)

    def test_some_exec_ok_returns_normal_sql(self):
        """When some candidates exec_ok, normal aggregation runs (not fallback)."""
        db_path = _make_test_db()
        try:
            # Default MockModel returns "SELECT <seed>" which is valid
            model = MockModel()
            params = _default_params(initial_candidates=3)
            sql, candidates, artifact = run_hybrid(
                task_id="test_no_fallback",
                get_context=lambda: {"question": "test"},
                model=model,
                db_path=db_path,
                dialect="sqlite",
                params=params,
                base_seed=42,
            )
            assert sql != ""
            assert artifact["aggregation"]["aggregation_reason"] != "all_candidates_failed"
        finally:
            os.unlink(db_path)

    def test_fallback_artifact_has_status(self):
        """The artifact reports all_candidates_failed in aggregation reason."""
        db_path = _make_test_db()
        try:
            model = MockModel(
                responses={
                    42: "NOT VALID SQL AT ALL",
                    43: "ANOTHER BROKEN QUERY",
                }
            )
            params = _default_params(initial_candidates=2)
            _, _, artifact = run_hybrid(
                task_id="test_status",
                get_context=lambda: {"question": "test"},
                model=model,
                db_path=db_path,
                dialect="sqlite",
                params=params,
                base_seed=42,
            )
            agg = artifact["aggregation"]
            assert agg["aggregation_reason"] == "all_candidates_failed"
            assert agg["total_pool_size"] > 0
        finally:
            os.unlink(db_path)
