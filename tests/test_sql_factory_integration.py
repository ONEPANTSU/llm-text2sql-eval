"""
Smoke integration: run spider2 with architecture sql_factory on a few examples.
"""

from __future__ import annotations

import json

import pytest

from evalsuite.architectures.plain import get_architecture_config
from evalsuite.architectures.sql_factory import build_sql_factory_params, run_sql_factory


class FakeModelSqlFactory:
    """Returns valid SQL for spider2 sample questions; supports temperature/seed for diversity."""

    def generate_sql(
        self,
        question: str,
        schema: str | None = None,
        messages=None,
        *,
        temperature=None,
        top_p=None,
        seed=None,
        **kwargs,
    ) -> str:
        if "how many employees" in question.lower():
            return "SELECT COUNT(*) FROM employees;"
        if "list departments" in question.lower() or "headcount" in question.lower():
            return "SELECT dept, COUNT(*) AS c FROM employees GROUP BY dept ORDER BY c DESC;"
        if "average salary" in question.lower():
            return "SELECT dept, AVG(salary) FROM employees GROUP BY dept ORDER BY dept;"
        if "total salary" in question.lower():
            return "SELECT SUM(salary) FROM employees;"
        if "count" in question.lower() and "table" in question.lower():
            return "SELECT COUNT(*) FROM sqlite_master WHERE type='table';"
        if "table name" in question.lower():
            return "SELECT name FROM sqlite_master WHERE type='table' LIMIT 1;"
        # Default
        return "SELECT COUNT(*) FROM employees;"


def test_sql_factory_run_returns_sql(tmp_path):
    """run_sql_factory returns a non-empty SQL string and artifact."""
    model = FakeModelSqlFactory()
    params = build_sql_factory_params(
        {
            "max_rounds": 2,
            "warmup_rounds": 1,
            "gen_batch": 2,
            "exp_batch": 2,
            "target_pool_size": 5,
            "sim_threshold": 0.85,
        }
    )
    db_path = _spider2_sample_db(tmp_path)
    if not db_path:
        pytest.skip("spider2 sample db not available")

    def get_context():
        return {
            "question": "How many employees are stored?",
            "schema": "DIALECT: sqlite\nSCHEMA:\n- employees(id, name, dept, salary)",
        }

    final_sql, artifact, timeout_hit = run_sql_factory(
        task_id="test_1",
        get_context=get_context,
        model=model,
        db_path=db_path,
        dialect="sqlite",
        params=params,
        sql_execution_timeout_sec=None,
        all_table_names=["employees"],
        run_dir=tmp_path,
    )
    # run_sql_factory returns list of CandidateResult
    assert final_sql  # non-empty list
    assert any("SELECT" in c.sql.upper() for c in final_sql if c.sql)
    assert isinstance(artifact, dict)
    assert "rounds" in artifact
    assert "summary" in artifact
    assert "final" in artifact
    assert timeout_hit is False
    # Artifact file written
    raw = tmp_path / "raw" / "sql_factory_test_1.json"
    if raw.exists():
        data = json.loads(raw.read_text())
        assert "params" in data and "rounds" in data


def _spider2_sample_db(tmp_path):
    """Create minimal spider2 sample DB."""
    try:
        import sqlite3

        db = tmp_path / "spider2_sample.db"
        conn = sqlite3.connect(db)
        conn.executescript("""
            CREATE TABLE employees (id INT, name TEXT, dept TEXT, salary INT);
            INSERT INTO employees VALUES (1,'a','eng',100), (2,'b','ops',90);
        """)
        conn.commit()
        conn.close()
        return str(db)
    except Exception:
        return None


def test_config_sql_factory_parsed():
    """get_architecture_config parses sql_factory and params."""
    raw = {"architecture": {"name": "sql_factory", "params": {"max_rounds": 4, "sim_threshold": 0.9}}}
    arch_cfg = get_architecture_config(raw)
    assert arch_cfg.name == "sql_factory"
    assert arch_cfg.params.get("max_rounds") == 4
    assert arch_cfg.params.get("sim_threshold") == 0.9
