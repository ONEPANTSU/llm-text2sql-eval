"""
Unit and smoke tests for SGR: truncate_schema, build_constraints, structured parsing, standalone smoke.
"""

from __future__ import annotations

from evalsuite.architectures.sgr.layer import build_constraints
from evalsuite.architectures.sgr.schema import (
    Condition,
    JoinEdge,
    SGRGrounding,
    SGRPlan,
)
from evalsuite.architectures.sgr.utils import truncate_schema

# -------- truncate_schema --------


def test_truncate_schema_empty():
    assert truncate_schema(None) == ""
    assert truncate_schema("") == ""


def test_truncate_schema_under_limit():
    s = "CREATE TABLE t (a int);"
    assert truncate_schema(s, max_chars=100) == s


def test_truncate_schema_over_limit():
    s = "x" * 200 + "\n" + "y" * 200
    out = truncate_schema(s, max_chars=150)
    assert len(out) <= 150 + 30  # + truncation message
    assert "[... schema truncated ...]" in out or "truncated" in out.lower()


# -------- build_constraints --------


def test_build_constraints_includes_tables_joins_filters():
    g = SGRGrounding(
        tables=["t1", "t2"],
        columns={"t1": ["a", "b"], "t2": ["c"]},
        joins=[JoinEdge(left_table="t1", left_column="id", right_table="t2", right_column="t1_id")],
    )
    p = SGRPlan(
        select=["t1.a", "t2.c"],
        filters=[Condition(column_ref="t1.a", operator="=", value_hint="1")],
        aggregations=[],
        group_by=[],
        order_by=[],
        limit=10,
        distinct=False,
        ctes=[],
    )
    addendum = build_constraints(g, p)
    assert "t1" in addendum and "t2" in addendum
    assert "t1.id" in addendum or "t1_id" in addendum
    assert "t1.a" in addendum
    assert "Limit" in addendum or "10" in addendum
    assert len(addendum) <= 2100  # MAX_ADDENDUM_CHARS + small buffer


def test_build_constraints_empty_grounding():
    g = SGRGrounding(tables=[], columns={})
    p = SGRPlan()
    addendum = build_constraints(g, p)
    assert "Allowed tables" in addendum
    assert "(none)" in addendum or "[]" in addendum or addendum


# -------- Structured parsing (Pydantic validation) --------


def test_sgr_grounding_valid_json():
    data = {
        "tables": ["users", "orders"],
        "columns": {"users": ["id", "name"], "orders": ["user_id", "amount"]},
        "joins": [{"left_table": "users", "left_column": "id", "right_table": "orders", "right_column": "user_id"}],
        "notes": [],
        "confidence": 0.9,
    }
    g = SGRGrounding.model_validate(data)
    assert g.tables == ["users", "orders"]
    assert g.columns["users"] == ["id", "name"]
    assert len(g.joins) == 1
    assert g.joins[0].left_table == "users"


def test_sgr_plan_valid_json():
    data = {
        "select": ["t.a", "t.b"],
        "filters": [{"column_ref": "t.a", "operator": ">", "value_hint": "0"}],
        "aggregations": [{"function": "COUNT", "column_ref": "*", "alias": "cnt"}],
        "group_by": [],
        "order_by": [{"column_ref": "cnt", "direction": "DESC"}],
        "limit": 5,
        "distinct": False,
        "ctes": [],
    }
    p = SGRPlan.model_validate(data)
    assert p.select == ["t.a", "t.b"]
    assert len(p.filters) == 1
    assert p.filters[0].column_ref == "t.a"
    assert p.limit == 5


def test_sgr_grounding_invalid_extra_keys_allowed():
    data = {"tables": ["t"], "columns": {}, "unknown_key": 1}
    g = SGRGrounding.model_validate(data)
    assert g.tables == ["t"]


# -------- Smoke: run_sgr_standalone with fake model --------


class FakeSGRModel:
    """Fake model that returns valid grounding/plan and simple SQL for SGR smoke."""

    def generate_sql(
        self,
        question: str,
        schema: str | None = None,
        messages=None,
        temperature=None,
        **kwargs,
    ) -> str:
        if messages:
            for m in reversed(messages):
                if m.get("role") == "user" and "SQL" in (m.get("content") or ""):
                    return "SELECT 1;"
                if m.get("role") == "user" and "Repair" in (m.get("content") or ""):
                    return "SELECT 1;"
        return "SELECT 1;"

    def generate_structured(
        self,
        prompt: str,
        schema_model,
        *,
        system_prompt=None,
        temperature=None,
        max_tokens=None,
        max_retries=2,
    ):
        if schema_model.__name__ == "SGRGrounding":
            return SGRGrounding(
                tables=["t"],
                columns={"t": ["id", "name"]},
                joins=[],
                notes=[],
                confidence=0.9,
            )
        if schema_model.__name__ == "SGRPlan":
            return SGRPlan(
                select=["t.id", "t.name"],
                filters=[],
                aggregations=[],
                group_by=[],
                order_by=[],
                limit=None,
                distinct=False,
                ctes=[],
            )
        raise ValueError(f"Unknown schema_model {schema_model}")


def test_run_sgr_grounding_and_plan_smoke():
    from evalsuite.architectures.sgr.layer import run_sgr_grounding_and_plan

    model = FakeSGRModel()
    ctx = run_sgr_grounding_and_plan(
        question="What is in table t?",
        schema="CREATE TABLE t (id int, name text);",
        model=model,
    )
    assert ctx.grounding.tables
    assert ctx.plan.select is not None
    assert len(ctx.prompt_addendum) > 0


def test_run_sgr_standalone_smoke(tmp_path, monkeypatch):
    """Smoke: run_sgr_standalone returns candidates and artifact with non-empty grounding/plan."""
    import sqlite3

    from evalsuite.architectures.sgr.standalone import run_sgr_standalone

    db = tmp_path / "sgr_smoke.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE t (id INTEGER, name TEXT);")
    conn.execute("INSERT INTO t VALUES (1, 'a');")
    conn.commit()
    conn.close()

    model = FakeSGRModel()

    def get_context():
        return {"question": "Count rows in t", "schema": "CREATE TABLE t (id int, name text);"}

    candidates, artifact, timeout_hit = run_sgr_standalone(
        task_id="smoke1",
        get_context=get_context,
        model=model,
        db_path=str(db),
        dialect="sqlite",
        params={},
        num_candidates=2,
    )
    assert not timeout_hit
    assert len(candidates) >= 1
    assert artifact["sgr"]["grounding"] is not None
    assert artifact["sgr"]["plan"] is not None
    # At least one candidate should have sql and result_signature or exec_ok set after execution
    assert any(c.sql for c in candidates)
    assert "repair_attempts" in artifact["sgr"]
