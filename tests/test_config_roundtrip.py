"""Round-trip test: dump_config -> save -> load_config_json must preserve all Config fields."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from evalsuite.core.config import (
    ComparatorConfig,
    Config,
    DatasetPaths,
    ModelConfig,
    _redact_key,
    dump_config,
    load_config_json,
)


def _make_config_with_non_defaults() -> Config:
    """Create a Config with ALL fields set to non-default values."""
    return Config(
        model=ModelConfig(
            provider="openai",
            model="test-model",
            base_url="http://test:9999/v1",
            api_key="sk-test-key",
        ),
        comparator=ComparatorConfig(
            float_tol=1e-6,
            column_order_insensitive=False,
            string_normalize=False,
        ),
        datasets=DatasetPaths(
            bird_root=Path("/tmp/bird"),
            spider2_root=Path("/tmp/spider2"),
            tpcds_duckdb=Path("/tmp/tpcds.db"),
        ),
        context_mode="toolchain",
        schema_max_tables=10,
        schema_max_cols_per_table=5,
        schema_format="ddl",
        toolchain_max_steps=20,
        toolchain_max_describe=12,
        toolchain_max_list_tables=3,
        toolchain_max_describe_per_table=2,
        toolchain_max_tool_only_streak=8,
        toolchain_max_tool_calls=25,
        toolchain_timeout_sec=60,
        toolchain_allow_sample_values=1,
        sql_execution_timeout_sec=90,
        task_timeout_sec=300,
    )


def test_roundtrip_all_fields():
    """dump_config -> JSON -> load_config_json must preserve every Config field."""
    original = _make_config_with_non_defaults()

    with tempfile.TemporaryDirectory() as tmpdir:
        json_path = Path(tmpdir) / "config.json"
        data = dump_config(original)
        json_path.write_text(json.dumps(data, indent=2))
        restored = load_config_json(json_path)

    # Model
    assert restored.model.provider == original.model.provider
    assert restored.model.model == original.model.model
    assert restored.model.base_url == original.model.base_url
    # api_key is redacted in dump_config for security
    assert restored.model.api_key == _redact_key(original.model.api_key)

    # Comparator
    assert restored.comparator.float_tol == original.comparator.float_tol
    assert restored.comparator.column_order_insensitive == original.comparator.column_order_insensitive
    assert restored.comparator.string_normalize == original.comparator.string_normalize

    # Schema / context
    assert restored.context_mode == original.context_mode
    assert restored.schema_max_tables == original.schema_max_tables
    assert restored.schema_max_cols_per_table == original.schema_max_cols_per_table
    assert restored.schema_format == original.schema_format

    # Toolchain
    assert restored.toolchain_max_steps == original.toolchain_max_steps
    assert restored.toolchain_max_describe == original.toolchain_max_describe
    assert restored.toolchain_max_list_tables == original.toolchain_max_list_tables
    assert restored.toolchain_max_describe_per_table == original.toolchain_max_describe_per_table
    assert restored.toolchain_max_tool_only_streak == original.toolchain_max_tool_only_streak
    assert restored.toolchain_max_tool_calls == original.toolchain_max_tool_calls
    assert restored.toolchain_timeout_sec == original.toolchain_timeout_sec
    assert restored.toolchain_allow_sample_values == original.toolchain_allow_sample_values

    # Execution / timeouts
    assert restored.sql_execution_timeout_sec == original.sql_execution_timeout_sec
    assert restored.task_timeout_sec == original.task_timeout_sec


def test_roundtrip_defaults():
    """Round-trip with default values also works correctly."""
    original = Config(
        model=ModelConfig(),
        comparator=ComparatorConfig(),
        datasets=DatasetPaths(
            bird_root=Path("/tmp/bird"),
            spider2_root=Path("/tmp/spider2"),
            tpcds_duckdb=Path("/tmp/tpcds.db"),
        ),
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        json_path = Path(tmpdir) / "config.json"
        data = dump_config(original)
        json_path.write_text(json.dumps(data, indent=2))
        restored = load_config_json(json_path)

    assert restored.context_mode == "none"
    assert restored.schema_max_tables == 50
    assert restored.schema_max_cols_per_table == 30
    assert restored.schema_format == "compact"
    assert restored.toolchain_max_steps == 10
    assert restored.toolchain_max_describe == 6
    assert restored.toolchain_max_list_tables == 1
    assert restored.toolchain_max_describe_per_table == 1
    assert restored.toolchain_max_tool_only_streak == 4
    assert restored.toolchain_max_tool_calls == 10
    assert restored.toolchain_timeout_sec == 30
    assert restored.toolchain_allow_sample_values == 0
    assert restored.sql_execution_timeout_sec is None
    assert restored.task_timeout_sec == 120


def test_roundtrip_zero_values():
    """Falsy values like 0 must survive the round-trip (not fall to defaults)."""
    original = Config(
        model=ModelConfig(),
        comparator=ComparatorConfig(),
        datasets=DatasetPaths(
            bird_root=Path("/tmp/bird"),
            spider2_root=Path("/tmp/spider2"),
            tpcds_duckdb=Path("/tmp/tpcds.db"),
        ),
        schema_max_tables=0,
        schema_max_cols_per_table=0,
        toolchain_max_steps=0,
        toolchain_allow_sample_values=0,
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        json_path = Path(tmpdir) / "config.json"
        data = dump_config(original)
        json_path.write_text(json.dumps(data, indent=2))
        restored = load_config_json(json_path)

    assert restored.schema_max_tables == 0
    assert restored.schema_max_cols_per_table == 0
    assert restored.toolchain_max_steps == 0
    assert restored.toolchain_allow_sample_values == 0
