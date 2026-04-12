from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

VALID_ARCHITECTURES = {"plain", "self_consistency", "sql_factory", "sgr", "hybrid"}


def _validate_architecture(data: dict) -> None:
    """Warn if architecture.name in config is not a known value."""
    arch = data.get("architecture")
    if not isinstance(arch, dict):
        return
    name = (arch.get("name") or "").lower().strip()
    if name and name not in VALID_ARCHITECTURES:
        logger.warning(
            "Unknown architecture.name %r in config (valid: %s). Falling back to 'plain'.",
            name,
            ", ".join(sorted(VALID_ARCHITECTURES)),
        )
        arch["name"] = "plain"


def _redact_key(key: str) -> str:
    """Mask an API key for safe storage in artifacts.

    Short keys (<=8 chars) or default "EMPTY" are returned as-is.
    Otherwise keeps first 3 and last 3 characters: ``sk-or-v1-da2***ad7``.
    """
    if not key or key == "EMPTY" or len(key) <= 8:
        return key
    return key[:3] + "***" + key[-3:]


def _redact_dict(d: dict) -> dict:
    """Return a shallow copy of *d* with any ``api_key`` value redacted."""
    out = dict(d)
    if "api_key" in out and isinstance(out["api_key"], str):
        out["api_key"] = _redact_key(out["api_key"])
    return out


@dataclass
class ComparatorConfig:
    float_tol: float = 1e-4
    column_order_insensitive: bool = True
    string_normalize: bool = True


@dataclass
class ModelConfig:
    provider: str = "openai"
    model: str = "qwen2"
    base_url: str = "http://localhost:8000/v1"
    api_key: str = "EMPTY"
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class DatasetPaths:
    bird_root: Path
    spider2_root: Path
    tpcds_duckdb: Path


@dataclass
class Config:
    model: ModelConfig
    comparator: ComparatorConfig
    datasets: DatasetPaths
    context_mode: str = "none"  # none | full_schema | toolchain
    schema_max_tables: int = 50
    schema_max_cols_per_table: int = 30
    schema_format: str = "compact"  # compact | ddl | json
    toolchain_max_steps: int = 10
    toolchain_max_describe: int = 6
    toolchain_max_list_tables: int = 1
    toolchain_max_describe_per_table: int = 1
    toolchain_max_tool_only_streak: int = 4
    toolchain_max_tool_calls: int = 10
    toolchain_timeout_sec: int = 30
    toolchain_allow_sample_values: int = 0
    sql_execution_timeout_sec: int | None = (
        None  # None = no timeout (main thread); set to 120 etc. to use thread+timeout
    )
    task_timeout_sec: int | None = 120  # Max seconds per task; 0 or None = no limit (CLI default 300 if not in config)
    raw: dict[str, Any] = field(default_factory=dict)


def _default_config(base: Path) -> Config:
    datasets = DatasetPaths(
        bird_root=base / "data" / "bird",
        spider2_root=base / "data" / "spider2",
        tpcds_duckdb=base / "data" / "tpcds" / "duckdb.db",
    )
    return Config(
        model=ModelConfig(),
        comparator=ComparatorConfig(),
        datasets=datasets,
        context_mode="none",
        schema_max_tables=50,
        schema_max_cols_per_table=30,
        schema_format="compact",
        toolchain_max_steps=10,
        toolchain_max_describe=6,
        toolchain_max_list_tables=1,
        toolchain_max_describe_per_table=1,
        toolchain_max_tool_only_streak=4,
        toolchain_max_tool_calls=10,
        toolchain_timeout_sec=30,
        toolchain_allow_sample_values=0,
        sql_execution_timeout_sec=None,
        raw={
            "model": vars(ModelConfig()),
            "comparator": vars(ComparatorConfig()),
            "datasets": {
                "bird_root": str(datasets.bird_root),
                "spider2_root": str(datasets.spider2_root),
                "tpcds_duckdb": str(datasets.tpcds_duckdb),
            },
        },
    )


def _load_dotenv(env_path: Path) -> None:
    """Load KEY=VALUE pairs from .env without overriding existing environment."""
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


def _resolve_env(value: Any) -> Any:
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        env_name = value[2:-1]
        return os.getenv(env_name, "")
    return value


def load_config(path: Path) -> Config:
    base = path.parent.resolve()
    _load_dotenv(base / ".env")
    if not path.exists():
        return _default_config(base)

    with path.open("r") as f:
        data = yaml.safe_load(f) or {}

    _validate_architecture(data)

    model_cfg = data.get("model", {})
    comparator_cfg = data.get("comparator", {})
    datasets_cfg = data.get("datasets", {})

    resolved_base = _resolve_env(model_cfg.get("base_url", "http://localhost:8000/v1")) or "http://localhost:8000/v1"
    resolved_model = _resolve_env(model_cfg.get("model", "qwen2")) or "qwen2"
    resolved_key = _resolve_env(model_cfg.get("api_key", "EMPTY")) or "EMPTY"
    model = ModelConfig(
        provider=model_cfg.get("provider", "openai"),
        model=resolved_model,
        base_url=resolved_base,
        api_key=resolved_key,
        extra={k: v for k, v in model_cfg.items() if k not in {"provider", "model", "base_url", "api_key"}},
    )
    comparator = ComparatorConfig(
        float_tol=float(comparator_cfg.get("float_tol", 1e-4)),
        column_order_insensitive=bool(comparator_cfg.get("column_order_insensitive", True)),
        string_normalize=bool(comparator_cfg.get("string_normalize", True)),
    )
    bird_root = Path(datasets_cfg.get("bird_root", base / "data" / "bird"))

    datasets = DatasetPaths(
        bird_root=bird_root,
        spider2_root=Path(datasets_cfg.get("spider2_root", base / "data" / "spider2")),
        tpcds_duckdb=Path(datasets_cfg.get("tpcds_duckdb", base / "data" / "tpcds" / "duckdb.db")),
    )

    context_mode = data.get("context_mode", "none")
    schema_max_tables = int(data.get("schema_max_tables", 50))
    schema_max_cols_per_table = int(data.get("schema_max_cols_per_table", 30))
    schema_format = data.get("schema_format", "compact")
    toolchain_max_steps = int(data.get("toolchain_max_steps", 10))
    toolchain_max_describe = int(data.get("toolchain_max_describe", 6))
    toolchain_max_list_tables = int(data.get("toolchain_max_list_tables", 1))
    toolchain_max_describe_per_table = int(data.get("toolchain_max_describe_per_table", 1))
    toolchain_max_tool_only_streak = int(data.get("toolchain_max_tool_only_streak", 4))
    toolchain_max_tool_calls = int(data.get("toolchain_max_tool_calls", 10))
    toolchain_timeout_sec = int(data.get("toolchain_timeout_sec", 30))
    toolchain_allow_sample_values = int(data.get("toolchain_allow_sample_values", 0))
    sql_execution_timeout_sec = data.get("sql_execution_timeout_sec")
    if sql_execution_timeout_sec is not None:
        sql_execution_timeout_sec = int(sql_execution_timeout_sec)
    _task = data.get("task_timeout_sec", 120)
    task_timeout_sec = None if _task is None or _task == 0 else int(_task)

    return Config(
        model=model,
        comparator=comparator,
        datasets=datasets,
        context_mode=context_mode,
        schema_max_tables=schema_max_tables,
        schema_max_cols_per_table=schema_max_cols_per_table,
        schema_format=schema_format,
        toolchain_max_steps=toolchain_max_steps,
        toolchain_max_describe=toolchain_max_describe,
        toolchain_max_list_tables=toolchain_max_list_tables,
        toolchain_max_describe_per_table=toolchain_max_describe_per_table,
        toolchain_max_tool_only_streak=toolchain_max_tool_only_streak,
        toolchain_max_tool_calls=toolchain_max_tool_calls,
        toolchain_timeout_sec=toolchain_timeout_sec,
        toolchain_allow_sample_values=toolchain_allow_sample_values,
        sql_execution_timeout_sec=sql_execution_timeout_sec,
        task_timeout_sec=task_timeout_sec,
        raw=data,
    )


def dump_config(config: Config, effective_model_name: str | None = None) -> dict[str, Any]:
    models = (getattr(config, "raw", None) or {}).get("models") or {}
    key_by_lower = {k.lower(): k for k in models}
    resolved_key = key_by_lower.get(effective_model_name.lower()) if effective_model_name else None
    if effective_model_name and resolved_key is not None:
        m = models[resolved_key]
        model_block = {
            "provider": str(m.get("provider", config.model.provider)),
            "model": str(_resolve_env(m.get("model", resolved_key)) or resolved_key),
            "base_url": str(_resolve_env(m.get("base_url", "http://localhost:8000/v1")) or "http://localhost:8000/v1"),
            "api_key": _redact_key(str(_resolve_env(m.get("api_key", "EMPTY")) or "EMPTY")),
            **{k: v for k, v in m.items() if k not in {"provider", "model", "base_url", "api_key"}},
        }
    else:
        model_block = {
            "provider": config.model.provider,
            "model": config.model.model,
            "base_url": config.model.base_url,
            "api_key": _redact_key(config.model.api_key),
            **config.model.extra,
        }
    out = {
        "model": model_block,
        "comparator": {
            "float_tol": config.comparator.float_tol,
            "column_order_insensitive": config.comparator.column_order_insensitive,
            "string_normalize": config.comparator.string_normalize,
        },
        "datasets": {
            "bird_root": str(config.datasets.bird_root),
            "spider2_root": str(config.datasets.spider2_root),
            "tpcds_duckdb": str(config.datasets.tpcds_duckdb),
        },
        "context_mode": config.context_mode,
        "schema_max_tables": config.schema_max_tables,
        "schema_max_cols_per_table": config.schema_max_cols_per_table,
        "schema_format": config.schema_format,
        "toolchain_max_steps": config.toolchain_max_steps,
        "toolchain_max_describe": config.toolchain_max_describe,
        "toolchain_max_list_tables": config.toolchain_max_list_tables,
        "toolchain_max_describe_per_table": config.toolchain_max_describe_per_table,
        "toolchain_max_tool_only_streak": config.toolchain_max_tool_only_streak,
        "toolchain_max_tool_calls": config.toolchain_max_tool_calls,
        "toolchain_timeout_sec": config.toolchain_timeout_sec,
        "toolchain_allow_sample_values": config.toolchain_allow_sample_values,
        "sql_execution_timeout_sec": config.sql_execution_timeout_sec,
        "task_timeout_sec": config.task_timeout_sec,
    }
    if config.raw.get("models"):
        out["models"] = {k: _redact_dict(v) if isinstance(v, dict) else v for k, v in config.raw["models"].items()}
    return out


def save_config_json(config: Config, path: Path, effective_model_name: str | None = None) -> None:
    data = dump_config(config, effective_model_name=effective_model_name)
    path.write_text(json.dumps(data, indent=2))


def load_config_json(path: Path) -> Config:
    base = path.parent
    _load_dotenv(base / ".env")
    data = json.loads(path.read_text())
    model_cfg = data.get("model", {})
    comparator_cfg = data.get("comparator", {})
    datasets_cfg = data.get("datasets", {})
    resolved_base = _resolve_env(model_cfg.get("base_url", "http://localhost:8000/v1")) or "http://localhost:8000/v1"
    resolved_model = _resolve_env(model_cfg.get("model", "qwen2")) or "qwen2"
    resolved_key = _resolve_env(model_cfg.get("api_key", "EMPTY")) or "EMPTY"
    model = ModelConfig(
        provider=model_cfg.get("provider", "openai"),
        model=resolved_model,
        base_url=resolved_base,
        api_key=resolved_key,
        extra={k: v for k, v in model_cfg.items() if k not in {"provider", "model", "base_url", "api_key"}},
    )
    comparator = ComparatorConfig(
        float_tol=float(comparator_cfg.get("float_tol", 1e-4)),
        column_order_insensitive=bool(comparator_cfg.get("column_order_insensitive", True)),
        string_normalize=bool(comparator_cfg.get("string_normalize", True)),
    )
    bird_root = Path(datasets_cfg.get("bird_root", base / "data" / "bird"))

    datasets = DatasetPaths(
        bird_root=bird_root,
        spider2_root=Path(datasets_cfg.get("spider2_root", base / "data" / "spider2")),
        tpcds_duckdb=Path(datasets_cfg.get("tpcds_duckdb", base / "data" / "tpcds" / "duckdb.db")),
    )

    context_mode = data.get("context_mode", "none")
    schema_max_tables = int(data.get("schema_max_tables", 50))
    schema_max_cols_per_table = int(data.get("schema_max_cols_per_table", 30))
    schema_format = data.get("schema_format", "compact")
    toolchain_max_steps = int(data.get("toolchain_max_steps", 10))
    toolchain_max_describe = int(data.get("toolchain_max_describe", 6))
    toolchain_max_list_tables = int(data.get("toolchain_max_list_tables", 1))
    toolchain_max_describe_per_table = int(data.get("toolchain_max_describe_per_table", 1))
    toolchain_max_tool_only_streak = int(data.get("toolchain_max_tool_only_streak", 4))
    toolchain_max_tool_calls = int(data.get("toolchain_max_tool_calls", 10))
    toolchain_timeout_sec = int(data.get("toolchain_timeout_sec", 30))
    toolchain_allow_sample_values = int(data.get("toolchain_allow_sample_values", 0))
    sql_execution_timeout_sec = data.get("sql_execution_timeout_sec")
    if sql_execution_timeout_sec is not None:
        sql_execution_timeout_sec = int(sql_execution_timeout_sec)
    _task = data.get("task_timeout_sec", 120)
    task_timeout_sec = None if _task is None or _task == 0 else int(_task)

    return Config(
        model=model,
        comparator=comparator,
        datasets=datasets,
        context_mode=context_mode,
        schema_max_tables=schema_max_tables,
        schema_max_cols_per_table=schema_max_cols_per_table,
        schema_format=schema_format,
        toolchain_max_steps=toolchain_max_steps,
        toolchain_max_describe=toolchain_max_describe,
        toolchain_max_list_tables=toolchain_max_list_tables,
        toolchain_max_describe_per_table=toolchain_max_describe_per_table,
        toolchain_max_tool_only_streak=toolchain_max_tool_only_streak,
        toolchain_max_tool_calls=toolchain_max_tool_calls,
        toolchain_timeout_sec=toolchain_timeout_sec,
        toolchain_allow_sample_values=toolchain_allow_sample_values,
        sql_execution_timeout_sec=sql_execution_timeout_sec,
        task_timeout_sec=task_timeout_sec,
        raw=data,
    )


# ---------------------------------------------------------------------------
# Generation config resolution (architecture + reasoning + sampling from CLI)
# ---------------------------------------------------------------------------

from evalsuite.core.types import GenerationRunConfig  # noqa: E402

DEFAULT_SC_SAMPLES = {"plain": 8, "sql_factory": None, "sgr": 6, "hybrid": None}


def resolve_generation_config(
    architecture: str | None = None,
    reasoning: str | None = None,
    sampling: str | None = None,
    sc_samples: int | None = None,
    sc_aggregation: str | None = None,
    raw_config: dict[str, Any] | None = None,
) -> GenerationRunConfig:
    """Resolve GenerationRunConfig from CLI args and config.yaml."""
    raw = raw_config or {}
    arch_block = raw.get("generation") or raw.get("architecture") or {}
    if isinstance(arch_block, dict):
        arch_name = (arch_block.get("name") or "").lower().strip()
        arch_params = arch_block.get("params") or {}
    else:
        arch_name = ""
        arch_params = {}

    arch_cli = (architecture or "").lower().strip() or None
    reasoning_cli = (reasoning or "").lower().strip() or None
    sampling_cli = (sampling or "").lower().strip() or None

    if arch_cli in ("selfconsistency", "self_consistency"):
        arch_cli = "plain"
        sampling_cli = "self_consistency"
        logger.warning(
            "Deprecated: --architecture selfconsistency is deprecated. "
            "Use --architecture plain --sampling self_consistency instead."
        )

    if arch_name == "self_consistency" and not arch_cli:
        arch_name = "plain"
        if sampling_cli is None:
            sampling_cli = "self_consistency"

    out_arch = arch_cli or (arch_name if arch_name in ("plain", "sql_factory", "sgr", "hybrid") else "plain")
    out_reasoning = reasoning_cli or "none"
    if out_reasoning not in ("none", "sgr"):
        out_reasoning = "none"
    out_sampling = sampling_cli or "single"
    if out_sampling not in ("single", "self_consistency"):
        out_sampling = "single"

    default_sc = DEFAULT_SC_SAMPLES.get(out_arch, 8)
    out_sc_samples = sc_samples if sc_samples is not None else default_sc
    if out_sampling != "self_consistency":
        out_sc_samples = None
    out_sc_agg = (sc_aggregation or "majority_result").strip()
    if out_sc_agg not in ("majority_result", "best_score"):
        out_sc_agg = "majority_result"

    if out_sampling == "self_consistency" and out_sc_samples is None and arch_params:
        out_sc_samples = arch_params.get("num_samples") or DEFAULT_SC_SAMPLES.get(out_arch, 8)

    return GenerationRunConfig(
        architecture=out_arch,
        reasoning=out_reasoning,
        sampling=out_sampling,
        sc_samples=out_sc_samples,
        sc_aggregation=out_sc_agg,
    )
