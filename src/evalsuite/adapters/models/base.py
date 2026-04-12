from __future__ import annotations

import logging
import os
from typing import Any, Protocol, TypeVar

from evalsuite.adapters.models.openai import OpenAIChatAdapter
from evalsuite.core.config import Config, _resolve_env

log = logging.getLogger(__name__)

T = TypeVar("T")


class ModelAdapter(Protocol):
    """Contract for LLM adapters.

    To add a new LLM provider, create a class implementing these two methods
    and register it in build_model_adapter() below.

    Example (adapters/models/my_provider.py):
        class MyProviderAdapter:
            def __init__(self, model: str, base_url: str, api_key: str, extra: dict): ...
            def generate_sql(self, question, schema=None, messages=None) -> str: ...
            def generate_structured(self, prompt, schema_model, ...) -> T: ...
    """

    def generate_sql(
        self,
        question: str,
        schema: str | None = None,
        messages: list[dict[str, Any]] | None = None,
    ) -> str:
        """Generate SQL from a natural language question.

        Args:
            question: Natural language question.
            schema: Optional schema prompt (compact/DDL/JSON format).
            messages: Optional pre-built message list (for toolchain multi-turn).

        Returns:
            Generated SQL string (no markdown fences).
        """
        ...

    def generate_structured(
        self,
        prompt: str,
        schema_model: type[T],
        *,
        system_prompt: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        max_retries: int = 2,
    ) -> T:
        """Generate structured output (JSON) validated against a Pydantic model.

        Args:
            prompt: User prompt.
            schema_model: Pydantic model class to validate response.
            system_prompt: Optional system prompt override.
            temperature: Optional temperature override.
            max_tokens: Optional max tokens.
            max_retries: Retry count on validation failure.

        Returns:
            Validated instance of schema_model.
        """
        ...


def build_model_adapter(config: Config, model_name: str) -> ModelAdapter:
    """Use config.model, or if config.raw has 'models' and model_name is a key, use that entry.
    Supports openrouter:<model_id> to use OpenRouter with any model (e.g. openrouter:qwen/qwen3-coder-next).
    Lookup is case-insensitive."""
    raw = getattr(config, "raw", {}) or {}
    models = raw.get("models") or {}
    key_by_lower = {k.lower(): k for k in models}
    openrouter_model_override = None
    # "default" means use config.model section directly
    if not model_name or model_name.lower() == "default":
        model_name = ""
    lookup_name = model_name
    if model_name and model_name.lower().startswith("openrouter:"):
        openrouter_model_override = model_name.split(":", 1)[1].strip()
        if not openrouter_model_override:
            raise ValueError(
                "openrouter:<model_id> requires a model id after the colon, e.g. openrouter:qwen/qwen3-coder-next"
            )
        lookup_name = "openrouter"
    resolved_key = key_by_lower.get(lookup_name.lower()) if lookup_name else None
    if openrouter_model_override is not None and resolved_key is None:
        raise ValueError(
            "openrouter:<model_id> requires an 'openrouter' entry in config.models (base_url, api_key). "
            "Add it to config.yaml under models.openrouter."
        )
    if lookup_name and resolved_key is not None:
        m = models[resolved_key]
        base_url = str(_resolve_env(m.get("base_url", "http://localhost:8000/v1")) or "http://localhost:8000/v1")
        model_id = (
            openrouter_model_override
            if openrouter_model_override is not None
            else str(_resolve_env(m.get("model", lookup_name)) or lookup_name)
        )
        api_key = str(_resolve_env(m.get("api_key", "EMPTY")) or "EMPTY")
        provider = (m.get("provider") or config.model.provider or "openai").lower()
        extra = {k: v for k, v in m.items() if k not in {"provider", "model", "base_url", "api_key"}}
    else:
        base_url = config.model.base_url
        model_id = model_name or config.model.model
        api_key = config.model.api_key
        provider = config.model.provider.lower()
        extra = config.model.extra
    # Qwen safety net: if model looks like Qwen but base_url points to OpenAI, redirect to QWEN_BASE_URL
    if model_name and "qwen" in model_name.lower() and "api.openai.com" in (base_url or ""):
        qwen_url = os.environ.get("QWEN_BASE_URL", "http://localhost:8000/v1")
        log.warning(
            "Model %r resolved to api.openai.com; redirecting to QWEN_BASE_URL=%s",
            model_name,
            qwen_url,
        )
        base_url = qwen_url.rstrip("/")
        if not base_url.endswith("/v1"):
            base_url = base_url + "/v1"
        api_key = str(os.environ.get("LLM_API_KEY", api_key) or "EMPTY")
    if provider == "openai":
        return OpenAIChatAdapter(
            model=model_id,
            base_url=base_url,
            api_key=api_key,
            extra=extra,
        )
    raise ValueError(f"Unsupported model provider: {provider}")
