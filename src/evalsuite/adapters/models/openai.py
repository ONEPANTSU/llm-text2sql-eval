from __future__ import annotations

import json
import logging
import os
from typing import Any, TypeVar

import requests
from pydantic import BaseModel

log = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class OpenAIChatAdapter:
    def __init__(self, model: str, base_url: str, api_key: str, extra: dict):
        self.model = model
        if not base_url.startswith("http"):
            raise ValueError("OpenAI base_url must include scheme, e.g., https://api.openai.com/v1")
        cleaned = base_url.rstrip("/")
        if not cleaned.endswith("/v1"):
            cleaned = cleaned + "/v1"
        self.base_url = cleaned
        self.api_key = api_key
        self.extra = extra or {}
        log.info("OpenAI adapter: base_url=%s model=%s", self.base_url, self.model)

    def generate_sql(
        self,
        question: str,
        schema: str | None = None,
        messages: list[dict[str, Any]] | None = None,
        *,
        temperature: float | None = None,
        top_p: float | None = None,
        seed: int | None = None,
        **kwargs: Any,
    ) -> str:
        if messages is None:
            system_prompt = "You are a helpful SQL assistant. Return only SQL."
            user_content = question if schema is None else f"{question}\nSchema:\n{schema}"
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ]
        temp = temperature if temperature is not None else self.extra.get("temperature", 0.0)
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temp,
            **{k: v for k, v in self.extra.items() if k not in ("extra_headers", "temperature")},
        }
        if top_p is not None:
            payload["top_p"] = top_p
        if seed is not None:
            payload["seed"] = seed
        headers = {}
        if self.api_key and str(self.api_key).strip().upper() not in ("", "EMPTY", "NONE"):
            headers["Authorization"] = f"Bearer {self.api_key}"
        # OpenRouter optional headers (for rankings on openrouter.ai)
        if "openrouter.ai" in self.base_url:
            if os.environ.get("OPENROUTER_REFERER"):
                headers["HTTP-Referer"] = os.environ.get("OPENROUTER_REFERER")
            if os.environ.get("OPENROUTER_TITLE"):
                headers["X-Title"] = os.environ.get("OPENROUTER_TITLE")
        extra_headers = self.extra.get("extra_headers")
        if isinstance(extra_headers, dict):
            for k, v in extra_headers.items():
                if v is not None and str(v).strip():
                    headers[k] = str(v)
        url = f"{self.base_url}/chat/completions"
        resp = requests.post(url, json=payload, headers=headers or None, timeout=60)
        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:  # pragma: no cover - passthrough for debugging
            detail = ""
            try:
                detail = resp.text
            except Exception:
                detail = "<no body>"
            raise RuntimeError(f"openai api error {resp.status_code}: {detail}") from exc
        data = resp.json()
        raw = data["choices"][0]["message"]["content"].strip()
        return self._strip_code_fences(raw)

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
        """
        Call chat with prompt, extract JSON from response, validate with schema_model.
        Retries up to max_retries on validation/parse errors with error feedback in follow-up.
        """
        sys = system_prompt or "Return ONLY valid JSON. No markdown, no explanations."
        last_error: str = ""
        last_raw: str = ""
        for attempt in range(max_retries + 1):
            messages: list[dict[str, Any]] = [
                {"role": "system", "content": sys},
                {
                    "role": "user",
                    "content": prompt
                    if attempt == 0
                    else f"{prompt}\n\nPrevious attempt failed: {last_error}. Return ONLY valid JSON.",
                },
            ]
            payload: dict[str, Any] = {
                "model": self.model,
                "messages": messages,
                "temperature": temperature if temperature is not None else self.extra.get("temperature", 0.0),
                **{k: v for k, v in self.extra.items() if k not in ("extra_headers", "temperature")},
            }
            if max_tokens is not None:
                payload["max_tokens"] = max_tokens
            if self.extra.get("response_format") is not None:
                payload["response_format"] = self.extra["response_format"]
            # Else: do not force json_object so all providers get plain text; we parse JSON from text
            headers = {}
            if self.api_key and str(self.api_key).strip().upper() not in ("", "EMPTY", "NONE"):
                headers["Authorization"] = f"Bearer {self.api_key}"
            if "openrouter.ai" in self.base_url:
                if os.environ.get("OPENROUTER_REFERER"):
                    headers["HTTP-Referer"] = os.environ.get("OPENROUTER_REFERER")
                if os.environ.get("OPENROUTER_TITLE"):
                    headers["X-Title"] = os.environ.get("OPENROUTER_TITLE")
            extra_headers = self.extra.get("extra_headers")
            if isinstance(extra_headers, dict):
                for k, v in extra_headers.items():
                    if v is not None and str(v).strip():
                        headers[k] = str(v)
            url = f"{self.base_url}/chat/completions"
            try:
                resp = requests.post(url, json=payload, headers=headers or None, timeout=90)
                resp.raise_for_status()
            except requests.RequestException as e:
                last_error = str(e)
                last_raw = getattr(e, "response", None) and getattr(e.response, "text", "")[:500] or ""
                log.warning("generate_structured attempt %s request failed: %s", attempt + 1, last_error)
                if attempt == max_retries:
                    raise
                continue
            data = resp.json()
            raw = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
            if not raw:
                last_error = "empty response"
                last_raw = ""
                if attempt == max_retries:
                    raise ValueError("generate_structured: empty model response after retries")
                continue
            raw = raw.strip()
            # Extract JSON: whole response or first {...}
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                first = raw.find("{")
                if first >= 0:
                    end = raw.rfind("}") + 1
                    if end > first:
                        try:
                            obj = json.loads(raw[first:end])
                        except json.JSONDecodeError as e:
                            last_error = f"JSON decode: {e}"
                            last_raw = raw[:400]
                            log.warning(
                                "generate_structured attempt %s parse failed: %s snippet=%s",
                                attempt + 1,
                                last_error,
                                last_raw,
                            )
                            if attempt == max_retries:
                                raise ValueError(
                                    f"generate_structured: invalid JSON after retries: {last_error}"
                                ) from e
                            continue
                else:
                    last_error = "no JSON object in response"
                    last_raw = raw[:400]
                    if attempt == max_retries:
                        raise ValueError(f"generate_structured: {last_error}")
                    continue
            try:
                return schema_model.model_validate(obj)
            except Exception as e:
                last_error = str(e)
                last_raw = raw[:400]
                log.warning(
                    "generate_structured attempt %s validation failed: %s snippet=%s", attempt + 1, last_error, last_raw
                )
                if attempt == max_retries:
                    raise ValueError(f"generate_structured: validation failed after retries: {last_error}") from e
        raise ValueError("generate_structured: unexpected exit")

    @staticmethod
    def _strip_code_fences(text: str) -> str:
        from evalsuite.pipeline.sql_sanitize import strip_sql_fences

        return strip_sql_fences(text)
