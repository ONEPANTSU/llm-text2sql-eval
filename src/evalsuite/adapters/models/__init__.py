"""LLM model adapters.

Contract: every adapter must implement the ModelAdapter protocol (see base.py).

To add a new LLM provider:
1. Create evalsuite/adapters/models/my_provider.py
2. Implement the ModelAdapter protocol:

    class MyProviderAdapter:
        def __init__(self, model: str, base_url: str, api_key: str, extra: dict): ...

        def generate_sql(self, question, schema=None, messages=None) -> str:
            '''Generate SQL from natural language question.'''
            ...

        def generate_structured(self, prompt, schema_model, ...) -> T:
            '''Generate structured JSON output validated with Pydantic.'''
            ...

3. Register in base.py build_model_adapter():
    if provider == "my_provider":
        return MyProviderAdapter(...)

See openai.py for a complete reference implementation.
"""
