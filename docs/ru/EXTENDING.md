# Расширение EvalSuite

## Добавление новой LLM-модели

**Простой способ (OpenAI-совместимый API):** добавить запись в `config.yaml`:

```yaml
models:
  my-model:
    base_url: https://api.example.com/v1
    model: my-model-id
    api_key: ${LLM_API_KEY}
    temperature: 0.0
```

Запуск: `uv run python -m evalsuite run --model my-model --bench bird_sqlite --limit 5`

**Кастомный API:** реализовать протокол `ModelAdapter` (см. `adapters/models/base.py`).

Необходимые методы:
- `generate_sql(question, schema, messages) -> str` — генерация SQL из NL-вопроса
- `generate_structured(prompt, schema_model, ...) -> T` — генерация структурированного JSON

Пример: `adapters/models/openai.py`

---

## Добавление нового бенчмарка

1. Создать `evalsuite/benchmarks/my_bench.py`, наследуя `Benchmark` (ABC)

Обязательные методы:
- `discover_tasks() -> list[TaskSpec]` — загрузка задач из датасета
- `_get_dialect() -> str` — `"sqlite"` или `"duckdb"`
- `_get_constraints() -> DialectConstraints` — разрешённые SQL-операторы
- `_get_schema_context(db_path) -> SchemaContext` — извлечение схемы из БД
- `_get_tool_executor(db_path) -> SchemaToolsExecutor` — для toolchain-режима
- `_execute_sql(db_path, sql) -> ExecResult` — выполнение SQL

Опциональные:
- `_should_skip(task)` — логика пропуска задач
- `_post_execute(...)` — постобработка (авто-патч, валидация)

2. Зарегистрировать в `orchestrator.py` `BENCH_REGISTRY`
3. Положить данные в `data/my_bench/`

Примеры: `benchmarks/bird.py` (простой), `benchmarks/tpcds.py` (сложный)

---

## Добавление новой архитектуры

1. Создать `evalsuite/architectures/my_arch.py` с функцией:

```python
def run_my_arch(*, task_id, question, schema, model, db_path, dialect, params, **kwargs
) -> list[CandidateResult]:
    """Сгенерировать SQL-кандидаты. Фреймворк выберет лучшего."""
    ...
```

2. Зарегистрировать в `benchmarks/base.py` `_run_task_common()` (dispatch по имени)
3. Добавить в CLI choices в `cli.py` и `core/config.py` `VALID_ARCHITECTURES`

Примеры: `architectures/plain.py` (простейшая), `architectures/hybrid.py` (полная)

---

## Добавление нового DB-адаптера

Реализовать функцию:

```python
def execute_sql(db_path: str, sql: str, timeout_sec: int | None = None) -> ExecResult:
    """Выполнить SQL и вернуть ExecResult(ok, rows, error, exec_time_ms)."""
```

Примеры: `adapters/db/sqlite.py`, `adapters/db/duckdb.py`

---

## Ключевые типы данных (core/types.py)

| Тип | Назначение |
|-----|-----------|
| `TaskSpec` | Вход: task_id, question, gold_sql, db_path |
| `TaskResult` | Выход: pred_sql, match, status, error_type |
| `ExecResult` | Результат SQL: ok, rows, error, exec_time_ms |
| `CandidateResult` | Выход архитектуры: sql, exec_ok, score, result_signature |
| `SchemaContext` | Схема БД: таблицы с колонками, FK |
| `DialectConstraints` | Ограничения SQL: dialect, allowed_statements |
| `BenchSummary` | Агрегированные метрики по бенчмарку |
