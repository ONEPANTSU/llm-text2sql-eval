# Промпты и работа с моделью

Документ описывает системные промпты, формат пользовательского ввода и поток вызовов модели в evalsuite.

---

## 1. Интерфейс модели

- **Интерфейс:** `ModelAdapter.generate_sql(question, schema=None, messages=None)`
  Файл: `evalsuite/adapters/models/base.py`, реализация: `evalsuite/adapters/models/openai.py`
- **Вызов API:** OpenAI-compatible `POST .../chat/completions` с `messages`, `temperature=0.0`, плюс `extra` из конфига.

---

## 2. Режим full_schema (схема в промпте)

Используется при `--context-mode full_schema`.

### 2.1 Системный промпт

**Файл:** `evalsuite/adapters/models/openai.py`

```text
You are a helpful SQL assistant. Return only SQL.
```

Один и тот же для всех бенчмарков. Dialect и ограничения попадают в блок схемы.

### 2.2 Пользовательское сообщение

```text
{question}

Schema:
{schema_prompt}
```

### 2.3 Итоговые сообщения в API

```json
[
  {"role": "system", "content": "You are a helpful SQL assistant. Return only SQL."},
  {"role": "user", "content": "<question>\n\nSchema:\n<schema_prompt>"}
]
```

---

## 3. Формат блока схемы

**Файл:** `evalsuite/pipeline/schema.py`
Функция: `build_schema_prompt(schema_ctx, fmt, max_tables, max_cols, constraints)`.

Формат: `compact` (по умолчанию), `ddl` или `json`.

### 3.1 Compact (по умолчанию)

```text
DIALECT: duckdb
SCHEMA:
- store_sales(ss_sold_date_sk, ss_item_sk, ...)
FK:
- store_sales.ss_customer_sk -> customer.c_customer_sk
RULES:
- Allowed statements: SELECT, WITH
- Return SQL only. No markdown.
```

### 3.2 DDL / JSON

Та же информация в виде псевдо-DDL или JSON-объекта.

### 3.3 Ограничение размера

- `schema_max_tables` (по умолчанию 50)
- `schema_max_cols_per_table` (по умолчанию 30)

---

## 4. Ограничения по диалекту

**Тип:** `DialectConstraints` в `evalsuite/core/types.py`

| Бенчмарк | dialect | forbidden_tokens |
|---|---|---|
| TPC-DS | `duckdb` | — |
| Spider2 / Bird | `sqlite` | `information_schema`, `pg_catalog`, `describe`, `desc` |

---

## 5. Режим toolchain

**Файл:** `evalsuite/pipeline/toolchain.py`

Модель вызывает `TOOL_CALL: list_tables` / `TOOL_CALL: describe_table`, получает результаты, затем выдаёт SQL.

---

## 6. Где что менять

| Что менять | Файл |
|---|---|
| Системный промпт (full_schema) | `adapters/models/openai.py` |
| Правила в блоке схемы | `pipeline/schema.py` |
| Ограничения бенчмарка | `benchmarks/tpcds.py` и др. |
| Системный промпт (toolchain) | `pipeline/toolchain.py` |
| Сборка user-сообщения | `adapters/models/openai.py` |

---

## 7. Постобработка ответа

- **Удаление markdown:** `pipeline/sql_sanitize.strip_sql_fences()`
- **Извлечение SQL (toolchain):** `extract_sql()` в `pipeline/toolchain.py`
