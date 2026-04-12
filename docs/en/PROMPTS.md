# Prompts & Model Interaction

This document describes system prompts, user message format, and model call flow in evalsuite.

---

## 1. Model Interface

- **Interface:** `ModelAdapter.generate_sql(question, schema=None, messages=None)`
  File: `evalsuite/adapters/models/base.py`, implementation: `evalsuite/adapters/models/openai.py`
- **API call:** OpenAI-compatible `POST .../chat/completions` with `messages`, `temperature=0.0`, plus `extra` from config.

---

## 2. full_schema Mode (schema in prompt)

Used for all benchmarks with `--context-mode full_schema`.

### 2.1 System Prompt (hardcoded)

**File:** `evalsuite/adapters/models/openai.py`

```text
You are a helpful SQL assistant. Return only SQL.
```

Same for all benchmarks. Dialect and constraints are included in the schema block (see below).

### 2.2 User Message

```text
{question}

Schema:
{schema_prompt}
```

- `question` — task text from dataset
- `schema_prompt` — built by `build_schema_prompt()` (see section 3)

### 2.3 Final API Messages

```json
[
  {"role": "system", "content": "You are a helpful SQL assistant. Return only SQL."},
  {"role": "user", "content": "<question>\n\nSchema:\n<schema_prompt>"}
]
```

The model should return **only SQL**. Response is post-processed: markdown fences are stripped.

---

## 3. Schema Prompt Format

**File:** `evalsuite/pipeline/schema.py`
Function: `build_schema_prompt(schema_ctx, fmt, max_tables, max_cols, constraints)`.

Format is configured via `schema_format`: `compact` (default), `ddl`, or `json`.

### 3.1 Compact (default)

```text
DIALECT: duckdb
SCHEMA:
- store_sales(ss_sold_date_sk, ss_item_sk, ss_customer_sk, ...)
- item(i_item_sk, i_product_name, i_category, ...)
FK:
- store_sales.ss_customer_sk -> customer.c_customer_sk
RULES:
- Allowed statements: SELECT, WITH
- Use only provided tables/columns from the data warehouse schema.
- Return SQL only. No markdown.
- If a needed column is missing, use best effort with available schema.
```

### 3.2 DDL

Same information as pseudo-DDL with comments.

### 3.3 JSON

Schema and rules as a single JSON object with `dialect`, `tables`, and optional `rules`.

### 3.4 Size Limits

- `schema_max_tables` (default 50)
- `schema_max_cols_per_table` (default 30)

---

## 4. Dialect Constraints

**Type:** `DialectConstraints` in `evalsuite/core/types.py`:

- `dialect`: `"duckdb"` or `"sqlite"`
- `allowed_statements`: e.g. `["SELECT", "WITH"]`
- `forbidden_tokens`: substrings that must not appear in the response
- `notes`: short instructions (included in RULES block)

### TPC-DS

- `dialect`: `"duckdb"`, `allowed_statements`: `["SELECT", "WITH"]`

### Spider2 / Bird (SQLite)

- `dialect`: `"sqlite"`, `forbidden_tokens`: `["information_schema", "pg_catalog", "describe", "desc"]`

---

## 5. Toolchain Mode (model calls tools)

With `--context-mode toolchain`, the model does not receive the schema upfront — it calls tools, then outputs SQL.

### 5.1 System Prompt (toolchain)

**File:** `evalsuite/pipeline/toolchain.py`

```text
You may call tools to learn schema.
Use TOOL_CALL format exactly and as the FIRST non-empty line.
Call list_tables first, then describe_table for relevant tables.
After you have enough schema, output FINAL SQL only (no prose, no markdown).
```

### 5.2 Multi-turn Dialogue

1. Model responds with `TOOL_CALL: list_tables` / `TOOL_CALL: describe_table` or final SQL
2. Tool result added to messages as `TOOL_RESULT:\n<json>`
3. Loop repeats until `max_steps` or model returns SQL
4. SQL extracted via `extract_sql()`: strips markdown, finds first `SELECT`/`WITH`

---

## 6. Where to Modify Prompts

| What to Change | File | What to Do |
|---|---|---|
| System prompt (full_schema) | `adapters/models/openai.py` | Edit `system_prompt` string |
| Schema rules | `pipeline/schema.py` | Edit `compact_schema_prompt` / `ddl_schema_prompt` |
| Benchmark constraints | `benchmarks/tpcds.py` etc. | Edit `_constraints()` method |
| Toolchain system prompt | `pipeline/toolchain.py` | Edit `sys_lines` list |
| User message format | `adapters/models/openai.py` | Edit `user_content` string |

---

## 7. Response Post-Processing

- **Markdown removal:** delegates to `pipeline/sql_sanitize.strip_sql_fences()`
- **SQL extraction (toolchain):** `extract_sql()` in `pipeline/toolchain.py` finds first `SELECT`/`WITH`
