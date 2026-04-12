# Evaluation Results / Результаты оценки

## Datasets / Датасеты

- [BIRD](https://bird-bench.github.io) — 1534 задачи. Dev-split из полного датасета (12 751 задач). Остальное — для обучения/файнтюнинга.
- [Spider2-Lite](https://spider2-sql.github.io) — 123 задачи. Только SQLite-часть; Snowflake и BigQuery исключены (проблемы с доступом).
- [TPC-DS NL](https://www.tpc.org/tpcds/) — 99 задач. Не text-to-SQL бенчмарк изначально — это 99 аналитических SQL-запросов, для которых сгенерированы NL-описания задач.

## Models / Модели

Модели запускались в **plain-режиме** с `context_mode=toolchain`.

| Model / Модель        | BIRD                     | Spider2              | TPC-DS              | Total / Всего          |
|-----------------------|--------------------------|----------------------|---------------------|------------------------|
| qwen3-coder-next      | **0.263** (403 / 1534)   | 0.122 (15 / 123)     | 0.030 (3 / 99)      | **0.240** (421 / 1756) |
| qwen3-32b | 0.229 (352 / 1534)       | **0.293** (36 / 123) | **0.101** (10 / 99)  | 0.227 (398 / 1756)     |
| gpt-oss-120b          | 0.080 (123 / 1534)       | 0.065 (8 / 123)      | 0.061 (6 / 99)       | 0.078 (137 / 1756)     |

## Architectures / Архитектуры

Результаты для **qwen3-coder-next**. Подробное описание каждой архитектуры: [docs/architectures/](docs/architectures/).

| Architecture / Архитектура | BIRD                     | Spider2                  | TPC-DS                 | Total / Всего            |
|----------------------------|--------------------------|--------------------------|------------------------|--------------------------|
| Plain                      | 0.263 (403 / 1534)       | 0.122 (15 / 123)         | 0.030 (3 / 99)         | 0.240 (421 / 1756)       |
| Self-consistency           | 0.308 (472 / 1534)       | 0.650 (80 / 123)         | **0.101 (10 / 99)**     | 0.320 (562 / 1756)       |
| SGR                        | 0.301 (461 / 1534)       | 0.674 (83 / 123)         | 0.040 (4 / 99)         | 0.312 (548 / 1756)       |
| SQL Factory                | 0.298 (457 / 1534)       | 0.577 (71 / 123)         | 0.081 (8 / 99)         | 0.305 (536 / 1756)       |
| **Hybrid**                 | **0.323 (495 / 1534)**   | **0.715 (88 / 123)**     | 0.091 (9 / 99)         | **0.337 (592 / 1756)**   |

## Latency / Латентность (avg ms per task)

### By model (plain) / По моделям

| Model / Модель        | BIRD       | Spider2     | TPC-DS      |
|-----------------------|------------|-------------|-------------|
| qwen3-coder-next      | 8 267      | 12 801      | 20 536      |
| qwen3-32b | **5 149**  | **8 293**   | **12 638**  |
| gpt-oss-120b          | 21 250     | 16 947      | 19 747      |

### By architecture (qwen3-coder-next) / По архитектурам

| Architecture / Архитектура | BIRD       | Spider2     | TPC-DS      |
|----------------------------|------------|-------------|-------------|
| Plain                      | **8 267**  | **12 801**  | **20 536**  |
| Self-consistency           | 12 265     | 12 928      | 25 307      |
| SGR                        | 16 767     | 29 162      | 21 968      |
| SQL Factory                | 14 679     | 30 142      | 39 454      |
| Hybrid                     | 16 164     | 30 893      | 36 758      |

## Error Analysis / Анализ ошибок

### Error types / Типы ошибок

| Error type | Description / Описание |
|------------|------------------------|
| `pred_exec_fail` | SQL generated but failed at execution (syntax error, missing column/table) |
| `pred_bind_fail` | DuckDB binder error (strict column/table resolution) |
| `pred_parse_fail` | SQL parsing failed (usually markdown artifacts) |
| `pred_invalid_sql` | Placeholders left in SQL (`{{year}}`, `<replace>`) |
| `pred_generation_fail` | Toolchain failed to extract SQL from LLM response |
| `task_timeout` | Task exceeded total timeout (generation + execution) |
| `gold_exec_fail` | Gold SQL itself failed (test data issue, not model) |

### By architecture, BIRD (qwen3-coder-next) / По архитектурам, BIRD

| Error type             | Plain | Self-consistency | SGR  | SQL Factory | Hybrid      |
|------------------------|-------|------------------|------|-------------|-------------|
| pred_exec_fail         | 426   | 18               | 85   | 61          | **0**       |
| pred_generation_fail   | 26    | **0**            | **0**| **0**       | 4           |
| task_timeout           | 21    | 124              | 22   | 34          | **12**      |
| **Total failures**     | 473   | 142              | 108  | 101         | **16**      |

Hybrid радикально снижает failures (16 vs 473 у plain). Diagnostic retry + schema validation устраняют `pred_exec_fail` полностью. Smart early stop сокращает таймауты (12 vs 124 у self-consistency).

### By architecture, Spider2 (qwen3-coder-next) / По архитектурам, Spider2

| Error type             | Plain | Self-consistency | SGR  | SQL Factory | Hybrid      |
|------------------------|-------|------------------|------|-------------|-------------|
| pred_exec_fail         | 90    | 10               | 13   | 25          | **0**       |
| pred_generation_fail   | 0     | **0**            | **0**| **0**       | 5           |
| task_timeout           | **0** | 9                | **0**| **0**       | 2           |
| gold_exec_fail         | 5     | 5                | 5    | 5           | 5           |
| **Total failures**     | 96    | 19               | 16   | 27          | **12**      |

Hybrid — лучший результат (12 failures, из них 5 — gold_exec_fail, т.е. только 7 реальных ошибок модели).

### By architecture, TPC-DS (qwen3-coder-next) / По архитектурам, TPC-DS

| Error type             | Plain | Self-consistency | SGR  | SQL Factory | Hybrid  |
|------------------------|-------|------------------|------|-------------|---------|
| pred_parse_fail        | 32    | **0**            | **0**| **0**       | **0**   |
| pred_bind_fail         | 12    | 8                | 13   | 9           | **0**   |
| pred_generation_fail   | 23    | **0**            | 27   | **0**       | 1       |
| pred_invalid_sql       | **0** | 2                | **0**| **0**       | 16      |
| pred_runtime_fail      | **0** | **0**            | **0**| **0**       | 2       |
| task_timeout           | **0** | 7                | **0**| 10          | 3       |
| **Total failures**     | 69    | 20               | 44   | 25          | 22      |

TPC-DS — самый сложный бенчмарк (accuracy 9.1%). Hybrid показывает 16 `pred_invalid_sql` — плейсхолдеры в SQL, вероятно от SGR grounding промпта.

### By model (plain, BIRD) / По моделям

| Error type             | qwen3-coder | qwen3-32b | gpt-oss-120b |
|------------------------|-------------|--------------|--------------|
| pred_exec_fail         | 426         | **129**      | 237          |
| pred_generation_fail   | **26**      | 236          | 966          |
| task_timeout           | 21          | **0**        | **0**        |
| **Total failures**     | 473         | **370**      | 1206         |

gpt-oss-120b критически плох — 966 generation failures (63%), модель не генерирует SQL. qwen3-coder — самый надёжный генератор.

### Root Causes / Корневые причины

1. **Schema mismatch** — модель галлюцинирует имена: `CreationDate` вместо `creation_date`, `s_store_type` вместо `s_street_type`
2. **Markdown artifacts** — 32 parse_fail на TPC-DS plain (модель оборачивает SQL в ` ```sql ``` `)
3. **Timeouts** — K генераций x toolchain-вызовы > task_timeout (124 на BIRD SC)
4. **Toolchain extraction failures** — gpt-oss-120b «разговаривает» вместо SQL (966 случаев)
5. **Test data issues** — 5 gold_exec_fail в Spider2 (отсутствующие таблицы в fixtures)
