# Plain (Baseline) / Базовая архитектура

## Overview / Обзор

Single SQL generation per task. The simplest approach — one LLM call produces one SQL query, which is then executed against the database.

Одна генерация SQL на задачу. Простейший подход — один вызов LLM создаёт один SQL-запрос, который исполняется на базе данных.

## Algorithm / Алгоритм

1. Build schema context (full_schema / toolchain / none)
2. Send question + schema to LLM
3. Extract SQL from response
4. Execute SQL against the database
5. Compare result with gold standard

## Parameters / Параметры

No architecture-specific parameters. Uses global settings:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `context_mode` | `toolchain` | How schema is provided: none, full_schema, toolchain |
| `task_timeout_sec` | 120 | Max seconds per task |

## CLI

```bash
uv run python -m evalsuite run --model openrouter --bench bird_sqlite --architecture plain
```

## Results / Результаты

| Benchmark | Accuracy | Failures |
|-----------|----------|----------|
| BIRD | 26.3% (403/1534) | 473 |
| Spider2 | 12.2% (15/123) | 96 |
| TPC-DS | 3.0% (3/99) | 69 |

Основные ошибки: `pred_exec_fail` (модель галлюцинирует имена колонок), `pred_generation_fail` (не удаётся извлечь SQL из ответа).
