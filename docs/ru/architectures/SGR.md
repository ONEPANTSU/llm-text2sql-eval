# SGR (Semantic Graph Reasoning) / Семантическое обоснование

## Overview / Обзор

Schema grounding + structured plan + K SQL synthesis + repair. Before generating SQL, the LLM analyzes the question and identifies relevant tables, columns, and joins — reducing hallucination.

Обоснование схемы + структурный план + K генераций SQL + починка. Перед генерацией SQL модель анализирует вопрос и определяет релевантные таблицы, колонки и соединения — снижая галлюцинации.

## Algorithm / Алгоритм

1. **Grounding** — LLM identifies relevant tables, columns, joins, filters from schema
2. **Plan** — decompose query into: aggregations, GROUP BY, filters, ORDER BY
3. **SQL Synthesis** (K=3 candidates) — generate SQL using grounding + plan as context
4. **Preflight + Execution** — validate and execute each candidate
5. **Repair** (optional) — if candidate fails with `pred_bind_fail` or `pred_runtime_fail`, retry with error context
6. **Scoring** — base 1.0 if exec_ok; penalty -0.2 per table outside grounding; penalty -0.1 if SQL > 2000 chars

## Parameters / Параметры

| Parameter | Default | Description / Описание |
|-----------|---------|------------------------|
| `num_candidates` | 3 | SQL candidates per task / Число SQL-кандидатов |
| `generation_timeout` | 15s | Timeout per candidate / Таймаут на кандидата |
| `MAX_SCHEMA_CHARS` | 12000 | Schema truncation limit / Лимит длины схемы |

## Repair / Починка

Triggered for error types: `pred_bind_fail`, `pred_runtime_fail`. The failed SQL and error message are sent back to the LLM with a repair prompt. One repair attempt per candidate.

Срабатывает при ошибках: `pred_bind_fail`, `pred_runtime_fail`. Упавший SQL и текст ошибки отправляются обратно в LLM. Одна попытка починки на кандидата.

## CLI

```bash
uv run python -m evalsuite run --model openrouter --bench bird_sqlite --architecture sgr
```

## Results / Результаты

| Benchmark | Accuracy | Failures | Avg Latency |
|-----------|----------|----------|-------------|
| BIRD | 30.1% (461/1534) | 108 | 16.8s |
| Spider2 | 67.4% (83/123) | 16 | 29.2s |
| TPC-DS | 4.0% (4/99) | 44 | 22.0s |

Лучший результат на Spider2 среди одиночных архитектур. Grounding значительно помогает на задачах с большими схемами.
