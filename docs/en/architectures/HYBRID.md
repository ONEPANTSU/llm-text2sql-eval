# Hybrid / Гибридная архитектура

## Overview / Обзор

The best-performing architecture. Combines self-consistency with SGR grounding, seed expansion, diagnostic retry, smart early stop, and schema validation.

Лучшая архитектура по результатам. Комбинирует самосогласованность с SGR-обоснованием, расширением зёрен, диагностическим повтором, умной ранней остановкой и валидацией схемы.

## Algorithm / Алгоритм

### Phase 1: SGR Grounding (optional)

LLM analyzes the question and identifies relevant tables/columns from schema. On error — graceful fallback to full schema.

LLM анализирует вопрос и определяет релевантные таблицы/колонки. При ошибке — fallback на полную схему.

### Phase 2: Initial Generation (K=5)

Generate 5 independent SQL candidates (parallel). Each candidate goes through:

1. **Strip SQL fences** — remove markdown ` ```sql ``` ` wrappers
2. **Placeholder check** — reject if contains `<...>`, `YourState`, `TODO`, `???`
3. **Schema validation (fuzzy fix)** — edit distance <= 2 correction of table/column names
4. **Preflight + Execution** — validate and execute
5. **Result signature** — SHA-256 hash for majority voting

### Phase 2.5: Diagnostic Retry (conditional)

Triggered only if **all** K candidates failed with the **same error**. Generates 2 retry candidates with low temperature (0.3), including the error message in the prompt.

Срабатывает только если **все** K кандидатов упали с **одинаковой** ошибкой. Генерирует 2 повторных кандидата с низкой temperature (0.3), включая текст ошибки в промпт.

### Phase 3: Seed Expansion (optional)

**Smart Early Stop** — skip expansion if:
- No candidate has exec_ok (nothing to expand)
- All exec_ok candidates have same result_signature (consensus reached)

**Expansion:**
1. Pick top 2 seeds (greedy diverse selection, Jaccard < 0.85)
2. Generate 2 variations per seed ("Provide a different but equivalent SQL")
3. Filter by similarity (reject if Jaccard >= 0.85)
4. Execute accepted variations

### Phase 4: Aggregation

3-level fallback:
1. **exec_ok candidates** — group by result_signature, majority vote
2. **preflight_ok only** — group by normalized SQL, majority vote
3. **fallback** — return empty (no valid SQL)

## Parameters / Параметры

```yaml
architecture:
  name: hybrid
  params:
    sgr_grounding: true
    initial_candidates: 5
    temperature: 0.7
    top_p: 0.9
    parallelism: parallel
    max_workers: 5
    generation_timeout: 30
    expansion_enabled: true
    expansion_seeds: 2
    expansion_per_seed: 2
    expansion_sim_threshold: 0.85
    expansion_timeout: 15
    aggregation_mode: hybrid
    execution_timeout: 30
```

| Parameter | Default | Description / Описание |
|-----------|---------|------------------------|
| `sgr_grounding` | true | Enable SGR schema analysis / Включить SGR-анализ |
| `initial_candidates` | 5 | K parallel generations / Число начальных генераций |
| `expansion_enabled` | true | Enable seed expansion / Включить расширение |
| `expansion_seeds` | 2 | Seeds for expansion / Число зёрен |
| `expansion_per_seed` | 2 | Variations per seed / Вариаций на зерно |
| `expansion_sim_threshold` | 0.85 | Jaccard dedup threshold / Порог дедупликации |
| `aggregation_mode` | hybrid | 3-level fallback aggregation / 3-уровневая агрегация |

## Max Candidates / Макс. кандидатов

5 initial + 2 retry + 4 expansion = **11 candidates** per task.

## CLI

```bash
uv run python -m evalsuite run --model openrouter --bench bird_sqlite --architecture hybrid
```

## Results / Результаты

| Benchmark | Accuracy | Failures | Avg Latency |
|-----------|----------|----------|-------------|
| BIRD | **32.3%** (495/1534) | **16** | 16.2s |
| Spider2 | **71.5%** (88/123) | **12** | 30.9s |
| TPC-DS | 9.1% (9/99) | 22 | 36.8s |
| **Total** | **33.7%** (592/1756) | | |

Key improvement: `pred_exec_fail` = **0** on BIRD and Spider2 (vs 426 and 90 in plain). Diagnostic retry + schema validation eliminate execution failures. Smart early stop reduces timeouts (12 vs 124 in self-consistency on BIRD).

Ключевое улучшение: `pred_exec_fail` = **0** на BIRD и Spider2. Диагностический повтор + валидация схемы устраняют ошибки исполнения. Умная ранняя остановка сокращает таймауты.
