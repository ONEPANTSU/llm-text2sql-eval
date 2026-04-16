# Hybrid (гибридная архитектура)

## Обзор

Лучшая архитектура по результатам. Комбинирует самосогласованность с SGR-обоснованием, расширением зёрен, диагностическим повтором, умной ранней остановкой и валидацией схемы.

## Алгоритм

### Фаза 1: SGR Grounding (опционально)

LLM анализирует вопрос и определяет релевантные таблицы/колонки из схемы. При ошибке — graceful fallback на полную схему.

### Фаза 2: Начальная генерация (K=5)

Параллельная генерация 5 независимых SQL-кандидатов. Каждый кандидат проходит:

1. **Strip SQL fences** — удаление markdown-обёрток ` ```sql ``` `
2. **Placeholder check** — отклонение при наличии `<...>`, `YourState`, `TODO`, `???`
3. **Schema validation (fuzzy fix)** — коррекция имён таблиц/колонок по edit distance ≤ 2
4. **Preflight + Execution** — валидация и исполнение
5. **Result signature** — SHA-256 хеш для голосования большинством

### Фаза 2.5: Diagnostic Retry (условно)

Срабатывает только если **все** K кандидатов упали с **одинаковой** ошибкой. Генерирует 2 повторных кандидата с низкой temperature (0.3), включая текст ошибки в промпт.

### Фаза 3: Seed Expansion (опционально)

**Smart Early Stop** — пропуск расширения если:
- Нет кандидата с exec_ok (нечего расширять)
- Все exec_ok кандидаты имеют одинаковую result_signature (достигнут консенсус)

**Expansion:**
1. Выбрать 2 лучших зерна (жадный отбор разнообразия, Jaccard < 0.85)
2. Сгенерировать 2 вариации на каждое зерно ("Provide a different but equivalent SQL")
3. Отфильтровать по похожести (отклонить если Jaccard ≥ 0.85)
4. Исполнить принятые вариации

### Фаза 4: Aggregation

3-уровневый fallback:
1. **exec_ok кандидаты** — группировка по result_signature, голосование большинством
2. **только preflight_ok** — группировка по нормализованному SQL, голосование большинством
3. **fallback** — вернуть пустой SQL (нет валидного)

## Параметры

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

| Параметр | По умолчанию | Описание |
|-----------|---------|-------------|
| `sgr_grounding` | true | Включить SGR-анализ схемы |
| `initial_candidates` | 5 | Число начальных параллельных генераций |
| `expansion_enabled` | true | Включить расширение зёрен |
| `expansion_seeds` | 2 | Число зёрен для расширения |
| `expansion_per_seed` | 2 | Вариаций на зерно |
| `expansion_sim_threshold` | 0.85 | Порог дедупликации по Jaccard |
| `aggregation_mode` | hybrid | 3-уровневая агрегация |

## Максимум кандидатов

5 начальных + 2 retry + 4 expansion = **11 кандидатов** на задачу.

На практике (BIRD, n=1534): среднее = 5.3 кандидата, expansion пропускается на 85.5% задач за счёт smart early stop.

## CLI

```bash
uv run python -m evalsuite run --model qwen3-coder-next --bench bird_sqlite --architecture hybrid
```

## Результаты

Для qwen3-coder-next, `context_mode=toolchain`:

| Бенчмарк | Accuracy | Failures | Ср. латентность |
|----------|----------|----------|-----------------|
| BIRD | **32.3%** (495/1534) | **16** | 16.2s |
| Spider2 | **71.5%** (88/123) | **12** | 30.9s |
| TPC-DS | 9.1% (9/99) | 22 | 36.8s |
| **Всего** | **33.7%** (592/1756) | | |

Ключевое улучшение: `pred_exec_fail` = **0** на BIRD и Spider2 (vs 59 и 26 в plain). Diagnostic retry + schema validation устраняют ошибки исполнения. Smart early stop сокращает таймауты (12 vs 124 в self-consistency на BIRD).
