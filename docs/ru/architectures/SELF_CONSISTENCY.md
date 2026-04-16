# Self-Consistency (самосогласованность)

## Обзор

K независимых генераций SQL на задачу, затем голосование большинством по сигнатуре результата. Идея: если несколько независимых генераций дают одинаковый результат — он, вероятно, правильный.

## Алгоритм

1. Сгенерировать K независимых SQL-кандидатов (каждый со своим seed)
2. Выполнить каждого кандидата на базе данных
3. Вычислить сигнатуру результата (SHA-256 от нормализованных строк)
4. Сгруппировать кандидатов по сигнатуре результата
5. Выбрать группу с наибольшим числом голосов
6. При равенстве — самое быстрое время исполнения

## Параметры

```yaml
architecture:
  name: self_consistency
  params:
    num_samples: 5
    temperature: 0.7
    top_p: 0.9
    seed_strategy: per_attempt
    base_seed: 42
    parallelism: parallel
    max_workers: 5
    aggregation_mode: hybrid
```

| Параметр | По умолчанию | Описание |
|-----------|---------|-------------|
| `num_samples` | 5 | Число независимых генераций |
| `temperature` | 0.7 | Выше = больше разнообразия кандидатов |
| `aggregation_mode` | hybrid | Как выбирать победителя |
| `parallelism` | sequential | parallel быстрее, но использует больше ресурсов |

## Режимы агрегации

- **majority_result** — группировка по result_signature, выбор крупнейшей группы
- **best_score** — выбор кандидата с максимальной оценкой исполнения
- **hybrid** — сначала majority_result, при неуспехе fallback на best_score

## CLI

```bash
uv run python -m evalsuite run --model qwen3-coder-next --bench bird_sqlite --architecture self_consistency
```

## Результаты

Для qwen3-coder-next, `context_mode=toolchain`:

| Бенчмарк | Accuracy | Failures | Ср. латентность |
|----------|----------|----------|-----------------|
| BIRD | 30.8% (472/1534) | 142 | 12.3s |
| Spider2 | 65.0% (80/123) | 19 | 12.9s |
| TPC-DS | **10.1%** (10/99) | 20 | 25.3s |

Прирост относительно plain: BIRD −0.6pp, Spider2 +17.0pp, TPC-DS +2.0pp. SC лидирует на TPC-DS. Основной недостаток: таймауты при K генерациях (124 на BIRD).
