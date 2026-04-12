# Self-Consistency / Самосогласованность

## Overview / Обзор

K independent SQL generations per task, then majority vote by result signature. The idea: if multiple independent generations produce the same result, it's likely correct.

K независимых генераций SQL на задачу, затем голосование большинством по сигнатуре результата. Идея: если несколько независимых генераций дают одинаковый результат — он, вероятно, правильный.

## Algorithm / Алгоритм

1. Generate K independent SQL candidates (each with different seed)
2. Execute each candidate against the database
3. Compute result signature (SHA-256 hash of normalized rows)
4. Group candidates by result signature
5. Select the group with the most votes
6. Tie-break: fastest execution time

## Parameters / Параметры

```yaml
architecture:
  name: self_consistency
  params:
    num_samples: 5           # K candidates
    temperature: 0.7         # generation diversity
    top_p: 0.9
    seed_strategy: per_attempt  # fixed | per_attempt | random
    base_seed: 42
    parallelism: parallel    # parallel | sequential
    max_workers: 5
    aggregation_mode: hybrid # hybrid | majority_result | best_score
```

| Parameter | Default | Description / Описание |
|-----------|---------|------------------------|
| `num_samples` | 5 | Number of independent generations / Число генераций |
| `temperature` | 0.7 | Higher = more diverse candidates / Выше = больше разнообразия |
| `aggregation_mode` | hybrid | How to pick the winner / Как выбирать победителя |
| `parallelism` | sequential | parallel speeds up but uses more resources / parallel быстрее |

## Aggregation Modes / Режимы агрегации

- **majority_result** — group by result_signature, pick largest group
- **best_score** — pick candidate with highest execution score
- **hybrid** — majority_result first, fallback to best_score

## CLI

```bash
uv run python -m evalsuite run --model openrouter --bench bird_sqlite --architecture self_consistency
```

## Results / Результаты

| Benchmark | Accuracy | Failures | Avg Latency |
|-----------|----------|----------|-------------|
| BIRD | 30.8% (472/1534) | 142 | 12.3s |
| Spider2 | 65.0% (80/123) | 19 | 12.9s |
| TPC-DS | 10.1% (10/99) | 20 | 25.3s |

Значительный прирост на Spider2 (+52.8pp vs plain). Основной недостаток: таймауты при K генерациях (124 на BIRD).
