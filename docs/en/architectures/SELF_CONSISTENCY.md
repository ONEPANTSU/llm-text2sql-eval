# Self-Consistency

## Overview

K independent SQL generations per task, then majority vote by result signature. The idea: if multiple independent generations produce the same result, it is likely correct.

## Algorithm

1. Generate K independent SQL candidates (each with a different seed)
2. Execute each candidate against the database
3. Compute result signature (SHA-256 hash of normalized rows)
4. Group candidates by result signature
5. Select the group with the most votes
6. Tie-break: fastest execution time

## Parameters

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

| Parameter | Default | Description |
|-----------|---------|-------------|
| `num_samples` | 5 | Number of independent generations |
| `temperature` | 0.7 | Higher = more diverse candidates |
| `aggregation_mode` | hybrid | How to pick the winner |
| `parallelism` | sequential | `parallel` is faster but uses more resources |

## Aggregation Modes

- **majority_result** — group by result_signature, pick largest group
- **best_score** — pick candidate with highest execution score
- **hybrid** — majority_result first, fallback to best_score

## CLI

```bash
uv run python -m evalsuite run --model qwen3-coder-next --bench bird_sqlite --architecture self_consistency
```

## Results

For qwen3-coder-next, `context_mode=toolchain`:

| Benchmark | Accuracy | Failures | Avg Latency |
|-----------|----------|----------|-------------|
| BIRD | 30.8% (472/1534) | 142 | 12.3s |
| Spider2 | 65.0% (80/123) | 19 | 12.9s |
| TPC-DS | **10.1%** (10/99) | 20 | 25.3s |

Improvement over plain: BIRD −0.6pp, Spider2 +17.0pp, TPC-DS +2.0pp. SC leads on TPC-DS. Main downside: timeouts from K generations (124 on BIRD).
