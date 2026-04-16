# SQL Factory

## Overview

Multi-round generation with quality gates and similarity-based expansion. Iteratively builds a pool of SQL candidates through exploitation (refining good candidates) and exploration (generating diverse variants).

## Algorithm

1. **Warmup** (1 round) — generate initial batch, execute, build pool
2. **Main Loop** (up to 3 rounds):
   - **Exploitation** — generate `gen_batch` candidates guided by best in pool
   - **Exploration** — generate `exp_batch` variations, filter by similarity
   - **Score** each candidate: exec_ok + table coverage bonus − similarity penalty
   - **Pool update** — keep top `target_pool_size` candidates
3. **Stop conditions:**
   - Saturation (no new adds for N rounds)
   - Time budget exceeded
   - Best score stagnation
4. **Select** best from final pool

## Scoring

- Base: 1.0 if exec_ok, else 0.0
- Table coverage bonus: +0.05 per unique table (cap 6)
- Similarity penalty: if Jaccard > 0.85 to pool members
- Complexity bonus: +0.05 to +0.2 for complex queries

## Parameters

```yaml
architecture:
  name: sql_factory
  params:
    max_rounds: 3
    warmup_rounds: 1
    gen_batch: 2
    exp_batch: 2
    target_pool_size: 5
    sim_threshold: 0.85
    time_budget_per_task_sec: 40
    parallelism: parallel
    max_workers: 5
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_rounds` | 3 | Maximum generation rounds |
| `gen_batch` | 2 | Exploitation candidates per round |
| `exp_batch` | 2 | Exploration candidates per round |
| `target_pool_size` | 5 | Pool capacity |
| `sim_threshold` | 0.85 | Similarity dedup threshold |
| `time_budget_per_task_sec` | 40 | Time limit per task |

## Similarity Metrics

- **Token Jaccard** — `|A ∩ B| / |A ∪ B|` on SQL token sets
- **AST similarity** — structural comparison via parse tree
- Combined with configurable weights (default: 0.6 tok + 0.3 ast + 0.1 emb)

## CLI

```bash
uv run python -m evalsuite run --model qwen3-coder-next --bench bird_sqlite --architecture sql_factory
```

## Results

For qwen3-coder-next, `context_mode=toolchain`:

| Benchmark | Accuracy | Failures | Avg Latency |
|-----------|----------|----------|-------------|
| BIRD | 29.8% (457/1534) | 101 | 14.7s |
| Spider2 | 57.7% (71/123) | 27 | 30.1s |
| TPC-DS | 8.1% (8/99) | 25 | 39.5s |

Higher latency than plain due to multi-round generation, but lower accuracy than self-consistency and SGR on Spider2.
