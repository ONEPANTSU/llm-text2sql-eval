# Hybrid

## Overview

The best-performing architecture. Combines self-consistency with SGR grounding, seed expansion, diagnostic retry, smart early stop, and schema validation.

## Algorithm

### Phase 1: SGR Grounding (optional)

LLM analyzes the question and identifies relevant tables/columns from schema. On error — graceful fallback to full schema.

### Phase 2: Initial Generation (K=5)

Generate 5 independent SQL candidates in parallel. Each candidate goes through:

1. **Strip SQL fences** — remove markdown ` ```sql ``` ` wrappers
2. **Placeholder check** — reject if contains `<...>`, `YourState`, `TODO`, `???`
3. **Schema validation (fuzzy fix)** — edit distance ≤ 2 correction of table/column names
4. **Preflight + Execution** — validate and execute
5. **Result signature** — SHA-256 hash for majority voting

### Phase 2.5: Diagnostic Retry (conditional)

Triggered only if **all** K candidates failed with the **same error**. Generates 2 retry candidates with low temperature (0.3), including the error message in the prompt.

### Phase 3: Seed Expansion (optional)

**Smart Early Stop** — skip expansion if:
- No candidate has exec_ok (nothing to expand)
- All exec_ok candidates have same result_signature (consensus reached)

**Expansion:**
1. Pick top 2 seeds (greedy diverse selection, Jaccard < 0.85)
2. Generate 2 variations per seed ("Provide a different but equivalent SQL")
3. Filter by similarity (reject if Jaccard ≥ 0.85)
4. Execute accepted variations

### Phase 4: Aggregation

3-level fallback:
1. **exec_ok candidates** — group by result_signature, majority vote
2. **preflight_ok only** — group by normalized SQL, majority vote
3. **fallback** — return empty (no valid SQL)

## Parameters

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

| Parameter | Default | Description |
|-----------|---------|-------------|
| `sgr_grounding` | true | Enable SGR schema analysis |
| `initial_candidates` | 5 | K parallel generations |
| `expansion_enabled` | true | Enable seed expansion |
| `expansion_seeds` | 2 | Seeds for expansion |
| `expansion_per_seed` | 2 | Variations per seed |
| `expansion_sim_threshold` | 0.85 | Jaccard dedup threshold |
| `aggregation_mode` | hybrid | 3-level fallback aggregation |

## Max Candidates

5 initial + 2 retry + 4 expansion = **11 candidates** per task.

In practice (BIRD, n=1534): average = 5.3 candidates, expansion is skipped on 85.5% of tasks due to smart early stop.

## CLI

```bash
uv run python -m evalsuite run --model qwen3-coder-next --bench bird_sqlite --architecture hybrid
```

## Results

For qwen3-coder-next, `context_mode=toolchain`:

| Benchmark | Accuracy | Failures | Avg Latency |
|-----------|----------|----------|-------------|
| BIRD | **32.3%** (495/1534) | **16** | 16.2s |
| Spider2 | **71.5%** (88/123) | **12** | 30.9s |
| TPC-DS | 9.1% (9/99) | 22 | 36.8s |
| **Total** | **33.7%** (592/1756) | | |

Key improvement: `pred_exec_fail` = **0** on BIRD and Spider2 (vs 59 and 26 in plain). Diagnostic retry + schema validation eliminate execution failures. Smart early stop reduces timeouts (12 vs 124 in self-consistency on BIRD).
