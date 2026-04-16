# EvalSuite report for 20260308_231440_qwen3-coder-next

- Aggregate execution accuracy: 0.323

## Overall
- total: 1534
- executed: 1534 | skipped: 0
- gold_failed: 0 | pred_failed: 16
- ex_correct: 495 / compared: 1518

## Error breakdown
- task_timeout: 12
- pred_generation_fail: 4

## Autofix & schema (v1.2)
- schema_warn_count: 0
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 0

## Latency
- mean: 16163.7 ms
- median: 14541.9 ms
- p90: 24872.3 ms
- p99: 41756.2 ms
- tasks_with_latency: 1522
- bird_sqlite: mean=16163.7 ms, median=14541.9 ms, p90=24872.3 ms

## Candidates count
- mean: 5.22
- median: 5.0

## Top error patterns
### 1. task_timeout (count=12)
Error head: `task timeout`
- **bird_350** (bird_sqlite): task timeout...
- **bird_372** (bird_sqlite): task timeout...
- **bird_406** (bird_sqlite): task timeout...

### 2. pred_generation_fail (count=2)
Error head: `'NoneType' object has no attribute 'strip'`
- **bird_221** (bird_sqlite): 'NoneType' object has no attribute 'strip'...
- **bird_222** (bird_sqlite): 'NoneType' object has no attribute 'strip'...

### 3. pred_generation_fail (count=1)
Error head: `HTTPSConnectionPool(host='openrouter.ai', port=443): Read timed out. (read timeo...`
- **bird_198** (bird_sqlite): HTTPSConnectionPool(host='openrouter.ai', port=443): Read timed out. (read timeout=60)...

### 4. pred_generation_fail (count=1)
Error head: `('Connection aborted.', RemoteDisconnected('Remote end closed connection without...`
- **bird_403** (bird_sqlite): ('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))...

## Hybrid diagnostics
- tasks_with_hybrid: 1530
- variant_distribution: A=0, B=1530
- avg_initial_candidates: 5.00
- avg_expansion_accepted: 0.20
- avg_aggregation_groups: 1.17
- avg_pool_size: 5.28

## bird_sqlite
- coverage: 1534/1534 (100.0%)
- execution_accuracy: 0.323
- ex_correct: 495
- compared: 1518
- gold_failed: 0
- pred_failed: 16
- skipped: 0
- latency: mean=16163.7 ms, median=14541.9 ms, p90=24872.3 ms
