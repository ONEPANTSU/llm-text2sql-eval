# EvalSuite report for 20260309_120046_qwen3-coder-next

- Aggregate execution accuracy: 0.715

## Overall
- total: 123
- executed: 123 | skipped: 0
- gold_failed: 5 | pred_failed: 7
- ex_correct: 88 / compared: 111

## Error breakdown
- gold_exec_fail: 5
- pred_generation_fail: 5
- task_timeout: 2

## Autofix & schema (v1.2)
- schema_warn_count: 0
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 0

## Latency
- mean: 30893.1 ms
- median: 27335.5 ms
- p90: 51680.1 ms
- p99: 74972.2 ms
- tasks_with_latency: 121
- spider2: mean=30893.1 ms, median=27335.5 ms, p90=51680.1 ms

## Candidates count
- mean: 5.10
- median: 5.0

## Top error patterns
### 1. pred_generation_fail (count=5)
Error head: `'NoneType' object has no attribute 'strip'`
- **spider2_90** (spider2): 'NoneType' object has no attribute 'strip'...
- **spider2_91** (spider2): 'NoneType' object has no attribute 'strip'...
- **spider2_92** (spider2): 'NoneType' object has no attribute 'strip'...

### 2. task_timeout (count=2)
Error head: `task timeout`
- **spider2_70** (spider2): task timeout...
- **spider2_89** (spider2): task timeout...

### 3. gold_exec_fail (count=1)
Error head: `no such table: Belts`
- **spider2_10** (spider2): no such table: Belts...

### 4. gold_exec_fail (count=1)
Error head: `no such table: hardware_fact_sales_monthly`
- **spider2_34** (spider2): no such table: hardware_fact_sales_monthly...

### 5. gold_exec_fail (count=1)
Error head: `no such table: shopping_cart_page_hierarchy`
- **spider2_55** (spider2): no such table: shopping_cart_page_hierarchy...

### 6. gold_exec_fail (count=1)
Error head: `no such table: interest_map`
- **spider2_57** (spider2): no such table: interest_map...

### 7. gold_exec_fail (count=1)
Error head: `no such table: match`
- **spider2_83** (spider2): no such table: match...

## Hybrid diagnostics
- tasks_with_hybrid: 118
- variant_distribution: A=0, B=118
- avg_initial_candidates: 5.00
- avg_expansion_accepted: 0.09
- avg_aggregation_groups: 0.87
- avg_pool_size: 5.40

## spider2
- coverage: 123/123 (100.0%)
- execution_accuracy: 0.715
- ex_correct: 88
- compared: 111
- gold_failed: 5
- pred_failed: 7
- skipped: 0
- latency: mean=30893.1 ms, median=27335.5 ms, p90=51680.1 ms
