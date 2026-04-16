# EvalSuite report for 20260215_193733_openrouter:qwen_qwen3-coder-next

- Aggregate execution accuracy: 0.298

## Overall
- total: 1534
- executed: 1534 | skipped: 0
- gold_failed: 0 | pred_failed: 101
- ex_correct: 457 / compared: 1433

## Error breakdown
- pred_exec_fail: 61
- task_timeout: 34
- pred_generation_fail: 6

## Autofix & schema (v1.2)
- schema_warn_count: 0
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 0

## Top error patterns
### 1. task_timeout (count=34)
Error head: `task timeout`
- **bird_16** (bird_sqlite): task timeout...
- **bird_32** (bird_sqlite): task timeout...
- **bird_79** (bird_sqlite): task timeout...

### 2. pred_exec_fail (count=5)
Error head: `no such column: l.T`
- **bird_1158** (bird_sqlite): no such column: l.T...
- **bird_1172** (bird_sqlite): no such column: l.T...
- **bird_1192** (bird_sqlite): no such column: l.T...

### 3. pred_generation_fail (count=4)
Error head: `openai api error 500: {"error":{"message":"Internal Server Error","code":500}}`
- **bird_174** (bird_sqlite): openai api error 500: {"error":{"message":"Internal Server Error","code":500}}...
- **bird_205** (bird_sqlite): openai api error 500: {"error":{"message":"Internal Server Error","code":500}}...
- **bird_300** (bird_sqlite): openai api error 500: {"error":{"message":"Internal Server Error","code":500}}...

### 4. pred_exec_fail (count=4)
Error head: `near "set": syntax error`
- **bird_446** (bird_sqlite): near "set": syntax error...
- **bird_486** (bird_sqlite): near "set": syntax error...
- **bird_487** (bird_sqlite): near "set": syntax error...

### 5. pred_exec_fail (count=4)
Error head: `near "Date": syntax error`
- **bird_1162** (bird_sqlite): near "Date": syntax error...
- **bird_1179** (bird_sqlite): near "Date": syntax error...
- **bird_1183** (bird_sqlite): near "Date": syntax error...

### 6. pred_exec_fail (count=3)
Error head: `You can only execute one statement at a time.`
- **bird_173** (bird_sqlite): You can only execute one statement at a time....
- **bird_436** (bird_sqlite): You can only execute one statement at a time....
- **bird_883** (bird_sqlite): You can only execute one statement at a time....

### 7. pred_exec_fail (count=3)
Error head: `no such column: T`
- **bird_1177** (bird_sqlite): no such column: T...
- **bird_1227** (bird_sqlite): no such column: T...
- **bird_1297** (bird_sqlite): no such column: T...

### 8. pred_exec_fail (count=2)
Error head: `near "Grade": syntax error`
- **bird_26** (bird_sqlite): near "Grade": syntax error...
- **bird_83** (bird_sqlite): near "Grade": syntax error...

### 9. pred_generation_fail (count=2)
Error head: `openai api error 401: {"error":{"message":"User not found.","code":401}}`
- **bird_65** (bird_sqlite): openai api error 401: {"error":{"message":"User not found.","code":401}}...
- **bird_72** (bird_sqlite): openai api error 401: {"error":{"message":"User not found.","code":401}}...

### 10. pred_exec_fail (count=2)
Error head: `no such column: LowGrade`
- **bird_74** (bird_sqlite): no such column: LowGrade...
- **bird_78** (bird_sqlite): no such column: LowGrade...

## SQL-Factory diagnostics
- tasks_with_sql_factory: 1494
- avg_rounds: 2.937
- avg_pool_size: 3.019
- reject_exec_fail_rate: 0.080
- reject_similarity_rate: 0.383
- exploration_vs_exploitation_ratio: 0.716

## bird_sqlite
- coverage: 1534/1534 (100.0%)
- execution_accuracy: 0.298
- ex_correct: 457
- compared: 1433
- gold_failed: 0
- pred_failed: 101
- skipped: 0
