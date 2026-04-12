# Placeholder / Skip Logic

## TPC-DS

TPC-DS data is generated via DuckDB's built-in `tpcds` extension (`CALL dsdgen()`), so no external toolkit is needed.

NL task descriptions for the 99 TPC-DS queries are stored in `data/tpcds/tasks.jsonl`.

## Data Validation

The test `tests/test_placeholders.py::test_no_placeholders` scans prepared data directories (`data/bird/`, `data/spider2/`, `data/tpcds/`) and fails if any JSONL entry has:

- `question` and `prompt` both null
- `sql` == "" (empty string)
- `placeholder_tpcds` marker in the record

## Skip Reasons

Tasks may be skipped at runtime with `skip_reason`:
- `gold_exec_fail` — gold SQL fails to execute (broken test fixture)
- `task_timeout` — task exceeded the configured timeout
- `placeholder_tpcds` — legacy marker (should no longer appear)
