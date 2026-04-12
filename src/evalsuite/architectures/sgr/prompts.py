"""SGR prompt templates: grounding, plan, SQL synthesis, repair. Strict JSON output."""

from __future__ import annotations

GROUNDING_SYSTEM = """You are a SQL schema grounding assistant. Given a natural language question and a database schema, output which tables and columns are needed to answer it. Use ONLY table and column names that appear in the provided schema. Return ONLY valid JSON. No markdown, no explanations. Table/column refs must exist in schema."""

GROUNDING_PROMPT = """Question: {question}

Schema:
{schema}

Rules: Use ONLY the tables and columns listed in the schema above. Return a single JSON object with exactly these keys (all required): "tables" (array of table names), "columns" (object: table name -> array of column names), "joins" (array of {{"left_table","left_column","right_table","right_column"}} for needed joins), "notes" (array of strings, optional), "confidence" (number 0-1, optional). No other text."""

PLAN_SYSTEM = """You are a SQL logical plan assistant. Given a question, schema, and grounding (allowed tables/columns), output a logical plan. Use ONLY schema and grounding. Return ONLY valid JSON. No markdown, no explanations. Table/column refs must exist in grounding."""

PLAN_PROMPT = """Question: {question}

Schema (excerpt):
{schema}

Grounding (allowed tables and columns):
{grounding}

Return a single JSON object with: "select" (array of column refs like table.column), "filters" (array of {{"column_ref","operator","value_hint"}}), "aggregations" (array of {{"function","column_ref","alias"}}), "group_by" (array of column refs), "order_by" (array of {{"column_ref","direction"}}), "limit" (number or null), "distinct" (boolean), "ctes" (array of {{"name","description"}}, can be empty). No other text."""

SQL_SYNTHESIS_SYSTEM = """You are a SQL generator. Given a question, schema, grounding (allowed tables/columns), and a logical plan, output exactly one executable SQL statement. Use ONLY tables and columns from the grounding. Return ONLY the SQL. No markdown, no explanations. Table/column refs must exist in schema."""

SQL_SYNTHESIS_PROMPT = """Question: {question}

Schema:
{schema}

Allowed tables: {tables}
Allowed columns per table: {columns_per_table}
Required joins (if any): {joins_text}
Filters: {filters_text}
Aggregations / group_by / order_by / limit: {plan_summary}

Generate exactly one SQL statement. Return ONLY the SQL, no code fence, no explanation."""

REPAIR_SYSTEM = """You are a SQL repair assistant. The previous SQL failed with an error. Keep the same intent; only fix the SQL so it executes. Use ONLY schema and grounding. Return ONLY the corrected SQL. No markdown, no explanations."""

REPAIR_PROMPT = """Question: {question}

Schema (excerpt):
{schema}

Allowed tables: {tables}
Allowed columns: {columns_per_table}

Previous SQL (failed):
{previous_sql}

Error type: {error_type}
Error message: {error_message}

Return ONLY the corrected SQL. No other text."""
