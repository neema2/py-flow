"""
Deterministic Scoring Functions
================================
Pure functions that score agent artifacts without LLM calls.
Each scorer takes (case, artifacts) → float (0.0–1.0).

These are used by EvalDimension objects in the framework.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agents._eval.framework import AgentEvalCase


def score_naming_conventions(case: AgentEvalCase, artifacts: dict) -> float:
    """Score whether created artifacts follow naming conventions.

    Checks:
    - snake_case for field names
    - PascalCase for type/class names
    - Proper prefixes (fact_, dim_ for lakehouse tables)
    """
    schema = artifacts.get("created_schema", {})
    if not schema:
        return 1.0  # Nothing to check

    scores = []

    # Check field naming (snake_case)
    fields = schema.get("fields", [])
    for f in fields:
        name = f.get("name", "") if isinstance(f, dict) else str(f)
        if name and re.match(r'^[a-z][a-z0-9_]*$', name):
            scores.append(1.0)
        elif name:
            scores.append(0.0)

    # Check type naming (PascalCase)
    type_name = schema.get("type_name", "") or schema.get("class_name", "")
    if type_name and re.match(r'^[A-Z][a-zA-Z0-9]*$', type_name):
        scores.append(1.0)
    elif type_name:
        scores.append(0.0)

    return sum(scores) / len(scores) if scores else 1.0


def score_type_appropriateness(case: AgentEvalCase, artifacts: dict) -> float:
    """Score whether field types are appropriate for their names.

    Heuristic: fields named *_id, *_count → int; *_price, *_amount → float;
    *_name, *_description → str; is_*, has_* → bool.
    """
    schema = artifacts.get("created_schema", {})
    fields = schema.get("fields", [])
    if not fields:
        return 1.0

    scores = []
    for f in fields:
        if not isinstance(f, dict):
            continue
        name = f.get("name", "").lower()
        ftype = f.get("type", "").lower()

        # Check type heuristics
        if any(name.endswith(s) for s in ("_id", "_count", "_quantity", "_qty")):
            scores.append(1.0 if ftype in ("int", "integer") else 0.5)
        elif any(name.endswith(s) for s in ("_price", "_amount", "_rate", "_pct", "_ratio")):
            scores.append(1.0 if ftype in ("float", "double", "number") else 0.5)
        elif any(name.startswith(s) for s in ("is_", "has_", "can_", "should_")):
            scores.append(1.0 if ftype in ("bool", "boolean") else 0.5)
        elif any(name.endswith(s) for s in ("_name", "_description", "_label", "_code")):
            scores.append(1.0 if ftype in ("str", "string") else 0.5)
        else:
            scores.append(0.8)  # Neutral — can't judge

    return sum(scores) / len(scores) if scores else 1.0


def score_schema_completeness(case: AgentEvalCase, artifacts: dict) -> float:
    """Score whether the schema has a reasonable number of fields.

    A schema with 0 fields is 0.0. Schemas with 2+ fields get at least 0.5.
    Having common fields (id, timestamp, name) boosts score.
    """
    schema = artifacts.get("created_schema", {})
    fields = schema.get("fields", [])
    n = len(fields)

    if n == 0:
        return 0.0
    if n == 1:
        return 0.3
    if n >= 2:
        base = 0.5
    if n >= 3:
        base = 0.7
    if n >= 4:
        base = 0.85
    if n >= 5:
        base = 1.0

    return min(base, 1.0)


def score_row_count_preservation(case: AgentEvalCase, artifacts: dict) -> float:
    """Score whether ingestion preserved all rows.

    Compares rows_written to expected row count (if specified in case).
    """
    expected = case.expected_result
    if expected is None:
        return 1.0

    if isinstance(expected, dict) and "row_count" in expected:
        expected_count = expected["row_count"]
    elif isinstance(expected, int):
        expected_count = expected
    else:
        return 1.0

    actual_count = artifacts.get("rows_written", 0)
    if expected_count == 0:
        return 1.0 if actual_count == 0 else 0.0
    return float(min(actual_count / expected_count, 1.0))


def score_star_schema_design(case: AgentEvalCase, artifacts: dict) -> float:
    """Score the quality of a star schema design.

    Checks:
    - Has at least one fact table
    - Has at least one dimension table
    - Fact tables have measures (numeric columns)
    - Dimension tables have descriptive attributes
    - Relationships link facts to dimensions
    """
    design = artifacts.get("star_schema_design")
    if design is None:
        return 1.0  # Not applicable

    scores = []

    fact_tables = design.get("fact_tables", [])
    dim_tables = design.get("dimension_tables", [])
    relationships = design.get("relationships", [])

    # Has fact tables?
    scores.append(1.0 if fact_tables else 0.0)

    # Has dimension tables?
    scores.append(1.0 if dim_tables else 0.0)

    # Fact tables have measures?
    for ft in fact_tables:
        columns = ft.get("columns", [])
        has_measure = any(
            c.get("role") == "measure" or c.get("type") in ("int", "float")
            for c in columns
        )
        scores.append(1.0 if has_measure else 0.3)

    # Dimension tables have attributes?
    for dt in dim_tables:
        columns = dt.get("columns", [])
        has_attr = any(
            c.get("role") == "attribute" or c.get("type") == "str"
            for c in columns
        )
        scores.append(1.0 if has_attr else 0.3)

    # Relationships exist?
    scores.append(1.0 if relationships else 0.0)

    # Naming conventions (fact_, dim_)
    for ft in fact_tables:
        name = ft.get("name", "")
        scores.append(1.0 if name.startswith("fact_") else 0.5)
    for dt in dim_tables:
        name = dt.get("name", "")
        scores.append(1.0 if name.startswith("dim_") else 0.5)

    return sum(scores) / len(scores) if scores else 0.5


def score_sql_validity(case: AgentEvalCase, artifacts: dict) -> float:
    """Basic SQL validity check — does the SQL parse without obvious errors?

    Not a full parser — just catches common issues.
    """
    sql = artifacts.get("generated_sql", "") or artifacts.get("compiled_sql", "")
    if not sql:
        return 1.0  # Not applicable

    sql_upper = sql.upper().strip()

    # Must start with SELECT, INSERT, CREATE, or WITH
    if not any(sql_upper.startswith(kw) for kw in ("SELECT", "INSERT", "CREATE", "WITH")):
        return 0.3

    # Balanced parentheses
    if sql.count("(") != sql.count(")"):
        return 0.5

    # Has FROM clause (for SELECT)
    if sql_upper.startswith("SELECT") and "FROM" not in sql_upper:
        return 0.5

    return 1.0
