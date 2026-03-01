"""
Built-in Eval Datasets — Pre-built evaluation cases for common scenarios.

Generates eval cases from platform data (documents, lakehouse tables)
for systematic agent quality measurement.

Usage::

    from ai.eval import EvalRunner, EvalCase
    from ai.eval_datasets import qa_cases, tool_use_cases

    cases = tool_use_cases() + qa_cases(media_store)
    runner = EvalRunner(agent=agent)
    runner.run(cases)
"""

from __future__ import annotations

import logging
from typing import Optional

from ai.eval import EvalCase

logger = logging.getLogger(__name__)


def tool_use_cases() -> list[EvalCase]:
    """
    Pre-built eval cases for tool calling accuracy.

    Tests that agents correctly invoke tools for common patterns.
    """
    return [
        EvalCase(
            input="What documents do you have access to?",
            expected_tools=["list_documents"],
            tags=["tool_use", "discovery"],
        ),
        EvalCase(
            input="Search for documents about risk management.",
            expected_tools=["search_documents"],
            expected_output_contains=["risk"],
            tags=["tool_use", "search"],
        ),
        EvalCase(
            input="Find documents related to derivatives pricing using semantic search.",
            expected_tools=["semantic_search"],
            tags=["tool_use", "search"],
        ),
        EvalCase(
            input="Search for credit risk documents using the best available search method.",
            expected_tools=["hybrid_search"],
            tags=["tool_use", "search"],
        ),
    ]


def qa_cases(media_store, limit: int = 5) -> list[EvalCase]:
    """
    Generate QA eval cases from uploaded documents.

    Reads document titles and creates basic retrieval questions.

    Args:
        media_store: A MediaStore instance with uploaded documents.
        limit: Max number of cases to generate.

    Returns:
        List of EvalCase objects.
    """
    cases = []
    try:
        docs = media_store.list(limit=limit)
        for doc in docs:
            title = doc.title or doc.filename
            cases.append(EvalCase(
                input=f"What can you tell me about '{title}'?",
                expected_tools=["hybrid_search"],
                expected_output_contains=[title.split()[0]] if title else [],
                tags=["qa", "rag"],
            ))
    except Exception as e:
        logger.warning("Could not generate QA cases: %s", e)

    return cases


def sql_cases(lakehouse) -> list[EvalCase]:
    """
    Generate SQL eval cases from lakehouse tables.

    Args:
        lakehouse: A Lakehouse instance.

    Returns:
        List of EvalCase objects for SQL generation accuracy.
    """
    cases = []
    try:
        tables = lakehouse.list_tables()
        for table_info in tables[:5]:
            table_name = table_info if isinstance(table_info, str) else str(table_info)
            cases.append(EvalCase(
                input=f"How many rows are in the {table_name} table?",
                expected_tools=["query_lakehouse"],
                tags=["sql", "lakehouse"],
            ))
    except Exception as e:
        logger.warning("Could not generate SQL cases: %s", e)

    return cases
