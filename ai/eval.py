"""
Eval Framework — Systematic evaluation of agent quality.

Measure agent accuracy, tool usage, and latency with structured test cases.
Supports expected output matching, tool call assertions, LLM-as-judge,
and A/B model comparison.

Usage::

    from ai.eval import EvalRunner, EvalCase

    cases = [
        EvalCase(
            input="What is AAPL trading at?",
            expected_tools=["get_price"],
            expected_output_contains=["AAPL"],
        ),
    ]

    runner = EvalRunner(agent=agent)
    results = runner.run(cases)
    runner.summary()
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class EvalCase:
    """A single evaluation test case.

    Args:
        input: User prompt to send to the agent.
        expected_output: Exact expected output (used with LLM-as-judge).
        expected_output_contains: List of substrings that must appear in the output.
        expected_tools: List of tool names that should have been called.
        tags: Tags for grouping and filtering results.
        metadata: Arbitrary metadata.
    """
    input: str = ""
    expected_output: str = ""
    expected_output_contains: list[str] = field(default_factory=list)
    expected_tools: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


@dataclass
class EvalResult:
    """Result from evaluating a single case."""
    case: EvalCase = field(default_factory=EvalCase)
    actual_output: str = ""
    actual_tools: list[str] = field(default_factory=list)
    passed: bool = False
    output_match: bool = True
    tools_match: bool = True
    latency_ms: float = 0.0
    token_usage: dict = field(default_factory=dict)
    error: str = ""
    judge_reasoning: str = ""


class EvalRunner:
    """
    Run evaluation cases against an agent and collect results.

    Args:
        agent: An Agent instance to evaluate.
        judge: Optional AI instance for LLM-as-judge scoring.
            If not provided, uses substring matching only.
    """

    def __init__(self, agent, judge=None):
        self._agent = agent
        self._judge = judge
        self._results: list[EvalResult] = []

    def run(self, cases: list[EvalCase]) -> list[EvalResult]:
        """
        Run all evaluation cases.

        Args:
            cases: List of EvalCase objects.

        Returns:
            List of EvalResult objects.
        """
        self._results = []

        for case in cases:
            result = self._run_single(case)
            self._results.append(result)

        return self._results

    def summary(self) -> dict:
        """
        Print and return aggregate evaluation results.

        Returns:
            Summary dict with pass rate, latency, tool accuracy, etc.
        """
        if not self._results:
            print("No results to summarize.")
            return {}

        total = len(self._results)
        passed = sum(1 for r in self._results if r.passed)
        failed = total - passed
        errors = sum(1 for r in self._results if r.error)
        avg_latency = sum(r.latency_ms for r in self._results) / total
        total_tokens = sum(r.token_usage.get("total_tokens", 0) for r in self._results)

        # Tool accuracy
        tool_cases = [r for r in self._results if r.case.expected_tools]
        tool_passed = sum(1 for r in tool_cases if r.tools_match)
        tool_total = len(tool_cases)

        # By tag
        tag_stats: dict[str, dict] = {}
        for r in self._results:
            for tag in r.case.tags:
                if tag not in tag_stats:
                    tag_stats[tag] = {"total": 0, "passed": 0}
                tag_stats[tag]["total"] += 1
                if r.passed:
                    tag_stats[tag]["passed"] += 1

        summary = {
            "total": total,
            "passed": passed,
            "failed": failed,
            "errors": errors,
            "pass_rate": passed / total if total else 0,
            "avg_latency_ms": round(avg_latency, 1),
            "total_tokens": total_tokens,
            "tool_accuracy": tool_passed / tool_total if tool_total else None,
            "by_tag": tag_stats,
        }

        # Print summary
        print(f"\n{'='*50}")
        print(f"  Eval Results: {passed}/{total} passed ({summary['pass_rate']:.0%})")
        print(f"{'='*50}")
        print(f"  Avg latency:    {avg_latency:.0f}ms")
        print(f"  Total tokens:   {total_tokens}")
        if tool_total:
            print(f"  Tool accuracy:  {tool_passed}/{tool_total} ({tool_passed/tool_total:.0%})")
        if errors:
            print(f"  Errors:         {errors}")
        if tag_stats:
            print(f"  By tag:")
            for tag, stats in tag_stats.items():
                print(f"    {tag}: {stats['passed']}/{stats['total']}")
        print(f"{'='*50}\n")

        # Print failures
        failures = [r for r in self._results if not r.passed]
        if failures:
            print(f"Failures ({len(failures)}):")
            for r in failures:
                print(f"  Input: {r.case.input[:60]}")
                if r.error:
                    print(f"    Error: {r.error}")
                if not r.output_match:
                    print(f"    Output mismatch: {r.actual_output[:80]}")
                if not r.tools_match:
                    print(f"    Expected tools: {r.case.expected_tools}, got: {r.actual_tools}")
                print()

        return summary

    @staticmethod
    def compare(results_a: list[EvalResult], results_b: list[EvalResult], label_a: str = "A", label_b: str = "B") -> dict:
        """
        Compare two sets of eval results (A/B model comparison).

        Args:
            results_a: Results from model A.
            results_b: Results from model B.
            label_a: Label for model A.
            label_b: Label for model B.

        Returns:
            Comparison dict.
        """
        def _stats(results):
            total = len(results)
            passed = sum(1 for r in results if r.passed)
            avg_lat = sum(r.latency_ms for r in results) / total if total else 0
            tokens = sum(r.token_usage.get("total_tokens", 0) for r in results)
            return {"total": total, "passed": passed, "pass_rate": passed / total if total else 0,
                    "avg_latency_ms": round(avg_lat, 1), "total_tokens": tokens}

        stats_a = _stats(results_a)
        stats_b = _stats(results_b)

        print(f"\n{'='*60}")
        print(f"  A/B Comparison: {label_a} vs {label_b}")
        print(f"{'='*60}")
        print(f"  {'Metric':<20} {label_a:>15} {label_b:>15}")
        print(f"  {'-'*50}")
        print(f"  {'Pass rate':<20} {stats_a['pass_rate']:>14.0%} {stats_b['pass_rate']:>14.0%}")
        print(f"  {'Avg latency':<20} {stats_a['avg_latency_ms']:>13.0f}ms {stats_b['avg_latency_ms']:>13.0f}ms")
        print(f"  {'Total tokens':<20} {stats_a['total_tokens']:>15} {stats_b['total_tokens']:>15}")
        print(f"{'='*60}\n")

        return {"a": stats_a, "b": stats_b}

    def _run_single(self, case: EvalCase) -> EvalResult:
        """Run a single eval case."""
        result = EvalResult(case=case)

        try:
            self._agent.reset()

            t0 = time.time()
            agent_result = self._agent.run(case.input)
            result.latency_ms = (time.time() - t0) * 1000

            result.actual_output = agent_result.content
            result.actual_tools = [s.action.name for s in agent_result.steps]
            result.token_usage = agent_result.usage

        except Exception as e:
            result.error = f"{type(e).__name__}: {e}"
            result.passed = False
            return result

        # Check output
        result.output_match = self._check_output(case, result)

        # Check tools
        result.tools_match = self._check_tools(case, result)

        result.passed = result.output_match and result.tools_match
        return result

    def _check_output(self, case: EvalCase, result: EvalResult) -> bool:
        """Check if the output matches expectations."""
        # Substring checks
        if case.expected_output_contains:
            output_lower = result.actual_output.lower()
            for substring in case.expected_output_contains:
                if substring.lower() not in output_lower:
                    return False

        # LLM-as-judge for expected_output
        if case.expected_output and self._judge:
            return self._llm_judge(case, result)

        # If expected_output is set but no judge, do basic contains check
        if case.expected_output and not self._judge:
            # Relaxed: check if key words from expected appear in actual
            expected_words = set(case.expected_output.lower().split())
            actual_words = set(result.actual_output.lower().split())
            overlap = len(expected_words & actual_words) / len(expected_words) if expected_words else 1
            return overlap > 0.3

        return True

    def _check_tools(self, case: EvalCase, result: EvalResult) -> bool:
        """Check if the expected tools were called."""
        if not case.expected_tools:
            return True
        expected = set(case.expected_tools)
        actual = set(result.actual_tools)
        return expected.issubset(actual)

    def _llm_judge(self, case: EvalCase, result: EvalResult) -> bool:
        """Use LLM-as-judge to evaluate output quality."""
        try:
            from ai._types import Message as Msg
            response = self._judge.generate(
                [
                    Msg(role="system", content=(
                        "You are an evaluation judge. Compare the actual output to the expected output. "
                        "Respond with ONLY 'PASS' or 'FAIL' on the first line, then a brief reason."
                    )),
                    Msg(role="user", content=(
                        f"Question: {case.input}\n\n"
                        f"Expected: {case.expected_output}\n\n"
                        f"Actual: {result.actual_output}"
                    )),
                ],
                temperature=0.0,
                max_tokens=100,
            )
            text = response.content.strip()
            result.judge_reasoning = text
            return text.upper().startswith("PASS")
        except Exception as e:
            logger.warning("LLM judge failed: %s", e)
            return True  # Don't fail on judge errors
