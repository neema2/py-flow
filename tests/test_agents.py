"""
Tests for Phase 3 — Agents & Evaluation.

Tests the @tool decorator, Agent class, AgentMemory, AgentTeam,
EvalRunner, and eval datasets. Integration tests require GEMINI_API_KEY.
"""

import json
import os
import tempfile

import pytest
from ai._tools import ToolRegistry, _parse_param_docs, _schema_from_function, tool
from ai._types import Message, Tool
from ai.agent import Agent, AgentResult
from ai.eval import EvalCase, EvalRunner

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
requires_gemini = pytest.mark.skipif(not GEMINI_API_KEY, reason="GEMINI_API_KEY not set")


# ── @tool decorator ──────────────────────────────────────────────────────


class TestToolDecorator:

    def test_basic_decorator(self):
        """@tool creates a Tool from a typed function."""
        @tool
        def get_price(symbol: str) -> str:
            """Get the current price of a stock."""
            return json.dumps({"price": 150.0})

        assert hasattr(get_price, '_tool')
        t = get_price._tool
        assert t.name == "get_price"
        assert t.description == "Get the current price of a stock."
        assert t.parameters["properties"]["symbol"]["type"] == "string"
        assert t.parameters["required"] == ["symbol"]

    def test_decorator_preserves_callable(self):
        """Decorated function is still callable."""
        @tool
        def add(a: int, b: int) -> str:
            """Add two numbers."""
            return json.dumps({"result": a + b})

        result = json.loads(add(3, 4))
        assert result["result"] == 7

    def test_optional_params(self):
        """Parameters with defaults are not required."""
        @tool
        def search(query: str, limit: int = 10) -> str:
            """Search for items."""
            return "[]"

        t = search._tool  # type: ignore[attr-defined]
        assert "query" in t.parameters["required"]
        assert "limit" not in t.parameters.get("required", [])

    def test_type_mapping(self):
        """Python types are mapped to JSON Schema types."""
        @tool
        def multi(name: str, count: int, price: float, active: bool) -> str:
            """Multi-type function."""
            return "{}"

        props = multi._tool.parameters["properties"]  # type: ignore[attr-defined]
        assert props["name"]["type"] == "string"
        assert props["count"]["type"] == "integer"
        assert props["price"]["type"] == "number"
        assert props["active"]["type"] == "boolean"

    def test_list_type(self):
        """list[str] maps to array of strings."""
        @tool
        def tags(items: list[str]) -> str:
            """Accept a list."""
            return "[]"

        prop = tags._tool.parameters["properties"]["items"]  # type: ignore[attr-defined]
        assert prop["type"] == "array"
        assert prop["items"]["type"] == "string"

    def test_docstring_param_descriptions(self):
        """Parameter descriptions are extracted from docstring."""
        @tool
        def fetch(symbol: str, exchange: str = "NYSE") -> str:
            """Fetch market data.

            Args:
                symbol: The ticker symbol.
                exchange: The exchange name.
            """
            return "{}"

        props = fetch._tool.parameters["properties"]  # type: ignore[attr-defined]
        assert props["symbol"]["description"] == "The ticker symbol."
        assert props["exchange"]["description"] == "The exchange name."

    def test_no_docstring(self):
        """Function without docstring uses name as description."""
        @tool
        def mystery(x: int) -> str:
            return "{}"

        assert mystery._tool.description == "mystery"  # type: ignore[attr-defined]


# ── _parse_param_docs ────────────────────────────────────────────────────


class TestParseParamDocs:

    def test_google_style(self):
        result = _parse_param_docs("Do something.\n\nArgs:\n    name: The name.\n    age: The age.")
        assert result == {"name": "The name.", "age": "The age."}

    def test_with_type_annotation(self):
        result = _parse_param_docs("Do something.\n\nArgs:\n    name (str): The name.")
        assert result == {"name": "The name."}

    def test_empty(self):
        assert _parse_param_docs("") == {}
        assert _parse_param_docs("Just a description.") == {}


# ── _schema_from_function ────────────────────────────────────────────────


class TestSchemaFromFunction:

    def test_basic_schema(self):
        def fn(name: str, age: int = 0) -> str:
            return ""
        schema = _schema_from_function(fn)
        assert schema["type"] == "object"
        assert "name" in schema["properties"]
        assert schema["required"] == ["name"]

    def test_no_params(self):
        def fn() -> str:
            return ""
        schema = _schema_from_function(fn)
        assert schema["properties"] == {}
        assert "required" not in schema


# ── ToolRegistry ─────────────────────────────────────────────────────────


class TestToolRegistry:

    def test_register_decorated(self):
        """register_decorated accepts @tool functions."""
        @tool
        def a(x: str) -> str:
            """Tool A."""
            return "{}"

        @tool
        def b(y: int) -> str:
            """Tool B."""
            return "{}"

        reg = ToolRegistry()
        reg.register_decorated(a, b)
        assert set(reg.tool_names) == {"a", "b"}

    def test_register_decorated_tool_object(self):
        """register_decorated also accepts Tool objects."""
        t = Tool(name="c", description="C", parameters={}, fn=lambda: "{}")
        reg = ToolRegistry()
        reg.register_decorated(t)
        assert "c" in reg.tool_names

    def test_register_decorated_invalid(self):
        """register_decorated raises TypeError for invalid input."""
        reg = ToolRegistry()
        with pytest.raises(TypeError):
            reg.register_decorated(42)  # type: ignore[arg-type]

    def test_from_platform_empty(self):
        """from_platform with no args returns empty registry."""
        reg = ToolRegistry.from_platform()
        assert reg.tool_names == []

    def test_execute(self):
        """Execute a registered tool."""
        @tool
        def greet(name: str) -> str:
            """Greet someone."""
            return json.dumps({"greeting": f"Hello, {name}!"})

        reg = ToolRegistry()
        reg.register_decorated(greet)
        result = json.loads(reg.execute("greet", {"name": "Alice"}))
        assert result["greeting"] == "Hello, Alice!"


# ── Agent (real Gemini) ───────────────────────────────────────────────────


@requires_gemini
class TestAgentIntegration:

    def test_simple_run(self):
        """Agent can answer a simple question without tools."""
        agent = Agent(system_prompt="Answer in one word.")
        result = agent.run("What is 2 + 2?")
        assert isinstance(result, AgentResult)
        assert "4" in result.content or "four" in result.content.lower()
        assert result.iterations >= 1

    def test_run_with_tool(self):
        """Agent uses a tool to answer a question."""
        @tool
        def get_temperature(city: str) -> str:
            """Get the current temperature in a city.

            Args:
                city: Name of the city.
            """
            return json.dumps({"city": city, "temperature": "22°C", "condition": "sunny"})

        agent = Agent(
            tools=[get_temperature],
            system_prompt="You are a weather assistant. Use the get_temperature tool to answer.",
        )
        result = agent.run("What's the temperature in Tokyo?")
        assert result.content
        assert len(result.steps) >= 1
        assert result.steps[0].action.name == "get_temperature"
        assert "22" in result.content or "sunny" in result.content.lower()

    def test_multi_turn(self):
        """Agent preserves conversation history across runs."""
        agent = Agent(system_prompt="You are a math tutor. Be concise.")
        r1 = agent.run("What is 5 * 7?")
        assert "35" in r1.content

        r2 = agent.run("And what is that divided by 5?")
        assert "7" in r2.content
        assert len(agent.history) == 4  # 2 user + 2 assistant

    def test_reset(self):
        """reset() clears conversation history."""
        agent = Agent(system_prompt="Be concise.")
        agent.run("My name is Alice.")
        assert len(agent.history) >= 2
        agent.reset()
        assert len(agent.history) == 0

    def test_run_stream(self):
        """run_stream yields text chunks."""
        agent = Agent(system_prompt="Answer in one sentence.")
        chunks = list(agent.run_stream("What is Python?"))
        assert len(chunks) > 0
        full = "".join(chunks)
        assert len(full) > 0


# ── AgentMemory (real PG) ────────────────────────────────────────────────


@requires_gemini
class TestAgentMemory:

    @pytest.fixture
    def pg_and_memory(self, store_server):
        from ai.memory import AgentMemory

        store_server.provision_user("agent_user", "agent_pw")

        from store.connection import connect
        info = store_server.conn_info()
        conn = connect(user="agent_user", host=info["host"], port=info["port"],
                       dbname=info["dbname"], password="agent_pw")

        memory = AgentMemory(store_conn=conn)
        yield memory, conn
        conn.close()

    def test_save_load_conversation(self, pg_and_memory):
        """Save and load a conversation from PG."""
        memory, _ = pg_and_memory
        cid = memory.new_id()

        messages = [
            Message(role="user", content="Hello"),
            Message(role="assistant", content="Hi there!"),
        ]
        memory.save(cid, messages, agent_name="test_agent")

        loaded = memory.load(cid)
        assert loaded is not None
        assert loaded.message_count == 2
        assert loaded.agent_name == "test_agent"

        msgs = loaded.to_messages()
        assert len(msgs) == 2
        assert msgs[0].content == "Hello"

    def test_list_conversations(self, pg_and_memory):
        """List conversations filtered by agent name."""
        memory, _ = pg_and_memory

        memory.save(memory.new_id(), [Message(role="user", content="a")], agent_name="alpha")
        memory.save(memory.new_id(), [Message(role="user", content="b")], agent_name="beta")
        memory.save(memory.new_id(), [Message(role="user", content="c")], agent_name="alpha")

        all_convos = memory.list_conversations()
        assert len(all_convos) >= 3

        alpha = memory.list_conversations(agent_name="alpha")
        assert len(alpha) == 2

    def test_delete_conversation(self, pg_and_memory):
        """Delete a conversation."""
        memory, _ = pg_and_memory
        cid = memory.new_id()
        memory.save(cid, [Message(role="user", content="delete me")], agent_name="test")
        assert memory.load(cid) is not None

        assert memory.delete(cid)
        assert memory.load(cid) is None

    def test_agent_with_memory(self, pg_and_memory):
        """Agent auto-saves conversations to memory."""
        memory, _ = pg_and_memory
        agent = Agent(
            system_prompt="Be concise. Answer in one word if possible.",
            memory=memory,
            name="math_agent",
        )
        result = agent.run("What is 3 + 3?")
        assert "6" in result.content or "six" in result.content.lower()
        assert agent.conversation_id is not None

        # Verify saved
        loaded = memory.load(agent.conversation_id)
        assert loaded is not None
        assert loaded.message_count >= 2


# ── EvalRunner (real Gemini) ─────────────────────────────────────────────


@requires_gemini
class TestEvalRunner:

    @pytest.fixture(scope="class")
    def eval_agent(self):
        @tool
        def get_price(symbol: str) -> str:
            """Get the current price of a stock.

            Args:
                symbol: Ticker symbol.
            """
            return json.dumps({"symbol": symbol, "price": 150.25})

        return Agent(
            tools=[get_price],
            system_prompt="You are a stock assistant. Use tools when asked about prices.",
        )

    def test_run_cases(self, eval_agent):
        """Run eval cases and get results."""
        runner = EvalRunner(agent=eval_agent)
        cases = [
            EvalCase(
                input="What is AAPL trading at?",
                expected_tools=["get_price"],
                expected_output_contains=["150"],
                tags=["price"],
            ),
        ]
        results = runner.run(cases)
        assert len(results) == 1
        assert results[0].passed
        assert results[0].latency_ms > 0

    def test_output_contains_fail(self, eval_agent):
        """Fails when expected substring is missing."""
        runner = EvalRunner(agent=eval_agent)
        cases = [
            EvalCase(
                input="Say hello",
                expected_output_contains=["XYZNONEXISTENT"],
            ),
        ]
        results = runner.run(cases)
        assert not results[0].passed
        assert not results[0].output_match

    def test_tool_check_fail(self, eval_agent):
        """Fails when expected tool was not called."""
        runner = EvalRunner(agent=eval_agent)
        cases = [
            EvalCase(
                input="Say hello",
                expected_tools=["search_documents"],
            ),
        ]
        results = runner.run(cases)
        assert not results[0].passed
        assert not results[0].tools_match

    def test_summary(self, eval_agent, capsys):
        """Summary prints aggregate stats."""
        runner = EvalRunner(agent=eval_agent)
        cases = [
            EvalCase(input="What is AAPL price?", expected_output_contains=["150"], tags=["price"]),
            EvalCase(input="Say hello", expected_output_contains=["XYZNONEXISTENT"], tags=["greeting"]),
        ]
        runner.run(cases)
        summary = runner.summary()
        assert summary["total"] == 2
        assert summary["passed"] == 1
        assert summary["failed"] == 1

        out = capsys.readouterr().out
        assert "1/2" in out

    def test_compare(self, eval_agent, capsys):
        """Compare two result sets from real agent runs."""
        runner_a = EvalRunner(agent=eval_agent)
        runner_b = EvalRunner(agent=eval_agent)
        cases = [EvalCase(input="What is MSFT price?", expected_output_contains=["150"])]

        results_a = runner_a.run(cases)
        results_b = runner_b.run(cases)

        result = EvalRunner.compare(results_a, results_b, "RunA", "RunB")
        assert "a" in result and "b" in result
        assert result["a"]["total"] == 1

    def test_error_handling(self):
        """Agent tool errors are captured in results, not raised."""
        @tool
        def broken_tool(x: str) -> str:
            """Always fails."""
            raise RuntimeError("boom")

        agent = Agent(
            tools=[broken_tool],
            system_prompt="Always call the broken_tool with argument 'test'.",
        )
        runner = EvalRunner(agent=agent)
        results = runner.run([EvalCase(input="Call the tool")])
        assert len(results) == 1
        # The agent should handle the tool error internally and still return a result
        assert results[0].actual_output

    def test_no_expectations_passes(self, eval_agent):
        """Case with no expectations always passes."""
        runner = EvalRunner(agent=eval_agent)
        results = runner.run([EvalCase(input="Hello")])
        assert results[0].passed


# ── Eval Datasets ────────────────────────────────────────────────────────


class TestEvalDatasets:

    def test_tool_use_cases(self):
        """tool_use_cases returns pre-built cases."""
        from ai.eval_datasets import tool_use_cases
        cases = tool_use_cases()
        assert len(cases) >= 3
        assert all(isinstance(c, EvalCase) for c in cases)
        assert all(c.expected_tools for c in cases)

    def test_tool_use_cases_have_tags(self):
        """All built-in cases have tags."""
        from ai.eval_datasets import tool_use_cases
        for case in tool_use_cases():
            assert len(case.tags) > 0


# ── Import hygiene ──────────────────────────────────────────────────────


class TestImportHygiene:

    def test_all_symbols(self):
        """ai.__all__ has 15 symbols."""
        import ai
        expected = {
            "AI", "Message", "LLMResponse", "ToolCall",
            "RAGResult", "ExtractionResult", "Tool",
            "Agent", "AgentResult", "AgentStep",
            "AgentTeam",
            "TeamResult",
            "EvalRunner", "EvalCase", "EvalResult",
            "tool",
        }
        assert set(ai.__all__) == expected

    def test_no_private_in_dir(self):
        """Private classes don't appear in public dir."""
        import ai
        public = [x for x in dir(ai) if not x.startswith("_")]
        assert "ToolRegistry" not in public
        assert "AgentMemory" not in public
        assert "DelegationStep" not in public
