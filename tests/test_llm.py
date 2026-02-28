"""
Integration tests for LLM client — real Gemini API calls.

Requires GEMINI_API_KEY env var. Tests skip if not set.
"""

import os
import json
import pytest

from ai.llm import LLMClient, GeminiLLM, Message, LLMResponse, ToolCall


GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
requires_gemini = pytest.mark.skipif(not GEMINI_API_KEY, reason="GEMINI_API_KEY not set")


@pytest.fixture(scope="module")
def llm():
    if not GEMINI_API_KEY:
        pytest.skip("GEMINI_API_KEY not set")
    return GeminiLLM(api_key=GEMINI_API_KEY)


# ── Basic generation ─────────────────────────────────────────────────────


@requires_gemini
class TestGeminiGeneration:

    def test_provider_abc(self, llm):
        """GeminiLLM should be an instance of LLMClient."""
        assert isinstance(llm, LLMClient)

    def test_model_name(self, llm):
        """model_name should return configured model."""
        assert "gemini" in llm.model_name

    def test_generate_basic(self, llm):
        """Simple prompt should return a text response."""
        response = llm.generate([
            Message(role="user", content="What is 2 + 2? Answer with just the number."),
        ], temperature=0.0)

        assert isinstance(response, LLMResponse)
        assert response.content
        assert "4" in response.content
        assert response.model == llm.model_name

    def test_generate_with_system(self, llm):
        """System message should shape behavior."""
        response = llm.generate([
            Message(role="system", content="You are a pirate. Always respond in pirate speak."),
            Message(role="user", content="How are you today?"),
        ], temperature=0.5)

        assert response.content
        # Pirate speak should contain some pirate-ish words
        lower = response.content.lower()
        pirate_words = ["arr", "matey", "ahoy", "ye", "sea", "sail", "ship", "treasure", "aye"]
        assert any(w in lower for w in pirate_words), (
            f"Expected pirate speak, got: {response.content[:200]}"
        )

    def test_conversation_history(self, llm):
        """Multi-turn conversation should maintain context."""
        messages = [
            Message(role="user", content="My name is Alice."),
            Message(role="assistant", content="Hello Alice! Nice to meet you."),
            Message(role="user", content="What is my name?"),
        ]
        response = llm.generate(messages, temperature=0.0)
        assert "Alice" in response.content

    def test_usage_stats(self, llm):
        """Response should include token usage stats."""
        response = llm.generate([
            Message(role="user", content="Say hello."),
        ])
        assert "total_tokens" in response.usage or "prompt_tokens" in response.usage


# ── Streaming ────────────────────────────────────────────────────────────


@requires_gemini
class TestGeminiStreaming:

    def test_stream(self, llm):
        """Streaming should yield text chunks."""
        chunks = list(llm.stream([
            Message(role="user", content="Count from 1 to 5, one number per line."),
        ], temperature=0.0))

        assert len(chunks) > 0
        full_text = "".join(chunks)
        assert "1" in full_text
        assert "5" in full_text


# ── Tool calling ─────────────────────────────────────────────────────────


WEATHER_TOOL = {
    "name": "get_weather",
    "description": "Get the current weather for a location.",
    "parameters": {
        "type": "object",
        "properties": {
            "location": {
                "type": "string",
                "description": "City name, e.g. 'London'",
            },
        },
        "required": ["location"],
    },
}


@requires_gemini
class TestGeminiToolCalling:

    def test_tool_calling(self, llm):
        """Model should return a ToolCall when appropriate."""
        response = llm.generate(
            [Message(role="user", content="What's the weather in London?")],
            tools=[WEATHER_TOOL],
            temperature=0.0,
        )

        assert len(response.tool_calls) > 0
        tc = response.tool_calls[0]
        assert isinstance(tc, ToolCall)
        assert tc.name == "get_weather"
        assert "location" in tc.arguments
        assert "london" in tc.arguments["location"].lower()

    def test_tool_response_loop(self, llm):
        """Full tool calling loop: call → execute → respond."""
        # Step 1: Initial request triggers tool call
        messages = [Message(role="user", content="What's the weather in Paris?")]
        response = llm.generate(messages, tools=[WEATHER_TOOL], temperature=0.0)

        assert len(response.tool_calls) > 0
        tc = response.tool_calls[0]

        # Step 2: "Execute" the tool (mock result)
        tool_result = json.dumps({"temperature": "18°C", "condition": "partly cloudy"})

        # Step 3: Send tool result back (preserve raw content for thought_signature)
        assistant_msg = Message(
            role="assistant",
            content=response.content,
            tool_calls=response.tool_calls,
        )
        assistant_msg._raw_content = response._raw_content
        messages.append(assistant_msg)
        messages.append(Message(
            role="tool",
            content=tool_result,
            name=tc.name,
            tool_call_id=tc.id,
        ))

        # Step 4: Model generates final answer using tool result
        final = llm.generate(messages, tools=[WEATHER_TOOL], temperature=0.0)
        assert final.content
        assert "18" in final.content or "cloudy" in final.content.lower()


# ── Errors ───────────────────────────────────────────────────────────────


@requires_gemini
class TestGeminiErrors:

    def test_no_api_key_raises(self):
        """Missing API key should raise ValueError."""
        old = os.environ.pop("GEMINI_API_KEY", None)
        try:
            with pytest.raises(ValueError, match="API key required"):
                GeminiLLM(api_key=None)
        finally:
            if old:
                os.environ["GEMINI_API_KEY"] = old
