"""
Agent — Tool-calling agent with conversation memory.

An Agent wraps the AI class with a tool registry, conversation history,
and step tracking. It runs a plan → act → observe loop until the LLM
produces a final text response.

Usage::

    from ai import Agent, tool

    @tool
    def get_price(symbol: str) -> str:
        \"\"\"Get the current price of a stock.\"\"\"
        return '{"price": 150.25}'

    agent = Agent(tools=[get_price], system_prompt="You are a trading assistant.")
    result = agent.run("What is AAPL trading at?")
    print(result.content)
    print(result.steps)     # [AgentStep(...)]
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from dataclasses import dataclass, field

from ai._types import Message, Tool, ToolCall
from ai.client import AI
from ai.memory import AgentMemory

logger = logging.getLogger(__name__)


@dataclass
class AgentStep:
    """One round of agent reasoning: action taken and observation received."""
    action: ToolCall
    observation: str


@dataclass
class AgentResult:
    """Result from an agent run — extends LLMResponse with step tracking."""
    content: str = ""
    tool_calls: list[ToolCall] = field(default_factory=list)
    usage: dict = field(default_factory=dict)
    model: str = ""
    steps: list[AgentStep] = field(default_factory=list)
    iterations: int = 0


class Agent:
    """
    Tool-calling agent with conversation memory and step tracking.

    Creates its own AI instance internally (reads GEMINI_API_KEY from env).
    Pass ``ai=`` to share an existing AI instance.

    Args:
        tools: List of @tool-decorated functions, Tool objects, or tool dicts.
        system_prompt: System prompt for the agent.
        ai: Optional AI instance. Created automatically if not provided.
        max_iterations: Max tool-call rounds per run (default: 10).
        temperature: LLM sampling temperature (default: 0.7).
        model: LLM model name (default: provider's best).
    """

    def __init__(
        self,
        tools: list | None = None,
        system_prompt: str = "You are a helpful assistant.",
        ai: AI | None = None,
        max_iterations: int = 10,
        temperature: float = 0.7,
        model: str | None = None,
        memory: AgentMemory | None = None,
        name: str = "",
    ) -> None:
        # Lazy-create AI if not provided
        if ai is None:
            from ai import AI
            self._ai = AI(model=model)
        else:
            self._ai = ai

        self._system_prompt = system_prompt
        self._max_iterations = max_iterations
        self._temperature = temperature
        self._history: list[Message] = []
        self._memory = memory
        self._name = name or "agent"
        self._description: str = ""
        self._conversation_id: str | None = None

        # Build tool registry
        from ai._tools import ToolRegistry
        self._registry = ToolRegistry()
        for t in (tools or []):
            if hasattr(t, '_tool'):
                self._registry.register(t._tool)
            elif isinstance(t, Tool):
                self._registry.register(t)
            elif isinstance(t, dict):
                # Raw tool dict — wrap in Tool with no-op fn
                self._registry.register(Tool(
                    name=t["name"],
                    description=t.get("description", ""),
                    parameters=t.get("parameters", {}),
                    fn=lambda **kw: "{}",
                ))
            else:
                raise TypeError(f"Invalid tool: {t}")

    @property
    def history(self) -> list[Message]:
        """Read-only conversation history."""
        return list(self._history)

    @property
    def conversation_id(self) -> str | None:
        """Current conversation ID (set after first run or load)."""
        return self._conversation_id

    def reset(self) -> None:
        """Clear conversation history and start a new conversation."""
        self._history = []
        self._conversation_id = None

    def load_conversation(self, conversation_id: str) -> bool:
        """Load a previous conversation into history.

        Args:
            conversation_id: ID of the conversation to load.

        Returns:
            True if loaded successfully.
        """
        if self._memory is None:
            logger.warning("No memory configured — cannot load conversation")
            return False
        convo = self._memory.load(conversation_id)
        if convo is None:
            return False
        self._history = convo.to_messages()
        self._conversation_id = conversation_id
        # Prepend summary as context if available
        if convo.summary:
            self._history.insert(0, Message(
                role="assistant",
                content=f"[Previous conversation summary: {convo.summary}]",
            ))
        return True

    def list_conversations(self, limit: int = 20) -> list:
        """List past conversations for this agent."""
        if self._memory is None:
            return []
        return self._memory.list_conversations(agent_name=self._name, limit=limit)

    def run(self, prompt: str) -> AgentResult:
        """
        Run the agent with a user prompt.

        Appends the prompt to conversation history, runs the tool-calling loop,
        and returns an AgentResult with the final response and all steps taken.

        Args:
            prompt: User's message.

        Returns:
            AgentResult with content, steps, and usage.
        """
        self._history.append(Message(role="user", content=prompt))

        # Build messages: system + history
        messages = [Message(role="system", content=self._system_prompt), *self._history]

        tool_decls = self._registry.list_declarations() if self._registry.tool_names else None
        steps: list[AgentStep] = []
        iterations = 0
        total_usage: dict = {}

        for _ in range(self._max_iterations):
            iterations += 1

            response = self._ai.generate(
                messages,
                tools=tool_decls,
                temperature=self._temperature,
            )

            # Accumulate usage (skip None values — Gemini API occasionally omits counts)
            for k, v in response.usage.items():
                if v is not None:
                    total_usage[k] = total_usage.get(k, 0) + v

            if not response.tool_calls:
                # Final text response
                self._history.append(Message(role="assistant", content=response.content))
                self._auto_save()
                return AgentResult(
                    content=response.content,
                    usage=total_usage,
                    model=response.model,
                    steps=steps,
                    iterations=iterations,
                )

            # Tool calls — execute each one
            messages.append(response.to_message())

            for tc in response.tool_calls:
                try:
                    observation = self._registry.execute(tc.name, tc.arguments)
                except KeyError:
                    observation = f'{{"error": "Unknown tool: {tc.name}"}}'
                except Exception as e:
                    observation = f'{{"error": "{type(e).__name__}: {e}"}}'

                steps.append(AgentStep(action=tc, observation=observation))
                messages.append(Message(
                    role="tool",
                    content=observation,
                    name=tc.name,
                    tool_call_id=tc.id,
                ))

        # Max iterations reached — return last response
        self._history.append(Message(role="assistant", content=response.content))
        self._auto_save()
        return AgentResult(
            content=response.content,
            usage=total_usage,
            model=response.model,
            steps=steps,
            iterations=iterations,
        )

    def _auto_save(self) -> None:
        """Auto-save conversation to memory if configured."""
        if self._memory is None:
            return
        if self._conversation_id is None:
            self._conversation_id = self._memory.new_id()
        self._memory.save(
            conversation_id=self._conversation_id,
            messages=self._history,
            agent_name=self._name,
            ai=self._ai,
        )

    def run_stream(self, prompt: str) -> Generator[str, None, None]:
        """
        Run the agent with streaming output.

        Yields text chunks as they arrive. Tool calls are executed silently
        between chunks. Only the final text response is streamed.

        Note: Intermediate tool-call rounds use non-streaming generate().
        Only the final response (no tool calls) is streamed.

        Args:
            prompt: User's message.

        Yields:
            Partial content strings.
        """
        self._history.append(Message(role="user", content=prompt))
        messages = [Message(role="system", content=self._system_prompt), *self._history]
        tool_decls = self._registry.list_declarations() if self._registry.tool_names else None

        for _ in range(self._max_iterations):
            # First, do a non-streaming call to check for tool calls
            response = self._ai.generate(
                messages,
                tools=tool_decls,
                temperature=self._temperature,
            )

            if not response.tool_calls:
                # Final response — stream it
                self._history.append(Message(role="assistant", content=response.content))
                # Re-stream: since we already have the full response, yield it
                # For true streaming, we'd need to call stream() instead
                yield response.content
                return

            # Tool calls — execute
            messages.append(response.to_message())
            for tc in response.tool_calls:
                try:
                    observation = self._registry.execute(tc.name, tc.arguments)
                except Exception as e:
                    observation = f'{{"error": "{type(e).__name__}: {e}"}}'
                messages.append(Message(
                    role="tool",
                    content=observation,
                    name=tc.name,
                    tool_call_id=tc.id,
                ))

        # Max iterations — yield whatever we have
        self._history.append(Message(role="assistant", content=response.content))
        yield response.content
