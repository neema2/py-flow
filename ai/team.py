"""
AgentTeam — Multi-agent orchestration with LLM-based routing.

A team of specialist agents coordinated by a router that decomposes
tasks and delegates to the right agent.

Usage::

    from ai import Agent
    from ai.team import AgentTeam

    researcher = Agent(tools=[search_docs], system_prompt="You research documents.")
    analyst = Agent(tools=[query_lh], system_prompt="You analyze data.")

    team = AgentTeam(agents={"researcher": researcher, "analyst": analyst})
    result = team.run("Research Basel III docs and analyze capital impact")
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Optional

from ai._types import Message

logger = logging.getLogger(__name__)


@dataclass
class DelegationStep:
    """One delegation round in a team run."""
    agent_name: str
    prompt: str
    response: str
    duration_ms: float = 0.0


@dataclass
class TeamResult:
    """Result from an AgentTeam run."""
    content: str = ""
    delegation_log: list[DelegationStep] = field(default_factory=list)
    usage: dict = field(default_factory=dict)
    iterations: int = 0


class AgentTeam:
    """
    Multi-agent orchestration with LLM-based routing.

    The router LLM sees agent names and descriptions, decomposes the task,
    and delegates sub-tasks to specialist agents sequentially. After all
    delegations, the router synthesizes a final response.

    Args:
        agents: Dict of name → Agent instances.
        ai: Optional AI instance for the router. Created automatically if not provided.
        max_delegations: Max delegation rounds (default: 5).
        temperature: Router LLM temperature (default: 0.7).
    """

    def __init__(
        self,
        agents: dict,
        ai=None,
        max_delegations: int = 5,
        temperature: float = 0.7,
    ):
        self._agents = agents
        self._max_delegations = max_delegations
        self._temperature = temperature

        if ai is None:
            from ai import AI
            self._ai = AI()
        else:
            self._ai = ai

    def run(self, prompt: str) -> TeamResult:
        """
        Run the team on a task.

        The router decomposes the task and delegates to specialist agents.
        Returns a synthesized final response.

        Args:
            prompt: The user's task description.

        Returns:
            TeamResult with content and delegation log.
        """
        import time

        agent_descriptions = "\n".join(
            f"- **{name}**: {agent._system_prompt}"
            for name, agent in self._agents.items()
        )

        router_system = f"""You are a task router that coordinates specialist agents.

Available agents:
{agent_descriptions}

Your job:
1. Analyze the user's request.
2. Decide which agent(s) to delegate to and in what order.
3. For each delegation, respond with a JSON object:
   {{"delegate": "agent_name", "prompt": "specific sub-task for this agent"}}
4. When you have all the information needed, respond with a JSON object:
   {{"final": "your synthesized answer combining all agent results"}}

Always respond with exactly one JSON object per turn. No other text."""

        messages = [
            Message(role="system", content=router_system),
            Message(role="user", content=prompt),
        ]

        delegation_log: list[DelegationStep] = []
        total_usage: dict = {}

        for _ in range(self._max_delegations):
            response = self._ai.generate(
                messages,
                temperature=self._temperature,
            )

            for k, v in response.usage.items():
                total_usage[k] = total_usage.get(k, 0) + v

            # Parse router decision
            text = response.content.strip()
            decision = self._parse_decision(text)

            if decision is None:
                # Router gave plain text — treat as final
                return TeamResult(
                    content=text,
                    delegation_log=delegation_log,
                    usage=total_usage,
                    iterations=len(delegation_log),
                )

            if "final" in decision:
                return TeamResult(
                    content=decision["final"],
                    delegation_log=delegation_log,
                    usage=total_usage,
                    iterations=len(delegation_log),
                )

            if "delegate" in decision:
                agent_name = decision["delegate"]
                sub_prompt = decision.get("prompt", prompt)

                if agent_name not in self._agents:
                    messages.append(Message(role="assistant", content=text))
                    messages.append(Message(
                        role="user",
                        content=f"Agent '{agent_name}' not found. Available: {list(self._agents.keys())}",
                    ))
                    continue

                agent = self._agents[agent_name]
                t0 = time.time()
                result = agent.run(sub_prompt)
                duration_ms = (time.time() - t0) * 1000

                step = DelegationStep(
                    agent_name=agent_name,
                    prompt=sub_prompt,
                    response=result.content,
                    duration_ms=duration_ms,
                )
                delegation_log.append(step)

                for k, v in result.usage.items():
                    total_usage[k] = total_usage.get(k, 0) + v

                # Feed result back to router
                messages.append(Message(role="assistant", content=text))
                messages.append(Message(
                    role="user",
                    content=f"Agent '{agent_name}' responded:\n{result.content}",
                ))

                # Reset the delegated agent for next use
                agent.reset()

        # Max delegations reached — ask router to synthesize
        messages.append(Message(
            role="user",
            content="Maximum delegation rounds reached. Please provide your final synthesized answer now.",
        ))
        response = self._ai.generate(messages, temperature=self._temperature)
        for k, v in response.usage.items():
            total_usage[k] = total_usage.get(k, 0) + v

        return TeamResult(
            content=response.content,
            delegation_log=delegation_log,
            usage=total_usage,
            iterations=len(delegation_log),
        )

    def _parse_decision(self, text: str) -> Optional[dict]:
        """Parse a JSON decision from the router's response."""
        # Strip markdown code fences
        clean = text.strip()
        if clean.startswith("```"):
            lines = clean.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            clean = "\n".join(lines).strip()

        try:
            return json.loads(clean)
        except json.JSONDecodeError:
            # Try to find JSON in the text
            start = clean.find("{")
            end = clean.rfind("}") + 1
            if start >= 0 and end > start:
                try:
                    return json.loads(clean[start:end])
                except json.JSONDecodeError:
                    pass
            return None
