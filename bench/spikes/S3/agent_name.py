"""S3 check: where create_agent(name=...) shows up in callback metadata.

Run: uv run python -W ignore::DeprecationWarning bench/spikes/S3/run.py
"""

import asyncio
import json
from collections.abc import Iterator
from typing import Any
from uuid import UUID

from langchain.agents import create_agent
from langchain_core.callbacks import AsyncCallbackHandler
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import LLMResult
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool
from langgraph.graph import START, MessagesState, StateGraph

USAGE = {"input_tokens": 3, "output_tokens": 1, "total_tokens": 4}


class FakeToolModel(GenericFakeChatModel):
    """GenericFakeChatModel that accepts bind_tools (create_agent calls it)."""

    def bind_tools(self, tools: Any, **kwargs: Any) -> "FakeToolModel":
        return self


def fake(*messages: AIMessage) -> FakeToolModel:
    it: Iterator[AIMessage] = iter(messages)
    return FakeToolModel(messages=it)


class Recorder(AsyncCallbackHandler):
    def __init__(self) -> None:
        self.starts: dict[UUID, dict[str, Any]] = {}
        self.ends: list[dict[str, Any]] = []

    async def on_chat_model_start(
        self,
        serialized: dict[str, Any],
        messages: list[list[BaseMessage]],
        *,
        run_id: UUID,
        parent_run_id: UUID | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        md = metadata or {}
        self.starts[run_id] = {
            "lc_agent_name": md.get("lc_agent_name"),
            AGENT_KEY: md.get(AGENT_KEY),
            "langgraph_node": md.get("langgraph_node"),
            "checkpoint_ns": md.get("langgraph_checkpoint_ns"),
            "tags": tags,
        }

    async def on_llm_end(
        self,
        response: LLMResult,
        *,
        run_id: UUID,
        parent_run_id: UUID | None = None,
        tags: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        msg = response.generations[0][0].message  # type: ignore[attr-defined]
        self.ends.append(
            {
                "start": self.starts.get(run_id),
                "on_llm_end_kwargs": sorted(kwargs),
                "usage": msg.usage_metadata,
                "msg_name": msg.name,
            }
        )


AGENT_KEY = (
    "graph_agent_agent"  # candidate platform-owned key on the model's own metadata
)


def researcher(tag_model: bool = False) -> Any:
    model = fake(AIMessage(content="found it", usage_metadata=USAGE))
    if tag_model:
        # Local (non-inheritable) callback metadata of this model instance only.
        model = model.model_copy(update={"metadata": {AGENT_KEY: "researcher"}})
    return create_agent(model=model, tools=[], name="researcher")


def boss(tool_fn: Any, tag_model: bool = False) -> Any:
    model = fake(
        AIMessage(
            content="",
            tool_calls=[{"name": tool_fn.name, "args": {"task": "t"}, "id": "c1"}],
            usage_metadata=USAGE,
        ),
        AIMessage(content="done", usage_metadata=USAGE),
    )
    if tag_model:
        model = model.model_copy(update={"metadata": {AGENT_KEY: "boss"}})
    return create_agent(model=model, tools=[tool_fn], name="boss")


async def case_direct() -> list[dict[str, Any]]:
    rec = Recorder()
    await researcher().ainvoke({"messages": [("user", "q")]}, {"callbacks": [rec]})
    return rec.ends


async def case_tool(pass_config: bool, tag_model: bool = False) -> list[dict[str, Any]]:
    sub = researcher(tag_model)

    @tool
    async def ask_researcher(task: str, config: RunnableConfig) -> str:
        """Delegate a task to the researcher subagent."""
        out = await sub.ainvoke(
            {"messages": [("user", task)]}, config if pass_config else None
        )
        return str(out["messages"][-1].content)

    rec = Recorder()
    await boss(ask_researcher, tag_model).ainvoke(
        {"messages": [("user", "q")]}, {"callbacks": [rec]}
    )
    return rec.ends


async def case_node(
    inside_boss_tool: bool, tag_model: bool = False
) -> list[dict[str, Any]]:
    """Agent as a node of a plain graph, plus a node calling a model directly."""
    plain_model = fake(AIMessage(content="direct", usage_metadata=USAGE))

    async def direct_call(state: MessagesState) -> dict[str, Any]:
        return {"messages": [await plain_model.ainvoke(state["messages"])]}

    builder = StateGraph(MessagesState)
    builder.add_node("research", researcher(tag_model))
    builder.add_node("direct_call", direct_call)
    builder.add_edge(START, "research")
    builder.add_edge("research", "direct_call")
    flow = builder.compile()
    rec = Recorder()
    if not inside_boss_tool:
        await flow.ainvoke({"messages": [("user", "q")]}, {"callbacks": [rec]})
        return rec.ends

    @tool
    async def run_flow(task: str) -> str:
        """Run the research flow."""
        out = await flow.ainvoke({"messages": [("user", task)]})
        return str(out["messages"][-1].content)

    await boss(run_flow, tag_model).ainvoke(
        {"messages": [("user", "q")]}, {"callbacks": [rec]}
    )
    return rec.ends


def summary(ends: list[dict[str, Any]]) -> list[str]:
    """One line per model call: node, lc_agent_name, platform key, depth of checkpoint_ns."""
    return [
        f"node={e['start']['langgraph_node']} lc_agent_name={e['start']['lc_agent_name']!r} "
        f"{AGENT_KEY}={e['start'][AGENT_KEY]!r} ns_depth={e['start']['checkpoint_ns'].count('|') + 1} "
        f"on_llm_end_kwargs={e['on_llm_end_kwargs']} usage={e['usage']}"
        for e in ends
    ]


async def main() -> None:
    cases = {
        "direct": case_direct(),
        "tool_subagent": case_tool(pass_config=False),
        "tool_subagent_config_passed": case_tool(pass_config=True),
        "node_subgraph": case_node(inside_boss_tool=False),
        "node_subgraph_inside_boss_tool": case_node(inside_boss_tool=True),
        "tagged/tool_subagent_config_passed": case_tool(
            pass_config=True, tag_model=True
        ),
        "tagged/node_subgraph": case_node(inside_boss_tool=False, tag_model=True),
        "tagged/node_subgraph_inside_boss_tool": case_node(
            inside_boss_tool=True, tag_model=True
        ),
    }
    for name, coro in cases.items():
        print(f"== {name}")
        for line in summary(await coro):
            print("  " + line)
    print("raw sample (tool_subagent):")
    print(json.dumps(await case_tool(pass_config=False), indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(main())
