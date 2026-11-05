from collections.abc import Sequence

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import BaseTool, tool

from examples.flows.research import SUMMARIZER_PROMPT, research
from graph_agent.testing import FakeChatModel, flow_context, run_flow


@tool
async def search_docs(query: str) -> str:
    """Search the product docs."""
    return f"doc about {query}: retries are configured per tool"


class FakeDocsServer:
    async def tools(self, server: str) -> Sequence[BaseTool]:
        assert server == "docs"
        return [search_docs]


def _call(name: str, args: dict[str, str], call_id: str) -> AIMessage:
    return AIMessage(content="", tool_calls=[{"name": name, "args": args, "id": call_id}])


async def test_lead_uses_mcp_tool_and_subagent() -> None:
    # One model per profile, so the lead and the subagent share it: the script is consumed in
    # call order (lead, lead, subagent, lead).
    model = FakeChatModel(
        [
            _call("search_docs", {"query": "retries"}, "c1"),
            _call("summarize", {"task": "doc about retries"}, "c2"),
            "Retries are per tool.",  # the subagent's reply
            "Answer: retries are per tool.",  # the lead's final reply
        ]
    )
    ctx = flow_context(models={"fast": model}, mcp=FakeDocsServer())
    result = await run_flow(research, {"messages": [HumanMessage("how do retries work?")]}, ctx=ctx)

    # The subagent sees only its own prompt and the task string, not the lead's history.
    assert [(m.type, m.content) for m in model.calls[2]] == [
        ("system", SUMMARIZER_PROMPT),
        ("human", "doc about retries"),
    ]

    messages = result.values["messages"]
    tool_messages = {m.name: m.content for m in messages if m.type == "tool"}
    assert tool_messages["search_docs"] == "doc about retries: retries are configured per tool"
    assert tool_messages["summarize"] == "Retries are per tool."
    assert messages[-1].content == "Answer: retries are per tool."
