from langgraph.graph import START, MessagesState, StateGraph

from ranvier import AgentSpec, FlowContext, as_tool

LEAD_PROMPT = (
    "You answer questions about the product. Search the docs with the MCP tools and delegate "
    "deeper reading to the summarizer."
)
SUMMARIZER_PROMPT = "You summarize the text you are given in two sentences."


async def research(
    ctx: FlowContext,
) -> StateGraph[MessagesState, None, MessagesState, MessagesState]:
    # Subagent first: it is a compiled agent graph with its own (isolated) message history.
    summarizer = await ctx.agent(
        AgentSpec(name="summarizer", profile="fast", system_prompt=SUMMARIZER_PROMPT)
    )
    # As a tool, the lead agent calls it with a task string and gets the final reply back.
    summarize = as_tool(
        summarizer, name="summarize", description="Summarize a text in two sentences."
    )
    # MCP tools come from `[mcp.servers.docs]` through `mcp_servers`; no client code here.
    lead = await ctx.agent(
        AgentSpec(
            name="lead",
            profile="fast",
            system_prompt=LEAD_PROMPT,
            tools=[summarize],
            mcp_servers=["docs"],
        )
    )
    graph = StateGraph(MessagesState)
    graph.add_node("lead", lead)
    graph.add_edge(START, "lead")
    return graph
