from graph_agent import AgentSpec, FlowContext
from langgraph.graph import START, MessagesState, StateGraph

SYSTEM_PROMPT = "You are a concise assistant. Answer in at most two sentences."


async def example(
    ctx: FlowContext,
) -> StateGraph[MessagesState, None, MessagesState, MessagesState]:
    # A plain StateGraph: the platform compiles it with its checkpointer. The agent is a node;
    # its model comes from the `fast` profile in graph-agent.toml.
    spec = AgentSpec(name="assistant", profile="fast", system_prompt=SYSTEM_PROMPT)
    agent = await ctx.agent(spec)
    graph = StateGraph(MessagesState)
    graph.add_node("assistant", agent)
    graph.add_edge(START, "assistant")
    return graph
