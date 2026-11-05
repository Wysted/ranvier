import pytest
from langchain_core.tools import ToolException
from mcp_types import (
    BlobResourceContents,
    CallToolResult,
    EmbeddedResource,
    ImageContent,
    TextContent,
    TextResourceContents,
)

from graph_agent._internal.agents.mcp import _map_result, _Server
from graph_agent._internal.config.manifest import McpServerSpec

PNG_B64 = "iVBORw0KGgo="


def test_maps_text_image_and_resources_with_artifact() -> None:
    result = CallToolResult(
        content=[
            TextContent(text="caption"),
            ImageContent(data=PNG_B64, mime_type="image/png"),
            EmbeddedResource(
                resource=TextResourceContents(
                    uri="file:///n.txt", text="note", mime_type="text/plain"
                )
            ),
            EmbeddedResource(
                resource=BlobResourceContents(
                    uri="file:///d.pdf", blob=PNG_B64, mime_type="application/pdf"
                )
            ),
        ],
        structured_content={"n": 1},
    )
    blocks, artifact = _map_result("t", result)
    assert [b["type"] for b in blocks] == ["text", "image", "text", "file"]
    assert artifact["structured_content"] == {"n": 1}
    assert [r["uri"] for r in artifact["resources"]] == ["file:///n.txt", "file:///d.pdf"]


def test_is_error_result_raises_tool_exception_with_text() -> None:
    result = CallToolResult(content=[TextContent(text="bad input")], is_error=True)
    with pytest.raises(ToolException, match="bad input"):
        _map_result("t", result)


async def test_input_schema_becomes_args_schema() -> None:
    server = _Server("s", McpServerSpec(url="http://127.0.0.1:1/mcp"), None)
    schema = {"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"]}
    tool = server._tool("double", "Doubles n", schema)
    assert tool.name == "double"
    assert tool.args == {"n": {"type": "integer"}}
