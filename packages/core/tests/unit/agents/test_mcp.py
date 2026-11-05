import pytest
from langchain_core.tools import ToolException
from mcp_types import (
    AudioContent,
    BlobResourceContents,
    CallToolResult,
    EmbeddedResource,
    ImageContent,
    ResourceLink,
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
    metas = artifact["resources"]
    assert [m.get("uri") for m in metas] == [None, "file:///n.txt", "file:///d.pdf"]
    assert PNG_B64 not in str(artifact)  # metadata only, no duplicated bytes


def test_maps_audio_and_resource_link_without_error() -> None:
    result = CallToolResult(
        content=[
            AudioContent(data=PNG_B64, mime_type="audio/wav"),
            ResourceLink(name="report", uri="file:///r.pdf", mime_type="application/pdf", size=12),
        ]
    )
    blocks, artifact = _map_result("t", result)
    assert blocks[0]["type"] == "audio"
    assert blocks[0]["base64"] == PNG_B64
    assert blocks[1]["text"] == "report: file:///r.pdf"
    assert artifact["resources"][1] == {
        "type": "resource_link",
        "uri": "file:///r.pdf",
        "name": "report",
        "mime_type": "application/pdf",
        "size": 12,
    }


def test_is_error_result_raises_tool_exception_with_text() -> None:
    result = CallToolResult(content=[TextContent(text="bad input")], is_error=True)
    with pytest.raises(ToolException, match="bad input"):
        _map_result("t", result)


def test_input_schema_becomes_args_schema() -> None:
    server = _Server("s", McpServerSpec(url="http://127.0.0.1:1/mcp"), {}, None)
    schema = {"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"]}
    tool = server._tool("double", "Doubles n", schema)
    assert tool.name == "double"
    assert tool.args == {"n": {"type": "integer"}}
