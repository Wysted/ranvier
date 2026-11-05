"""Fake chat model for agent tests: `GenericFakeChatModel` lacks `bind_tools`."""

from collections.abc import Iterator
from typing import Any

from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage


class FakeToolModel(GenericFakeChatModel):
    def bind_tools(self, tools: Any, **kwargs: Any) -> "FakeToolModel":
        return self


def fake_model(*messages: AIMessage) -> FakeToolModel:
    it: Iterator[AIMessage] = iter(messages)
    return FakeToolModel(messages=it)
