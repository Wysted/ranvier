"""Scripted fake chat model for `validate --offline` (T19 re-exports it as the public one)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.language_models import BaseChatModel, LanguageModelInput
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables import Runnable
from pydantic import Field

DEFAULT_REPLY = "ok"


class FakeChatModel(BaseChatModel):
    """Replies with `responses` in order, repeating the last one; records every call.

    `calls` and the reply index are not safe under parallel branches (shared mutable state).
    """

    responses: Sequence[str | AIMessage] = Field(default=(DEFAULT_REPLY,), min_length=1)
    calls: list[list[BaseMessage]] = Field(default_factory=list)

    @property
    def _llm_type(self) -> str:
        return "graph-agent-fake"

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        reply = self.responses[min(len(self.calls), len(self.responses) - 1)]
        self.calls.append(list(messages))
        # langchain assigns ids in place: hand out a copy so scripted replies stay pristine
        message = AIMessage(content=reply) if isinstance(reply, str) else reply.model_copy()
        return ChatResult(generations=[ChatGeneration(message=message)])

    def bind_tools(
        self, tools: Sequence[Any], *, tool_choice: str | None = None, **kwargs: Any
    ) -> Runnable[LanguageModelInput, AIMessage]:
        # The fake never calls tools: binding is a no-op so agent factories build.
        return self
