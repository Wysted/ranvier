import pytest
from langchain_core.messages import AIMessage, HumanMessage

from ranvier._internal.runtime.offline import FakeChatModel


def test_scripted_replies_repeat_last_and_record_calls() -> None:
    model = FakeChatModel(responses=["one", AIMessage("two")])
    out = [model.invoke([HumanMessage(str(i))]).content for i in range(3)]
    assert out == ["one", "two", "two"]
    assert [c[0].content for c in model.calls] == ["0", "1", "2"]


def test_bind_tools_is_a_noop() -> None:
    model = FakeChatModel()
    assert model.bind_tools([]) is model


def test_scripted_message_is_copied_not_mutated() -> None:
    scripted = AIMessage("fixed")
    reply = FakeChatModel(responses=[scripted]).invoke([HumanMessage("hi")])
    assert reply is not scripted
    assert scripted.id is None


def test_empty_responses_rejected() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        FakeChatModel(responses=[])
