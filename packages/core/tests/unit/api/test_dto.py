import json
import uuid

import pytest
from pydantic import ValidationError

from ranvier._internal.api.dto import ERROR_STATUS, RunRequest, SseEncoder
from ranvier._internal.runtime.events import (
    CustomEvent,
    EndEvent,
    ErrorEvent,
    InterruptEvent,
    InterruptItem,
    MessagesEvent,
    MetadataEvent,
    UpdatesEvent,
    UsageTotals,
    ValuesEvent,
)
from ranvier._internal.runtime.runs import ErrorCode


def test_every_error_code_has_a_status() -> None:
    assert set(ERROR_STATUS) == set(ErrorCode)


def test_request_requires_exactly_one_of_input_and_command() -> None:
    assert RunRequest.model_validate({"flow": "f", "input": {"a": 1}}).stream_mode == [
        "updates",
        "custom",
    ]
    assert RunRequest.model_validate({"flow": "f", "command": {"resume": 1}}).command
    for body in (
        {"flow": "f"},
        {"flow": "f", "input": None},
        {"flow": "f", "command": {"resume": None}},
        {"flow": "f", "input": 1, "command": {"resume": 1}},
        {"flow": "f", "input": 1, "stream_mode": ["debug"]},
    ):
        with pytest.raises(ValidationError):
            RunRequest.model_validate(body)


def test_encoder_ids_are_monotonic_from_one_and_data_follows_the_table() -> None:
    run_id = uuid.uuid4()
    enc = SseEncoder(run_id)
    meta = enc.encode(MetadataEvent(str(run_id), "t", "flow", 1, 1))
    upd = enc.encode(UpdatesEvent(("sub:1",), "node", {"x": 1}))
    cus = enc.encode(CustomEvent((), None, "hi"))
    assert [f.id for f in (meta, upd, cus)] == [f"{run_id}:{n}" for n in (1, 2, 3)]
    assert [f.event for f in (meta, upd, cus)] == ["metadata", "updates", "custom"]
    assert json.loads(upd.raw_data or "") == {"ns": ["sub:1"], "node": "node", "update": {"x": 1}}
    intr = enc.encode(InterruptEvent((InterruptItem("i1", {"q": 1}),)))
    assert json.loads(intr.raw_data or "") == {"interrupts": [{"id": "i1", "value": {"q": 1}}]}
    end = enc.encode(EndEvent("success", UsageTotals(1, 2, 0)))
    assert json.loads(end.raw_data or "")["usage"] == {
        "input_tokens": 1,
        "output_tokens": 2,
        "cached_input_tokens": 0,
    }


def test_unserializable_value_yields_error_event() -> None:
    enc = SseEncoder("r1")
    frame = enc.encode(UpdatesEvent((), "node", {"bad": object()}))
    assert frame.event == "error"
    assert frame.id == "r1:1"
    body = json.loads(frame.raw_data or "")
    assert body["code"] == ErrorCode.INTERNAL_ERROR
    assert body["retryable"] is False
    assert "RuntimeError" not in (frame.raw_data or "")
    assert "UpdatesEvent" not in (frame.raw_data or "")
    with pytest.raises(RuntimeError):
        enc.encode(EndEvent("success", UsageTotals(0, 0, 0)))


def test_nan_and_infinity_become_null() -> None:
    frame = SseEncoder("r1").encode(ValuesEvent((), {"a": float("nan"), "b": float("inf")}))
    assert frame.event == "values"
    assert json.loads(frame.raw_data or "") == {"ns": [], "values": {"a": None, "b": None}}


def test_values_messages_and_error_frames() -> None:
    enc = SseEncoder("r1")
    val = enc.encode(ValuesEvent(("s",), {"k": 1}))
    msg = enc.encode(MessagesEvent((), "n", None, "ai", "hi", [{"name": "t"}]))
    err = enc.encode(ErrorEvent("internal_error", "boom", False))
    assert [f.event for f in (val, msg, err)] == ["values", "messages", "error"]
    assert json.loads(val.raw_data or "") == {"ns": ["s"], "values": {"k": 1}}
    assert json.loads(msg.raw_data or "") == {
        "ns": [],
        "node": "n",
        "id": None,
        "type": "ai",
        "content": "hi",
        "tool_call_chunks": [{"name": "t"}],
    }
    assert json.loads(err.raw_data or "") == {
        "code": "internal_error",
        "message": "boom",
        "retryable": False,
    }
