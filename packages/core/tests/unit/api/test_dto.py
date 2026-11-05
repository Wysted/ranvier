import json
import uuid

import pytest
from pydantic import ValidationError

from graph_agent._internal.api.dto import ERROR_STATUS, RunRequest, SseEncoder
from graph_agent._internal.runtime.events import (
    CustomEvent,
    EndEvent,
    InterruptEvent,
    InterruptItem,
    MetadataEvent,
    UpdatesEvent,
    UsageTotals,
)
from graph_agent._internal.runtime.runs import ErrorCode


def test_every_error_code_has_a_status() -> None:
    assert set(ERROR_STATUS) == set(ErrorCode)


def test_request_requires_exactly_one_of_input_and_command() -> None:
    assert RunRequest.model_validate({"flow": "f", "input": {"a": 1}}).stream_mode == [
        "updates",
        "custom",
    ]
    assert RunRequest.model_validate({"flow": "f", "input": None}).command is None
    assert RunRequest.model_validate({"flow": "f", "command": {"resume": 1}}).command
    for body in (
        {"flow": "f"},
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
