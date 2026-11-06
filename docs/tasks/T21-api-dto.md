---
id: T21
title: API wire DTOs and SSE mapping
part: 7
wave: F
depends_on: [T12, T13]
unblocks: [T23, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T21-api-dto
status: done
---

## Goal
Pydantic request/response DTOs, the single error body, the error-code to HTTP status table, and the mapping of run events to SSE with `{run_id}:{seq}` ids.

## Design references
- design.md §6 "HTTP API" (table, run request body, admission paragraph)
- design.md §6 "SSE events" table and the transport paragraph
- design.md §6 "HTTP API" intro (`{code, message, retryable}`, `http.HTTPStatus`)

## Scope: files this task may create or edit
- packages/core/src/ranvier/_internal/api/dto.py
- packages/core/tests/unit/api/test_dto.py
Shared files: none.

## Out of scope
- Routes, auth and app factory (T22, T23)
- Replay via `Last-Event-ID` (out of scope v0.1)
- Event types themselves (T12)

## Acceptance criteria
- [ ] Request DTO `{flow, input | command: {resume}, stream_mode?, metadata?}` with exactly one of input/command and `stream_mode` restricted to `updates|values|messages|custom` (default `["updates", "custom"]`)
- [ ] Response DTOs: run accepted `{run_id, thread_id}`, run, flow list item, thread state `{values, next, interrupts, checkpoint_id}`, error body `{code, message, retryable}`
- [ ] `ErrorCode -> HTTPStatus` table in one place (thread_busy 409, thread_interrupted 409, flow_version_mismatch 409, too_many_runs 429, not found 404, validation 422, ...)
- [ ] `SseEncoder(run_id)` maps each `RunEvent` to `fastapi.sse.ServerSentEvent` with event name per design, data per the table, id `{run_id}:{seq}` and `seq` monotonic from 1
- [ ] A value the JSON encoder cannot serialize yields an `error` event, never a silent drop (unit test)
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Port from `/home/wysted/code/agintis/src/agintis/modules/agents_catalog/domain/events.py` and `http.py` (it already imports `fastapi.sse`).
- `fastapi` is allowed only under `api` (C3). Use `http.HTTPStatus`, no numeric literals.
- Error codes come from `runtime/runs.py` (T13).
- Conventions: follow the project's Python conventions and the decision log; never read `.env`; agintis is read-only reference.
