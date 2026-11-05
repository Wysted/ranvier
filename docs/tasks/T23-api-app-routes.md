---
id: T23
title: FastAPI app, routes, admission and serve
part: 7
wave: H
depends_on: [T16, T21, T22]
unblocks: [T26, T34, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T23-api-app-routes
status: todo
---

## Goal
The HTTP API of design §6: app factory with lifespan, all endpoints, admission control, error handling, and `graph-agent serve`.

## Design references
- design.md §6 "HTTP API" (endpoint table, run request body, admission)
- design.md §6 "SSE events"
- design.md §5 "Interrupts" (`thread_interrupted`, resume map) and "Serialization" (`thread_busy`)
- design.md OD-1 row (inline in API process, background via queue)
- design.md §8 row "Loop health" (`/readyz`)

## Scope: files this task may create or edit
- packages/core/src/graph_agent/_internal/api/app.py
- packages/core/src/graph_agent/_internal/api/routes.py
- packages/core/tests/integration/api/test_routes.py
- packages/core/tests/unit/api/test_errors.py
Shared files (append-only, keep edits minimal to avoid merge conflicts):
- packages/core/src/graph_agent/_internal/cli.py (append `_add_serve` and handler; T18 appends `run`/`migrate` in the same wave)

## Out of scope
- Procrastinate implementation behind `RunQueue` (T24); background endpoints use `container.queue` and answer `503` with a clear code when it is `None`
- Graceful shutdown wiring (T26)
- OTel (T33)

## Acceptance criteria
- [ ] `create_app(settings)` with lifespan opening the container once; auth dependency on every route except `/healthz` and `/readyz`
- [ ] Endpoints exactly per the table; background runs return 202 `{run_id, thread_id}` and defer through `RunQueue`; inline runs stream SSE from `execute_run` through `SseEncoder`; client disconnect cancels the run
- [ ] Admission: inline run waits up to `admission_wait_s` then `429 too_many_runs`; inline run on a busy thread `409 thread_busy`; new `input` on a thread with pending interrupts `409 thread_interrupted`; version mismatch `409 flow_version_mismatch`
- [ ] `GET /threads/{id}/state` from the checkpointer; `DELETE /threads/{id}` refused (409) while running, else removes checkpoints and runs, 204
- [ ] One exception handler produces `{code, message, retryable}` for every error including request validation; statuses from `http.HTTPStatus`
- [ ] `/readyz` checks pools and loop lag against the threshold; `serve` runs uvicorn with host/port from settings
- [ ] Integration tests over ASGI against compose Postgres with a toy flow: run, stream, interrupt then resume, cancel, state, delete
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Port from `/home/wysted/code/agintis/src/agintis/modules/agents_catalog/http.py` and `/home/wysted/code/agintis/src/agintis/modules/conversations/http.py`.
- Why OD-1 (a): inline runs execute in the API process with the same `execute_run`; do not add a pub/sub bus.
- tasks.md does not make T23 depend on T24, but background runs need a queue; the `RunQueue` Protocol from T16 is the seam.
- `fastapi`/`starlette` only here (C3); `uvicorn` only here and in `cli`.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
- From the T22 review (orchestrator): `create_app` must set `app.state.authenticator = build_authenticator(settings.api)` in the lifespan (otherwise every route 500s); re-export `Principal` and `AuthError` from `graph_agent/__init__.py` (append-only shared file); `serve` host/port overrides must go through the same settings object before `build_authenticator` so the loopback rule cannot be bypassed.
- From the T21 review (orchestrator): the 422 handler must not echo pydantic's `input_value` (whole body); after an encoder `error` frame stop the stream and cancel/mark the run; checking "exactly one pending interrupt" for a bare resume value needs the thread state (do it here or in T16); request body size is not limited by uvicorn — document a gateway/body limit in deployment notes.
