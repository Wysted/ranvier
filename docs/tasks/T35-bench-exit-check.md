---
id: T35
title: Bench run and v0.1 exit check
part: 12
wave: K
depends_on: [T01, T02, T03, T04, T05, T06, T07, T08, T09, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24, T25, T26, T27, T28, T29, T30, T31, T32, T33, T34]
unblocks: []
owner: sa-build-py
reviewer: sa-review-py
branch: task/T35-bench-exit-check
status: done
---

## Goal
Measure N concurrent runs with a fake LLM (loop lag, RSS) and run the v0.1 exit check over the `examples/` flows.

## Design references
- design.md §2 "Workspace" (`bench/`)
- design.md §11 "Platform test layers" row Bench
- design.md §14 part 12 and "Out of scope for v0.1"
- tasks.md final note on the T35 exit criterion

## Scope: files this task may create or edit
- bench/bench_runs.py
- bench/README.md
- bench/results-v0.1.md
Shared files: none.

## Out of scope
- Fixing defects found (open an issue list in the results file; fixes are separate tasks)
- Tagging a release
- Running the agintis flows (criterion not decided; see Notes)

## Acceptance criteria
- [ ] `bench_runs.py` starts N concurrent runs (N, step latency and steps as arguments) through the real container with a fake LLM, samples loop lag and RSS, prints a summary
- [ ] Results recorded in `bench/results-v0.1.md` with machine, Python version, N, p50/p95 run time, max loop lag, peak RSS
- [ ] Exit check executed and recorded: `scripts/check.sh`, `pytest -m integration` on compose Postgres, `validate` and `run --memory` on the `examples/` flows, clean wheel install, starter renders and checks
- [ ] Any failing item is listed with its task id
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- design.md proposes 'agintis flows run unchanged on ranvier' as the exit check; tasks.md says the criterion is undecided, so use the `examples/` flows and say so in the results.
- Do not modify `~/code/agintis*`; reading is fine.
- Cost-free: fake model only, no provider keys.
- Conventions: follow the project's Python conventions and the decision log; never read `.env`; agintis is read-only reference.
- From the T34 review (orchestrator): the exit check renders the starter (`uvx copier copy` with `--data ranvier_repo=<local path>` and ranvier pointed at the local repo), runs the rendered `uv sync` + `scripts/check.sh`, and brings up the full compose stack (api + worker, after T25/T26) with `uv run --env-file` / a dummy env file. Consider a platform CI job that renders the starter and runs its check.sh (follow-up, not v0.1-blocking).
