# bench

Manual benchmarks, run before each release. Results of the last run: `results-v0.1.md`.
`spikes/` holds the throwaway design spikes (S1-S5); only `bench_runs.py` is the release bench.

## bench_runs.py

N concurrent runs through the real container (Postgres pools, claim and heartbeat, synchronous
checkpoints, usage ledger) with a fake LLM that only sleeps. It measures what the platform adds
around the model call: run time against the ideal `steps * step-latency`, event-loop lag and the
peak RSS of the process.

Needs the dev Postgres (`docker compose up -d`). The script creates a throwaway database on the
server of `--admin-url` (default: the compose one), migrates it, and drops it at the end. No
provider keys and no network beyond Postgres.

```bash
cd packages/core
uv run python ../../bench/bench_runs.py -n 50 --steps 5 --step-latency-s 0.2
```

| Argument | Default | Meaning |
|----------|---------|---------|
| `-n` | 50 | concurrent runs (also `limits.max_active_runs`, so none waits for admission) |
| `--steps` | 5 | supersteps per run, one fake model call and one checkpoint each |
| `--step-latency-s` | 0.2 | sleep of the fake model call |
| `--admin-url` | compose URL | server that may `CREATE DATABASE` |

It prints a JSON summary (exit code 1 if any run did not end `success`):

- `p50_run_s`, `p95_run_s`, `max_run_s`: row creation to terminal event. Compare with `ideal_run_s`.
- `max_loop_lag_ms`: worst overshoot of a 10 ms sleep (own sampler); `container_max_loop_lag_ms` is
  the container's 1 s sampler (the one behind `/readyz`).
- `peak_rss_mib`: `VmHWM` of the process; `rss_before_mib` is after the flow build, before the runs.
  CPU-pool worker processes are not counted (this flow uses none).

`--step-latency-s 0` turns it into a throughput test of the platform overhead (checkpoint writes).
Keep N modest on an 8 GB machine: each run is a few hundred KiB, the pools are the limit.
