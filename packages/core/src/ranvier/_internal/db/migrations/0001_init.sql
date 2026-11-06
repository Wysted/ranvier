-- Own tables of ranvier (design sections 5 and 9). The schema itself and
-- schema_version are created by the runner.

CREATE TABLE ranvier.runs (
    run_id            uuid PRIMARY KEY,
    -- Thread identity = checkpointer thread_id (OD-3: no threads table).
    thread_id         uuid NOT NULL,
    flow              text NOT NULL,
    flow_version      integer NOT NULL CHECK (flow_version > 0),
    tenant_id         text,
    status            text NOT NULL
        CHECK (status IN ('pending', 'running', 'success', 'error', 'interrupted', 'cancelled')),
    input             jsonb,
    command           jsonb,
    error             jsonb,
    attempt           integer NOT NULL DEFAULT 0,
    lease_owner       text,
    lease_expires_at  timestamptz,
    cancel_requested  boolean NOT NULL DEFAULT false,
    created_at        timestamptz NOT NULL DEFAULT now(),
    started_at        timestamptz,
    finished_at       timestamptz,
    metadata          jsonb NOT NULL DEFAULT '{}'::jsonb,
    -- Exactly one of input / command is set (JSON null counts as absent).
    CONSTRAINT runs_input_xor_command CHECK (
        (input IS NULL OR jsonb_typeof(input) = 'null')
        <> (command IS NULL OR jsonb_typeof(command) = 'null')
    ),
    CONSTRAINT runs_running_has_lease CHECK (
        status <> 'running' OR (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    )
);

-- At most one running run per thread, whatever the entry path.
CREATE UNIQUE INDEX runs_one_running_per_thread
    ON ranvier.runs (thread_id) WHERE status = 'running';

-- Thread listing / history.
CREATE INDEX runs_thread_created_idx ON ranvier.runs (thread_id, created_at DESC);

-- Orphan sweep: running runs with an expired lease.
CREATE INDEX runs_running_lease_idx
    ON ranvier.runs (lease_expires_at) WHERE status = 'running';

-- Pending sweep: runs whose job was never deferred (crash between insert and defer).
CREATE INDEX runs_pending_created_idx
    ON ranvier.runs (created_at) WHERE status = 'pending';

-- No foreign key to runs on purpose: the ledger outlives thread (and run) deletion.
CREATE TABLE ranvier.usage (
    run_id               uuid NOT NULL,
    model                text NOT NULL,
    -- AgentSpec.name; '' for direct flow calls.
    agent                text NOT NULL DEFAULT '',
    flow                 text NOT NULL,
    thread_id            uuid NOT NULL,
    tenant_id            text,
    created_at           timestamptz NOT NULL DEFAULT now(),
    input_tokens         bigint NOT NULL DEFAULT 0,
    output_tokens        bigint NOT NULL DEFAULT 0,
    cached_input_tokens  bigint NOT NULL DEFAULT 0,
    reasoning_tokens     bigint NOT NULL DEFAULT 0,
    calls                integer NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, model, agent)
);
