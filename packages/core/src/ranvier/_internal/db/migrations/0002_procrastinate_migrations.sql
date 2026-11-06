-- Files of the Procrastinate schema already applied (design section 9): Procrastinate tracks none.
CREATE TABLE ranvier.procrastinate_migrations (
    name        text PRIMARY KEY,
    applied_at  timestamptz NOT NULL DEFAULT now()
);
