-- Existing attempts remain local; no frozen historical task is rewritten.
ALTER TABLE step_attempts ADD COLUMN execution_backend text NOT NULL DEFAULT 'local'
    CHECK (execution_backend IN ('local','external'));
ALTER TABLE step_attempts ADD COLUMN executor_id text;
ALTER TABLE step_attempts ADD COLUMN external_job_id uuid;

CREATE TABLE execution_commands (
    id uuid PRIMARY KEY REFERENCES step_attempts(id),
    executor_id text NOT NULL,
    external_job_id uuid NOT NULL UNIQUE,
    binding jsonb NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    dispatched_at timestamptz,
    exit_confirmed_at timestamptz,
    exit_receipt jsonb
);
