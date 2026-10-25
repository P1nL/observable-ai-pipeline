CREATE TABLE tasks (
    id uuid PRIMARY KEY,
    idempotency_key text UNIQUE,
    request_fingerprint text NOT NULL,
    pipeline_id text NOT NULL,
    config_snapshot jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELING','CANCELED')),
    phase text NOT NULL DEFAULT 'QUEUED',
    wait_reason text,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    deadline_at timestamptz,
    recovery_round integer NOT NULL DEFAULT 0,
    next_event_seq bigint NOT NULL DEFAULT 1,
    output_refs jsonb NOT NULL DEFAULT '[]'
);
CREATE TABLE steps (
    id uuid PRIMARY KEY,
    task_id uuid NOT NULL REFERENCES tasks(id),
    step_key text NOT NULL,
    spec jsonb NOT NULL,
    depends_on jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('PENDING','READY','RUNNING','RETRY_WAIT','SUCCEEDED','CACHED','FAILED','BLOCKED','CANCELED')),
    current_attempt_id uuid,
    execution_generation bigint NOT NULL DEFAULT 0,
    next_retry_at timestamptz,
    checkpoint_artifact_id uuid,
    output_refs jsonb NOT NULL DEFAULT '[]',
    compatibility_key text,
    cache_key text,
    reused_from_step_id uuid REFERENCES steps(id),
    wait_reason text,
    first_started_at timestamptz,
    UNIQUE(task_id, step_key)
);
CREATE TABLE step_attempts (
    id uuid PRIMARY KEY,
    step_id uuid NOT NULL REFERENCES steps(id),
    attempt_number integer NOT NULL,
    recovery_round integer NOT NULL,
    worker_id text NOT NULL,
    fencing_token bigint NOT NULL,
    status text NOT NULL CHECK (status IN ('RUNNING','SUCCEEDED','FAILED','TIMED_OUT','LOST','CANCELED')),
    lease_expires_at timestamptz NOT NULL,
    heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    progress jsonb NOT NULL DEFAULT '{}',
    error_code text,
    error_message text,
    retryable boolean,
    executor_pid integer,
    stop_requested_at timestamptz,
    process_exit_confirmed_at timestamptz,
    resource_cleanup_state text NOT NULL DEFAULT 'CPU_ONLY',
    resumed_from_checkpoint_id uuid,
    UNIQUE(step_id, attempt_number)
);
ALTER TABLE steps ADD CONSTRAINT current_attempt_fk FOREIGN KEY(current_attempt_id) REFERENCES step_attempts(id);
CREATE TABLE artifacts (
    id uuid PRIMARY KEY,
    task_id uuid NOT NULL REFERENCES tasks(id),
    step_id uuid NOT NULL REFERENCES steps(id),
    kind text NOT NULL,
    relative_path text NOT NULL UNIQUE,
    content_fingerprint text NOT NULL,
    producer_attempt_id uuid NOT NULL REFERENCES step_attempts(id),
    metadata jsonb NOT NULL,
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    availability text NOT NULL DEFAULT 'AVAILABLE'
);
ALTER TABLE steps ADD CONSTRAINT checkpoint_artifact_fk FOREIGN KEY(checkpoint_artifact_id) REFERENCES artifacts(id);
CREATE TABLE task_events (
    task_id uuid NOT NULL REFERENCES tasks(id),
    seq bigint NOT NULL,
    type text NOT NULL,
    step_id uuid,
    attempt_id uuid,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(task_id, seq)
);
CREATE TABLE recovery_operations (
    task_id uuid NOT NULL REFERENCES tasks(id),
    idempotency_key text NOT NULL,
    operation text NOT NULL,
    recovery_round integer NOT NULL,
    PRIMARY KEY(task_id,idempotency_key)
);
CREATE INDEX ready_steps ON steps(status, task_id);
CREATE INDEX expired_attempts ON step_attempts(lease_expires_at) WHERE status='RUNNING';
CREATE INDEX artifact_cache ON steps(cache_key) WHERE status IN ('SUCCEEDED','CACHED');
