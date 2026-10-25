-- Historical frozen workflows are already closed to registration; do not rewrite them.
ALTER TABLE tasks ADD COLUMN registration_closed boolean NOT NULL DEFAULT true;
ALTER TABLE steps ADD COLUMN registration_order integer CHECK (registration_order > 0);
ALTER TABLE steps ADD COLUMN input_snapshot jsonb;
CREATE UNIQUE INDEX managed_step_order ON steps(task_id, registration_order)
    WHERE registration_order IS NOT NULL;
CREATE TABLE business_operations (
    task_id uuid NOT NULL REFERENCES tasks(id),
    idempotency_key text NOT NULL,
    operation text NOT NULL CHECK (operation IN ('register','finish')),
    request_fingerprint text NOT NULL,
    step_id uuid REFERENCES steps(id),
    PRIMARY KEY (task_id, idempotency_key)
);
