ALTER TABLE step_attempts ADD COLUMN executor_host text;
ALTER TABLE step_attempts ADD COLUMN executor_created_at double precision;
ALTER TABLE step_attempts ADD COLUMN resource_slot_id smallint;

CREATE TABLE resource_slots (
    id smallint PRIMARY KEY CHECK (id = 1),
    kind text NOT NULL CHECK (kind = 'mock-gpu'),
    state text NOT NULL CHECK (state IN ('AVAILABLE','RESERVED','RUNNING','QUARANTINED')),
    host_id text,
    owner_attempt_id uuid REFERENCES step_attempts(id),
    owner_generation bigint,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((state = 'AVAILABLE' AND owner_attempt_id IS NULL AND owner_generation IS NULL)
        OR (state <> 'AVAILABLE' AND owner_attempt_id IS NOT NULL AND owner_generation IS NOT NULL))
);
INSERT INTO resource_slots(id,kind,state) VALUES (1,'mock-gpu','AVAILABLE');
ALTER TABLE step_attempts ADD CONSTRAINT attempt_resource_slot_fk
    FOREIGN KEY(resource_slot_id) REFERENCES resource_slots(id);
