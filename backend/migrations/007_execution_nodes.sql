-- The old single-slot execution domain remains identifiable and untouched.
CREATE TABLE execution_nodes (
    node_id text PRIMARY KEY,
    executor_id text,
    origin text,
    accepting boolean NOT NULL,
    configured boolean NOT NULL DEFAULT true,
    cpu_capacity integer NOT NULL CHECK (cpu_capacity>=0),
    storage_id text,
    processors jsonb NOT NULL DEFAULT '{}',
    status text NOT NULL DEFAULT 'UNKNOWN',
    capabilities jsonb NOT NULL DEFAULT '{}',
    last_seen_at timestamptz,
    last_probe_at timestamptz,
    last_error text
);
INSERT INTO execution_nodes(node_id,accepting,cpu_capacity,configured)
    VALUES ('legacy',true,0,true);
ALTER TABLE step_attempts DROP CONSTRAINT attempt_resource_slot_fk;
ALTER TABLE step_attempts ALTER COLUMN resource_slot_id TYPE integer;
ALTER TABLE resource_slots DROP CONSTRAINT resource_slots_id_check;
ALTER TABLE resource_slots ALTER COLUMN id TYPE integer;
ALTER TABLE step_attempts ADD CONSTRAINT attempt_resource_slot_fk
    FOREIGN KEY(resource_slot_id) REFERENCES resource_slots(id);
CREATE SEQUENCE resource_slot_ids START WITH 2;
ALTER TABLE resource_slots ALTER COLUMN id SET DEFAULT nextval('resource_slot_ids');
ALTER TABLE resource_slots ADD COLUMN node_id text NOT NULL DEFAULT 'legacy' REFERENCES execution_nodes(node_id);
ALTER TABLE resource_slots ADD COLUMN device_id text NOT NULL DEFAULT 'gpu-0';
ALTER TABLE resource_slots ADD COLUMN enabled boolean NOT NULL DEFAULT true;
CREATE UNIQUE INDEX resource_node_device ON resource_slots(node_id,device_id);
ALTER TABLE step_attempts ADD COLUMN node_id text NOT NULL DEFAULT 'legacy' REFERENCES execution_nodes(node_id);
ALTER TABLE step_attempts ADD COLUMN resource_device_id text NOT NULL DEFAULT 'cpu';
UPDATE step_attempts SET resource_device_id='gpu-0' WHERE resource_slot_id IS NOT NULL;
CREATE INDEX node_attempts ON step_attempts(node_id,status);
ALTER TABLE execution_commands ADD COLUMN route jsonb;
CREATE TABLE node_events (
    seq bigserial PRIMARY KEY,
    node_id text NOT NULL REFERENCES execution_nodes(node_id),
    type text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
