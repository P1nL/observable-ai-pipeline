-- Additive only: legacy frozen graphs, attempts and artifacts are unchanged.
ALTER TABLE steps ADD COLUMN cache_source_task_id uuid REFERENCES tasks(id);
CREATE TABLE step_cache (
    cache_key text PRIMARY KEY,
    source_task_id uuid NOT NULL REFERENCES tasks(id),
    source_step_id uuid NOT NULL REFERENCES steps(id),
    artifact_ids jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
