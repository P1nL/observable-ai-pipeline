-- Notification metadata only; never confirms exit or releases resources.
CREATE TABLE monitor_acknowledgements (
    attempt_id uuid PRIMARY KEY REFERENCES step_attempts(id),
    attempt_status text NOT NULL,
    resource_cleanup_state text NOT NULL,
    stop_requested_at timestamptz,
    acknowledged_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
