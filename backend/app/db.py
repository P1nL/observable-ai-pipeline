"""Short PostgreSQL transactions. Computation never holds a connection."""
import argparse
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from app.config import Settings


def connect(settings: Settings):
    return psycopg.connect(settings.database_url, row_factory=dict_row, connect_timeout=3)


def migrate(settings: Settings):
    directory = Path(__file__).resolve().parents[1] / "migrations"
    with connect(settings) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(20261024)")
        conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())")
        for file in sorted(directory.glob("*.sql")):
            if not conn.execute("SELECT 1 FROM schema_migrations WHERE version=%s", (file.name,)).fetchone():
                conn.execute(file.read_text(encoding="utf-8"))
                conn.execute("INSERT INTO schema_migrations(version) VALUES (%s)", (file.name,))


if __name__ == "__main__":
    argparse.ArgumentParser(description="Apply explicit SQL migrations").parse_args()
    migrate(Settings.from_env())
