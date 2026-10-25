"""Single-instance dependency/retry/lease coordinator, separate from API/Worker."""
import logging
import signal
import time

import psycopg

from app.config import Settings
from app.db import connect
from app.repository import Repository


def main():
    settings = Settings.from_env()
    repo = Repository(settings)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s coordinator %(message)s")
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    interval = settings.runtime_policy.coordinator_scan_interval_seconds
    while not stopping:
        try:
            # Session advisory lock is not an execution transaction or a lease.
            with connect(settings) as guard:
                guard.autocommit = True
                if not guard.execute("SELECT pg_try_advisory_lock(20261025) AS acquired").fetchone()["acquired"]:
                    raise SystemExit("Another coordinator holds this database's singleton lock")
                while not stopping:
                    guard.execute("SELECT 1")
                    repo.reconcile()
                    time.sleep(interval)
        except psycopg.Error:
            logging.exception("database unavailable; reconnecting without dispatch")
            time.sleep(interval)


if __name__ == "__main__":
    main()
