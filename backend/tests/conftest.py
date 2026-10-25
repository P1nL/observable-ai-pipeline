"""Opt-in PostgreSQL integration fixtures; never truncate a development DB."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import httpx
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
import pytest

from app.config import Settings
from app.db import connect, migrate
from app.domain import SubmitTask, freeze_config
from app.repository import Repository, fingerprint
from app.processes import stop_executor

ROOT = Path(__file__).resolve().parents[2]


class Engine:
    def __init__(self, settings, directory, environment):
        self.settings = settings
        self.repo = Repository(settings)
        self.directory = directory
        self.environment = environment
        self.processes = []
        self.handles = []
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.client = httpx.Client(base_url=f"http://127.0.0.1:{self.port}", timeout=5)
        self.api = None

    def start(self, module, *args):
        output = (self.directory / f"{module}-{len(self.processes)}.log").open("w", encoding="utf-8")
        self.handles.append(output)
        process = subprocess.Popen([sys.executable, "-m", module, *args], cwd=ROOT, env=self.environment, stdout=output, stderr=subprocess.STDOUT, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        self.processes.append(process)
        return process

    def start_api(self):
        self.api = self.start("uvicorn", "app.api:app", "--host", "127.0.0.1", "--port", str(self.port))
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.api.poll() is not None:
                raise AssertionError("API exited; logs: " + self.logs())
            try:
                if self.client.get("/health").status_code == 200:
                    return
            except httpx.TransportError:
                pass
            time.sleep(.05)
        raise AssertionError("API did not start; " + self.logs())

    def worker(self, name="test-worker"):
        return self.start("app.worker", "--worker-id", name)

    def submit(self, body, key=None):
        response = self.client.post("/api/v1/tasks", json=body, headers={"Idempotency-Key": key or str(uuid4())})
        assert response.status_code == 202, response.text
        return response.json()["id"]

    def seed_legacy(self, body=None, key=None):
        """Persist a pre-upgrade three-step task, not a new public submission."""
        request = SubmitTask(**{**(body or {}), "pipeline_id": "animation-v1"})
        snapshot = freeze_config(request, self.settings)
        snapshot.pop("execution_backend")
        snapshot["request"].pop("execution_backend")
        task_id = str(uuid4())
        request_hash = fingerprint({"request": snapshot["request"], "explicit_policy": request.policy.model_dump(mode="json", exclude_unset=True)})
        with connect(self.settings) as conn:
            conn.execute("""INSERT INTO tasks(id,idempotency_key,request_fingerprint,pipeline_id,config_snapshot,status,deadline_at)
                VALUES (%s,%s,%s,'animation-v1',%s,'QUEUED',clock_timestamp()+(%s*interval '1 second'))""",
                (task_id, key or str(uuid4()), request_hash, Jsonb(snapshot), snapshot["runtime_policy"]["task_deadline_seconds"]))
            for spec in snapshot["steps"]:
                conn.execute("INSERT INTO steps(id,task_id,step_key,spec,depends_on,status) VALUES (%s,%s,%s,%s,%s,%s)",
                    (str(uuid4()), task_id, spec["key"], Jsonb(spec), Jsonb(spec["depends_on"]), "PENDING" if spec["depends_on"] else "READY"))
            self.repo._event(conn, task_id, "TASK_SUBMITTED", pipeline_id="animation-v1")
        return task_id

    def task(self, task_id):
        response = self.client.get(f"/api/v1/tasks/{task_id}")
        assert response.status_code == 200, response.text
        return response.json()

    def wait(self, task_id, predicate=lambda task: task["status"] in ("SUCCEEDED", "FAILED"), timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            task = self.task(task_id)
            if predicate(task):
                return task
            time.sleep(.05)
        raise AssertionError(f"Timed out: {json.dumps(task)}\n{self.logs()}")

    def logs(self):
        return "\n".join(f"{p.name}: {p.read_text(encoding='utf-8', errors='replace')}" for p in self.directory.glob("*.log"))

    def close(self):
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        # A failed assertion can leave a computation after hard-stopping its
        # Windows supervisor. Stop only identities recorded in this test schema.
        from app.db import connect
        with connect(self.settings) as conn:
            executors = conn.execute("SELECT executor_pid,executor_created_at,executor_host FROM step_attempts WHERE executor_pid IS NOT NULL").fetchall()
        for executor in executors:
            stop_executor(executor["executor_pid"], executor["executor_created_at"], executor["executor_host"])
        self.client.close()
        for handle in self.handles:
            handle.close()


@pytest.fixture
def engine(tmp_path):
    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("Set TEST_DATABASE_URL to a dedicated *_test database for real process tests")
    url = urlsplit(database_url)
    if not url.path.endswith("_test"):
        raise RuntimeError("Refusing integration tests outside a dedicated *_test database")
    schema = "run_" + uuid4().hex
    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    query = dict(parse_qsl(url.query))
    query["options"] = f"-csearch_path={schema}"
    isolated_url = urlunsplit(url._replace(query=urlencode(query)))
    environment = {**os.environ, "PYTHONPATH": str(ROOT / "backend"), "DATABASE_URL": isolated_url, "ARTIFACT_ROOT": str(tmp_path / "artifacts"), "PIPELINE_PROFILE": "fast-recovery", "ENABLE_TEST_FAULTS": "true", "PYTHONUNBUFFERED": "1"}
    for key in ("EXTERNAL_EXECUTOR_URL", "EXTERNAL_EXECUTOR_ID", "EXTERNAL_EXECUTOR_SECRET", "EXTERNAL_RPC_TIMEOUT_SECONDS", "EXECUTION_NODES_JSON", "ARTIFACT_STORAGE_ID", "NODE_PROBE_INTERVAL_SECONDS"):
        environment.pop(key, None)
    # Avoid caller environment silently replacing the explicitly tested profile.
    for key in ("WORKER_HEARTBEAT_INTERVAL_SECONDS", "WORKER_LEASE_TTL_SECONDS", "COORDINATOR_SCAN_INTERVAL_SECONDS", "ATTEMPT_HARD_TIMEOUT_SECONDS", "TASK_DEADLINE_SECONDS", "STEP_SOFT_WARNING_SECONDS", "TASK_SOFT_WARNING_SECONDS", "MAX_ATTEMPTS_PER_RECOVERY_ROUND", "RETRY_INITIAL_DELAY_SECONDS", "RETRY_MAX_DELAY_SECONDS"):
        environment.pop(key, None)
    settings = Settings.from_env(environment)
    migrate(settings)
    migrate(settings)  # Migration must be safely repeatable, not implicit API DDL.
    instance = Engine(settings, tmp_path, environment)
    try:
        instance.start_api()
        yield instance
    finally:
        instance.close()
        with psycopg.connect(database_url, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
