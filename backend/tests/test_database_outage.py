"""A12: opt-in server outages, never stop the project's development database."""
from datetime import datetime
import json
import os
from pathlib import Path
import socket
import subprocess
import time
from uuid import uuid4

import psycopg
import pytest

from conftest import Engine, ROOT
from app.artifacts import write_json_artifact
from app.config import Settings
from app.db import connect, migrate
from app.processes import executor_state
from test_execution import attempts, step

pytestmark = [pytest.mark.integration, pytest.mark.a12]
IMAGE = "postgres:15@sha256:9b1d34adbce1dd07ee6e94b4a2cf698884b89bd44a6c9c12f5da8f3acbfe4957"


def docker(*arguments):
    return subprocess.run(["docker", *arguments], check=True, text=True, capture_output=True, timeout=60, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0)).stdout.strip()


class OwnedPostgres:
    """Only this fixture's returned container ID can be stopped or removed."""
    def __init__(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        self.url = f"postgresql://pipeline:pipeline@127.0.0.1:{port}/pipeline_test"
        self.id = docker("run", "-d", "--name", "observable-pipeline-a12-"+uuid4().hex[:12],
            "--label", "observable-pipeline.test=a12", "-e", "POSTGRES_USER=pipeline",
            "-e", "POSTGRES_PASSWORD=pipeline", "-e", "POSTGRES_DB=pipeline_test",
            "-p", f"127.0.0.1:{port}:5432", IMAGE)
        self.running = True

    def ready(self):
        deadline = time.monotonic()+30
        while time.monotonic() < deadline:
            try:
                with psycopg.connect(self.url, connect_timeout=1) as conn:
                    conn.execute("SELECT 1")
                return
            except psycopg.OperationalError:
                time.sleep(.1)
        raise AssertionError("A12 PostgreSQL did not become ready")

    def stop(self):
        docker("stop", "--timeout", "5", self.id)
        self.running = False

    def start(self):
        if not self.running:
            docker("start", self.id)
            self.running = True
        self.ready()

    def remove(self):
        # Remove only the container and anonymous data volume created above.
        docker("rm", "-f", "-v", self.id)


@pytest.fixture
def outage_engine(tmp_path, request):
    if os.getenv("RUN_A12_POSTGRES_OUTAGE") != "1":
        pytest.skip("Set RUN_A12_POSTGRES_OUTAGE=1 to create and restart an isolated PostgreSQL container")
    database = OwnedPostgres()
    engine = None
    try:
        database.ready()
        env = {**os.environ, "DATABASE_URL": database.url, "PYTHONPATH": str(ROOT/"backend"),
            "ARTIFACT_ROOT": str(tmp_path/"artifacts"), "PIPELINE_PROFILE": "fast-recovery",
            "WORKER_HEARTBEAT_INTERVAL_SECONDS": ".25", "WORKER_LEASE_TTL_SECONDS": str(request.param),
            "COORDINATOR_SCAN_INTERVAL_SECONDS": ".2", "ATTEMPT_HARD_TIMEOUT_SECONDS": "0",
            "TASK_DEADLINE_SECONDS": "0", "GPU_PROVIDER": "mock", "MOCK_GPU_SLOTS": "1",
            "ENABLE_TEST_FAULTS": "false", "PYTHONUNBUFFERED": "1"}
        for key in ("EXTERNAL_EXECUTOR_URL", "EXTERNAL_EXECUTOR_ID", "EXTERNAL_EXECUTOR_SECRET", "EXTERNAL_RPC_TIMEOUT_SECONDS", "EXECUTION_NODES_JSON", "ARTIFACT_STORAGE_ID", "NODE_PROBE_INTERVAL_SECONDS"):
            env.pop(key, None)
        settings = Settings.from_env(env)
        migrate(settings)
        engine = Engine(settings, tmp_path, env)
        engine.start_api()
        yield engine, database
    finally:
        try:
            if engine:
                database.start()
                engine.close()
        finally:
            database.remove()


def wait_until(predicate, timeout=10):
    deadline = time.monotonic()+timeout
    while time.monotonic()<deadline:
        if predicate():
            return
        time.sleep(.05)
    raise AssertionError("A12 condition timed out")


def log_events(engine):
    events = []
    for path in engine.directory.glob("app.worker-*.log"):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.startswith("{"):
                events.append(json.loads(line))
    return events


@pytest.mark.parametrize("outage_engine", [15], indirect=True)
def test_a12_reconnect_within_valid_lease_without_new_claims_during_outage(outage_engine):
    engine, database = outage_engine
    # Keep computation alive beyond container stop/start and HTTP timeout
    # observation. A dead child must NOT be renewed merely to satisfy a test.
    task_id = engine.submit({"mock": {"batch_count": 40, "batch_duration_seconds": .4}})
    engine.worker("a12-valid-owner")
    engine.wait(task_id, lambda task: step(task, "generate_frames")["checkpoint_artifact_id"] is not None)
    before = attempts(engine, task_id, "generate_frames")[0]
    queued = engine.submit({"mock": {"batch_count": 1, "batch_duration_seconds": 0, "branch_duration_seconds": 0}})
    disconnected_at = time.monotonic()
    database.stop()
    idle = engine.worker("a12-idle-during-outage")
    response = engine.client.get(f"/api/v1/tasks/{task_id}")
    assert response.status_code == 503, response.text
    assert response.json()["error"]["code"] == "DATABASE_UNAVAILABLE"
    rejected = engine.client.post("/api/v1/tasks", json={}, headers={"Idempotency-Key": "offline-submit"})
    assert rejected.status_code == 503
    time.sleep(.4)
    assert idle.poll() is None
    assert not any(event.get("event") == "attempt_claimed" and event.get("task_id") == queued for event in log_events(engine))
    database.start()
    outage = time.monotonic()-disconnected_at
    assert outage < 15, "Outage exceeded this case's explicitly selected valid lease"
    with connect(engine.settings) as conn:
        restarted_at = conn.execute("SELECT pg_postmaster_start_time() AS started").fetchone()["started"]
    assert engine.wait(task_id)["status"] == "SUCCEEDED", engine.logs()
    assert engine.wait(queued)["status"] == "SUCCEEDED", engine.logs()
    history = attempts(engine, task_id, "generate_frames")
    assert [item["status"] for item in history] == ["SUCCEEDED"]
    assert history[0]["id"] == before["id"]
    assert datetime.fromisoformat(history[0]["heartbeat_at"]) > restarted_at
    queued_attempt = attempts(engine, queued, "normalize_asset")[0]
    assert datetime.fromisoformat(queued_attempt["started_at"]) >= restarted_at
    assert any(event.get("event") == "database_retry" and event.get("task_id") == task_id for event in log_events(engine))
    assert engine.repo.resources()["slots"][0]["state"] == "AVAILABLE"
    print(json.dumps({"scenario": "A12-within-lease", "task_id": task_id, "attempt_id": before["id"], "outage_seconds": round(outage, 3), "lease_seconds": 15, "attempt_statuses": [a["status"] for a in history], "queued_task_id": queued, "offline_api_status": response.status_code}))


@pytest.mark.parametrize("outage_engine", [4], indirect=True)
def test_a12_expired_lease_stops_child_fences_old_writes_and_recovers_checkpoint(outage_engine):
    engine, database = outage_engine
    task_id = engine.submit({"mock": {"batch_count": 40, "batch_duration_seconds": .25}})
    worker = engine.worker("a12-expiring-owner")
    running = engine.wait(task_id, lambda task: step(task, "generate_frames")["checkpoint_artifact_id"] is not None)
    active = step(running, "generate_frames")
    old = attempts(engine, task_id, "generate_frames")[0]
    claim = {"task_id": task_id, "step_id": active["id"], "step_key": "generate_frames", "attempt_id": old["id"], "generation": active["execution_generation"], "worker_id": old["worker_id"], "compatibility_key": active["compatibility_key"]}
    disconnected_at = time.monotonic()
    database.stop()
    time.sleep(5.2)
    wait_until(lambda: executor_state(old["executor_pid"], old["executor_created_at"], old["executor_host"]) == "EXITED", timeout=5)
    wait_until(lambda: any(e.get("event") == "lease_lost" and e.get("attempt_id") == old["id"] for e in log_events(engine)))
    worker.terminate()
    worker.wait(timeout=5)
    database.start()
    outage = time.monotonic()-disconnected_at
    assert outage > 4
    persisted = engine.task(task_id)
    committed_id = step(persisted, "generate_frames")["checkpoint_artifact_id"]
    checkpoint = next(item for item in engine.repo.artifacts(task_id) if item["id"] == committed_id)
    prefix = checkpoint["metadata"]["completed_batches"]
    assert prefix
    old_namespace = engine.settings.artifact_root/task_id/"generate_frames"/old["id"]
    assert len(list(old_namespace.glob("*batch-*.json"))) > len(prefix), "Expected uncommitted candidate batches during outage"
    late = write_json_artifact(engine.settings.artifact_root, claim, "checkpoint", checkpoint["metadata"])
    with connect(engine.settings) as conn:
        assert conn.execute("SELECT lease_expires_at<clock_timestamp() AS expired FROM step_attempts WHERE id=%s", (old["id"],)).fetchone()["expired"]
    # No replacement exists yet: expiry alone must reject resurrection/writes.
    assert engine.repo.heartbeat(claim) is False
    assert engine.repo.progress(claim, {"completed_units": 999}) is False
    assert engine.repo.commit_checkpoint(claim, late) is False
    engine.worker("a12-replacement")
    final = engine.wait(task_id, timeout=30)
    assert final["status"] == "SUCCEEDED", engine.logs()
    history = attempts(engine, task_id, "generate_frames")
    assert [item["status"] for item in history] == ["LOST", "SUCCEEDED"]
    assert history[1]["resumed_from_checkpoint_id"] == committed_id
    output = next(item for item in engine.repo.artifacts(task_id) if item["id"] == step(final, "generate_frames")["output_refs"][0])
    assert output["metadata"]["resumed_batches"] == len(prefix)
    assert output["metadata"]["completed_batches"][:len(prefix)] == prefix
    assert all(history[1]["id"] in item["relative_path"] for item in output["metadata"]["completed_batches"][len(prefix):])
    assert len(attempts(engine, task_id, "normalize_asset")) == 1
    assert history[0]["resource_cleanup_state"] == "RELEASED"
    assert engine.repo.resources()["slots"][0]["state"] == "AVAILABLE"
    print(json.dumps({"scenario": "A12-expired-lease", "task_id": task_id, "old_attempt_id": old["id"], "new_attempt_id": history[1]["id"], "outage_seconds": round(outage, 3), "lease_seconds": 4, "attempt_statuses": [a["status"] for a in history], "resumed_batches": len(prefix), "uncommitted_candidates_reused": False}))
