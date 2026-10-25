"""Real local processes and PostgreSQL: no separate coordinator or fake GPU state."""
from datetime import datetime
import json
import subprocess
import sys
import time

import pytest

from app.artifacts import write_json_artifact
from app.db import connect
from app.processes import current_host
from test_execution import attempts, step

pytestmark = pytest.mark.integration


def wait_until(condition, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = condition()
        if value:
            return value
        time.sleep(.05)
    raise AssertionError("Recovery condition timed out")


def slot(engine):
    response = engine.client.get("/api/v1/resources")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["source"] == "mock" and body["physical_backend"] == "cpu"
    assert len(body["slots"]) == 1
    return body["slots"][0]


def test_busy_supervisor_scans_and_same_worker_takes_over(engine):
    stranded = engine.submit({"pipeline_id": "smoke-v1", "mock": {"duration_seconds": .05}})
    abandoned = engine.repo.claim("crashed-before-executor")
    assert abandoned["task_id"] == stranded
    long_task = engine.submit({"pipeline_id": "smoke-v1", "mock": {"duration_seconds": 5.2}})
    engine.worker("sole-worker")
    engine.wait(long_task, lambda t: step(t, "mock")["status"] == "RUNNING")
    wait_until(lambda: attempts(engine, stranded, "mock")[0]["status"] == "LOST", timeout=4)
    # The only live Worker is still supervising its long, silent child. Its
    # in-supervision scan, not a coordinator/idle loop, discovered the loss.
    assert engine.task(long_task)["status"] == "RUNNING"
    assert engine.wait(long_task)["status"] == "SUCCEEDED", engine.logs()
    assert engine.wait(stranded)["status"] == "SUCCEEDED", engine.logs()
    assert [a["status"] for a in attempts(engine, stranded, "mock")] == ["LOST", "SUCCEEDED"]
    assert len(attempts(engine, long_task, "mock")) == 1


def test_single_mock_slot_mutex_waits_without_creating_attempt(engine):
    first = engine.seed_legacy({"mock": {"batch_count": 1, "batch_duration_seconds": 3}})
    engine.worker("gpu-owner")
    engine.wait(first, lambda t: step(t, "generate_frames")["status"] == "RUNNING")
    wait_until(lambda: slot(engine)["state"] == "RUNNING")
    second = engine.seed_legacy({"mock": {"batch_count": 1, "batch_duration_seconds": .2}})
    engine.worker("gpu-waiter")
    waiting = engine.wait(second, lambda t: step(t, "generate_frames")["wait_reason"] == "WAITING_GPU")
    assert step(waiting, "generate_frames")["status"] == "READY"
    assert attempts(engine, second, "generate_frames") == []
    assert slot(engine)["owner_attempt_id"] == attempts(engine, first, "generate_frames")[0]["id"]
    assert engine.wait(first)["status"] == "SUCCEEDED", engine.logs()
    assert engine.wait(second)["status"] == "SUCCEEDED", engine.logs()
    old, new = attempts(engine, first, "generate_frames"), attempts(engine, second, "generate_frames")
    assert len(old) == len(new) == 1
    assert datetime.fromisoformat(old[0]["process_exit_confirmed_at"]) <= datetime.fromisoformat(new[0]["started_at"])
    available = slot(engine)
    assert available["state"] == "AVAILABLE" and available["owner_attempt_id"] is None


def complete_preparation(engine, task_id):
    claim = engine.repo.claim("fixture-preparation")
    assert claim["task_id"] == task_id and claim["step_key"] == "normalize_asset"
    output = write_json_artifact(engine.settings.artifact_root, claim, "output", {"source": "mock", "compatibility_key": claim["compatibility_key"]})
    assert engine.repo.succeed(claim, output)


def test_uncertain_slot_stays_quarantined_until_identity_and_exit_confirmed(engine):
    first = engine.seed_legacy({"mock": {"batch_count": 1, "batch_duration_seconds": 30}})
    complete_preparation(engine, first)
    old = engine.repo.claim("abandoned-gpu-owner")
    assert old["task_id"] == first and old["resource_slot_id"] == 1
    second = engine.seed_legacy({"mock": {"batch_count": 1, "batch_duration_seconds": .05}})
    complete_preparation(engine, second)
    process = subprocess.Popen(
        [sys.executable, "-m", "app.executor", "--attempt-id", old["attempt_id"], "--generation", str(old["generation"])],
        env=engine.environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        text=True, encoding="utf-8", creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    try:
        assert engine.repo.record_executor(old, process.pid)
        process.stdin.write(json.dumps({"claim": old, "artifact_root": str(engine.settings.artifact_root)}, default=str)+"\n")
        process.stdin.close()
        assert json.loads(process.stdout.readline())["type"] == "progress"
        with connect(engine.settings) as conn:
            conn.execute("UPDATE step_attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", (old["attempt_id"],))
        engine.repo.reconcile()
        quarantined = slot(engine)
        assert quarantined["state"] == "QUARANTINED"
        assert quarantined["owner_attempt_id"] == old["attempt_id"]
        assert quarantined["process_exit_confirmed_at"] is None
        assert process.poll() is None  # Lease loss did not magically stop it.
        assert engine.repo.claim("waiting-worker") is None
        assert attempts(engine, second, "generate_frames") == []
        assert step(engine.task(second), "generate_frames")["wait_reason"] == "WAITING_GPU"

        # An unverified host/identity cannot authorize local termination or
        # release, even though its database lease has definitely expired.
        with connect(engine.settings) as conn:
            conn.execute("UPDATE step_attempts SET executor_host='unknown-host' WHERE id=%s", (old["attempt_id"],))
        engine.repo.reconcile_resources()
        assert slot(engine)["state"] == "QUARANTINED"
        assert process.poll() is None
        with connect(engine.settings) as conn:
            conn.execute("UPDATE step_attempts SET executor_host=%s WHERE id=%s", (current_host(), old["attempt_id"]))
        process.terminate()
        process.wait(timeout=5)
        engine.repo.reconcile_resources()
        assert slot(engine)["state"] == "AVAILABLE"
        old_history = attempts(engine, first, "generate_frames")[0]
        assert old_history["status"] == "LOST"
        assert old_history["process_exit_confirmed_at"] is not None
        assert old_history["resource_cleanup_state"] == "RELEASED"

        new = engine.repo.claim("waiting-worker")
        assert new["task_id"] == second
        assert slot(engine)["owner_attempt_id"] == new["attempt_id"]
        assert engine.repo.fail(old, "EXECUTOR_EXIT", "stale cleanup callback", True) is False
        assert slot(engine)["owner_attempt_id"] == new["attempt_id"]
        # This test never started a child for the new claim; owned startup
        # failure can truthfully confirm that no process needs cleanup.
        assert engine.repo.fail(new, "EXECUTOR_EXIT", "test stops before executor launch", False, True)
        assert slot(engine)["state"] == "AVAILABLE"
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        for stream in (process.stdin, process.stdout):
            if stream is not None and not stream.closed:
                stream.close()


def test_encode_failure_resume_preserves_generated_frames(engine):
    task_id = engine.submit({"mock": {"batch_count": 2, "batch_duration_seconds": .05, "fail_first_attempts": 1, "fail_step": "encode_package"}, "retry_policy": {"max_attempts_per_recovery_round": 1}})
    engine.worker("resume-worker")
    failed = engine.wait(task_id)
    assert failed["status"] == "FAILED", engine.logs()
    original = step(failed, "generate_frames")["output_refs"]
    assert step(failed, "generate_frames")["status"] == "SUCCEEDED"
    headers = {"Idempotency-Key": "resume-encoding"}
    response = engine.client.post(f"/api/v1/tasks/{task_id}/resume", headers=headers)
    repeated = engine.client.post(f"/api/v1/tasks/{task_id}/resume", headers=headers)
    assert response.status_code == repeated.status_code == 200
    assert response.json() == repeated.json()
    final = engine.wait(task_id)
    assert final["status"] == "SUCCEEDED", engine.logs()
    assert step(final, "generate_frames")["output_refs"] == original
    assert len(attempts(engine, task_id, "normalize_asset")) == len(attempts(engine, task_id, "generate_frames")) == 1
    assert [a["status"] for a in attempts(engine, task_id, "encode_package")] == ["FAILED", "SUCCEEDED"]
    assert slot(engine)["state"] == "AVAILABLE"
