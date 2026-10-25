import json
import time
from datetime import datetime

import pytest

from app.artifacts import write_json_artifact
from app.db import connect
from app.processes import stop_executor

pytestmark = pytest.mark.integration


def step(task, key):
    return next(s for s in task["steps"] if s["step_key"] == key)


def attempts(engine, task_id, key):
    response = engine.client.get(f"/api/v1/tasks/{task_id}/steps/{key}/attempts")
    assert response.status_code == 200, response.text
    return response.json()


def terminate_pid(pid, created_at, host):
    assert stop_executor(pid, created_at, host), "Could not confirm this test executor stopped"


def test_cross_three_leases_api_restart_and_submission_idempotency(engine):
    body = {"pipeline_id": "smoke-v1", "mock": {"duration_seconds": 5.2}, "cache_enabled": False, "policy": {"attempt_hard_timeout_seconds": 0, "task_deadline_seconds": 0}}
    started = time.monotonic()
    task_id = engine.submit(body, "long-run")
    assert engine.submit(body, "long-run") == task_id
    conflict = engine.client.post("/api/v1/tasks", json={**body, "input": {"kind": "synthetic", "seed": 99}}, headers={"Idempotency-Key": "long-run"})
    assert conflict.status_code == 409
    engine.worker()
    engine.wait(task_id, lambda t: step(t, "mock")["status"] == "RUNNING")
    engine.api.terminate()
    engine.api.wait(timeout=5)
    engine.start_api()
    task = engine.wait(task_id)
    assert task["status"] == "SUCCEEDED", engine.logs()
    assert time.monotonic() - started >= 5.2
    history = attempts(engine, task_id, "mock")
    assert [a["status"] for a in history] == ["SUCCEEDED"]
    assert (datetime.fromisoformat(history[0]["heartbeat_at"]) - datetime.fromisoformat(history[0]["started_at"])).total_seconds() >= 4.5
    assert task["config_snapshot"]["runtime_policy"]["attempt_hard_timeout_seconds"] is None
    events = engine.client.get(f"/api/v1/tasks/{task_id}/events").json()
    assert [e["seq"] for e in events] == list(range(1, len(events)+1))
    assert not any(e["type"] == "ATTEMPT_LOST" for e in events)
    metadata = engine.client.get(f"/api/v1/tasks/{task_id}/artifacts").json()
    assert metadata and metadata[-1]["availability"] == "AVAILABLE"
    assert engine.client.get(metadata[-1]["download_url"]).status_code == 200


def test_killed_supervisor_checkpoint_takeover_and_all_old_commits_fenced(engine):
    task_id = engine.seed_legacy({"pipeline_id": "animation-v1", "mock": {"batch_count": 18, "batch_duration_seconds": .25}, "cache_enabled": False})
    worker = engine.worker("old-supervisor")
    task = engine.wait(task_id, lambda t: step(t, "generate_frames")["checkpoint_artifact_id"] is not None)
    current = step(task, "generate_frames")
    history = attempts(engine, task_id, "generate_frames")
    old_attempt = history[0]
    claim = {"task_id": task_id, "step_id": current["id"], "step_key": "generate_frames", "attempt_id": old_attempt["id"], "generation": current["execution_generation"], "worker_id": "old-supervisor", "compatibility_key": current["compatibility_key"]}
    # An old executor's fully written file is still only a candidate.
    candidate = write_json_artifact(engine.settings.artifact_root, claim, "output", {"compatibility_key": claim["compatibility_key"], "source": "mock", "late_candidate": True})
    previous_checkpoint = next(a for a in engine.repo.artifacts(task_id) if a["id"] == current["checkpoint_artifact_id"])
    late_checkpoint = write_json_artifact(engine.settings.artifact_root, claim, "checkpoint", previous_checkpoint["metadata"])
    worker.kill()
    worker.wait(timeout=5)
    try:
        engine.worker("new-supervisor")
        # Reject old writes while replacement work is RUNNING, not only after
        # Task=SUCCEEDED (which alone could reject all writes).
        replacement = engine.wait(task_id, lambda t: step(t, "generate_frames")["status"] == "RUNNING" and step(t, "generate_frames")["execution_generation"] == 2)
        assert replacement["status"] == "RUNNING"
        old_events = [e for e in engine.repo.events(task_id) if e["attempt_id"] == old_attempt["id"]]
        assert engine.repo.heartbeat(claim) is False
        assert engine.repo.progress(claim, {"completed_units": 999}) is False
        assert engine.repo.commit_checkpoint(claim, late_checkpoint) is False
        assert engine.repo.succeed(claim, candidate) is False
        assert engine.repo.fail(claim, "TRANSIENT_ERROR", "late failure", True) is False
        assert engine.repo.warning(claim, "STALE", "late warning") is False
        assert [e for e in engine.repo.events(task_id) if e["attempt_id"] == old_attempt["id"]] == old_events
        final = engine.wait(task_id)
        assert final["status"] == "SUCCEEDED", engine.logs()
        history = attempts(engine, task_id, "generate_frames")
        assert [a["status"] for a in history] == ["LOST", "SUCCEEDED"]
        assert history[0]["process_exit_confirmed_at"] is not None
        assert history[0]["resource_cleanup_state"] == "RELEASED"
        assert history[1]["resumed_from_checkpoint_id"] is not None
        assert len(attempts(engine, task_id, "normalize_asset")) == 1
        artifacts = engine.client.get(f"/api/v1/tasks/{task_id}/artifacts").json()
        output_id = step(final, "generate_frames")["output_refs"][0]
        output = next(a for a in artifacts if a["id"] == output_id)
        assert output["metadata"]["resumed_batches"] >= 1
        batches = output["metadata"]["completed_batches"]
        assert f"/{old_attempt['id']}/" in "/" + batches[0]["relative_path"]
        before = engine.client.get(f"/api/v1/tasks/{task_id}/events").json()
        assert engine.repo.heartbeat(claim) is False
        assert engine.repo.progress(claim, {"completed_units": 999}) is False
        assert engine.repo.commit_checkpoint(claim, candidate) is False
        assert engine.repo.succeed(claim, candidate) is False
        assert engine.repo.fail(claim, "TRANSIENT_ERROR", "late failure", True) is False
        assert engine.task(task_id)["output_refs"] == final["output_refs"]
        assert engine.client.get(f"/api/v1/tasks/{task_id}/events").json() == before
        assert not any(a["id"] == candidate["id"] for a in engine.client.get(f"/api/v1/tasks/{task_id}/artifacts").json())
        assert not any(a["id"] == late_checkpoint["id"] for a in engine.repo.artifacts(task_id))
        events = engine.repo.events(task_id)
        quarantined = next(e for e in events if e["type"] == "RESOURCE_QUARANTINED" and e["attempt_id"] == old_attempt["id"])
        released = next(e for e in events if e["type"] == "RESOURCE_RELEASED" and e["attempt_id"] == old_attempt["id"])
        assert quarantined["seq"] < released["seq"]
    finally:
        if old_attempt["executor_pid"]:
            terminate_pid(old_attempt["executor_pid"], old_attempt["executor_created_at"], old_attempt["executor_host"])


def test_finite_retry_and_manual_recovery_preserve_success_and_checkpoint(engine):
    task_id = engine.seed_legacy({"pipeline_id": "animation-v1", "mock": {"batch_count": 3, "batch_duration_seconds": .05, "fail_first_attempts": 1}, "retry_policy": {"max_attempts_per_recovery_round": 1}, "cache_enabled": False})
    engine.worker()
    failed = engine.wait(task_id)
    assert failed["status"] == "FAILED", engine.logs()
    assert step(failed, "normalize_asset")["status"] == "SUCCEEDED"
    assert step(failed, "encode_package")["status"] == "BLOCKED"
    assert attempts(engine, task_id, "generate_frames")[0]["error_code"] == "TRANSIENT_ERROR"
    headers = {"Idempotency-Key": "manual-recovery"}
    first = engine.client.post(f"/api/v1/tasks/{task_id}/steps/generate_frames/retry", headers=headers)
    repeated = engine.client.post(f"/api/v1/tasks/{task_id}/steps/generate_frames/retry", headers=headers)
    assert first.status_code == repeated.status_code == 200
    assert first.json() == repeated.json() == {"id": task_id, "recovery_round": 1}
    assert engine.client.post(f"/api/v1/tasks/{task_id}/resume", headers=headers).status_code == 409
    final = engine.wait(task_id)
    assert final["status"] == "SUCCEEDED", engine.logs()
    assert len(attempts(engine, task_id, "normalize_asset")) == 1
    generated = attempts(engine, task_id, "generate_frames")
    assert [a["recovery_round"] for a in generated] == [0, 1]
    assert generated[1]["resumed_from_checkpoint_id"] is not None
    output = next(a for a in engine.repo.artifacts(task_id) if a["id"] == step(final, "generate_frames")["output_refs"][0])
    assert output["metadata"]["resumed_batches"] == 3


def test_automatic_retry_preserved_but_cross_task_cache_disabled(engine):
    engine.worker()
    first = engine.submit({"pipeline_id": "smoke-v1", "mock": {"duration_seconds": .05, "fail_first_attempts": 1}})
    assert engine.wait(first)["status"] == "SUCCEEDED", engine.logs()
    assert [a["status"] for a in attempts(engine, first, "mock")] == ["FAILED", "SUCCEEDED"]
    reused = engine.submit({"pipeline_id": "smoke-v1", "mock": {"duration_seconds": .05}})
    cached = engine.wait(reused)
    assert cached["status"] == "SUCCEEDED"
    assert step(cached, "mock")["status"] == "SUCCEEDED"
    assert step(cached, "mock")["reused_from_step_id"] is None
    assert len(attempts(engine, reused, "mock")) == 1
    rejected = engine.client.post("/api/v1/tasks", json={"cache_enabled": True, "cache_scope": "other-user"}, headers={"Idempotency-Key": "invalid-cache-scope"})
    assert rejected.status_code == 422
    changed = engine.submit({"pipeline_id": "smoke-v1", "processor_version": "2", "mock": {"duration_seconds": .05}})
    assert step(engine.wait(changed), "mock")["status"] == "SUCCEEDED"
    assert len(attempts(engine, changed, "mock")) == 1


def test_explicit_timeout_and_queued_deadline_have_distinct_semantics(engine):
    queued = engine.submit({"pipeline_id": "smoke-v1", "policy": {"task_deadline_seconds": .2}, "cache_enabled": False})
    time.sleep(.25)
    engine.worker()
    assert engine.wait(queued)["error_code"] == "TASK_DEADLINE"
    assert attempts(engine, queued, "mock") == []
    task_id = engine.submit({"pipeline_id": "smoke-v1", "mock": {"duration_seconds": 2}, "policy": {"attempt_hard_timeout_seconds": .4}, "cache_enabled": False})
    task = engine.wait(task_id)
    assert task["status"] == "FAILED", engine.logs()
    history = attempts(engine, task_id, "mock")
    assert [a["status"] for a in history] == ["TIMED_OUT"]
    assert history[0]["error_code"] == "ATTEMPT_TIMEOUT"
    assert history[0]["process_exit_confirmed_at"] is not None


def test_dead_child_not_renewed_forever_and_silent_child_only_warned(engine):
    engine.worker()
    task_id = engine.submit({"pipeline_id": "smoke-v1", "mock": {"duration_seconds": 10}, "retry_policy": {"max_attempts_per_recovery_round": 1}, "cache_enabled": False})
    engine.wait(task_id, lambda t: step(t, "mock")["status"] == "RUNNING")
    deadline = time.monotonic()+5
    pid = None
    while time.monotonic() < deadline:
        executor = attempts(engine, task_id, "mock")[0]
        pid = executor["executor_pid"]
        if pid:
            break
        time.sleep(.02)
    assert pid
    terminate_pid(pid, executor["executor_created_at"], executor["executor_host"])
    assert engine.wait(task_id)["status"] == "FAILED", engine.logs()
    assert attempts(engine, task_id, "mock")[0]["error_code"] == "EXECUTOR_EXIT"
    silent = engine.submit({"pipeline_id": "smoke-v1", "mock": {"duration_seconds": 1.2}, "policy": {"step_soft_warning_seconds": .1, "task_soft_warning_seconds": .1}, "cache_enabled": False})
    assert engine.wait(silent)["status"] == "SUCCEEDED", engine.logs()
    events = engine.client.get(f"/api/v1/tasks/{silent}/events").json()
    assert any(e["type"] == "SOFT_WARNING" for e in events)
    assert len(attempts(engine, silent, "mock")) == 1


def test_missing_committed_artifact_blocks_dependents_not_history(engine):
    task_id = engine.seed_legacy({"pipeline_id": "animation-v1", "cache_enabled": False})
    claim = engine.repo.claim("manual-test")
    assert claim["step_key"] == "normalize_asset"
    artifact = write_json_artifact(engine.settings.artifact_root, claim, "output", {"compatibility_key": claim["compatibility_key"], "source": "mock"})
    assert engine.repo.succeed(claim, artifact)
    (engine.settings.artifact_root / artifact["relative_path"]).unlink()
    assert engine.repo.claim("manual-test") is None
    task = engine.task(task_id)
    assert step(task, "normalize_asset")["status"] == "SUCCEEDED"
    assert step(task, "generate_frames")["wait_reason"] == "ARTIFACT_UNAVAILABLE"
    unavailable = engine.repo.artifacts(task_id)
    assert unavailable[0]["availability"] == "UNAVAILABLE"
    assert engine.client.get(unavailable[0]["download_url"]).status_code == 409


def test_phase_one_fixed_three_steps_persist_and_survive_api_restart(engine):
    task_id = engine.seed_legacy({"mock": {"batch_count": 3, "batch_duration_seconds": .4}})
    engine.worker("phase-one-worker")
    engine.wait(task_id, lambda t: step(t, "generate_frames")["status"] == "RUNNING")
    engine.api.terminate()
    engine.api.wait(timeout=5)
    engine.start_api()
    task = engine.wait(task_id)
    assert task["pipeline_id"] == "animation-v1"
    assert task["status"] == "SUCCEEDED", engine.logs()
    assert {s["step_key"] for s in task["steps"]} == {"normalize_asset", "generate_frames", "encode_package"}
    assert all(s["status"] == "SUCCEEDED" for s in task["steps"])
    for key in ("normalize_asset", "generate_frames", "encode_package"):
        assert [a["status"] for a in attempts(engine, task_id, key)] == ["SUCCEEDED"]
    result_id = task["output_refs"][0]
    artifacts = engine.client.get(f"/api/v1/tasks/{task_id}/artifacts").json()
    result = next(a for a in artifacts if a["id"] == result_id)
    payload = engine.client.get(result["download_url"]).json()
    assert payload["source"] == "mock"
    assert payload["result_manifest"]["format"] == "mock-animation-package-v1"
    assert payload["result_manifest"]["frame_manifests"]
    with connect(engine.settings) as conn:
        assert conn.execute("SELECT count(*) AS n FROM steps WHERE task_id=%s", (task_id,)).fetchone()["n"] == 3
        assert conn.execute("SELECT count(*) AS n FROM task_events WHERE task_id=%s", (task_id,)).fetchone()["n"] >= 8
