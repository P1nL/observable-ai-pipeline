"""A12 on the external HTTP path, using only an owned disposable PostgreSQL."""
from datetime import datetime
import json
import time

import pytest

from app.artifacts import write_json_artifact
from app.backends import ExternalClient
from app.db import connect
from test_database_outage import outage_engine, wait_until, log_events
from test_execution import attempts, step
from test_external_execution import external_service, command, remote_status

pytestmark = pytest.mark.integration


@pytest.fixture(params=["builtin", "standalone"])
def external_outage(outage_engine, request, monkeypatch):
    engine, database = outage_engine
    engine.executor_implementation = request.param
    if request.param == "standalone":
        original = engine.submit
        monkeypatch.setattr(engine, "submit", lambda body, key=None: original({"processor_version": "independent-mock-v1", **body}, key))
    with external_service(engine, implementation=request.param):
        try:
            yield engine, database
        finally:
            # Restore OUR container before bound-job cleanup queries its DB.
            database.start()


@pytest.mark.parametrize("outage_engine", [15], indirect=True)
def test_external_a12_reconnect_within_lease_keeps_same_job(external_outage):
    engine, database = external_outage
    task_id = engine.submit({"execution_backend": "external", "bypass_cache": True,
        "mock": {"batch_count": 40, "batch_duration_seconds": .4, "branch_duration_seconds": .01}})
    engine.worker("external-a12-valid")
    before = engine.wait(task_id, lambda t: step(t, "generate_frames")["checkpoint_artifact_id"] is not None)
    old = attempts(engine, task_id, "generate_frames")[0]
    original_refs = {key: step(before, key)["output_refs"] for key in ("normalize_asset", "segment_subject", "analyze_motion")}
    queued = engine.submit({"execution_backend": "external", "bypass_cache": True, "mock": {"batch_count": 1, "batch_duration_seconds": 0}})
    started = time.monotonic()
    database.stop()
    idle = engine.worker("external-a12-idle")
    response = engine.client.get(f"/api/v1/tasks/{task_id}")
    assert response.status_code == 503 and response.json()["error"]["code"] == "DATABASE_UNAVAILABLE"
    assert engine.client.post("/api/v1/tasks", json={}, headers={"Idempotency-Key": "external-offline"}).status_code == 503
    assert idle.poll() is None
    assert not any(e.get("event") == "attempt_claimed" and e.get("task_id") == queued for e in log_events(engine))
    database.start()
    elapsed = time.monotonic() - started
    assert elapsed < 15
    with connect(engine.settings) as conn:
        restarted = conn.execute("SELECT pg_postmaster_start_time() AS t").fetchone()["t"]
    final = engine.wait(task_id, timeout=35)
    assert final["status"] == "SUCCEEDED", engine.logs()
    assert engine.wait(queued)["status"] == "SUCCEEDED", engine.logs()
    history = attempts(engine, task_id, "generate_frames")
    assert len(history) == 1 and history[0]["id"] == old["id"] and history[0]["status"] == "SUCCEEDED"
    assert datetime.fromisoformat(history[0]["heartbeat_at"]) > restarted
    assert remote_status(engine, old["id"])["start_count"] == 1
    for key, refs in original_refs.items():
        assert step(final, key)["output_refs"] == refs and len(attempts(engine, task_id, key)) == 1
    assert datetime.fromisoformat(attempts(engine, queued, "normalize_asset")[0]["started_at"]) >= restarted
    assert engine.repo.resources()["slots"][0]["state"] == "AVAILABLE"
    print(json.dumps({"scenario": "external-A12-within-lease", "executor_implementation": engine.executor_implementation, "task_id": task_id,
        "attempt_id": old["id"], "outage_seconds": round(elapsed, 3), "lease_seconds": 15, "start_count": 1}))


@pytest.mark.parametrize("outage_engine", [4], indirect=True)
def test_external_a12_expiry_fences_candidates_before_clean_takeover(external_outage):
    engine, database = external_outage
    task_id = engine.submit({"execution_backend": "external", "bypass_cache": True,
        "mock": {"batch_count": 40, "batch_duration_seconds": .25, "branch_duration_seconds": .01}})
    owner = engine.worker("external-a12-expiring")
    before = engine.wait(task_id, lambda t: step(t, "generate_frames")["checkpoint_artifact_id"] is not None)
    old = attempts(engine, task_id, "generate_frames")[0]
    persisted_command = command(engine, old["id"])
    claim = persisted_command["payload"]
    remote = ExternalClient(engine.settings, persisted_command["binding"])
    originals = {key: step(before, key)["output_refs"] for key in ("normalize_asset", "segment_subject", "analyze_motion")}
    started = time.monotonic()
    database.stop()
    time.sleep(5.2)
    wait_until(lambda: remote.call("GET")["exit_confirmed"], timeout=6)
    wait_until(lambda: any(e.get("event") == "lease_lost" and e.get("attempt_id") == old["id"] for e in log_events(engine)))
    owner.terminate(); owner.wait(timeout=5)
    database.start()
    elapsed = time.monotonic() - started
    assert elapsed > 4
    task = engine.task(task_id)
    checkpoint_id = step(task, "generate_frames")["checkpoint_artifact_id"]
    checkpoint = next(a for a in engine.repo.artifacts(task_id) if a["id"] == checkpoint_id)
    prefix = checkpoint["metadata"]["completed_batches"]
    namespace = engine.settings.artifact_root / task_id / "generate_frames" / old["id"]
    assert prefix and len(list(namespace.glob("*batch-*.json"))) > len(prefix)
    late = write_json_artifact(engine.settings.artifact_root, claim, "checkpoint", checkpoint["metadata"])
    assert not engine.repo.heartbeat(claim)
    assert not engine.repo.progress(claim, {"completed_units": 999})
    assert not engine.repo.commit_checkpoint(claim, late)
    assert not engine.repo.succeed(claim, late)
    assert len(attempts(engine, task_id, "generate_frames")) == 1
    engine.worker("external-a12-replacement")
    final = engine.wait(task_id, timeout=30)
    assert final["status"] == "SUCCEEDED", engine.logs()
    history = attempts(engine, task_id, "generate_frames")
    assert [a["status"] for a in history] == ["LOST", "SUCCEEDED"]
    assert history[1]["resumed_from_checkpoint_id"] == checkpoint_id
    assert history[0]["resource_cleanup_state"] == "RELEASED"
    assert datetime.fromisoformat(history[0]["process_exit_confirmed_at"]) <= datetime.fromisoformat(history[1]["started_at"])
    output = next(a for a in engine.repo.artifacts(task_id) if a["id"] in step(final, "generate_frames")["output_refs"])
    assert output["metadata"]["resumed_batches"] == len(prefix)
    assert output["metadata"]["completed_batches"][:len(prefix)] == prefix
    assert all(history[1]["id"] in ref["relative_path"] for ref in output["metadata"]["completed_batches"][len(prefix):])
    for key, refs in originals.items():
        assert step(final, key)["output_refs"] == refs and len(attempts(engine, task_id, key)) == 1
    assert all(remote_status(engine, a["id"])["start_count"] == 1 for a in history)
    assert engine.repo.resources()["slots"][0]["state"] == "AVAILABLE"
    print(json.dumps({"scenario": "external-A12-expired", "executor_implementation": engine.executor_implementation, "task_id": task_id, "old_attempt_id": old["id"],
        "new_attempt_id": history[1]["id"], "outage_seconds": round(elapsed, 3), "lease_seconds": 4,
        "resumed_batches": len(prefix), "candidate_batches_reused": False}))
