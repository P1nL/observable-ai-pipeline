"""Interop evidence for a document-only implementation, not the bundled executor."""
import json
from datetime import datetime
from urllib.error import HTTPError

import pytest

from app.backends import ExternalClient
from test_execution import attempts, step
from test_external_execution import (
    external_service, command, remote_status,
    test_start_response_loss_retries_one_durable_command_without_duplicate_compute as test_standalone_response_loss,
    test_external_failed_step_resume_reuses_success_and_committed_checkpoint as test_standalone_resume,
    test_external_worker_kill_takeover_and_late_attempt_fencing as test_standalone_takeover_fencing,
    test_external_disconnect_cancel_quarantines_until_actual_exit_reconciled as test_standalone_disconnect_cancel,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def external(engine, monkeypatch):
    original_submit = engine.submit
    def submit(body, key=None):
        return original_submit({"processor_version": "independent-mock-v1", "bypass_cache": True, **body}, key)
    monkeypatch.setattr(engine, "submit", submit)
    with external_service(engine, implementation="standalone"):
        yield engine


def test_standalone_six_steps_real_json_and_bound_auth(external):
    engine = external
    task_id = engine.submit({"execution_backend": "external", "mock": {"batch_count": 3, "batch_duration_seconds": .05}})
    engine.worker("independent-six")
    task = engine.wait(task_id)
    assert task["status"] == "SUCCEEDED" and len(task["steps"]) == 6, engine.logs()
    artifacts = engine.repo.artifacts(task_id)
    for current in task["steps"]:
        history = attempts(engine, task_id, current["step_key"])
        assert len(history) == 1 and history[0]["execution_backend"] == "external"
        assert remote_status(engine, history[0]["id"])["start_count"] == 1
        record = command(engine, history[0]["id"])
        assert record["dispatched_at"] and record["exit_confirmed_at"]
        artifact = next(a for a in artifacts if a["id"] in current["output_refs"])
        decoded = json.loads((engine.settings.artifact_root / artifact["relative_path"]).read_bytes())
        assert decoded == artifact["metadata"] and decoded["source"] == "mock"
        assert decoded["processor_version"] == "independent-mock-v1"
    final = next(a for a in artifacts if a["id"] in task["output_refs"])
    assert final["metadata"]["result_manifest"]["format"] == "mock-animation-package-v1"
    assert final["metadata"]["publication"] == "local-only-mock"
    first = attempts(engine, task_id, "normalize_asset")[0]
    binding = command(engine, first["id"])["binding"]
    # A correctly signed DIFFERENT binding still cannot read/stop this job.
    wrong = ExternalClient(engine.settings, {**binding, "worker_id": "not-owner"})
    with pytest.raises(HTTPError) as rejected:
        wrong.call("POST", "/stop", {})
    assert rejected.value.code == 403
    assert engine.repo.resources()["slots"][0]["state"] == "AVAILABLE"


def test_standalone_silent_batch_crosses_renewed_leases(external):
    engine = external
    task_id = engine.submit({"execution_backend": "external", "mock": {"batch_count": 1, "batch_duration_seconds": 5.2, "branch_duration_seconds": .01}})
    engine.worker("independent-long")
    assert engine.wait(task_id)["status"] == "SUCCEEDED", engine.logs()
    history = attempts(engine, task_id, "generate_frames")
    assert len(history) == 1 and history[0]["status"] == "SUCCEEDED"
    assert (datetime.fromisoformat(history[0]["heartbeat_at"]) - datetime.fromisoformat(history[0]["started_at"])).total_seconds() >= 4.5
