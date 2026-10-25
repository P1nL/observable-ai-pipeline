import json
from queue import Queue

from fastapi.testclient import TestClient
from pydantic import ValidationError
import pytest

from app.api import create_app
from app.config import Settings
from app.domain import SubmitTask, freeze_config


def test_external_binding_is_frozen_but_credentials_and_endpoint_are_not():
    secret = "a-private-runtime-secret-at-least-32-characters"
    settings = Settings.from_env({"EXTERNAL_EXECUTOR_URL": "http://127.0.0.1:8090", "EXTERNAL_EXECUTOR_ID": "executor-one", "EXTERNAL_EXECUTOR_SECRET": secret})
    snapshot = freeze_config(SubmitTask(execution_backend="external"), settings)
    assert snapshot["execution_backend"] == {"kind": "external", "executor_id": "executor-one"}
    encoded = json.dumps(snapshot)
    assert secret not in encoded and "127.0.0.1:8090" not in encoded
    assert secret not in repr(settings)
    assert SubmitTask().execution_backend == "local"
    with pytest.raises(ValidationError):
        SubmitTask(execution_backend="arbitrary-command")


@pytest.mark.parametrize("url,secret", [
    ("http://external.example:8090", "s" * 32),
    ("http://127.0.0.1:8090/path", "s" * 32),
    ("http://user:password@127.0.0.1:8090", "s" * 32),
    ("http://127.0.0.1:8090", "short"),
])
def test_first_external_backend_requires_loopback_origin_and_explicit_secret(url, secret):
    with pytest.raises(ValidationError):
        Settings.from_env({"EXTERNAL_EXECUTOR_URL": url, "EXTERNAL_EXECUTOR_SECRET": secret})


@pytest.mark.integration
def test_external_submission_without_config_does_not_write_a_task(engine):
    with TestClient(create_app(engine.settings.model_copy(update={"external_executor_url": ""}))) as client:
        response = client.post("/api/v1/tasks", json={"execution_backend": "external"}, headers={"Idempotency-Key": "unconfigured-backend"})
        assert response.status_code == 422 and response.json()["error"]["code"] == "BACKEND_UNAVAILABLE"
        assert client.get("/api/v1/monitor").json()["total"] == 0


@pytest.mark.parametrize("override", [
    {"protocol_version": 2}, {"capabilities": ["durable-start-id"]},
    {"processors": {"mock": ["unknown"]}}, {"artifact_schema_versions": [2]},
    {"input_kinds": ["uploaded"]}, {"step_keys": []}, {"executor_id": "someone-else"},
])
def test_capability_mismatch_is_explicit(override):
    from app.external_protocol import REQUIRED_CAPABILITIES, validate_capabilities
    claim = {"step_key": "normalize_asset", "input": {"kind": "synthetic"},
             "spec": {"processor": "mock", "processor_version": "1"}}
    caps = {"protocol_version": 1, "executor_id": "one", "capabilities": sorted(REQUIRED_CAPABILITIES),
            "processors": {"mock": ["1"]}, "step_keys": ["normalize_asset"],
            "input_kinds": ["synthetic"], "artifact_schema_versions": [1]}
    validate_capabilities(caps, {"executor_id": "one"}, claim)
    with pytest.raises(ValueError):
        validate_capabilities({**caps, **override}, {"executor_id": "one"}, claim)


def test_computation_processes_do_not_inherit_other_node_credentials(tmp_path, monkeypatch):
    from app.backends import LocalExecution
    from app.external_service import ExternalService
    from app.external_protocol import binding_for
    from test_external_service import claim_for, expiry
    observed = []
    for name in ("NODE_A_SECRET", "NODE_B_SECRET", "DATABASE_URL", "EXTERNAL_EXECUTOR_SECRET"):
        monkeypatch.setenv(name, "private-" + name)
    def cannot_start(*args, **kwargs):
        observed.append(kwargs["env"])
        raise OSError("Expected test-only startup failure")
    monkeypatch.setattr("subprocess.Popen", cannot_start)
    claim = claim_for()
    with pytest.raises(OSError):
        LocalExecution(Settings.from_env({}), claim, Queue())
    service = ExternalService(claim["executor_id"], "s" * 32, (tmp_path / "artifacts").resolve(), tmp_path / "state")
    result = service.put(binding_for(claim), {"claim": claim, "lease_expires_at": expiry()})
    assert result["exit_confirmed"] and result["start_count"] == 0
    assert len(observed) == 2
    assert all(not any(name in env for name in ("NODE_A_SECRET", "NODE_B_SECRET", "DATABASE_URL", "EXTERNAL_EXECUTOR_SECRET")) for env in observed)
