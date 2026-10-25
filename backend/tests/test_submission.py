"""Current six-step admission is separate from historical task compatibility."""
import json
from pathlib import Path

from fastapi.testclient import TestClient
import pytest

from app.api import create_app
from app.domain import SubmitTask
from test_execution import attempts, step

pytestmark = pytest.mark.integration


def test_public_submission_default_example_and_retired_pipeline(engine):
    example = json.loads((Path(__file__).parents[2] / "examples" / "animation-pipeline.json").read_text(encoding="utf-8"))
    assert SubmitTask(**example).pipeline_id == "animation-dag-v2"
    # Exercise the normal, fault-disabled admission boundary, not the test opt-in.
    settings = engine.settings.model_copy(update={"enable_test_faults": False})
    with TestClient(create_app(settings)) as client:
        schema = client.get("/openapi.json").json()["components"]["schemas"]["SubmitTask"]["properties"]["pipeline_id"]
        assert schema["enum"] == ["animation-dag-v2", "managed-v1"] and schema["default"] == "animation-dag-v2"
        response = client.post("/api/v1/tasks", json={k: v for k, v in example.items() if k != "pipeline_id"}, headers={"Idempotency-Key": "six-default"})
        assert response.status_code == 202, response.text
        task_id = response.json()["id"]
        for pipeline, code in (("animation-v1", "PIPELINE_RETIRED"), ("smoke-v1", "INTERNAL_PIPELINE_DISABLED")):
            rejected = client.post("/api/v1/tasks", json={"pipeline_id": pipeline}, headers={"Idempotency-Key": pipeline})
            assert rejected.status_code == 422 and rejected.json()["error"]["code"] == code
        assert client.get("/api/v1/monitor").json()["total"] == 1
    frozen = engine.task(task_id)
    assert frozen["pipeline_id"] == "animation-dag-v2" and len(frozen["steps"]) == 6
    engine.worker("six-step-upstream")
    final = engine.wait(task_id)
    assert final["status"] == "SUCCEEDED", engine.logs()
    assert all(s["status"] == "SUCCEEDED" for s in final["steps"])
    assert all(len(attempts(engine, task_id, s["step_key"])) == 1 for s in final["steps"])
    assert final["output_refs"] == step(final, "publish_result")["output_refs"]
    result = next(a for a in engine.repo.artifacts(task_id) if a["id"] in final["output_refs"])
    assert engine.client.get(result["download_url"]).status_code == 200


def test_old_key_replay_does_not_reinterpret_omitted_pipeline_or_mutate_history(engine):
    body = {"mock": {"batch_count": 2, "batch_duration_seconds": .02}}
    legacy = engine.seed_legacy(body, "before-retirement")
    before = engine.task(legacy)
    events = engine.repo.events(legacy)
    for request in (body, {**body, "pipeline_id": "animation-v1"}):
        assert engine.submit(request, "before-retirement") == legacy
    for changed in ({**body, "pipeline_id": "animation-dag-v2"}, {**body, "input": {"kind": "synthetic", "seed": 99}}):
        response = engine.client.post("/api/v1/tasks", json=changed, headers={"Idempotency-Key": "before-retirement"})
        assert response.status_code == 409
    assert engine.task(legacy) == before
    assert engine.repo.events(legacy) == events
    assert engine.client.get("/api/v1/monitor").json()["total"] == 1
