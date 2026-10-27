"""Business registration is durable intent, not an execution grant or completion."""
from contextlib import contextmanager, nullcontext
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import json
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from app.backends import ExternalClient
from app.db import connect
from app.external_protocol import auth_headers, binding_for
from test_execution import attempts, step
from test_external_execution import external_service
from conftest import ROOT

pytestmark = pytest.mark.integration


@pytest.fixture
def managed(engine, request):
    kind = getattr(request, "param", "local")
    engine.managed_backend = "local" if kind == "local" else "external"
    engine.managed_version = "independent-mock-v1" if kind == "standalone" else "1"
    with (nullcontext(engine) if kind == "local" else external_service(engine, implementation=kind)):
        yield engine


def create(engine, **extra):
    return engine.submit({"pipeline_id": "managed-v1", "execution_backend": engine.managed_backend, **extra})


def register(engine, task_id, key, after=None, *, operation=None, **extra):
    body = {"key": key, "after_step": after, "processor_version": engine.managed_version, **extra}
    response = engine.client.post(f"/api/v1/tasks/{task_id}/steps", json=body, headers={"Idempotency-Key": operation or key})
    assert response.status_code == 202, response.text
    return response.json(), body


def finish(engine, task_id, tail):
    response = engine.client.post(f"/api/v1/tasks/{task_id}/finish", json={"last_step": tail}, headers={"Idempotency-Key": "finish"})
    assert response.status_code == 200, response.text
    return response.json()


def test_task_detail_keeps_one_snapshot_while_worker_finishes(managed, monkeypatch):
    engine = managed
    task_id = create(engine)
    register(engine, task_id, "snapshot-step", parameters={"batch_count": 2, "batch_duration_seconds": .05})

    @contextmanager
    def interleaved_connect(settings):
        with connect(settings) as conn:
            def execute(query, *args, **kwargs):
                if query.startswith("SELECT s.*"):
                    # Commit real completion between the task and step SELECTs.
                    engine.worker("snapshot-worker")
                    engine.wait(task_id, lambda t: t["phase"] == "WAITING_STEPS"
                                and step(t, "snapshot-step")["status"] == "SUCCEEDED")
                return conn.execute(query, *args, **kwargs)
            yield SimpleNamespace(execute=execute)

    with monkeypatch.context() as patch:
        patch.setattr("app.repository.connect", interleaved_connect)
        snapshot = engine.repo.get_task(task_id)

    assert snapshot["status"] == "QUEUED" and snapshot["phase"] == "QUEUED"
    assert step(snapshot, "snapshot-step")["status"] == "READY"
    latest = engine.task(task_id)
    assert latest["status"] == "RUNNING" and latest["phase"] == "WAITING_STEPS"
    assert step(latest, "snapshot-step")["status"] == "SUCCEEDED"
    assert not latest["registration_closed"]


@pytest.mark.parametrize("managed", ["local", "builtin", "standalone"], indirect=True)
def test_business_reads_history_then_extends_real_sequential_work(managed):
    engine = managed
    task_id = create(engine)
    assert engine.task(task_id)["steps"] == [] and not engine.task(task_id)["registration_closed"]
    first, _ = register(engine, task_id, "inspect-data", input={"kind": "synthetic", "seed": 101}, parameters={"batch_count": 3, "batch_duration_seconds": .3})
    assert first["status"] == "READY" and first["execution_generation"] == 0
    assert attempts(engine, task_id, "inspect-data") == []
    assert not (engine.settings.artifact_root / task_id).exists(), "Registration must not compute without a Worker grant"
    engine.worker("business-a")
    engine.wait(task_id, lambda t: step(t, "inspect-data")["status"] == "RUNNING")
    second, _ = register(engine, task_id, "compare-data", "inspect-data")
    assert second["status"] == "PENDING" and attempts(engine, task_id, "compare-data") == []
    engine.worker("business-b")
    ready = engine.wait(task_id, lambda t: all(s["status"] == "SUCCEEDED" for s in t["steps"]))
    assert ready["status"] == "RUNNING" and ready["phase"] == "WAITING_STEPS" and not ready["registration_closed"]
    first_history = attempts(engine, task_id, "inspect-data")
    second_history = attempts(engine, task_id, "compare-data")
    assert len(first_history) == len(second_history) == 1
    assert datetime.fromisoformat(second_history[0]["started_at"]) >= datetime.fromisoformat(first_history[0]["finished_at"])
    # The business client/API can disappear. The next client reads real history,
    # not a browser counter or an assumed predeclared total.
    engine.api.terminate(); engine.api.wait(timeout=5); engine.start_api()
    observed = engine.task(task_id)
    assert [s["registration_order"] for s in observed["steps"]] == [1, 2]
    assert step(observed, "inspect-data")["input_snapshot"]["seed"] == 101
    original_refs = step(observed, "inspect-data")["output_refs"]
    third, _ = register(engine, task_id, "encode_package", "compare-data", parameters={"batch_count": 3, "batch_duration_seconds": .25})
    assert third["spec"]["processor"] == "mock-step"  # Name is NOT animation code dispatch.
    engine.wait(task_id, lambda t: step(t, "encode_package")["status"] == "RUNNING")
    closed = finish(engine, task_id, "encode_package")
    assert closed["registration_closed"] and closed["status"] != "SUCCEEDED"
    assert engine.client.post(f"/api/v1/tasks/{task_id}/steps", json={"key": "too-late", "after_step": "encode_package"}, headers={"Idempotency-Key": "late"}).status_code == 409
    final = engine.wait(task_id)
    assert final["status"] == "SUCCEEDED" and len(final["steps"]) == 3
    assert step(final, "inspect-data")["output_refs"] == original_refs
    artifact = next(a for a in engine.repo.artifacts(task_id) if a["id"] in final["output_refs"])
    data = json.loads((engine.settings.artifact_root / artifact["relative_path"]).read_bytes())
    assert data["step_key"] == "encode_package" and data["source"] == "mock" and "result_manifest" not in data
    assert data["total_batches"] == len(data["completed_batches"]) == 3
    assert finish(engine, task_id, "encode_package")["status"] == "SUCCEEDED"
    print(json.dumps({"scenario": "managed-business-continuation", "backend": engine.managed_backend,
        "processor_version": engine.managed_version, "task_id": task_id, "registered_steps": 3}))


def test_registration_replay_and_one_step_waiting_are_persisted(managed):
    engine = managed
    task_id = create(engine)
    body = {"key": "summarize", "input": {"kind": "synthetic", "seed": 17}}
    def send():
        return engine.client.post(f"/api/v1/tasks/{task_id}/steps", json=body, headers={"Idempotency-Key": "lost-response"})
    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(lambda _: send(), range(2)))
    assert all(r.status_code == 202 for r in responses)
    assert responses[0].json()["id"] == responses[1].json()["id"]
    duplicate = engine.client.post(f"/api/v1/tasks/{task_id}/steps", json={**body, "after_step": "summarize"}, headers={"Idempotency-Key": "another-operation"})
    assert duplicate.status_code == 409 and duplicate.json()["error"]["code"] == "STEP_EXISTS"
    assert engine.client.post(f"/api/v1/tasks/{task_id}/steps", json={**body, "input": {"kind": "synthetic", "seed": 18}}, headers={"Idempotency-Key": "lost-response"}).status_code == 409
    assert engine.client.post(f"/api/v1/tasks/{task_id}/steps", json={"key": "stale-next"}, headers={"Idempotency-Key": "stale"}).status_code == 409
    engine.worker("one-dynamic-step")
    waiting = engine.wait(task_id, lambda t: t["phase"] == "WAITING_STEPS" and bool(t["steps"]) and t["steps"][0]["status"] == "SUCCEEDED")
    assert waiting["status"] != "SUCCEEDED" and len(waiting["steps"]) == 1
    assert len(attempts(engine, task_id, "summarize")) == 1
    monitor = engine.client.get("/api/v1/monitor").json()
    item = next(t for t in monitor["items"] if t["id"] == task_id)
    assert not item["registration_closed"] and item["completed_steps"] == item["step_count"] == 1
    assert finish(engine, task_id, "summarize")["status"] == "SUCCEEDED"
    assert send().json()["id"] == responses[0].json()["id"]
    events = engine.repo.events(task_id)
    assert sum(e["type"] == "STEP_REGISTERED" for e in events) == 1
    assert sum(e["type"] == "WORKFLOW_CLOSED" for e in events) == 1


def test_empty_open_workflow_is_not_implicitly_finished(managed):
    engine = managed
    task_id = create(engine)
    engine.worker("waiting-for-business")
    time.sleep(.4)
    task = engine.task(task_id)
    assert task["steps"] == [] and task["status"] == "QUEUED" and task["phase"] == "WAITING_STEPS"
    assert finish(engine, task_id, None)["status"] == "SUCCEEDED"


@pytest.mark.parametrize("managed", ["standalone"], indirect=True)
def test_business_adapter_rerun_reads_completed_history_without_reregistering(managed):
    engine = managed
    engine.worker("business-adapter-worker")
    args = [sys.executable, "-I", str(ROOT / "examples" / "business_sequence.py"),
            "--url", f"http://127.0.0.1:{engine.port}", "--business-id", "business-adapter-repeat",
            "--steps", "2", "--backend", "external", "--processor-version", engine.managed_version]
    first = subprocess.run(args, cwd=engine.directory, capture_output=True, text=True, timeout=30)
    assert first.returncode == 0, first.stderr + engine.logs()
    outcome = json.loads(first.stdout.splitlines()[-1])
    task_id = outcome["task_id"]
    assert outcome["status"] == "SUCCEEDED" and outcome["registered_steps"] == 2
    before = engine.task(task_id)
    second = subprocess.run(args, cwd=engine.directory, capture_output=True, text=True, timeout=15)
    assert second.returncode == 0 and json.loads(second.stdout.splitlines()[-1]) == outcome
    assert engine.task(task_id) == before
    assert all(len(attempts(engine, task_id, s["step_key"])) == 1 for s in before["steps"])


@pytest.mark.parametrize("managed", ["local", "builtin", "standalone"], indirect=True)
def test_managed_failed_step_resume_keeps_checkpoint_and_prior_success(managed):
    engine = managed
    task_id = create(engine, retry_policy={"max_attempts_per_recovery_round": 1})
    register(engine, task_id, "prepare-records")
    register(engine, task_id, "score-records", "prepare-records", resource_kind="mock-gpu",
             parameters={"batch_count": 3, "batch_duration_seconds": .05, "fail_first_attempts": 1})
    engine.worker("managed-resume")
    failed = engine.wait(task_id)
    assert failed["status"] == "FAILED", engine.logs()
    old_refs = step(failed, "prepare-records")["output_refs"]
    cp = step(failed, "score-records")["checkpoint_artifact_id"]
    assert cp
    assert engine.client.post(f"/api/v1/tasks/{task_id}/resume", headers={"Idempotency-Key": "resume"}).status_code == 200
    waiting = engine.wait(task_id, lambda t: t["phase"] == "WAITING_STEPS")
    assert waiting["status"] == "RUNNING" and not waiting["registration_closed"]
    history = attempts(engine, task_id, "score-records")
    assert [a["status"] for a in history] == ["FAILED", "SUCCEEDED"]
    assert history[1]["resumed_from_checkpoint_id"] == cp
    assert len(attempts(engine, task_id, "prepare-records")) == 1 and step(waiting, "prepare-records")["output_refs"] == old_refs
    output = next(a for a in engine.repo.artifacts(task_id) if a["id"] in step(waiting, "score-records")["output_refs"])
    assert output["metadata"]["resumed_batches"] == 3
    assert finish(engine, task_id, "score-records")["status"] == "SUCCEEDED"
    assert engine.repo.resources()["slots"][0]["state"] == "AVAILABLE"


@pytest.mark.parametrize("managed", ["builtin", "standalone"], indirect=True)
def test_incompatible_executor_and_inspection_token_never_authorize_compute(managed):
    engine = managed
    task_id = create(engine)
    bad_version = "1" if engine.managed_version != "1" else "independent-mock-v1"
    registered, _ = register(engine, task_id, "not-supported", processor_version=bad_version)
    # A candidate identity used only for inspection is not an execution grant.
    from uuid import uuid4
    probe = {"task_id": task_id, "step_id": registered["id"], "attempt_id": str(uuid4()),
             "generation": 1, "worker_id": "ungranted", "executor_id": engine.settings.external_executor_id}
    headers = {**auth_headers(engine.settings.external_executor_secret.get_secret_value(), binding_for(probe), scope="capabilities"), "Content-Type": "application/json"}
    import httpx
    base = engine.settings.external_executor_url
    assert httpx.get(base + "/v1/capabilities", headers=headers).status_code == 200
    assert httpx.put(base + f"/v1/jobs/{probe['attempt_id']}", json={}, headers=headers).status_code == 403
    assert httpx.post(base + f"/v1/jobs/{probe['attempt_id']}/stop", json={}, headers=headers).status_code == 403
    engine.worker("capability-before-grant")
    assert engine.wait(task_id)["status"] == "FAILED"
    assert attempts(engine, task_id, "not-supported") == []
    assert step(engine.task(task_id), "not-supported")["execution_generation"] == 0
    with connect(engine.settings) as conn:
        assert conn.execute("SELECT count(*) AS n FROM execution_commands").fetchone()["n"] == 0
    assert not (engine.settings.artifact_root / task_id).exists()
