"""Monitoring is an accurate read model, not a second scheduler."""
import pytest
from uuid import uuid4

from app.db import connect

pytestmark = pytest.mark.integration


def test_monitor_global_counts_filter_search_pagination_and_read_only(engine):
    empty = engine.client.get("/api/v1/monitor").json()
    assert empty["total"] == 0 and empty["items"] == []
    assert empty["unconfirmed_count"] == 0 and empty["unconfirmed_executors"] == []
    assert empty["counts"] == {"QUEUED": 0, "RUNNING": 0, "SUCCEEDED": 0, "FAILED": 0, "CANCELING": 0, "CANCELED": 0}
    ids = [engine.submit({}) for _ in range(4)]
    engine.repo.cancel(ids[0])
    baseline_events = {task_id: engine.repo.events(task_id) for task_id in ids}
    response = engine.client.get("/api/v1/monitor?limit=2")
    assert response.status_code == 200, response.text
    overview = response.json()
    assert overview["total"] == overview["filtered_total"] == 4
    assert overview["counts"]["QUEUED"] == 3 and overview["counts"]["CANCELED"] == 1
    assert overview["waiting"] == {"gpu": 0, "cpu": 0, "retry": 0}
    assert [item["id"] for item in overview["items"]] == ids[:1:-1]
    assert all(item["step_count"] == 6 and item["completed_steps"] == item["attempt_count"] == 0 for item in overview["items"])
    older = engine.client.get("/api/v1/monitor?limit=2&offset=2").json()
    assert [item["id"] for item in older["items"]] == ids[1::-1]
    canceled = engine.client.get("/api/v1/monitor?status=CANCELED").json()
    assert canceled["filtered_total"] == 1 and canceled["counts"] == overview["counts"]
    assert canceled["items"][0]["id"] == ids[0]
    searched = engine.client.get("/api/v1/monitor", params={"q": ids[-1][:12]}).json()
    assert searched["filtered_total"] == 1 and searched["items"][0]["id"] == ids[-1]
    assert engine.client.get("/api/v1/monitor", params={"q": "%"}).json()["filtered_total"] == 0
    assert engine.client.get("/api/v1/monitor", params={"q": "ANIMATION-DAG-V2"}).json()["filtered_total"] == 4
    assert isinstance(engine.client.get("/api/v1/tasks").json(), list)
    assert {task_id: engine.repo.events(task_id) for task_id in ids} == baseline_events


def test_monitor_reports_real_active_attempts_then_completed_steps(engine):
    task_id = engine.submit({"pipeline_id": "smoke-v1", "mock": {"duration_seconds": 2}})
    engine.worker("monitor-live-worker")
    engine.wait(task_id, lambda task: task["status"] == "RUNNING")
    current = engine.client.get("/api/v1/monitor").json()["items"][0]
    assert current["running_steps"] == current["attempt_count"] == 1
    assert current["completed_steps"] == 0 and current["heartbeat_at"]
    assert engine.wait(task_id)["status"] == "SUCCEEDED"
    completed = engine.client.get("/api/v1/monitor").json()
    assert completed["counts"]["SUCCEEDED"] == 1
    assert completed["items"][0]["completed_steps"] == completed["items"][0]["step_count"] == 1
    assert completed["items"][0]["running_steps"] == 0 and completed["items"][0]["heartbeat_at"] is None


def test_monitor_validates_bounded_queries(engine):
    for params in ({"status": "UNKNOWN"}, {"limit": 101}, {"offset": -1}, {"q": "x"*65}):
        assert engine.client.get("/api/v1/monitor", params=params).status_code == 422


def abandoned_attempt(engine, cleanup="CPU_PROCESS_UNCONFIRMED"):
    task_id = engine.submit({"pipeline_id": "smoke-v1"})
    claim = engine.repo.claim("monitor-fixture")
    assert claim["task_id"] == task_id
    with connect(engine.settings) as conn:
        conn.execute("""UPDATE step_attempts SET status='LOST',resource_cleanup_state=%s,
            stop_requested_at=clock_timestamp(),finished_at=clock_timestamp() WHERE id=%s""", (cleanup, claim["attempt_id"]))
    return task_id, claim["attempt_id"]


def observed_condition(item):
    return {key: item[key] for key in ("status", "resource_cleanup_state", "stop_requested_at")}


def test_acknowledgement_is_persisted_idempotent_and_never_confirms_exit(engine):
    task_id, attempt_id = abandoned_attempt(engine)
    path = f"/api/v1/monitor/attempts/{attempt_id}/acknowledgement"
    before = engine.client.get("/api/v1/monitor").json()
    condition = observed_condition(before["unacknowledged_executors"][0])
    original = (engine.repo.get_task(task_id), engine.repo.attempts(task_id, "mock"), engine.repo.events(task_id), engine.repo.resources())
    response = engine.client.post(path, json=condition)
    assert response.status_code == 200, response.text
    assert engine.client.post(path, json=condition).json() == response.json()
    overview = engine.client.get("/api/v1/monitor").json()
    assert overview["unconfirmed_count"] == overview["acknowledged_count"] == 1
    assert overview["unacknowledged_count"] == 0 and overview["unacknowledged_executors"] == []
    assert overview["acknowledged_executors"][0]["attempt_id"] == attempt_id
    assert overview["acknowledged_executors"][0]["acknowledged_at"] == response.json()["acknowledged_at"]
    assert (engine.repo.get_task(task_id), engine.repo.attempts(task_id, "mock"), engine.repo.events(task_id), engine.repo.resources()) == original
    with connect(engine.settings) as conn:
        row = conn.execute("SELECT * FROM step_attempts WHERE id=%s", (attempt_id,)).fetchone()
        assert row["status"] == "LOST" and row["process_exit_confirmed_at"] is None
        assert row["resource_cleanup_state"] == "CPU_PROCESS_UNCONFIRMED"
        assert conn.execute("SELECT count(*) AS n FROM monitor_acknowledgements").fetchone()["n"] == 1
    assert engine.client.delete(path).status_code == 200
    assert engine.client.delete(path).status_code == 200
    restored = engine.client.get("/api/v1/monitor").json()
    assert restored["unacknowledged_count"] == 1 and restored["acknowledged_count"] == 0


def test_acknowledgement_does_not_hide_changed_conditions_or_release_quarantine(engine):
    _, attempt_id = abandoned_attempt(engine, "QUARANTINED")
    with connect(engine.settings) as conn:
        conn.execute("UPDATE resource_slots SET state='QUARANTINED',owner_attempt_id=%s,owner_generation=1 WHERE id=1", (attempt_id,))
    original_resources = engine.repo.resources()
    path = f"/api/v1/monitor/attempts/{attempt_id}/acknowledgement"
    old = observed_condition(engine.client.get("/api/v1/monitor").json()["unacknowledged_executors"][0])
    assert engine.client.post(path, json=old).status_code == 200
    assert engine.repo.resources() == original_resources
    with connect(engine.settings) as conn:
        conn.execute("UPDATE step_attempts SET stop_requested_at=stop_requested_at+interval '1 second' WHERE id=%s", (attempt_id,))
    changed = engine.client.get("/api/v1/monitor").json()
    assert changed["unacknowledged_count"] == 1 and changed["acknowledged_count"] == 0
    assert engine.client.post(path, json=old).status_code == 409
    current = observed_condition(changed["unacknowledged_executors"][0])
    assert engine.client.post(path, json=current).status_code == 200
    with connect(engine.settings) as conn:
        conn.execute("UPDATE step_attempts SET process_exit_confirmed_at=clock_timestamp() WHERE id=%s", (attempt_id,))
    assert engine.client.post(path, json=current).status_code == 409
    assert engine.client.get("/api/v1/monitor").json()["unconfirmed_count"] == 0
    assert engine.client.post(f"/api/v1/monitor/attempts/{uuid4()}/acknowledgement", json=current).status_code == 404


def test_acknowledged_recent_items_do_not_hide_older_pending_warning(engine):
    ids = [abandoned_attempt(engine, "CPU_ONLY")[1] for _ in range(6)]
    with connect(engine.settings) as conn:
        conn.execute("UPDATE step_attempts SET resource_cleanup_state='CPU_PROCESS_UNCONFIRMED'")
    overview = engine.client.get("/api/v1/monitor").json()
    assert overview["unacknowledged_count"] == 6 and len(overview["unacknowledged_executors"]) == 5
    for item in overview["unacknowledged_executors"]:
        assert engine.client.post(f"/api/v1/monitor/attempts/{item['attempt_id']}/acknowledgement", json=observed_condition(item)).status_code == 200
    overview = engine.client.get("/api/v1/monitor").json()
    assert overview["unconfirmed_count"] == 6 and overview["acknowledged_count"] == 5
    assert overview["unacknowledged_count"] == 1
    assert overview["unacknowledged_executors"][0]["attempt_id"] == ids[0]
