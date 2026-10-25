"""Versioned DAG joins, actual parallel processes and compatible byte reuse."""
from datetime import datetime
from uuid import uuid4

import pytest

from app.db import connect
from app.domain import StepSpec, SubmitTask, build_steps, validate_dependencies
from app.repository import fingerprint
from psycopg.types.json import Jsonb
from test_enhancements import upload
from test_execution import attempts, step


def body(source, **changes):
    return {"pipeline_id": "animation-dag-v2", "cache_enabled": True,
            "input": {"kind": "uploaded", "input_id": source["id"], "seed": 42},
            "mock": {"batch_count": 2, "frames_per_batch": 3, "batch_duration_seconds": .03, "branch_duration_seconds": .15}, **changes}


def test_fixed_dag_dependencies_and_validation():
    specs = build_steps(SubmitTask(pipeline_id="animation-dag-v2", cache_enabled=True))
    assert [s.key for s in specs] == ["normalize_asset", "segment_subject", "analyze_motion", "generate_frames", "encode_package", "publish_result"]
    assert specs[3].depends_on == ["segment_subject", "analyze_motion"]
    assert not specs[-1].cache_enabled
    for graph in ([StepSpec(key="a", depends_on=["missing"])], [StepSpec(key="a", depends_on=["b"]), StepSpec(key="b", depends_on=["a"])], [StepSpec(key="a"), StepSpec(key="a")]):
        with pytest.raises(ValueError):
            validate_dependencies(graph)


@pytest.mark.integration
def test_dag_parallel_join_and_cross_task_cache_actual_media(engine):
    source = upload(engine)
    payload = body(source, mock={"batch_count": 2, "frames_per_batch": 3, "batch_duration_seconds": .05, "branch_duration_seconds": .8})
    first = engine.submit(payload)
    engine.worker("parallel-a"); engine.worker("parallel-b")
    engine.wait(first, lambda task: all(step(task, key)["status"] == "RUNNING" for key in ("segment_subject", "analyze_motion")))
    assert engine.repo.resources()["cpu"] == {"capacity": 2, "in_use": 2}
    result = engine.wait(first)
    assert result["status"] == "SUCCEEDED", engine.logs()
    branches = [attempts(engine, first, key)[0] for key in ("segment_subject", "analyze_motion")]
    assert max(datetime.fromisoformat(a["started_at"]) for a in branches) < min(datetime.fromisoformat(a["finished_at"]) for a in branches)
    generated = attempts(engine, first, "generate_frames")[0]
    assert datetime.fromisoformat(generated["started_at"]) >= max(datetime.fromisoformat(a["finished_at"]) for a in branches)
    # A new upload ID containing identical bytes must still reuse actual results.
    duplicate = upload(engine)
    second = engine.submit({**payload, "input": {"kind": "uploaded", "input_id": duplicate["id"], "seed": 42}})
    cached = engine.wait(second)
    assert cached["status"] == "SUCCEEDED", engine.logs()
    for current in cached["steps"]:
        if current["step_key"] == "publish_result":
            assert current["status"] == "SUCCEEDED" and len(attempts(engine, second, "publish_result")) == 1
        else:
            assert current["status"] == "CACHED" and current["cache_source_task_id"] == first
            assert current["output_refs"] == step(result, current["step_key"])["output_refs"]
            assert attempts(engine, second, current["step_key"]) == []
    preview = engine.client.get(f"/api/v1/tasks/{second}/artifacts/{cached['output_refs'][0]}/preview")
    assert preview.status_code == 200 and preview.headers["content-type"].startswith("image/gif")
    assert engine.repo.resources()["slots"][0]["state"] == "AVAILABLE"


@pytest.mark.integration
def test_cache_misses_for_changed_semantics_bypass_and_missing_media(engine):
    source = upload(engine)
    original = body(source)
    engine.worker("cache-worker")
    first = engine.submit(original)
    first_result = engine.wait(first)
    assert first_result["status"] == "SUCCEEDED", engine.logs()
    variations = (
        {"processor_version": "new-version"},
        {"input": {**original["input"], "seed": 43}},
        {"generation_tag": "regenerated-v2"},
        {"bypass_cache": True},
        {"input": {"kind": "uploaded", "input_id": upload(engine, "blue")["id"], "seed": 42}},
    )
    for change in variations:
        task_id = engine.submit({**original, **change})
        final = engine.wait(task_id)
        assert final["status"] == "SUCCEEDED", engine.logs()
        assert all(s["status"] == "SUCCEEDED" for s in final["steps"]), change
    # Nonsemantic timings do not invalidate cache; actual frame parameters do.
    changed = {**original, "mock": {**original["mock"], "frames_per_batch": 4}}
    final = engine.wait(engine.submit(changed))
    assert all(step(final, key)["status"] == "CACHED" for key in ("normalize_asset", "segment_subject", "analyze_motion"))
    assert step(final, "generate_frames")["status"] == "SUCCEEDED"
    with connect(engine.settings) as conn:
        producer = conn.execute("SELECT artifact_ids FROM step_cache WHERE cache_key=%s", (step(first_result, "normalize_asset")["cache_key"],)).fetchone()["artifact_ids"][0]
        artifact = conn.execute("SELECT metadata FROM artifacts WHERE id=%s", (producer,)).fetchone()["metadata"]
    (engine.settings.artifact_root / artifact["normalized_asset"]["relative_path"]).unlink()
    repaired = engine.wait(engine.submit(original))
    assert repaired["status"] == "SUCCEEDED", engine.logs()
    assert step(repaired, "normalize_asset")["status"] == "SUCCEEDED"


@pytest.mark.integration
def test_failed_parallel_branch_recovers_without_rerunning_success_or_caching_failure(engine):
    source = upload(engine)
    payload = body(source, mock={"batch_count": 2, "frames_per_batch": 3, "branch_duration_seconds": .5, "batch_duration_seconds": .03, "fail_first_attempts": 1, "fail_step": "analyze_motion"}, retry_policy={"max_attempts_per_recovery_round": 1})
    task_id = engine.submit(payload)
    engine.worker("branch-recovery-a"); engine.worker("branch-recovery-b")
    failed = engine.wait(task_id)
    assert failed["status"] == "FAILED", engine.logs()
    assert step(failed, "segment_subject")["status"] == "SUCCEEDED"
    preserved = step(failed, "segment_subject")["output_refs"]
    with connect(engine.settings) as conn:
        assert conn.execute("SELECT count(*) AS n FROM step_cache WHERE source_task_id=%s", (task_id,)).fetchone()["n"] == 0
    assert engine.client.post(f"/api/v1/tasks/{task_id}/resume", headers={"Idempotency-Key": str(uuid4())}).status_code == 200
    final = engine.wait(task_id)
    assert final["status"] == "SUCCEEDED", engine.logs()
    assert step(final, "segment_subject")["output_refs"] == preserved
    assert len(attempts(engine, task_id, "segment_subject")) == 1
    assert [a["status"] for a in attempts(engine, task_id, "analyze_motion")] == ["FAILED", "SUCCEEDED"]


@pytest.mark.integration
def test_parallel_cancel_cleans_both_cpu_executors_and_prevents_cache(engine):
    task_id = engine.submit(body(upload(engine), mock={"batch_count": 2, "frames_per_batch": 3, "branch_duration_seconds": 4}))
    engine.worker("cancel-branch-a"); engine.worker("cancel-branch-b")
    engine.wait(task_id, lambda task: all(step(task, key)["status"] == "RUNNING" for key in ("segment_subject", "analyze_motion")))
    assert engine.client.post(f"/api/v1/tasks/{task_id}/cancel", headers={"Idempotency-Key": "cancel-both"}).json()["status"] == "CANCELING"
    canceled = engine.wait(task_id, lambda task: task["status"] == "CANCELED")
    for key in ("segment_subject", "analyze_motion"):
        attempt = attempts(engine, task_id, key)[0]
        assert attempt["status"] == "CANCELED" and attempt["process_exit_confirmed_at"]
    assert not canceled["output_refs"] and engine.repo.resources()["cpu"]["in_use"] == 0
    with connect(engine.settings) as conn:
        assert conn.execute("SELECT count(*) AS n FROM step_cache WHERE source_task_id=%s", (task_id,)).fetchone()["n"] == 0


@pytest.mark.integration
def test_cpu_capacity_and_legacy_three_step_snapshot(engine):
    ids = [engine.submit({}) for _ in range(3)]
    assert engine.repo.claim("capacity-one") and engine.repo.claim("capacity-two")
    assert engine.repo.claim("capacity-three") is None
    assert engine.repo.resources()["cpu"]["in_use"] == 2
    # Finish only this test's unstarted leases, then run an old-schema snapshot.
    with connect(engine.settings) as conn:
        conn.execute("UPDATE step_attempts SET status='FAILED',process_exit_confirmed_at=clock_timestamp(),resource_cleanup_state='CPU_ONLY'")
        conn.execute("UPDATE tasks SET status='CANCELED' WHERE id=ANY(%s)", (ids,))
    legacy_body = {"mock": {"batch_count": 2, "batch_duration_seconds": .03}}
    legacy = engine.seed_legacy(legacy_body, "legacy-submit-replay")
    with connect(engine.settings) as conn:
        snapshot = conn.execute("SELECT config_snapshot FROM tasks WHERE id=%s", (legacy,)).fetchone()["config_snapshot"]
        snapshot.pop("execution_schema_version")
        for key in ("bypass_cache", "generation_tag"):
            snapshot["request"].pop(key)
        snapshot["request"]["mock"].pop("branch_duration_seconds")
        old_hash = fingerprint({"request": snapshot["request"], "explicit_policy": {}})
        conn.execute("UPDATE tasks SET config_snapshot=%s,request_fingerprint=%s WHERE id=%s", (Jsonb(snapshot), old_hash, legacy))
    assert engine.submit(legacy_body, "legacy-submit-replay") == legacy
    changed = engine.client.post("/api/v1/tasks", json={**legacy_body, "generation_tag": "different"}, headers={"Idempotency-Key": "legacy-submit-replay"})
    assert changed.status_code == 409
    engine.worker("legacy-v1")
    result = engine.wait(legacy)
    assert result["status"] == "SUCCEEDED" and len(result["steps"]) == 3, engine.logs()
    assert step(result, "generate_frames")["depends_on"] == ["normalize_asset"]
