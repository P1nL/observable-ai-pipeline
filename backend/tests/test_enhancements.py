"""Real upload/media and cancellation acceptance; no browser-side execution."""
import io
import time
from uuid import uuid4

import pytest
from PIL import Image

from app.db import connect
from app.artifacts import write_json_artifact
from test_execution import attempts, step

pytestmark = pytest.mark.integration


def upload(engine, color="red"):
    stream = io.BytesIO()
    image = Image.new("RGB", (48, 32), color)
    image.putpixel((24, 16), (0, 255, 0))
    image.save(stream, format="PNG")
    response = engine.client.post("/api/v1/inputs", content=stream.getvalue(), headers={"Content-Type": "image/png"})
    assert response.status_code in (200, 201), response.text
    return response.json()


def test_uploaded_image_really_processed_and_gif_readable(engine):
    source = upload(engine)
    task_id = engine.submit({"input": {"kind": "uploaded", "input_id": source["id"]}, "mock": {"batch_count": 2, "frames_per_batch": 3, "batch_duration_seconds": .05}})
    engine.worker("actual-image-worker")
    task = engine.wait(task_id)
    assert task["status"] == "SUCCEEDED", engine.logs()
    assert task["config_snapshot"]["input_asset"]["content_fingerprint"] == source["content_fingerprint"]
    artifacts = engine.client.get(f"/api/v1/tasks/{task_id}/artifacts").json()
    normalized = next(a for a in artifacts if a["id"] in step(task, "normalize_asset")["output_refs"])
    with Image.open(engine.settings.artifact_root / normalized["metadata"]["normalized_asset"]["relative_path"]) as image:
        red, green, blue = image.convert("RGB").getpixel((0, 0))
        assert red > 200 and green < 30 and blue < 30  # Read the uploaded pixels.
    result = next(a for a in artifacts if a["id"] in task["output_refs"])
    response = engine.client.get(f"/api/v1/tasks/{task_id}/artifacts/{result['id']}/preview")
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("image/gif")
    with Image.open(io.BytesIO(response.content)) as image:
        assert image.format == "GIF" and image.n_frames > 1
        assert max(image.size) <= 256
    assert result["metadata"]["source"] == "mock"
    assert engine.client.get(f"/api/v1/tasks/{uuid4()}/artifacts/{result['id']}/preview").status_code == 404
    invalid = engine.client.post("/api/v1/tasks", json={"input": {"kind": "uploaded", "input_id": str(uuid4())}}, headers={"Idempotency-Key": str(uuid4())})
    assert invalid.status_code == 404


def test_upload_rejects_unsupported_spoofed_and_large_content(engine):
    assert engine.client.post("/api/v1/inputs", content=b"<svg/>", headers={"Content-Type": "image/svg+xml"}).status_code in (415, 422)
    assert engine.client.post("/api/v1/inputs", content=b"not a PNG", headers={"Content-Type": "image/png"}).status_code in (415, 422)
    assert engine.client.post("/api/v1/inputs", content=b"x"*(8*1024*1024+1), headers={"Content-Type": "image/png"}).status_code == 413
    response = engine.client.post("/api/v1/tasks", json={"input": {"kind": "uploaded", "input_id": str(uuid4()), "relative_path": "../outside"}}, headers={"Idempotency-Key": str(uuid4())})
    assert response.status_code == 422


def test_queued_cancel_is_persisted_idempotent_and_never_claimed(engine):
    task_id = engine.submit({})
    path = f"/api/v1/tasks/{task_id}/cancel"
    headers = {"Idempotency-Key": "cancel-queued"}
    assert engine.client.post(path, headers=headers).json()["status"] == "CANCELED"
    first = engine.client.get(f"/api/v1/tasks/{task_id}/events").json()
    assert engine.client.post(path, headers=headers).json()["status"] == "CANCELED"
    assert engine.client.get(f"/api/v1/tasks/{task_id}/events").json() == first
    assert engine.repo.claim("must-not-claim-canceled") is None
    task = engine.task(task_id)
    assert all(s["status"] == "CANCELED" for s in task["steps"])
    assert all(not attempts(engine, task_id, s["step_key"]) for s in task["steps"])


def test_running_cancel_stops_before_release_and_fences_result(engine):
    task_id = engine.submit({"mock": {"batch_count": 30, "batch_duration_seconds": .2}})
    engine.worker("cancel-running")
    running = engine.wait(task_id, lambda t: step(t, "generate_frames")["checkpoint_artifact_id"] is not None)
    active = step(running, "generate_frames")
    old = attempts(engine, task_id, "generate_frames")[0]
    claim = {"task_id": task_id, "step_id": active["id"], "step_key": "generate_frames", "attempt_id": old["id"], "generation": old["fencing_token"], "worker_id": old["worker_id"], "compatibility_key": active["compatibility_key"]}
    response = engine.client.post(f"/api/v1/tasks/{task_id}/cancel", headers={"Idempotency-Key": "cancel-running"})
    assert response.status_code == 200 and response.json()["status"] == "CANCELING"
    assert engine.repo.progress(claim, {"completed_units": 999}) is False
    assert engine.repo.heartbeat(claim) is False
    candidate = write_json_artifact(engine.settings.artifact_root, claim, "output", {"compatibility_key": claim["compatibility_key"]})
    assert engine.repo.succeed(claim, candidate) is False
    canceled = engine.wait(task_id, lambda t: t["status"] == "CANCELED")
    history = attempts(engine, task_id, "generate_frames")
    assert history[0]["status"] == "CANCELED" and history[0]["process_exit_confirmed_at"]
    assert history[0]["resource_cleanup_state"] == "RELEASED"
    assert step(canceled, "normalize_asset")["status"] == "SUCCEEDED"
    assert step(canceled, "encode_package")["status"] == "CANCELED"
    assert not canceled["output_refs"]
    assert engine.repo.resources()["slots"][0]["state"] == "AVAILABLE"
    events = engine.repo.events(task_id)
    kinds = [e["type"] for e in events]
    assert kinds.index("TASK_CANCEL_REQUESTED") < kinds.index("RESOURCE_RELEASED") < kinds.index("TASK_CANCELED")


def test_cancel_unknown_executor_stays_quarantined_until_confirmed(engine):
    task_id = engine.submit({"mock": {"batch_count": 30, "batch_duration_seconds": .2}})
    engine.worker("cancel-unknown")
    engine.wait(task_id, lambda t: step(t, "generate_frames")["checkpoint_artifact_id"] is not None)
    old = attempts(engine, task_id, "generate_frames")[0]
    with connect(engine.settings) as conn:
        conn.execute("UPDATE step_attempts SET executor_host='unknown-host' WHERE id=%s", (old["id"],))
    engine.client.post(f"/api/v1/tasks/{task_id}/cancel", headers={"Idempotency-Key": "cancel-unknown"})
    time.sleep(.8)
    assert engine.task(task_id)["status"] == "CANCELING"
    assert engine.repo.resources()["slots"][0]["state"] == "QUARANTINED"
    with connect(engine.settings) as conn:
        conn.execute("UPDATE step_attempts SET executor_host=%s WHERE id=%s", (old["executor_host"], old["id"]))
    assert engine.wait(task_id, lambda t: t["status"] == "CANCELED")["status"] == "CANCELED"
