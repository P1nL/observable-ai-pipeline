"""Real HTTP executor + PostgreSQL + independent supervisor/computation processes."""
from datetime import datetime
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import subprocess
import sys
import socket
from threading import Thread
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from uuid import uuid4

from PIL import Image
import pytest

from app.config import Settings
from app.db import connect
from app.repository import Repository
from conftest import ROOT
from test_execution import attempts, step

pytestmark = pytest.mark.integration


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def external(engine):
    with external_service(engine) as configured:
        yield configured


@contextmanager
def external_service(engine, implementation="builtin"):
    port = free_port()
    engine.environment.update(EXTERNAL_EXECUTOR_URL=f"http://127.0.0.1:{port}", EXTERNAL_EXECUTOR_ID="test-executor", EXTERNAL_EXECUTOR_SECRET=uuid4().hex + uuid4().hex)
    engine.settings = Settings.from_env(engine.environment)
    engine.repo = Repository(engine.settings)
    engine.api.terminate(); engine.api.wait(timeout=5)
    engine.start_api()
    engine.service_args = ("--executor-id", "test-executor", "--secret-env", "EXTERNAL_EXECUTOR_SECRET", "--artifact-root", str(engine.settings.artifact_root), "--state-dir", str(engine.directory / "external-state"), "--port", str(port))

    def start_service():
        if implementation == "builtin":
            process = engine.start("app.external_service", *engine.service_args)
        else:
            assert implementation == "standalone"
            # Isolated Python and a cwd outside the checkout: the independent
            # implementation cannot rely on importing central app modules.
            output = (engine.directory / f"standalone-{len(engine.processes)}.log").open("w", encoding="utf-8")
            engine.handles.append(output)
            environment = {key: value for key, value in engine.environment.items() if key not in {"PYTHONPATH", "DATABASE_URL", "TEST_DATABASE_URL"}}
            process = subprocess.Popen([sys.executable, "-I", str(ROOT / "executors" / "standalone_mock.py"), *engine.service_args],
                cwd=engine.directory, env=environment, stdout=output, stderr=subprocess.STDOUT,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            engine.processes.append(process)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            assert process.poll() is None, engine.logs()
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=.1):
                    return process
            except OSError:
                time.sleep(.05)
        raise AssertionError("Executor did not bind; " + engine.logs())

    engine.restart_external = start_service
    engine.external_service = start_service()
    try:
        yield engine
    finally:
        if engine.external_service.poll() is not None:
            engine.external_service = start_service()
        with connect(engine.settings) as conn:
            rows = conn.execute("SELECT a.*,s.task_id FROM step_attempts a JOIN steps s ON s.id=a.step_id WHERE a.execution_backend='external'").fetchall()
        for row in rows:
            engine.repo._stop_attempt(row)


def command(engine, attempt_id):
    with connect(engine.settings) as conn:
        return conn.execute("SELECT * FROM execution_commands WHERE id=%s", (attempt_id,)).fetchone()


def remote_status(engine, attempt_id):
    from app.backends import ExternalClient
    return ExternalClient(engine.settings, command(engine, attempt_id)["binding"]).call("GET")


def test_external_single_step_then_six_step_real_media(external):
    engine = external
    single = engine.submit({"pipeline_id": "smoke-v1", "execution_backend": "external", "mock": {"duration_seconds": .1}})
    engine.worker("external-first-step")
    assert engine.wait(single)["status"] == "SUCCEEDED", engine.logs()
    first = attempts(engine, single, "mock")[0]
    assert first["execution_backend"] == "external" and first["executor_pid"] is None
    assert remote_status(engine, first["id"])["start_count"] == 1
    image = io.BytesIO(); Image.new("RGB", (32, 24), "orange").save(image, format="PNG")
    uploaded = engine.client.post("/api/v1/inputs", content=image.getvalue(), headers={"Content-Type": "image/png"}).json()
    task_id = engine.submit({"execution_backend": "external", "input": {"kind": "uploaded", "input_id": uploaded["id"], "seed": 42}, "bypass_cache": True, "mock": {"batch_count": 2, "frames_per_batch": 3, "batch_duration_seconds": .05, "branch_duration_seconds": .05}})
    task = engine.wait(task_id)
    assert task["status"] == "SUCCEEDED" and len(task["steps"]) == 6, engine.logs()
    for current in task["steps"]:
        history = attempts(engine, task_id, current["step_key"])
        assert len(history) == 1 and history[0]["execution_backend"] == "external"
        record = command(engine, history[0]["id"])
        assert str(record["id"]) == str(record["external_job_id"]) == history[0]["id"]
        assert record["dispatched_at"] and record["exit_confirmed_at"]
        assert remote_status(engine, history[0]["id"])["start_count"] == 1
    result = next(a for a in engine.repo.artifacts(task_id) if a["id"] in task["output_refs"])
    gif = engine.client.get(f"/api/v1/tasks/{task_id}/artifacts/{result['id']}/preview")
    with Image.open(io.BytesIO(gif.content)) as decoded:
        assert decoded.format == "GIF" and decoded.n_frames > 1
    assert engine.repo.resources()["slots"][0]["state"] == "AVAILABLE"


def test_start_response_loss_retries_one_durable_command_without_duplicate_compute(external):
    engine = external
    target = engine.settings.external_executor_url
    seen = []

    class Proxy(BaseHTTPRequestHandler):
        def forward(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            headers = {key: value for key, value in self.headers.items() if key.lower() not in {"host", "connection", "content-length"}}
            request = Request(target + self.path, data=body if self.command != "GET" else None, headers=headers, method=self.command)
            try:
                with urlopen(request, timeout=3) as response:
                    content = response.read(); status = response.status
            except HTTPError as error:
                content, status = error.read(), error.code
            if self.command == "PUT":
                seen.append(self.path)
                if len(seen) == 1:
                    status, content = 503, b'{}'  # Job already persisted/started upstream.
            self.send_response(status); self.send_header("Content-Type", "application/json"); self.send_header("Content-Length", str(len(content))); self.end_headers()
            try:
                self.wfile.write(content)
            except OSError:
                pass
        do_PUT = do_GET = do_POST = forward
        def log_message(self, *args):
            pass

    proxy = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    thread = Thread(target=proxy.serve_forever, daemon=True); thread.start()
    try:
        engine.environment["EXTERNAL_EXECUTOR_URL"] = f"http://127.0.0.1:{proxy.server_port}"
        engine.settings = Settings.from_env(engine.environment); engine.repo = Repository(engine.settings)
        engine.api.terminate(); engine.api.wait(timeout=5); engine.start_api()
        task_id = engine.submit({"execution_backend": "external", "bypass_cache": True, "mock": {"batch_count": 2, "batch_duration_seconds": .1}})
        engine.worker("ambiguous-start")
        assert engine.wait(task_id)["status"] == "SUCCEEDED", engine.logs()
        history = attempts(engine, task_id, "normalize_asset")
        assert len(history) == 1 and seen.count(seen[0]) >= 2 and len(set(seen)) == 6
        assert remote_status(engine, history[0]["id"])["start_count"] == 1
    finally:
        engine.environment["EXTERNAL_EXECUTOR_URL"] = target
        engine.settings = Settings.from_env(engine.environment); engine.repo = Repository(engine.settings)
        proxy.shutdown(); proxy.server_close(); thread.join(timeout=3)


def test_external_unsupported_processor_refused_before_start(external):
    engine = external
    task_id = engine.submit({"execution_backend": "external", "processor_version": "not-installed", "bypass_cache": True})
    engine.worker("incompatible-executor")
    assert engine.wait(task_id)["status"] == "FAILED", engine.logs()
    first = attempts(engine, task_id, "normalize_asset")[0]
    assert first["error_code"] == "INVALID_CONFIG" and not first["retryable"]
    assert command(engine, first["id"])["dispatched_at"] is None
    # A bound stop tombstone, not a fabricated physical process exit.
    status = remote_status(engine, first["id"])
    assert status["start_count"] == 0 and status["exit_confirmed"]
    assert not (engine.settings.artifact_root / task_id).exists()


def test_external_failed_step_resume_reuses_success_and_committed_checkpoint(external):
    engine = external
    task_id = engine.submit({"execution_backend": "external", "mock": {"batch_count": 3, "batch_duration_seconds": .05, "branch_duration_seconds": .01, "fail_first_attempts": 1}, "retry_policy": {"max_attempts_per_recovery_round": 1}})
    engine.worker("external-manual-resume")
    failed = engine.wait(task_id)
    assert failed["status"] == "FAILED", engine.logs()
    keys = ("normalize_asset", "segment_subject", "analyze_motion")
    originals = {key: step(failed, key)["output_refs"] for key in keys}
    checkpoint = step(failed, "generate_frames")["checkpoint_artifact_id"]
    assert checkpoint and all(originals.values())
    response = engine.client.post(f"/api/v1/tasks/{task_id}/resume", headers={"Idempotency-Key": "external-resume"})
    assert response.status_code == 200
    final = engine.wait(task_id)
    assert final["status"] == "SUCCEEDED", engine.logs()
    for key in keys:
        assert len(attempts(engine, task_id, key)) == 1 and step(final, key)["output_refs"] == originals[key]
    history = attempts(engine, task_id, "generate_frames")
    assert [a["status"] for a in history] == ["FAILED", "SUCCEEDED"]
    assert history[1]["resumed_from_checkpoint_id"] == checkpoint
    output = next(a for a in engine.repo.artifacts(task_id) if a["id"] in step(final, "generate_frames")["output_refs"])
    assert output["metadata"]["resumed_batches"] == 3


def test_external_silent_computation_crosses_multiple_renewed_leases(external):
    engine = external
    task_id = engine.submit({"pipeline_id": "smoke-v1", "execution_backend": "external", "mock": {"duration_seconds": 5.2}})
    engine.worker("external-long-running")
    assert engine.wait(task_id)["status"] == "SUCCEEDED", engine.logs()
    history = attempts(engine, task_id, "mock")
    assert len(history) == 1 and history[0]["status"] == "SUCCEEDED"
    assert (datetime.fromisoformat(history[0]["heartbeat_at"]) - datetime.fromisoformat(history[0]["started_at"])).total_seconds() >= 4.5


def test_external_worker_kill_takeover_and_late_attempt_fencing(external):
    engine = external
    task_id = engine.submit({"execution_backend": "external", "mock": {"batch_count": 18, "batch_duration_seconds": .2, "branch_duration_seconds": .01}})
    old_worker = engine.worker("external-old")
    task = engine.wait(task_id, lambda t: step(t, "generate_frames")["checkpoint_artifact_id"] is not None)
    old = attempts(engine, task_id, "generate_frames")[0]
    old_claim = command(engine, old["id"])["payload"]
    checkpoint = next(a for a in engine.repo.artifacts(task_id) if a["id"] == step(task, "generate_frames")["checkpoint_artifact_id"])
    old_worker.kill(); old_worker.wait(timeout=5)
    engine.worker("external-new")
    replacement = engine.wait(task_id, lambda t: step(t, "generate_frames")["status"] == "RUNNING" and step(t, "generate_frames")["execution_generation"] == 2)
    owner = engine.repo.resources()["slots"][0]["owner_attempt_id"]
    assert owner != old["id"]
    current_claim = command(engine, owner)["payload"]
    assert engine.repo.heartbeat({**current_claim, "executor_id": "another-executor"}) is False
    assert engine.repo.progress({**current_claim, "external_job_id": old["id"]}, {"completed_units": 999}) is False
    assert engine.repo.heartbeat(old_claim) is False
    assert engine.repo.progress(old_claim, {"completed_units": 999}) is False
    assert engine.repo.commit_checkpoint(old_claim, checkpoint) is False
    assert engine.repo.succeed(old_claim, checkpoint) is False
    assert engine.repo.fail(old_claim, "TRANSIENT_ERROR", "late", True) is False
    assert engine.repo.warning(old_claim, "LATE", "late") is False
    assert engine.repo.record_external_exit(old_claim, remote_status(engine, old["id"]))
    assert engine.repo.resources()["slots"][0]["owner_attempt_id"] == owner
    final = engine.wait(task_id)
    assert final["status"] == "SUCCEEDED", engine.logs()
    history = attempts(engine, task_id, "generate_frames")
    assert [a["status"] for a in history] == ["LOST", "SUCCEEDED"]
    assert history[1]["resumed_from_checkpoint_id"] and len(attempts(engine, task_id, "normalize_asset")) == 1


def test_external_disconnect_cancel_quarantines_until_actual_exit_reconciled(external):
    engine = external
    task_id = engine.submit({"execution_backend": "external", "mock": {"batch_count": 30, "batch_duration_seconds": .2, "branch_duration_seconds": .01}})
    engine.worker("external-cancel")
    engine.wait(task_id, lambda t: step(t, "generate_frames")["checkpoint_artifact_id"] is not None)
    old = attempts(engine, task_id, "generate_frames")[0]
    engine.external_service.kill(); engine.external_service.wait(timeout=5)
    response = engine.client.post(f"/api/v1/tasks/{task_id}/cancel", headers={"Idempotency-Key": "cancel-unreachable-executor"})
    assert response.status_code == 200 and response.json()["status"] == "CANCELING"
    time.sleep(.5)
    assert engine.task(task_id)["status"] == "CANCELING"
    assert engine.repo.resources()["slots"][0]["state"] == "QUARANTINED"
    assert attempts(engine, task_id, "generate_frames")[0]["process_exit_confirmed_at"] is None
    engine.external_service = engine.restart_external()
    assert engine.wait(task_id, lambda t: t["status"] == "CANCELED")["status"] == "CANCELED", engine.logs()
    assert remote_status(engine, old["id"])["exit_confirmed"]
    assert engine.repo.resources()["slots"][0]["state"] == "AVAILABLE"
