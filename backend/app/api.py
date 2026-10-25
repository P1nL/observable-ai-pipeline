"""HTTP persists and reads work; it never starts an executor."""
from pathlib import Path
from uuid import UUID

from fastapi import FastAPI, Header, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from psycopg import InterfaceError, OperationalError

from app.config import Settings
from app.db import connect
from app.domain import DomainError, FinishTask, MonitorAcknowledgement, RegisterStep, SubmitTask, TaskStatus
from app.repository import Repository


def create_app(settings=None):
    settings = settings or Settings.from_env()
    repo = Repository(settings)
    application = FastAPI(title="Observable AI Pipeline", version="0.1.0")
    from app.inputs import build_input_router
    application.include_router(build_input_router(settings))
    web_root = Path(__file__).with_name("web")
    application.mount("/assets", StaticFiles(directory=web_root), name="assets")

    @application.get("/", include_in_schema=False)
    @application.get("/tasks/{task_id}", include_in_schema=False)
    def task_page(task_id: UUID | None = None):
        # The shell remains reachable during DB outages. Only API responses
        # determine execution state; disconnecting this page never stops work.
        return FileResponse(web_root / "index.html", media_type="text/html", headers={"Cache-Control": "no-cache"})

    @application.exception_handler(DomainError)
    def domain_error(request: Request, error: DomainError):
        return JSONResponse(status_code=error.http_status, content={"error": {"code": str(error.code), "message": error.message}})

    @application.exception_handler(OperationalError)
    @application.exception_handler(InterfaceError)
    def database_unavailable(request: Request, error):
        # A lost response does not prove a write failed to commit. The existing
        # idempotency contract, not a new Task, resolves ambiguous write retries.
        return JSONResponse(status_code=503, headers={"Retry-After": "1"}, content={"error": {
            "code": "DATABASE_UNAVAILABLE",
            "message": "Database temporarily unavailable. Retry reads, or reuse the same Idempotency-Key for writes.",
        }})

    @application.get("/health")
    def health():
        with connect(settings) as conn:
            conn.execute("SELECT 1")
        return {"status": "ok", "execution_source": "mock", "physical_backend": "cpu", "resources": "single-mock-gpu"}

    @application.get("/api/v1/resources")
    def resources():
        return repo.resources()

    @application.get("/api/v1/nodes")
    def nodes():
        return repo.nodes()

    @application.get("/api/v1/monitor")
    def monitor(status: TaskStatus | None = None, q: str = Query("", max_length=64),
                limit: int = Query(20, ge=1, le=100), offset: int = Query(0, ge=0)):
        return repo.monitor(status.value if status else None, q, limit, offset)

    @application.post("/api/v1/monitor/attempts/{attempt_id}/acknowledgement")
    def acknowledge(attempt_id: UUID, body: MonitorAcknowledgement):
        return repo.acknowledge_monitor_attempt(attempt_id, body)

    @application.delete("/api/v1/monitor/attempts/{attempt_id}/acknowledgement")
    def restore_reminder(attempt_id: UUID):
        return repo.restore_monitor_reminder(attempt_id)

    @application.post("/api/v1/tasks", status_code=202)
    def submit(body: SubmitTask, idempotency_key: str = Header(min_length=1, max_length=200)):
        if body.mock.fail_first_attempts and not settings.enable_test_faults:
            raise DomainError("TEST_FAULTS_DISABLED", "Set ENABLE_TEST_FAULTS explicitly for local fault injection", 422)
        return repo.submit(body, idempotency_key)

    @application.get("/api/v1/tasks")
    def tasks(status: TaskStatus | None = None, limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)):
        return repo.list_tasks(status.value if status else None, limit, offset)

    @application.get("/api/v1/tasks/{task_id}")
    def task(task_id: UUID):
        return repo.get_task(task_id)

    @application.post("/api/v1/tasks/{task_id}/steps", status_code=202)
    def register_step(task_id: UUID, body: RegisterStep, idempotency_key: str = Header(min_length=1, max_length=200)):
        return repo.register_step(task_id, body, idempotency_key)

    @application.post("/api/v1/tasks/{task_id}/finish")
    def finish(task_id: UUID, body: FinishTask, idempotency_key: str = Header(min_length=1, max_length=200)):
        return repo.finish(task_id, body, idempotency_key)

    @application.get("/api/v1/tasks/{task_id}/events")
    def events(task_id: UUID, after_seq: int = Query(0, ge=0), limit: int = Query(200, ge=1, le=1000)):
        return repo.events(task_id, after_seq, limit)

    @application.get("/api/v1/tasks/{task_id}/steps/{step_id}/attempts")
    def attempts(task_id: UUID, step_id: str):
        return repo.attempts(task_id, step_id)

    @application.post("/api/v1/tasks/{task_id}/resume")
    def resume(task_id: UUID, idempotency_key: str = Header(min_length=1, max_length=200)):
        return repo.resume(task_id, idempotency_key)

    @application.post("/api/v1/tasks/{task_id}/cancel")
    def cancel(task_id: UUID, idempotency_key: str = Header(min_length=1, max_length=200)):
        # Idempotent on persisted task state; does not perform process cleanup.
        return repo.cancel(task_id)

    @application.get("/api/v1/tasks/{task_id}/artifacts/{artifact_id}/preview")
    def preview(task_id: UUID, artifact_id: UUID):
        from app.media import preview_path
        item = next((a for a in repo.artifacts(task_id) if a["id"] == str(artifact_id)), None)
        if not item:
            raise DomainError("NOT_FOUND", "Artifact is not referenced by this Task", 404)
        if item["availability"] != "AVAILABLE":
            raise DomainError("ARTIFACT_UNAVAILABLE", "Preview is no longer available", 409)
        try:
            path = preview_path(settings.artifact_root, item)
        except (ValueError, OSError) as error:
            raise DomainError("ARTIFACT_UNAVAILABLE", "Artifact has no readable GIF preview", 409) from error
        return FileResponse(path, media_type="image/gif", filename="mock-preview.gif", content_disposition_type="inline")

    @application.post("/api/v1/tasks/{task_id}/steps/{step_id}/retry")
    def retry(task_id: UUID, step_id: str, idempotency_key: str = Header(min_length=1, max_length=200)):
        # Step keys are the stable public identifiers; UUIDs remain queryable.
        return repo.resume(task_id, idempotency_key, step_id)

    @application.get("/api/v1/tasks/{task_id}/artifacts")
    def artifacts(task_id: UUID):
        return repo.artifacts(task_id)

    @application.get("/api/v1/tasks/{task_id}/artifacts/{artifact_id}")
    def download(task_id: UUID, artifact_id: UUID):
        item = next((a for a in repo.artifacts(task_id) if a["id"] == str(artifact_id)), None)
        if not item:
            raise DomainError("NOT_FOUND", "Artifact is not referenced by this Task", 404)
        if item["availability"] != "AVAILABLE":
            raise DomainError("ARTIFACT_UNAVAILABLE", "Committed artifact is no longer readable", 409)
        root = settings.artifact_root.resolve()
        path = (root / item["relative_path"]).resolve()
        if not path.is_relative_to(root):
            raise DomainError("ARTIFACT_UNAVAILABLE", "Invalid artifact path", 409)
        return FileResponse(path, media_type="application/json", filename=path.name)

    return application


app = create_app()
