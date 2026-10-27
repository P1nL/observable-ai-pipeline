"""The database is authoritative; all worker mutations share one fencing gate.

Lock order is Task -> Step -> Attempt -> Slot. Task row serialization also allocates
event sequences in commit order. No connection survives a repository call.
"""
import hashlib
import json
import random
import time
from uuid import uuid4

from psycopg.types.json import Jsonb

from app.artifacts import validate_artifact
from app.config import Settings
from app.db import connect
from app.domain import DomainError, FinishTask, RegisterStep, StepSpec, SubmitTask, TaskStatus, freeze_config
from app.processes import current_host, executor_state, identify_executor, stop_executor


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def plain(row):
    if row is None:
        return None
    return {k: str(v) if type(v).__name__ == "UUID" else v for k, v in row.items()}


def available(root, artifact):
    try:
        return validate_artifact(root, artifact)
    except (ValueError, OSError):
        return False


class Repository:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._nodes_synced = False

    def _event(self, conn, task_id, kind, step_id=None, attempt_id=None, **payload):
        seq = conn.execute("UPDATE tasks SET next_event_seq=next_event_seq+1 WHERE id=%s RETURNING next_event_seq-1 AS seq", (task_id,)).fetchone()["seq"]
        conn.execute("INSERT INTO task_events(task_id,seq,type,step_id,attempt_id,payload) VALUES (%s,%s,%s,%s,%s,%s)", (task_id, seq, kind, step_id, attempt_id, Jsonb(payload)))

    def _same_submission(self, previous, request_hash, request):
        old = previous["config_snapshot"]["request"]
        if "pipeline_id" not in request.model_fields_set and old["pipeline_id"] != request.pipeline_id:
            # An omitted default on an existing key keeps its original version;
            # changing today's default must not reinterpret yesterday's retry.
            request = request.model_copy(update={"pipeline_id": old["pipeline_id"]})
            request_hash = fingerprint({"request": request.model_dump(mode="json"), "explicit_policy": request.policy.model_dump(mode="json", exclude_unset=True)})
        if previous["request_fingerprint"] == request_hash:
            return True
        # Added default fields must not invalidate an old client's idempotency
        # key after an upgrade. Nondefault new semantics still conflict.
        current = request.model_dump(mode="json")
        for key, default in (("bypass_cache", False), ("generation_tag", ""), ("execution_backend", "local")):
            if key not in old and current[key] == default:
                del current[key]
        if "branch_duration_seconds" not in old["mock"] and current["mock"]["branch_duration_seconds"] == .5:
            del current["mock"]["branch_duration_seconds"]
        return previous["request_fingerprint"] == fingerprint({"request": current, "explicit_policy": request.policy.model_dump(mode="json", exclude_unset=True)})

    def submit(self, request: SubmitTask, idempotency_key=None):
        request_hash = fingerprint({"request": request.model_dump(mode="json"), "explicit_policy": request.policy.model_dump(mode="json", exclude_unset=True)})
        task_id = str(uuid4())
        with connect(self.settings) as conn:
            previous = conn.execute("SELECT id,status,request_fingerprint,config_snapshot FROM tasks WHERE idempotency_key=%s", (idempotency_key,)).fetchone()
            if previous:
                if not self._same_submission(previous, request_hash, request):
                    raise DomainError("IDEMPOTENCY_CONFLICT", "Same key has a different request", 409)
                return plain({"id": previous["id"], "status": previous["status"]})
            if request.pipeline_id == "animation-v1":
                raise DomainError("PIPELINE_RETIRED", "animation-v1 is retired for new tasks; use animation-dag-v2. Existing tasks remain queryable and recoverable.", 422)
            if request.pipeline_id == "smoke-v1" and not self.settings.enable_test_faults:
                raise DomainError("INTERNAL_PIPELINE_DISABLED", "smoke-v1 is an internal test pipeline; new product tasks use animation-dag-v2.", 422)
            if request.execution_backend == "external" and not (self.settings.external_executor_url or self.settings.execution_nodes):
                raise DomainError("BACKEND_UNAVAILABLE", "External execution requires an explicitly configured executor", 422)
            snapshot = freeze_config(request, self.settings)
            if request.input.get("kind") == "uploaded":
                from app.inputs import resolve_input
                snapshot["input_asset"] = resolve_input(conn, request.input["input_id"], self.settings.artifact_root)
            timeout = snapshot["runtime_policy"]["task_deadline_seconds"]
            row = conn.execute("""INSERT INTO tasks(id,idempotency_key,request_fingerprint,pipeline_id,config_snapshot,status,deadline_at)
                VALUES (%s,%s,%s,%s,%s,'QUEUED',clock_timestamp()+(%s*interval '1 second'))
                ON CONFLICT (idempotency_key) DO NOTHING RETURNING id,status""",
                (task_id, idempotency_key, request_hash, request.pipeline_id, Jsonb(snapshot), timeout)).fetchone()
            if row is None:
                previous = conn.execute("SELECT id,status,request_fingerprint,config_snapshot FROM tasks WHERE idempotency_key=%s", (idempotency_key,)).fetchone()
                if not self._same_submission(previous, request_hash, request):
                    raise DomainError("IDEMPOTENCY_CONFLICT", "Same key has a different request", 409)
                return plain({"id": previous["id"], "status": previous["status"]})
            for spec in snapshot["steps"]:
                conn.execute("INSERT INTO steps(id,task_id,step_key,spec,depends_on,status) VALUES (%s,%s,%s,%s,%s,%s)",
                    (str(uuid4()), task_id, spec["key"], Jsonb(spec), Jsonb(spec["depends_on"]), "PENDING" if spec["depends_on"] else "READY"))
            self._event(conn, task_id, "TASK_SUBMITTED", pipeline_id=request.pipeline_id)
            if request.pipeline_id == "managed-v1":
                conn.execute("UPDATE tasks SET registration_closed=false,phase='WAITING_STEPS',wait_reason='WAITING_STEPS' WHERE id=%s", (task_id,))
            return plain(row)

    def _business_task(self, conn, task_id):
        task = conn.execute("SELECT * FROM tasks WHERE id=%s FOR UPDATE", (task_id,)).fetchone()
        if not task:
            raise DomainError("NOT_FOUND", "Task not found", 404)
        if task["pipeline_id"] != "managed-v1":
            raise DomainError("FIXED_WORKFLOW", "Frozen example workflows cannot register or finish business steps", 409)
        return task

    def _business_replay(self, conn, task_id, key, operation, request_hash):
        old = conn.execute("SELECT * FROM business_operations WHERE task_id=%s AND idempotency_key=%s", (task_id, key)).fetchone()
        if old and (old["operation"] != operation or old["request_fingerprint"] != request_hash):
            raise DomainError("IDEMPOTENCY_CONFLICT", "Business operation key has a different request", 409)
        return old

    def register_step(self, task_id, request: RegisterStep, idempotency_key):
        request_hash = fingerprint(request.model_dump(mode="json"))
        with connect(self.settings) as conn:
            task = self._business_task(conn, task_id)
            old = self._business_replay(conn, task_id, idempotency_key, "register", request_hash)
            if old:
                return plain(conn.execute("SELECT * FROM steps WHERE id=%s", (old["step_id"],)).fetchone())
            if task["registration_closed"] or task["status"] not in ("QUEUED", "RUNNING"):
                raise DomainError("REGISTRATION_CLOSED", "Task is closed or must be resumed before registering work", 409)
            tail = conn.execute("SELECT * FROM steps WHERE task_id=%s ORDER BY registration_order DESC LIMIT 1", (task_id,)).fetchone()
            if request.after_step != (tail["step_key"] if tail else None):
                raise DomainError("TAIL_CONFLICT", "Read persisted history and register after the current last step", 409)
            if conn.execute("SELECT 1 FROM steps WHERE task_id=%s AND step_key=%s", (task_id, request.key)).fetchone():
                raise DomainError("STEP_EXISTS", "Step key is already registered; replay its original operation key", 409)
            backend = task["config_snapshot"]["execution_backend"]["kind"]
            if backend == "local" and request.processor_version != "1":
                raise DomainError("INVALID_CONFIG", "Local mock-step supports only processor version 1", 422)
            if request.parameters.fail_first_attempts and not self.settings.enable_test_faults:
                raise DomainError("TEST_FAULTS_DISABLED", "Failure injection requires ENABLE_TEST_FAULTS", 422)
            spec = StepSpec(key=request.key, processor=request.processor, processor_version=request.processor_version,
                depends_on=[tail["step_key"]] if tail else [], parameters=request.parameters.model_dump(mode="json"),
                resource_kind=request.resource_kind, checkpoint_mode="committed-batch-manifest", cache_enabled=False).model_dump(mode="json")
            value = request.input if request.input is not None else task["config_snapshot"]["request"]["input"]
            step_id = str(uuid4())
            order = tail["registration_order"] + 1 if tail else 1
            conn.execute("""INSERT INTO steps(id,task_id,step_key,spec,depends_on,status,registration_order,input_snapshot)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", (step_id, task_id, request.key, Jsonb(spec), Jsonb(spec["depends_on"]), "PENDING" if tail else "READY", order, Jsonb(value)))
            conn.execute("INSERT INTO business_operations VALUES (%s,%s,'register',%s,%s)", (task_id, idempotency_key, request_hash, step_id))
            self._event(conn, task_id, "STEP_REGISTERED", step_id, step_key=request.key, registration_order=order, after_step=request.after_step)
            if task["phase"] == "WAITING_STEPS":
                conn.execute("UPDATE tasks SET phase='QUEUED',wait_reason=NULL WHERE id=%s", (task_id,))
            self._advance(conn, task)
            return plain(conn.execute("SELECT * FROM steps WHERE id=%s", (step_id,)).fetchone())

    def finish(self, task_id, request: FinishTask, idempotency_key):
        request_hash = fingerprint(request.model_dump(mode="json"))
        with connect(self.settings) as conn:
            task = self._business_task(conn, task_id)
            old = self._business_replay(conn, task_id, idempotency_key, "finish", request_hash)
            if old:
                return plain({key: task[key] for key in ("id", "status", "registration_closed")})
            if task["registration_closed"] or task["status"] not in ("QUEUED", "RUNNING"):
                raise DomainError("REGISTRATION_CLOSED", "Task is closed or must be resumed before finishing", 409)
            tail = conn.execute("SELECT step_key FROM steps WHERE task_id=%s ORDER BY registration_order DESC LIMIT 1", (task_id,)).fetchone()
            if request.last_step != (tail["step_key"] if tail else None):
                raise DomainError("TAIL_CONFLICT", "Finish must name the persisted last step", 409)
            conn.execute("UPDATE tasks SET registration_closed=true WHERE id=%s", (task_id,))
            conn.execute("INSERT INTO business_operations VALUES (%s,%s,'finish',%s,NULL)", (task_id, idempotency_key, request_hash))
            self._event(conn, task_id, "WORKFLOW_CLOSED", last_step=request.last_step)
            self._advance(conn, {**task, "registration_closed": True})
            return plain(conn.execute("SELECT id,status,registration_closed FROM tasks WHERE id=%s", (task_id,)).fetchone())

    def get_task(self, task_id):
        with connect(self.settings) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            task = conn.execute("SELECT * FROM tasks WHERE id=%s", (task_id,)).fetchone()
            if not task:
                raise DomainError("NOT_FOUND", "Task not found", 404)
            steps = conn.execute("""SELECT s.*,a.node_id AS execution_node_id,a.resource_device_id AS execution_resource_id
                FROM steps s LEFT JOIN LATERAL (SELECT node_id,resource_device_id FROM step_attempts
                    WHERE step_id=s.id ORDER BY attempt_number DESC LIMIT 1) a ON true
                WHERE s.task_id=%s ORDER BY registration_order NULLS LAST,step_key""", (task_id,)).fetchall()
            return {**plain(task), "steps": [plain(s) for s in steps]}

    def list_tasks(self, status=None, limit=50, offset=0):
        with connect(self.settings) as conn:
            return [plain(r) for r in conn.execute("SELECT id,pipeline_id,status,phase,wait_reason,created_at,finished_at FROM tasks WHERE (%s::text IS NULL OR status=%s) ORDER BY created_at DESC,id LIMIT %s OFFSET %s", (status, status, limit, offset)).fetchall()]

    def monitor(self, status=None, q="", limit=20, offset=0):
        """Read-only, coherent queue snapshot; never drives task execution."""
        search = q.strip().lower()
        predicate = "(%s::text IS NULL OR t.status=%s) AND (%s='' OR strpos(t.id::text,%s)>0 OR strpos(lower(t.pipeline_id),%s)>0)"
        values = (status, status, search, search, search)
        with connect(self.settings) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            counts = {state.value: 0 for state in TaskStatus}
            for row in conn.execute("SELECT status,count(*) AS n FROM tasks GROUP BY status").fetchall():
                counts[row["status"]] = row["n"]
            waiting = conn.execute("""SELECT
                count(*) FILTER (WHERE status='RUNNING' AND wait_reason='WAITING_GPU') AS gpu,
                count(*) FILTER (WHERE status='RUNNING' AND wait_reason='WAITING_CPU') AS cpu,
                count(*) FILTER (WHERE status='RUNNING' AND wait_reason='RETRY_BACKOFF') AS retry
                FROM tasks""").fetchone()
            total = conn.execute("SELECT count(*) AS n FROM tasks t WHERE " + predicate, values).fetchone()["n"]
            items = conn.execute("""SELECT t.id,t.pipeline_id,t.status,t.phase,t.wait_reason,t.error_code,t.registration_closed,
                t.created_at,t.finished_at,t.recovery_round,progress.step_count,progress.completed_steps,
                progress.cached_steps,progress.running_steps,
                (SELECT count(*) FROM step_attempts a JOIN steps s ON s.id=a.step_id WHERE s.task_id=t.id) AS attempt_count,
                (SELECT max(a.heartbeat_at) FROM step_attempts a JOIN steps s ON s.id=a.step_id
                    WHERE s.task_id=t.id AND a.status='RUNNING') AS heartbeat_at
                FROM tasks t CROSS JOIN LATERAL (SELECT count(*) AS step_count,
                    count(*) FILTER (WHERE status IN ('SUCCEEDED','CACHED')) AS completed_steps,
                    count(*) FILTER (WHERE status='CACHED') AS cached_steps,
                    count(*) FILTER (WHERE status='RUNNING') AS running_steps
                    FROM steps WHERE task_id=t.id) progress WHERE """ + predicate +
                " ORDER BY t.created_at DESC,t.id DESC LIMIT %s OFFSET %s", (*values, limit, offset)).fetchall()
            unconfirmed = "a.process_exit_confirmed_at IS NULL AND a.resource_cleanup_state IN ('CPU_PROCESS_UNCONFIRMED','QUARANTINED')"
            acknowledgement_join = """LEFT JOIN monitor_acknowledgements m ON m.attempt_id=a.id
                AND m.attempt_status=a.status AND m.resource_cleanup_state=a.resource_cleanup_state
                AND m.stop_requested_at IS NOT DISTINCT FROM a.stop_requested_at"""
            attention_counts = conn.execute("""SELECT count(*) AS total,
                count(m.attempt_id) AS acknowledged FROM step_attempts a """ + acknowledgement_join +
                " WHERE " + unconfirmed).fetchone()
            attention_query = """SELECT s.task_id,s.step_key,a.id AS attempt_id,a.status,a.worker_id,
                a.resource_cleanup_state,a.stop_requested_at,m.acknowledged_at
                FROM step_attempts a JOIN steps s ON s.id=a.step_id """ + acknowledgement_join + " WHERE " + unconfirmed
            order = " ORDER BY a.started_at DESC,a.id DESC LIMIT 5"
            attention = conn.execute(attention_query + order).fetchall()
            pending_attention = conn.execute(attention_query + " AND m.attempt_id IS NULL" + order).fetchall()
            acknowledged_attention = conn.execute(attention_query + " AND m.attempt_id IS NOT NULL" + order).fetchall()
            return {"counts": counts, "total": sum(counts.values()), "waiting": waiting,
                    "filtered_total": total, "limit": limit, "offset": offset,
                    "unconfirmed_count": attention_counts["total"], "unconfirmed_executors": [plain(row) for row in attention],
                    "unacknowledged_count": attention_counts["total"] - attention_counts["acknowledged"],
                    "unacknowledged_executors": [plain(row) for row in pending_attention],
                    "acknowledged_count": attention_counts["acknowledged"],
                    "acknowledged_executors": [plain(row) for row in acknowledged_attention],
                    "observed_at": conn.execute("SELECT clock_timestamp() AS observed").fetchone()["observed"],
                    "items": [plain(row) for row in items]}

    def acknowledge_monitor_attempt(self, attempt_id, request):
        """Acknowledge the observed warning, never an executor exit or release."""
        with connect(self.settings) as conn:
            attempt = conn.execute("SELECT * FROM step_attempts WHERE id=%s FOR UPDATE", (attempt_id,)).fetchone()
            if not attempt:
                raise DomainError("NOT_FOUND", "Attempt not found", 404)
            if (attempt["process_exit_confirmed_at"] is not None
                    or attempt["resource_cleanup_state"] not in ("CPU_PROCESS_UNCONFIRMED", "QUARANTINED")
                    or any(attempt[key] != getattr(request, key) for key in ("status", "resource_cleanup_state", "stop_requested_at"))):
                raise DomainError("MONITOR_CONDITION_CHANGED", "Warning changed or was resolved; refresh before acknowledging", 409)
            row = conn.execute("""INSERT INTO monitor_acknowledgements
                (attempt_id,attempt_status,resource_cleanup_state,stop_requested_at) VALUES (%s,%s,%s,%s)
                ON CONFLICT(attempt_id) DO UPDATE SET
                  acknowledged_at=CASE WHEN monitor_acknowledgements.attempt_status=excluded.attempt_status
                    AND monitor_acknowledgements.resource_cleanup_state=excluded.resource_cleanup_state
                    AND monitor_acknowledgements.stop_requested_at IS NOT DISTINCT FROM excluded.stop_requested_at
                    THEN monitor_acknowledgements.acknowledged_at ELSE clock_timestamp() END,
                  attempt_status=excluded.attempt_status,resource_cleanup_state=excluded.resource_cleanup_state,
                  stop_requested_at=excluded.stop_requested_at RETURNING attempt_id,acknowledged_at""",
                (attempt_id, request.status.value, request.resource_cleanup_state, request.stop_requested_at)).fetchone()
            return plain(row)

    def restore_monitor_reminder(self, attempt_id):
        with connect(self.settings) as conn:
            if not conn.execute("SELECT id FROM step_attempts WHERE id=%s FOR UPDATE", (attempt_id,)).fetchone():
                raise DomainError("NOT_FOUND", "Attempt not found", 404)
            conn.execute("DELETE FROM monitor_acknowledgements WHERE attempt_id=%s", (attempt_id,))
            return {"attempt_id": str(attempt_id), "acknowledged_at": None}

    def resources(self):
        with connect(self.settings) as conn:
            slots = conn.execute("""SELECT s.*,a.worker_id,a.executor_pid,a.executor_host,
                a.process_exit_confirmed_at,a.resource_cleanup_state
                FROM resource_slots s LEFT JOIN step_attempts a ON a.id=s.owner_attempt_id WHERE s.node_id='legacy' ORDER BY s.id""").fetchall()
            from .nodes import read_nodes
            return {"source": "mock", "physical_backend": "cpu", "slots": [plain(s) for s in slots],
                    "cpu": {"capacity": self.settings.cpu_max_concurrency, "in_use": self._cpu_in_use(conn)}, "nodes": read_nodes(conn, self.settings)}

    def _cpu_in_use(self, conn, node_id="legacy"):
        return conn.execute("""SELECT count(*) AS n FROM step_attempts a JOIN steps s ON s.id=a.step_id
            WHERE coalesce(s.spec->>'resource_kind','cpu')='cpu'
            AND a.node_id=%s AND (a.status='RUNNING' OR (a.resource_cleanup_state='CPU_PROCESS_UNCONFIRMED' AND a.process_exit_confirmed_at IS NULL))""", (node_id,)).fetchone()["n"]

    def nodes(self):
        from .nodes import read_nodes
        with connect(self.settings) as conn:
            return {"nodes": read_nodes(conn, self.settings), "storage_id": self.settings.artifact_storage_id}

    def sync_nodes(self, force=False):
        if force or not self._nodes_synced:
            from .nodes import sync_nodes
            with connect(self.settings) as conn:
                sync_nodes(conn, self.settings)
            self._nodes_synced = True

    def observe_nodes(self, stop=None):
        """Called by the separate Worker scan thread, never by a read API."""
        if not self.settings.execution_nodes:
            return
        from .backends import ExternalClient
        from .nodes import inspection_binding, observe, storage_reference, validate_report
        # Configuration is immutable for this process. Observation must not
        # rewrite every device row while active Attempts hold resource locks.
        self.sync_nodes()
        reference = storage_reference(self.settings)
        for node in self.settings.execution_nodes:
            if stop is not None and stop.is_set():
                break
            data, error = None, None
            try:
                data = ExternalClient(self.settings, inspection_binding(node)).inspect(self.settings.artifact_storage_id, [reference])
                validate_report(data, node)
                if not data["references_readable"]:
                    error = "STORAGE_UNAVAILABLE"
            except ValueError:
                data, error = None, "CAPABILITY_MISMATCH"
            except (OSError, TimeoutError) as failure:
                data, error = None, "TLS_ERROR" if "SSL" in type(getattr(failure, "reason", failure)).__name__ else "UNREACHABLE"
            with connect(self.settings) as conn:
                observe(conn, node, data, error)

    def _finish_cancel(self, conn, task_id):
        uncertain = conn.execute("""SELECT 1 FROM step_attempts a JOIN steps s ON s.id=a.step_id
            WHERE s.task_id=%s AND (a.status='RUNNING' OR (a.stop_requested_at IS NOT NULL AND a.process_exit_confirmed_at IS NULL)) LIMIT 1""", (task_id,)).fetchone()
        slot = conn.execute("""SELECT 1 FROM resource_slots r JOIN step_attempts a ON a.id=r.owner_attempt_id
            JOIN steps s ON s.id=a.step_id WHERE s.task_id=%s LIMIT 1""", (task_id,)).fetchone()
        if not uncertain and not slot:
            conn.execute("UPDATE tasks SET status='CANCELED',phase='CANCELED',wait_reason=NULL,finished_at=clock_timestamp() WHERE id=%s", (task_id,))
            self._event(conn, task_id, "TASK_CANCELED")
            return True
        return False

    def cancel(self, task_id):
        # The request revokes commit authority, never waits for physical exit.
        with connect(self.settings) as conn:
            task = conn.execute("SELECT * FROM tasks WHERE id=%s FOR UPDATE", (task_id,)).fetchone()
            if not task:
                raise DomainError("NOT_FOUND", "Task not found", 404)
            if task["status"] in ("CANCELED", "CANCELING"):
                return {"id": str(task_id), "status": task["status"]}
            if task["status"] not in ("QUEUED", "RUNNING"):
                raise DomainError("INVALID_STATE", "Only queued or running tasks can be canceled", 409)
            conn.execute("UPDATE tasks SET status='CANCELING',phase='CANCELING',wait_reason='PROCESS_EXIT_PENDING' WHERE id=%s", (task_id,))
            self._event(conn, task_id, "TASK_CANCEL_REQUESTED")
            for step in conn.execute("UPDATE steps SET status='CANCELED',wait_reason=NULL,next_retry_at=NULL WHERE task_id=%s AND status NOT IN ('SUCCEEDED','CACHED','RUNNING') RETURNING id", (task_id,)).fetchall():
                self._event(conn, task_id, "STEP_CANCELED", step["id"])
            for step in conn.execute("SELECT * FROM steps WHERE task_id=%s AND status='RUNNING' FOR UPDATE", (task_id,)).fetchall():
                conn.execute("UPDATE step_attempts SET stop_requested_at=coalesce(stop_requested_at,clock_timestamp()) WHERE id=%s", (step["current_attempt_id"],))
                self._resource_end(conn, task_id, step, step["current_attempt_id"], False)
            finished = self._finish_cancel(conn, task_id)
            return {"id": str(task_id), "status": "CANCELED" if finished else "CANCELING"}

    def cancel_stopped(self, claim, confirmed):
        # Physical cleanup acknowledgment, not an execution/result commit.
        if not confirmed:
            return False
        with connect(self.settings) as conn:
            task = conn.execute("SELECT * FROM tasks WHERE id=%s FOR UPDATE", (claim["task_id"],)).fetchone()
            if not task or task["status"] != "CANCELING":
                return False
            step = conn.execute("SELECT * FROM steps WHERE id=%s AND task_id=%s FOR UPDATE", (claim["step_id"], task["id"])).fetchone()
            attempt = conn.execute("SELECT * FROM step_attempts WHERE id=%s AND step_id=%s FOR UPDATE", (claim["attempt_id"], step["id"])).fetchone()
            if not attempt or attempt["worker_id"] != claim["worker_id"] or attempt["fencing_token"] != claim["generation"]:
                return False
            if attempt["execution_backend"] == "external" and not self._exit_confirmed(conn, attempt["id"]):
                return False
            if attempt["execution_backend"] == "local" and attempt["executor_pid"] is not None and executor_state(attempt["executor_pid"], attempt["executor_created_at"], attempt["executor_host"]) != "EXITED":
                return False
            conn.execute("UPDATE step_attempts SET process_exit_confirmed_at=coalesce(process_exit_confirmed_at,clock_timestamp()),resource_cleanup_state=CASE WHEN resource_slot_id IS NULL THEN 'CPU_ONLY' ELSE resource_cleanup_state END WHERE id=%s", (attempt["id"],))
            if attempt["status"] == "RUNNING":
                if step["current_attempt_id"] != attempt["id"] or step["execution_generation"] != claim["generation"]:
                    return False
                conn.execute("UPDATE step_attempts SET status='CANCELED',finished_at=clock_timestamp(),retryable=false WHERE id=%s", (attempt["id"],))
                conn.execute("UPDATE steps SET status='CANCELED',current_attempt_id=NULL,wait_reason=NULL WHERE id=%s", (step["id"],))
                self._event(conn, task["id"], "ATTEMPT_CANCELED", step["id"], attempt["id"])
            self._resource_end(conn, task["id"], {**step, "execution_generation": claim["generation"]}, attempt["id"], True)
            self._finish_cancel(conn, task["id"])
            return True

    def reconcile_cancellations(self):
        with connect(self.settings) as conn:
            attempts = conn.execute("""SELECT a.*,s.task_id,s.execution_generation FROM step_attempts a
                JOIN steps s ON s.id=a.step_id JOIN tasks t ON t.id=s.task_id WHERE t.status='CANCELING'
                AND (a.status='RUNNING' OR (a.stop_requested_at IS NOT NULL AND a.process_exit_confirmed_at IS NULL))""").fetchall()
        for attempt in attempts:
            if self._stop_attempt(attempt):
                self.cancel_stopped({"task_id": attempt["task_id"], "step_id": attempt["step_id"], "attempt_id": attempt["id"], "generation": attempt["fencing_token"], "worker_id": attempt["worker_id"]}, True)
        with connect(self.settings) as conn:
            for task in conn.execute("SELECT id FROM tasks WHERE status='CANCELING' FOR UPDATE SKIP LOCKED").fetchall():
                self._finish_cancel(conn, task["id"])

    def events(self, task_id, after_seq=0, limit=200):
        self.get_task(task_id)
        with connect(self.settings) as conn:
            return [plain(r) for r in conn.execute("SELECT * FROM task_events WHERE task_id=%s AND seq>%s ORDER BY seq LIMIT %s", (task_id, after_seq, limit)).fetchall()]

    def attempts(self, task_id, step_key):
        self.get_task(task_id)
        with connect(self.settings) as conn:
            step = conn.execute("SELECT id FROM steps WHERE task_id=%s AND (step_key=%s OR id::text=%s)", (task_id, step_key, step_key)).fetchone()
            if not step:
                raise DomainError("NOT_FOUND", "Step not found", 404)
            return [plain(r) for r in conn.execute("SELECT * FROM step_attempts WHERE step_id=%s ORDER BY attempt_number", (step["id"],)).fetchall()]

    def _artifact(self, conn, artifact_id, validate=True):
        row = conn.execute("SELECT * FROM artifacts WHERE id=%s", (artifact_id,)).fetchone()
        if row and validate:
            valid = row["availability"] == "AVAILABLE" and available(self.settings.artifact_root, plain(row))
            if not valid:
                conn.execute("UPDATE artifacts SET availability='UNAVAILABLE' WHERE id=%s", (artifact_id,))
                return None
        return plain(row)

    def artifacts(self, task_id):
        task = self.get_task(task_id)
        # Include reused artifacts, not only files produced by this Task.
        ids = {r for s in task["steps"] for r in s["output_refs"]}
        ids.update(s["checkpoint_artifact_id"] for s in task["steps"] if s["checkpoint_artifact_id"])
        with connect(self.settings) as conn:
            for row in conn.execute("SELECT id FROM artifacts WHERE task_id=%s", (task_id,)).fetchall():
                ids.add(str(row["id"]))
            result = []
            for artifact_id in sorted(ids):
                self._artifact(conn, artifact_id)
                artifact = self._artifact(conn, artifact_id, validate=False)
                if artifact:
                    artifact["download_url"] = f"/api/v1/tasks/{task_id}/artifacts/{artifact_id}"
                    result.append(artifact)
            return result

    def _owned(self, conn, claim, allow_deadline=False):
        task = conn.execute("SELECT * FROM tasks WHERE id=%s FOR UPDATE", (claim["task_id"],)).fetchone()
        if not task or task["status"] != "RUNNING":
            return None
        row = conn.execute("""SELECT s.*,a.lease_expires_at,a.status AS attempt_status,a.execution_backend,a.executor_id,a.external_job_id,a.node_id,a.resource_device_id,a.resource_slot_id FROM steps s
            JOIN step_attempts a ON a.id=s.current_attempt_id
            WHERE s.id=%s AND s.task_id=%s AND a.id=%s AND s.execution_generation=%s
              AND a.fencing_token=%s AND a.worker_id=%s AND s.status='RUNNING' AND a.status='RUNNING'
              AND a.lease_expires_at>clock_timestamp() FOR UPDATE OF s,a""",
            (claim["step_id"], claim["task_id"], claim["attempt_id"], claim["generation"], claim["generation"], claim["worker_id"])).fetchone()
        if not row:
            return None
        if row["execution_backend"] != claim.get("execution_backend", "local"):
            return None
        if row["node_id"] != claim.get("node_id", "legacy"):
            return None
        if row["node_id"] != "legacy" and (row["resource_device_id"] != claim.get("resource_device_id") or row["resource_slot_id"] != claim.get("resource_slot_id")):
            return None
        if row["execution_backend"] == "external" and (row["executor_id"] != claim.get("executor_id") or str(row["external_job_id"]) != claim.get("external_job_id")):
            return None
        if row["spec"].get("resource_kind") == "mock-gpu":
            slot = conn.execute("""SELECT id FROM resource_slots WHERE owner_attempt_id=%s
                AND node_id=%s AND device_id=%s
                AND owner_generation=%s AND state IN ('RESERVED','RUNNING') FOR UPDATE""",
                (claim["attempt_id"], row["node_id"], row["resource_device_id"], claim["generation"])).fetchone()
            if not slot:
                return None
        if not allow_deadline and task["deadline_at"] and conn.execute("SELECT %s>clock_timestamp() AS valid", (task["deadline_at"],)).fetchone()["valid"] is False:
            return None
        return task, row

    def heartbeat(self, claim):
        with connect(self.settings) as conn:
            owned = self._owned(conn, claim)
            if not owned:
                return False
            ttl = owned[0]["config_snapshot"]["runtime_policy"]["lease_ttl_seconds"]
            renewed = conn.execute("UPDATE step_attempts SET heartbeat_at=clock_timestamp(),lease_expires_at=clock_timestamp()+(%s*interval '1 second') WHERE id=%s AND lease_expires_at>clock_timestamp() RETURNING lease_expires_at", (ttl, claim["attempt_id"])).fetchone()
            if renewed:
                claim["lease_expires_at"] = renewed["lease_expires_at"]
            return renewed is not None

    def _final_fence(self, conn, claim):
        # Artifact I/O may have consumed the remaining lease. Roll back the
        # whole transaction if it expired after the initial ownership gate.
        valid = conn.execute("SELECT lease_expires_at>clock_timestamp() AS valid FROM step_attempts WHERE id=%s", (claim["attempt_id"],)).fetchone()
        if not valid or not valid["valid"]:
            raise DomainError("LEASE_LOST", "Lease expired before authoritative commit", 409)

    def record_executor(self, claim, pid):
        with connect(self.settings) as conn:
            if claim.get("execution_backend", "local") != "local" or not self._owned(conn, claim):
                return False
            try:
                identity = identify_executor(pid, claim)
            except ValueError as error:
                raise DomainError("EXECUTOR_EXIT", "Executor identity registration failed", 409) from error
            conn.execute("UPDATE step_attempts SET executor_pid=%s,executor_created_at=%s,executor_host=%s WHERE id=%s", (pid, identity["created_at"], identity["host"], claim["attempt_id"]))
            conn.execute("UPDATE resource_slots SET state='RUNNING',updated_at=clock_timestamp() WHERE owner_attempt_id=%s AND owner_generation=%s AND state='RESERVED'", (claim["attempt_id"], claim["generation"]))
            self._event(conn, claim["task_id"], "EXECUTOR_STARTED", claim["step_id"], claim["attempt_id"], pid=pid)
            self._final_fence(conn, claim)
            return True

    def record_external_started(self, claim):
        with connect(self.settings) as conn:
            if not self._owned(conn, claim):
                return False
            changed = conn.execute("UPDATE execution_commands SET dispatched_at=clock_timestamp() WHERE id=%s AND dispatched_at IS NULL RETURNING id", (claim["attempt_id"],)).fetchone()
            if changed:
                conn.execute("UPDATE resource_slots SET state='RUNNING',updated_at=clock_timestamp() WHERE owner_attempt_id=%s AND owner_generation=%s AND state='RESERVED'", (claim["attempt_id"], claim["generation"]))
                self._event(conn, claim["task_id"], "EXTERNAL_COMMAND_OBSERVED", claim["step_id"], claim["attempt_id"], executor_id=claim["executor_id"], job_id=claim["external_job_id"], command_id=claim["attempt_id"])
            return True

    def record_external_exit(self, claim, receipt):
        # Physical facts may arrive after lease loss; never commit results here.
        from .external_protocol import binding_for, validate_status
        binding = binding_for(claim)
        receipt = validate_status(receipt, binding)
        if not receipt["exit_confirmed"]:
            return False
        with connect(self.settings) as conn:
            conn.execute("SELECT id FROM tasks WHERE id=%s FOR UPDATE", (claim["task_id"],))
            conn.execute("SELECT id FROM steps WHERE id=%s AND task_id=%s FOR UPDATE", (claim["step_id"], claim["task_id"]))
            attempt = conn.execute("SELECT id FROM step_attempts WHERE id=%s AND step_id=%s AND fencing_token=%s AND worker_id=%s AND execution_backend='external' FOR UPDATE", (claim["attempt_id"], claim["step_id"], claim["generation"], claim["worker_id"])).fetchone()
            if not attempt:
                return False
            command = conn.execute("SELECT binding,route FROM execution_commands WHERE id=%s FOR UPDATE", (claim["attempt_id"],)).fetchone()
            if not command or command["binding"] != binding:
                return False
            if command["route"] and receipt.get("state_id") != command["route"]["state_id"]:
                return False
            fact = {k: v for k, v in receipt.items() if k != "events"}
            conn.execute("UPDATE execution_commands SET exit_confirmed_at=coalesce(exit_confirmed_at,clock_timestamp()),exit_receipt=%s WHERE id=%s", (Jsonb(fact), claim["attempt_id"]))
            return True

    def _stop_attempt(self, attempt):
        if attempt.get("execution_backend", "local") != "external":
            return stop_executor(attempt["executor_pid"], attempt["executor_created_at"], attempt["executor_host"])
        from .backends import stop_external
        if attempt.get("node_id", "legacy") != "legacy":
            with connect(self.settings) as conn:
                command = conn.execute("SELECT route FROM execution_commands WHERE id=%s", (attempt.get("id", attempt.get("attempt_id")),)).fetchone()
            if not command or not command["route"]:
                return False
            attempt = {**attempt, "node_route": command["route"]}
        receipt = stop_external(self.settings, attempt)
        if receipt is None:
            return False
        claim = {**attempt, "attempt_id": str(attempt["id"]), "generation": attempt["fencing_token"]}
        return self.record_external_exit(claim, receipt)

    def _exit_confirmed(self, conn, attempt_id):
        attempt = conn.execute("SELECT * FROM step_attempts WHERE id=%s", (attempt_id,)).fetchone()
        if attempt["execution_backend"] == "external":
            return conn.execute("SELECT 1 FROM execution_commands WHERE id=%s AND exit_confirmed_at IS NOT NULL", (attempt_id,)).fetchone() is not None
        return executor_state(attempt["executor_pid"], attempt["executor_created_at"], attempt["executor_host"]) == "EXITED"

    def _resource_end(self, conn, task_id, step, attempt_id, confirmed):
        slot = conn.execute("SELECT * FROM resource_slots WHERE owner_attempt_id=%s AND owner_generation=%s FOR UPDATE", (attempt_id, step["execution_generation"])).fetchone()
        if not slot:
            return
        if confirmed:
            conn.execute("UPDATE resource_slots SET state='AVAILABLE',owner_attempt_id=NULL,owner_generation=NULL,updated_at=clock_timestamp() WHERE id=%s", (slot["id"],))
            state, event = "RELEASED", "RESOURCE_RELEASED"
        else:
            conn.execute("UPDATE resource_slots SET state='QUARANTINED',updated_at=clock_timestamp() WHERE id=%s", (slot["id"],))
            state, event = "QUARANTINED", "RESOURCE_QUARANTINED"
        conn.execute("UPDATE step_attempts SET resource_cleanup_state=%s WHERE id=%s", (state, attempt_id))
        self._event(conn, task_id, event, step["id"], attempt_id, slot_id=slot["id"], node_id=slot["node_id"], resource_id=slot["device_id"], generation=step["execution_generation"], source="mock")

    def _gpu_exit_confirmed(self, conn, attempt_id):
        return self._exit_confirmed(conn, attempt_id)

    def progress(self, claim, progress):
        with connect(self.settings) as conn:
            if not self._owned(conn, claim):
                return False
            conn.execute("UPDATE step_attempts SET progress=%s WHERE id=%s", (Jsonb(progress), claim["attempt_id"]))
            self._final_fence(conn, claim)
            return True

    def warning(self, claim, code, message):
        with connect(self.settings) as conn:
            if not self._owned(conn, claim):
                return False
            self._event(conn, claim["task_id"], "SOFT_WARNING", claim["step_id"], claim["attempt_id"], code=code, message=message)
            self._final_fence(conn, claim)
            return True

    def _register(self, conn, claim, artifact):
        # Files are candidates until this fenced transaction commits.
        prefix = f"{claim['task_id']}/{claim['step_key']}/{claim['attempt_id']}/"
        if not artifact["relative_path"].replace("\\", "/").startswith(prefix):
            raise DomainError("INVALID_ARTIFACT", "Artifact is outside this Attempt namespace", 409)
        if not available(self.settings.artifact_root, artifact):
            raise DomainError("ARTIFACT_UNAVAILABLE", "Artifact or referenced batch is unavailable", 409)
        if artifact["metadata"].get("compatibility_key") != claim["compatibility_key"]:
            raise DomainError("CHECKPOINT_INCOMPATIBLE", "Artifact compatibility key differs", 409)
        conn.execute("""INSERT INTO artifacts(id,task_id,step_id,kind,relative_path,content_fingerprint,producer_attempt_id,metadata)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(id) DO NOTHING""",
            (artifact["id"], claim["task_id"], claim["step_id"], artifact["kind"], artifact["relative_path"], artifact["content_fingerprint"], claim["attempt_id"], Jsonb(artifact["metadata"])))

    def commit_checkpoint(self, claim, artifact):
        with connect(self.settings) as conn:
            if not self._owned(conn, claim):
                return False
            if artifact["kind"] != "checkpoint":
                raise DomainError("INVALID_ARTIFACT", "Expected a checkpoint", 409)
            prior = conn.execute("SELECT a.metadata FROM steps s JOIN artifacts a ON a.id=s.checkpoint_artifact_id WHERE s.id=%s", (claim["step_id"],)).fetchone()
            if prior:
                prefix = prior["metadata"]["completed_batches"]
                if artifact["metadata"].get("completed_batches", [])[:len(prefix)] != prefix:
                    raise DomainError("CHECKPOINT_INCOMPATIBLE", "A committed batch prefix cannot be replaced", 409)
            self._register(conn, claim, artifact)
            conn.execute("UPDATE steps SET checkpoint_artifact_id=%s WHERE id=%s", (artifact["id"], claim["step_id"]))
            self._event(conn, claim["task_id"], "CHECKPOINT_COMMITTED", claim["step_id"], claim["attempt_id"], artifact_id=artifact["id"], completed_batches=len(artifact["metadata"].get("completed_batches", [])))
            self._final_fence(conn, claim)
            return True

    def succeed(self, claim, artifact):
        with connect(self.settings) as conn:
            owned = self._owned(conn, claim)
            if not owned:
                return False
            if artifact["kind"] != "output":
                raise DomainError("INVALID_ARTIFACT", "Expected an output", 409)
            self._register(conn, claim, artifact)
            external = owned[1]["execution_backend"] == "external"
            confirmed = self._exit_confirmed(conn, claim["attempt_id"]) if external else owned[1]["spec"].get("resource_kind") != "mock-gpu" or self._gpu_exit_confirmed(conn, claim["attempt_id"])
            if external and not confirmed:
                raise DomainError("EXECUTOR_EXIT", "External process exit has not been confirmed", 409)
            conn.execute("UPDATE step_attempts SET status='SUCCEEDED',finished_at=clock_timestamp(),process_exit_confirmed_at=CASE WHEN %s THEN clock_timestamp() ELSE NULL END,progress=%s WHERE id=%s", (confirmed, Jsonb({"completed": 1, "total": 1}), claim["attempt_id"]))
            conn.execute("UPDATE steps SET status='SUCCEEDED',output_refs=%s,current_attempt_id=NULL,wait_reason=NULL WHERE id=%s", (Jsonb([artifact["id"]]), claim["step_id"]))
            self._event(conn, claim["task_id"], "STEP_SUCCEEDED", claim["step_id"], claim["attempt_id"], artifact_id=artifact["id"])
            self._resource_end(conn, claim["task_id"], owned[1], claim["attempt_id"], confirmed)
            self._advance(conn, owned[0])
            self._final_fence(conn, claim)
            return True

    def _failure(self, conn, task, step, attempt, code, message, retryable, status, confirmed=False):
        backend = conn.execute("SELECT execution_backend FROM step_attempts WHERE id=%s", (attempt["id"],)).fetchone()["execution_backend"]
        if confirmed and backend == "external":
            confirmed = self._exit_confirmed(conn, attempt["id"])
        if confirmed and step["spec"].get("resource_kind") == "mock-gpu":
            recorded = conn.execute("SELECT executor_pid FROM step_attempts WHERE id=%s", (attempt["id"],)).fetchone()
            # No PID with an owned, confirmed startup failure means the child
            # never started. Lost/unknown startup windows are never confirmed.
            if recorded["executor_pid"] is not None:
                confirmed = self._gpu_exit_confirmed(conn, attempt["id"])
        conn.execute("""UPDATE step_attempts SET status=%s,error_code=%s,error_message=%s,retryable=%s,finished_at=clock_timestamp(),
            process_exit_confirmed_at=CASE WHEN %s THEN clock_timestamp() ELSE NULL END,
            resource_cleanup_state=CASE WHEN %s THEN 'CPU_ONLY' ELSE 'CPU_PROCESS_UNCONFIRMED' END,
            stop_requested_at=CASE WHEN %s THEN NULL ELSE clock_timestamp() END WHERE id=%s""",
            (status, code, message[:2000], retryable, confirmed, confirmed, confirmed, attempt["id"]))
        self._resource_end(conn, task["id"], step, attempt["id"], confirmed)
        count = conn.execute("SELECT count(*) AS n FROM step_attempts WHERE step_id=%s AND recovery_round=%s", (step["id"], task["recovery_round"])).fetchone()["n"]
        policy = task["config_snapshot"]["retry_policy"]
        retry = retryable and count < policy["max_attempts_per_recovery_round"]
        delay = min(policy["initial_delay_seconds"] * (2 ** (count - 1)), policy["max_delay_seconds"])
        if policy["jitter"]:
            delay *= random.uniform(.5, 1)
        conn.execute("UPDATE steps SET status=%s,current_attempt_id=NULL,next_retry_at=CASE WHEN %s THEN clock_timestamp()+(%s*interval '1 second') ELSE NULL END,wait_reason=%s WHERE id=%s",
            ("RETRY_WAIT" if retry else "FAILED", retry, delay, "RETRY_BACKOFF" if retry else code, step["id"]))
        conn.execute("UPDATE tasks SET phase=%s,wait_reason=%s WHERE id=%s", ("RETRY_WAIT" if retry else "FAILED", "RETRY_BACKOFF" if retry else code, task["id"]))
        self._event(conn, task["id"], "ATTEMPT_"+status, step["id"], attempt["id"], code=code, retryable=retryable, will_retry=retry, recovery_round=task["recovery_round"])

    def fail(self, claim, error_code, message, retryable, process_exit_confirmed=True):
        with connect(self.settings) as conn:
            owned = self._owned(conn, claim, allow_deadline=True)
            if not owned:
                return False
            task, step = owned
            attempt = conn.execute("SELECT * FROM step_attempts WHERE id=%s", (claim["attempt_id"],)).fetchone()
            status = "TIMED_OUT" if error_code in ("ATTEMPT_TIMEOUT", "TASK_DEADLINE") else "FAILED"
            self._failure(conn, task, step, attempt, error_code, message, retryable, status, process_exit_confirmed)
            self._advance(conn, task)
            self._final_fence(conn, claim)
            return True

    def _refs(self, conn, refs):
        artifacts = [self._artifact(conn, r) for r in refs]
        return artifacts if all(artifacts) else None

    def _advance(self, conn, task):
        steps = conn.execute("SELECT * FROM steps WHERE task_id=%s ORDER BY step_key FOR UPDATE", (task["id"],)).fetchall()
        by_key = {s["step_key"]: s for s in steps}
        failed = any(s["status"] == "FAILED" for s in steps)
        for step in steps:
            if failed and step["status"] in ("PENDING", "READY", "RETRY_WAIT"):
                conn.execute("UPDATE steps SET status='BLOCKED',wait_reason='UPSTREAM_FAILED' WHERE id=%s", (step["id"],))
                step["status"] = "BLOCKED"
            elif step["status"] == "PENDING" and all(by_key[k]["status"] in ("SUCCEEDED", "CACHED") for k in step["depends_on"]):
                refs = [r for k in step["depends_on"] for r in by_key[k]["output_refs"]]
                if self._refs(conn, refs) is None:
                    conn.execute("UPDATE steps SET wait_reason='ARTIFACT_UNAVAILABLE' WHERE id=%s", (step["id"],))
                    conn.execute("UPDATE tasks SET wait_reason='ARTIFACT_UNAVAILABLE' WHERE id=%s", (task["id"],))
                else:
                    conn.execute("UPDATE steps SET status='READY',wait_reason=NULL WHERE id=%s", (step["id"],))
            elif step["status"] == "RETRY_WAIT":
                conn.execute("UPDATE steps SET status='READY',wait_reason=NULL WHERE id=%s AND next_retry_at<=clock_timestamp()", (step["id"],))
        if failed:
            if not any(s["status"] == "RUNNING" for s in steps):
                conn.execute("UPDATE tasks SET status='FAILED',phase='FAILED',finished_at=clock_timestamp() WHERE id=%s", (task["id"],))
                self._event(conn, task["id"], "TASK_FAILED")
        elif all(s["status"] in ("SUCCEEDED", "CACHED") for s in steps):
            if not task["registration_closed"]:
                conn.execute("UPDATE tasks SET phase='WAITING_STEPS',wait_reason='WAITING_STEPS' WHERE id=%s", (task["id"],))
                if task["phase"] != "WAITING_STEPS":
                    self._event(conn, task["id"], "WAITING_STEPS", registered_steps=len(steps))
                return
            if task["pipeline_id"] == "managed-v1" and conn.execute("""SELECT 1 FROM step_attempts a JOIN steps s ON s.id=a.step_id
                    WHERE s.task_id=%s AND (a.status='RUNNING' OR a.process_exit_confirmed_at IS NULL) LIMIT 1""", (task["id"],)).fetchone():
                conn.execute("UPDATE tasks SET phase='WAITING_CLEANUP',wait_reason='WAITING_CLEANUP' WHERE id=%s", (task["id"],))
                return
            terminal = [s for s in steps if not any(s["step_key"] in other["depends_on"] for other in steps)]
            refs = [r for s in terminal for r in s["output_refs"]]
            if self._refs(conn, refs) is not None:
                conn.execute("UPDATE tasks SET status='SUCCEEDED',phase='SUCCEEDED',finished_at=clock_timestamp(),output_refs=%s,wait_reason=NULL WHERE id=%s", (Jsonb(refs), task["id"]))
                self._event(conn, task["id"], "TASK_SUCCEEDED", output_refs=refs)
                # Publish reusable entries only with the successful Task commit.
                # A failed/canceled producer never replaces a usable cache entry.
                for step in steps:
                    if step["status"] == "SUCCEEDED" and step["spec"].get("cache_enabled") and step["step_key"] != "publish_result":
                        conn.execute("""INSERT INTO step_cache(cache_key,source_task_id,source_step_id,artifact_ids)
                            VALUES (%s,%s,%s,%s) ON CONFLICT(cache_key) DO UPDATE SET
                            source_task_id=excluded.source_task_id,source_step_id=excluded.source_step_id,
                            artifact_ids=excluded.artifact_ids,created_at=clock_timestamp()""",
                            (step["cache_key"], task["id"], step["id"], Jsonb(step["output_refs"])))

    def reconcile_cpu(self):
        with connect(self.settings) as conn:
            pending = conn.execute("""SELECT a.*,s.task_id FROM step_attempts a JOIN steps s ON s.id=a.step_id
                WHERE a.status<>'RUNNING' AND a.resource_cleanup_state='CPU_PROCESS_UNCONFIRMED'
                AND a.process_exit_confirmed_at IS NULL""").fetchall()
        for attempt in pending:
            if not self._stop_attempt(attempt):
                continue
            with connect(self.settings) as conn:
                conn.execute("SELECT id FROM tasks WHERE id=%s FOR UPDATE", (attempt["task_id"],))
                conn.execute("SELECT id FROM steps WHERE id=%s FOR UPDATE", (attempt["step_id"],))
                updated = conn.execute("""UPDATE step_attempts SET process_exit_confirmed_at=clock_timestamp(),resource_cleanup_state='CPU_ONLY'
                    WHERE id=%s AND fencing_token=%s AND status<>'RUNNING' AND process_exit_confirmed_at IS NULL RETURNING id""", (attempt["id"], attempt["fencing_token"])).fetchone()
                if updated:
                    self._event(conn, attempt["task_id"], "CPU_EXECUTOR_CLEANED", attempt["step_id"], attempt["id"])

    def reconcile_resources(self):
        """Stop/inspect only identified local orphans, outside DB transactions.

        A physical-exit acknowledgment is not a result commit: it can only
        clear the exact quarantined owner/generation, never revive an Attempt.
        Missing identity or another host stays quarantined for reconciliation.
        """
        with connect(self.settings) as conn:
            pending = conn.execute("""SELECT r.id AS slot_id,r.owner_attempt_id,r.owner_generation,
                a.*,s.task_id
                FROM resource_slots r JOIN step_attempts a ON a.id=r.owner_attempt_id
                JOIN steps s ON s.id=a.step_id WHERE r.state='QUARANTINED'""").fetchall()
        for item in pending:
            if not self._stop_attempt(item):
                continue
            with connect(self.settings) as conn:
                conn.execute("SELECT id FROM tasks WHERE id=%s FOR UPDATE", (item["task_id"],))
                conn.execute("SELECT id FROM steps WHERE id=%s FOR UPDATE", (item["step_id"],))
                conn.execute("SELECT id FROM step_attempts WHERE id=%s FOR UPDATE", (item["owner_attempt_id"],))
                slot = conn.execute("""SELECT * FROM resource_slots WHERE id=%s AND state='QUARANTINED'
                    AND owner_attempt_id=%s AND owner_generation=%s FOR UPDATE""",
                    (item["slot_id"], item["owner_attempt_id"], item["owner_generation"])).fetchone()
                if not slot:
                    continue
                conn.execute("UPDATE step_attempts SET process_exit_confirmed_at=coalesce(process_exit_confirmed_at,clock_timestamp()),resource_cleanup_state='RELEASED' WHERE id=%s", (item["owner_attempt_id"],))
                conn.execute("UPDATE resource_slots SET state='AVAILABLE',owner_attempt_id=NULL,owner_generation=NULL,updated_at=clock_timestamp() WHERE id=%s", (slot["id"],))
                self._event(conn, item["task_id"], "RESOURCE_RELEASED", item["step_id"], item["owner_attempt_id"], slot_id=slot["id"], generation=item["owner_generation"], source="mock", reconciled=True)

    def reconcile(self):
        """Worker-driven, lock-safe scan; computation runs in another process."""
        # Clean only previously quarantined resources. A newly expired lease
        # must first commit its LOST + QUARANTINED observation, not jump to free.
        self.reconcile_resources()
        self.reconcile_cpu()
        self.reconcile_cancellations()
        with connect(self.settings) as conn:
            ids = [r["id"] for r in conn.execute("SELECT id FROM tasks WHERE status IN ('QUEUED','RUNNING') ORDER BY created_at").fetchall()]
        for task_id in ids:
            with connect(self.settings) as conn:
                task = conn.execute("SELECT * FROM tasks WHERE id=%s AND status IN ('QUEUED','RUNNING') FOR UPDATE SKIP LOCKED", (task_id,)).fetchone()
                if not task:
                    continue
                expired_deadline = task["deadline_at"] and conn.execute("SELECT clock_timestamp()>=%s AS expired", (task["deadline_at"],)).fetchone()["expired"]
                running = conn.execute("SELECT s.*,a.id AS aid,a.lease_expires_at FROM steps s JOIN step_attempts a ON a.id=s.current_attempt_id WHERE s.task_id=%s AND s.status='RUNNING' FOR UPDATE OF s,a", (task_id,)).fetchall()
                for step in running:
                    expired = conn.execute("SELECT clock_timestamp()>=%s AS expired", (step["lease_expires_at"],)).fetchone()["expired"]
                    if expired or expired_deadline:
                        self._failure(conn, task, step, {"id": step["aid"]}, "TASK_DEADLINE" if expired_deadline else "LEASE_LOST", "Task deadline reached" if expired_deadline else "Supervisor lease expired; physical process may still exist", not expired_deadline, "TIMED_OUT" if expired_deadline else "LOST")
                if expired_deadline:
                    conn.execute("UPDATE steps SET status=CASE WHEN status IN ('PENDING','READY','RETRY_WAIT') THEN 'BLOCKED' ELSE status END WHERE task_id=%s", (task_id,))
                    conn.execute("UPDATE tasks SET status='FAILED',phase='FAILED',error_code='TASK_DEADLINE',finished_at=clock_timestamp() WHERE id=%s", (task_id,))
                    self._event(conn, task_id, "TASK_DEADLINE")
                else:
                    self._advance(conn, task)

    def _prepare(self, conn, task, step):
        dependencies = conn.execute("SELECT output_refs FROM steps WHERE task_id=%s AND step_key=ANY(%s)", (task["id"], step["depends_on"])).fetchall()
        refs = [r for s in dependencies for r in s["output_refs"]]
        artifacts = self._refs(conn, refs)
        if artifacts is None:
            conn.execute("UPDATE steps SET wait_reason='ARTIFACT_UNAVAILABLE' WHERE id=%s", (step["id"],))
            return None
        request = task["config_snapshot"]["request"]
        parameters = {k: v for k, v in step["spec"]["parameters"].items() if k not in ("duration_seconds", "batch_duration_seconds", "fail_first_attempts")}
        identity = step["input_snapshot"] if step.get("input_snapshot") is not None else request["input"]
        extra = {}
        if task["config_snapshot"].get("execution_schema_version", 1) >= 2:
            asset = task["config_snapshot"].get("input_asset")
            if asset:
                identity = {"kind": "uploaded", "fingerprint": asset["content_fingerprint"], "media_type": asset["media_type"], "width": asset["width"], "height": asset["height"], "seed": request["input"].get("seed", 42)}
            extra = {"protocol": 2, "generation_tag": request.get("generation_tag", "")}
        key = fingerprint({"scope": request["cache_scope"], "pipeline": task["pipeline_id"], "step": step["step_key"], "processor": step["spec"]["processor"], "version": step["spec"]["processor_version"], "input": identity, "parameters": parameters, "upstream": sorted(a["content_fingerprint"] for a in artifacts), **extra})
        conn.execute("UPDATE steps SET compatibility_key=%s,cache_key=%s WHERE id=%s", (key, key, step["id"]))
        # Older snapshots keep their exact checkpoint-compatibility protocol.
        checkpoint = self._artifact(conn, step["checkpoint_artifact_id"]) if step["checkpoint_artifact_id"] else None
        if step["checkpoint_artifact_id"] and not checkpoint:
            conn.execute("UPDATE steps SET wait_reason='ARTIFACT_UNAVAILABLE' WHERE id=%s", (step["id"],))
            return None
        if checkpoint and checkpoint["metadata"].get("compatibility_key") != key:
            conn.execute("UPDATE steps SET wait_reason='CHECKPOINT_INCOMPATIBLE' WHERE id=%s", (step["id"],))
            return None
        return key, artifacts, checkpoint

    def _use_cache(self, conn, task, step, key):
        request = task["config_snapshot"]["request"]
        if not step["spec"].get("cache_enabled") or request.get("bypass_cache") or step["step_key"] == "publish_result":
            return False
        cached = conn.execute("""SELECT c.* FROM step_cache c JOIN tasks t ON t.id=c.source_task_id
            JOIN steps s ON s.id=c.source_step_id WHERE c.cache_key=%s AND t.status='SUCCEEDED'
            AND s.status='SUCCEEDED' AND s.output_refs=c.artifact_ids AND c.source_task_id<>%s""", (key, task["id"])).fetchone()
        if not cached or not cached["artifact_ids"] or self._refs(conn, cached["artifact_ids"]) is None:
            return False
        conn.execute("""UPDATE steps SET status='CACHED',output_refs=%s,current_attempt_id=NULL,wait_reason=NULL,
            reused_from_step_id=%s,cache_source_task_id=%s WHERE id=%s""", (Jsonb(cached["artifact_ids"]), cached["source_step_id"], cached["source_task_id"], step["id"]))
        conn.execute("UPDATE tasks SET status='RUNNING',phase='CACHE_HIT',wait_reason=NULL WHERE id=%s", (task["id"],))
        self._event(conn, task["id"], "STEP_CACHED", step["id"], source_task_id=str(cached["source_task_id"]), source_step_id=str(cached["source_step_id"]), artifact_ids=cached["artifact_ids"])
        self._advance(conn, task)
        return True

    def claim(self, worker_id):
        # Do not let an older GPU waiter block another task's CPU preparation.
        # Ready branches may be claimed by independent Workers; dependencies
        # and Task transitions remain serialized, computation does not.
        self.sync_nodes()
        with connect(self.settings) as conn:
            candidates = conn.execute("""SELECT t.id FROM tasks t WHERE t.status IN ('QUEUED','RUNNING')
                AND (t.deadline_at IS NULL OR t.deadline_at>clock_timestamp())
                AND EXISTS (SELECT 1 FROM steps s WHERE s.task_id=t.id AND s.status='READY')
                AND NOT EXISTS (SELECT 1 FROM steps s WHERE s.task_id=t.id AND s.status='FAILED')
                ORDER BY t.created_at""").fetchall()
        for candidate in candidates:
            claim = self._claim_task(candidate["id"], worker_id)
            if claim:
                return claim
        return None

    def _pool_wait(self, task_id, step_id, generation, reason, reject=False):
        with connect(self.settings) as conn:
            task = conn.execute("SELECT * FROM tasks WHERE id=%s AND status IN ('QUEUED','RUNNING') FOR UPDATE", (task_id,)).fetchone()
            if not task:
                return
            step = conn.execute("SELECT * FROM steps WHERE id=%s AND status='READY' AND execution_generation=%s FOR UPDATE", (step_id, generation)).fetchone()
            if not step:
                return
            conn.execute("UPDATE steps SET status=%s,wait_reason=%s WHERE id=%s", ("FAILED" if reject else "READY", reason, step_id))
            conn.execute("UPDATE tasks SET phase=%s,wait_reason=%s WHERE id=%s", ("WAITING_NODE", reason, task_id))
            if step["wait_reason"] != reason:
                self._event(conn, task_id, "STEP_DISPATCH_REJECTED" if reject else "NODE_WAITING", step_id, reason=reason)
            if reject:
                self._advance(conn, task)

    def _pool_plans(self, task_id, worker_id):
        """Plan using read-only RPCs before any lease exists; return alternatives.

        None is a frozen legacy route. Empty means cached/waiting/rejected.
        The authorization transaction must recheck every node/resource below.
        """
        from urllib.error import HTTPError
        from .backends import ExternalClient
        from .nodes import inspection_binding, observe, storage_reference, validate_report
        with connect(self.settings) as conn:
            task = conn.execute("SELECT * FROM tasks WHERE id=%s AND status IN ('QUEUED','RUNNING') FOR UPDATE SKIP LOCKED", (task_id,)).fetchone()
            if not task:
                return []
            routing = task["config_snapshot"].get("execution_backend", {})
            if "node_ids" not in routing:
                return None
            step = conn.execute("SELECT * FROM steps WHERE task_id=%s AND status='READY' ORDER BY registration_order NULLS LAST,step_key FOR UPDATE LIMIT 1", (task_id,)).fetchone()
            if not step:
                return []
            prepared = self._prepare(conn, task, step)
            if not prepared:
                return []
            key, dependencies, checkpoint = prepared
            if self._use_cache(conn, task, step, key):
                return []
            input_value = step["input_snapshot"] if step["input_snapshot"] is not None else task["config_snapshot"]["request"]["input"]
        nodes = [n for n in self.settings.execution_nodes if n.node_id in routing["node_ids"]]
        compatible = [n for n in nodes if step["spec"]["processor_version"] in n.processors.get(step["spec"]["processor"], [])]
        if nodes and not compatible:
            self._pool_wait(task_id, step["id"], step["execution_generation"], "INVALID_CONFIG", reject=True)
            return []
        references = [storage_reference(self.settings), *dependencies]
        if checkpoint:
            references.append(checkpoint)
        if task["config_snapshot"].get("input_asset"):
            references.append(task["config_snapshot"]["input_asset"])
        proposed = {"task_id": str(task_id), "step_id": str(step["id"]), "attempt_id": str(uuid4()),
                    "generation": step["execution_generation"] + 1, "worker_id": worker_id,
                    "spec": step["spec"], "step_key": step["step_key"], "input": input_value}
        plans, reason = [], "WAITING_NODE"
        for node in compatible:
            if not node.accepting:
                continue
            if node.storage_id != routing["storage_id"]:
                reason = "ARTIFACT_UNAVAILABLE"
                continue
            with connect(self.settings) as conn:
                if step["spec"]["resource_kind"] == "cpu":
                    busy = node.cpu_capacity <= self._cpu_in_use(conn, node.node_id)
                else:
                    busy = not conn.execute("SELECT 1 FROM resource_slots WHERE node_id=%s AND enabled AND state='AVAILABLE' LIMIT 1", (node.node_id,)).fetchone()
            if busy:
                reason = "WAITING_CPU" if step["spec"]["resource_kind"] == "cpu" else "WAITING_GPU"
                continue
            data, error = None, None
            try:
                binding = inspection_binding(node, proposed)
                data = ExternalClient(self.settings, binding).inspect(routing["storage_id"], references, proposed)
                validate_report(data, node)
                if not data["references_readable"]:
                    reason, error = "ARTIFACT_UNAVAILABLE", "STORAGE_UNAVAILABLE"
                else:
                    plans.append({**proposed, "node": node, "report": data, "route": {**node.route(), "state_id": data["state_id"]}})
            except ValueError:
                data, error, reason = None, "CAPABILITY_MISMATCH", "NO_COMPATIBLE_NODE"
            except HTTPError as failure:
                data, error = None, "AUTH_OR_PROTOCOL_REJECTED" if failure.code < 500 else "UNREACHABLE"
            except (OSError, TimeoutError):
                data, error = None, "UNREACHABLE"
            with connect(self.settings) as conn:
                observe(conn, node, data, error)
        if not plans:
            self._pool_wait(task_id, step["id"], step["execution_generation"], reason)
        return plans

    def _managed_preflight(self, task_id, worker_id):
        """Read-only capability inspection before allocating an execution grant.

        The scoped token cannot start jobs. Network I/O is outside row locks;
        _claim_task rechecks the persisted Step/generation before granting work.
        """
        from urllib.error import HTTPError, URLError
        from .backends import ExternalClient
        from .external_protocol import binding_for
        with connect(self.settings) as conn:
            task = conn.execute("""SELECT * FROM tasks WHERE id=%s AND pipeline_id='managed-v1'
                AND config_snapshot->'execution_backend'->>'kind'='external'""", (task_id,)).fetchone()
            if not task:
                return {}
            routing = task["config_snapshot"]["execution_backend"]
            if not self.settings.external_executor_url or routing["executor_id"] != self.settings.external_executor_id:
                return None
            step = conn.execute("SELECT * FROM steps WHERE task_id=%s AND status='READY' ORDER BY registration_order LIMIT 1", (task_id,)).fetchone()
            if not step or task["status"] not in ("QUEUED", "RUNNING"):
                return None
        probe = {"task_id": str(task_id), "step_id": str(step["id"]), "attempt_id": str(uuid4()),
                 "generation": step["execution_generation"] + 1, "worker_id": worker_id,
                 "executor_id": routing["executor_id"], "spec": step["spec"], "step_key": step["step_key"],
                 "input": step["input_snapshot"]}
        rejection = False
        try:
            ExternalClient(self.settings, binding_for(probe)).check_capabilities(probe)
            return probe
        except HTTPError as error:
            rejection = 400 <= error.code < 500
        except (URLError, TimeoutError, OSError):
            pass
        except ValueError:
            rejection = True
        with connect(self.settings) as conn:
            current = conn.execute("SELECT * FROM tasks WHERE id=%s FOR UPDATE", (task_id,)).fetchone()
            if current["status"] not in ("QUEUED", "RUNNING"):
                return None
            row = conn.execute("SELECT * FROM steps WHERE id=%s AND status='READY' AND execution_generation=%s FOR UPDATE", (step["id"], step["execution_generation"])).fetchone()
            if row:
                reason = "INVALID_CONFIG" if rejection else "EXECUTOR_UNAVAILABLE"
                conn.execute("UPDATE steps SET status=%s,wait_reason=%s WHERE id=%s", ("FAILED" if rejection else "READY", reason, row["id"]))
                conn.execute("UPDATE tasks SET phase='WAITING_EXECUTOR',wait_reason=%s WHERE id=%s", (reason, task_id))
                if row["wait_reason"] != reason:
                    self._event(conn, task_id, "STEP_DISPATCH_REJECTED" if rejection else "EXECUTOR_UNAVAILABLE", row["id"], reason=reason)
                if rejection:
                    conn.execute("UPDATE tasks SET error_code='INVALID_CONFIG' WHERE id=%s", (task_id,))
                    self._advance(conn, current)
        return None

    def _claim_task(self, task_id, worker_id):
        plans = self._pool_plans(task_id, worker_id)
        probe = self._managed_preflight(task_id, worker_id) if plans is None else {}
        if probe is None or plans == []:
            return None
        grant_started = time.monotonic()
        with connect(self.settings) as conn:
            task = conn.execute("""SELECT t.* FROM tasks t WHERE t.id=%s AND t.status IN ('QUEUED','RUNNING')
                AND (t.deadline_at IS NULL OR t.deadline_at>clock_timestamp())
                AND EXISTS (SELECT 1 FROM steps s WHERE s.task_id=t.id AND s.status='READY')
                AND NOT EXISTS (SELECT 1 FROM steps s WHERE s.task_id=t.id AND s.status='FAILED')
                FOR UPDATE OF t SKIP LOCKED""", (task_id,)).fetchone()
            if not task:
                return None
            routing = task["config_snapshot"].get("execution_backend", {"kind": "local"})
            if plans is None and routing["kind"] == "external" and (not self.settings.external_executor_url or routing["executor_id"] != self.settings.external_executor_id):
                return None
            step = conn.execute("SELECT * FROM steps WHERE task_id=%s AND status='READY' ORDER BY registration_order NULLS LAST,step_key FOR UPDATE SKIP LOCKED LIMIT 1", (task["id"],)).fetchone()
            if not step:
                return None
            if probe and (str(step["id"]) != probe["step_id"] or step["execution_generation"] + 1 != probe["generation"]):
                return None
            prepared = self._prepare(conn, task, step)
            if not prepared:
                return None
            key, dependencies, checkpoint = prepared
            if self._use_cache(conn, task, step, key):
                return None
            selected, slot = None, None
            if plans is not None:
                for plan in plans:
                    node = plan["node"]
                    if plan["step_id"] != str(step["id"]) or plan["generation"] != step["execution_generation"] + 1:
                        continue
                    # Serialize quota decisions without conflicting with the
                    # FK key-share locks held by the node's existing children.
                    registered = conn.execute("SELECT * FROM execution_nodes WHERE node_id=%s AND accepting AND configured FOR NO KEY UPDATE SKIP LOCKED", (node.node_id,)).fetchone()
                    if not registered or registered["origin"] != node.url or registered["executor_id"] != node.executor_id:
                        continue
                    if step["spec"]["resource_kind"] == "cpu":
                        capacity = min(registered["cpu_capacity"], plan["report"]["cpu_capacity"])
                        if max(self._cpu_in_use(conn, node.node_id), plan["report"]["cpu_in_use"]) >= capacity:
                            continue
                    else:
                        available = [s["device_id"] for s in plan["report"]["mock_gpu_slots"] if s["state"] == "AVAILABLE" and s["device_id"] in node.mock_gpu_slots]
                        slot = conn.execute("""SELECT * FROM resource_slots WHERE node_id=%s AND enabled AND state='AVAILABLE'
                            AND device_id=ANY(%s) ORDER BY device_id FOR UPDATE SKIP LOCKED LIMIT 1""", (node.node_id, available)).fetchone()
                        if not slot:
                            continue
                    selected = plan
                    break
                if selected is None:
                    reason = "WAITING_CPU" if step["spec"]["resource_kind"] == "cpu" else "WAITING_GPU"
                    conn.execute("UPDATE steps SET wait_reason=%s WHERE id=%s", (reason, step["id"]))
                    conn.execute("UPDATE tasks SET phase=%s,wait_reason=%s WHERE id=%s", (reason, reason, task["id"]))
                    return None
            elif step["spec"].get("resource_kind", "cpu") == "cpu":
                conn.execute("SELECT pg_advisory_xact_lock(20261025)")
                if self._cpu_in_use(conn) >= self.settings.cpu_max_concurrency:
                    conn.execute("UPDATE steps SET wait_reason='WAITING_CPU' WHERE id=%s", (step["id"],))
                    conn.execute("UPDATE tasks SET phase='WAITING_CPU',wait_reason='WAITING_CPU' WHERE id=%s", (task["id"],))
                    return None
            if plans is None and step["spec"].get("resource_kind") == "mock-gpu":
                slot = conn.execute("SELECT * FROM resource_slots WHERE id=1 FOR UPDATE SKIP LOCKED").fetchone()
                enabled = task["config_snapshot"].get("resource_policy", {}).get("mock_gpu_slots", 1) == 1
                if not enabled or not slot or slot["state"] != "AVAILABLE" or slot["host_id"] not in (None, current_host()):
                    if step["wait_reason"] != "WAITING_GPU":
                        self._event(conn, task["id"], "RESOURCE_WAITING", step["id"], reason="WAITING_GPU", source="mock")
                    conn.execute("UPDATE steps SET wait_reason='WAITING_GPU' WHERE id=%s", (step["id"],))
                    conn.execute("UPDATE tasks SET phase='WAITING_GPU',wait_reason='WAITING_GPU' WHERE id=%s", (task["id"],))
                    return None
            attempt_id = selected["attempt_id"] if selected else probe["attempt_id"] if probe else str(uuid4())
            generation = step["execution_generation"] + 1
            number = conn.execute("SELECT coalesce(max(attempt_number),0)+1 AS n FROM step_attempts WHERE step_id=%s", (step["id"],)).fetchone()["n"]
            runtime = task["config_snapshot"]["runtime_policy"]
            backend = task["config_snapshot"].get("execution_backend", {"kind": "local", "executor_id": None})
            executor_id = selected["node"].executor_id if selected else backend.get("executor_id") if backend["kind"] == "external" else None
            node_id = selected["node"].node_id if selected else "legacy"
            device_id = slot["device_id"] if slot else "cpu"
            attempt = conn.execute("""INSERT INTO step_attempts(id,step_id,attempt_number,recovery_round,worker_id,fencing_token,status,lease_expires_at,resumed_from_checkpoint_id,executor_host,resource_slot_id,resource_cleanup_state,execution_backend,executor_id,external_job_id,node_id,resource_device_id)
                VALUES (%s,%s,%s,%s,%s,%s,'RUNNING',clock_timestamp()+(%s*interval '1 second'),%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
                (attempt_id, step["id"], number, task["recovery_round"], worker_id, generation, runtime["lease_ttl_seconds"], step["checkpoint_artifact_id"], None if selected else current_host(), slot["id"] if slot else None, "RESERVED" if slot else "CPU_ONLY", backend["kind"], executor_id, attempt_id if executor_id else None, node_id, device_id)).fetchone()
            conn.execute("UPDATE steps SET status='RUNNING',current_attempt_id=%s,execution_generation=%s,first_started_at=coalesce(first_started_at,clock_timestamp()),wait_reason=NULL WHERE id=%s", (attempt_id, generation, step["id"]))
            conn.execute("UPDATE tasks SET status='RUNNING',phase='EXECUTING',wait_reason=NULL WHERE id=%s", (task["id"],))
            self._event(conn, task["id"], "ATTEMPT_STARTED", step["id"], attempt_id, generation=generation, worker_id=worker_id, attempt_number=number, node_id=node_id, resource_id=device_id, checkpoint_artifact_id=str(step["checkpoint_artifact_id"]) if checkpoint else None)
            if slot:
                conn.execute("UPDATE resource_slots SET state='RESERVED',host_id=%s,owner_attempt_id=%s,owner_generation=%s,updated_at=clock_timestamp() WHERE id=%s", (None if selected else current_host(), attempt_id, generation, slot["id"]))
                self._event(conn, task["id"], "RESOURCE_RESERVED", step["id"], attempt_id, slot_id=slot["id"], node_id=node_id, resource_id=device_id, generation=generation, source="mock")
            claim = {"task_id": str(task["id"]), "step_id": str(step["id"]), "step_key": step["step_key"], "attempt_id": attempt_id, "generation": generation, "attempt_number": number, "recovery_round": task["recovery_round"], "worker_id": worker_id, "lease_expires_at": attempt["lease_expires_at"], "started_at": attempt["started_at"], "first_started_at": step["first_started_at"] or attempt["started_at"], "created_at": task["created_at"], "deadline_at": task["deadline_at"], "runtime_policy": runtime, "retry_policy": task["config_snapshot"]["retry_policy"], "spec": step["spec"], "input": step["input_snapshot"] if step["input_snapshot"] is not None else task["config_snapshot"]["request"]["input"], "input_asset": task["config_snapshot"].get("input_asset"), "compatibility_key": key, "checkpoint": checkpoint, "dependencies": dependencies, "resource_slot_id": slot["id"] if slot else None, "execution_backend": backend["kind"], "executor_id": executor_id, "external_job_id": attempt_id if executor_id else None}
            if selected:
                claim.update(node_id=node_id, resource_device_id=device_id, artifact_storage_id=routing["storage_id"],
                             executor_state_id=selected["route"]["state_id"])
            if executor_id:
                from .external_protocol import binding_for
                payload = json.loads(json.dumps(claim, default=lambda v: v.isoformat() if hasattr(v, "isoformat") else str(v)))
                conn.execute("INSERT INTO execution_commands(id,executor_id,external_job_id,binding,payload,route) VALUES (%s,%s,%s,%s,%s,%s)", (attempt_id, executor_id, attempt_id, Jsonb(binding_for(claim)), Jsonb(payload), Jsonb(selected["route"]) if selected else None))
                claim["external_command"] = payload
                self._event(conn, task["id"], "EXECUTION_COMMAND_PERSISTED", step["id"], attempt_id, command_id=attempt_id, executor_id=executor_id, job_id=attempt_id)
            if selected:
                claim["node_route"] = selected["route"]
            claim["_lease_request_started"] = grant_started
            return claim

    def resume(self, task_id, idempotency_key, step_key=None):
        operation = "resume" if step_key is None else "retry:"+step_key
        with connect(self.settings) as conn:
            task = conn.execute("SELECT * FROM tasks WHERE id=%s FOR UPDATE", (task_id,)).fetchone()
            if not task:
                raise DomainError("NOT_FOUND", "Task not found", 404)
            previous = conn.execute("SELECT * FROM recovery_operations WHERE task_id=%s AND idempotency_key=%s", (task_id, idempotency_key)).fetchone()
            if previous:
                if previous["operation"] != operation:
                    raise DomainError("IDEMPOTENCY_CONFLICT", "Recovery key has a different operation", 409)
                return {"id": str(task_id), "recovery_round": previous["recovery_round"]}
            if task["status"] != "FAILED":
                raise DomainError("INVALID_STATE", "Only FAILED tasks can be resumed", 409)
            if task["deadline_at"] and conn.execute("SELECT %s<=clock_timestamp() AS expired", (task["deadline_at"],)).fetchone()["expired"]:
                raise DomainError("TASK_DEADLINE", "Immutable deadline has expired; submit a new Task", 409)
            failed = conn.execute("SELECT step_key FROM steps WHERE task_id=%s AND status='FAILED'", (task_id,)).fetchall()
            if step_key is not None and (len(failed) != 1 or failed[0]["step_key"] != step_key):
                raise DomainError("INVALID_STATE", "Step retry requires the unique FAILED step; otherwise resume Task", 409)
            recovery_round = task["recovery_round"] + 1
            conn.execute("UPDATE tasks SET status='RUNNING',phase='RECOVERING',recovery_round=%s,finished_at=NULL,error_code=NULL,wait_reason=NULL WHERE id=%s", (recovery_round, task_id))
            conn.execute("UPDATE steps SET status='PENDING',next_retry_at=NULL,wait_reason=NULL WHERE task_id=%s AND status IN ('FAILED','BLOCKED')", (task_id,))
            conn.execute("UPDATE steps SET status='READY' WHERE task_id=%s AND status='PENDING' AND depends_on='[]'::jsonb", (task_id,))
            conn.execute("INSERT INTO recovery_operations VALUES (%s,%s,%s,%s)", (task_id, idempotency_key, operation, recovery_round))
            self._event(conn, task_id, "TASK_RESUMED", operation=operation, recovery_round=recovery_round)
            task["status"] = "RUNNING"
            self._advance(conn, task)
            return {"id": str(task_id), "recovery_round": recovery_round}
