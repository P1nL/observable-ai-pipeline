"""Independent lease supervisor for a database-free computation subprocess."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from queue import Empty, Queue
import signal
import socket
import sys
import time
from typing import Any
from uuid import uuid4
from threading import Event, Lock, Thread


_LOG_LOCK = Lock()


def _log(event: str, claim: dict[str, Any] | None = None, **fields: Any) -> None:
    context = {}
    if claim:
        context = {key: claim.get(key) for key in ("task_id", "step_id", "step_key", "attempt_id", "generation", "worker_id", "node_id", "resource_device_id")}
    # Never log claims, input, database URLs, or exception messages.
    with _LOG_LOCK:
        print(json.dumps({"event": event, **context, **fields}, default=str), file=sys.stderr, flush=True)


def _datetime(value: datetime | str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value


def _transient(error: Exception) -> bool:
    # Repository operations may reconnect after transient database failures, but
    # DomainError and programming errors are not connection-retry signals.
    from psycopg import InterfaceError, OperationalError

    return isinstance(error, (InterfaceError, OperationalError)) or getattr(error, "sqlstate", None) in {
        "40001", "40P01", "57P01", "57P02", "57P03", "53300",
    }


def _reconcile(repository, claim: dict[str, Any] | None = None) -> bool:
    try:
        repository.reconcile()
    except Exception as error:
        if not _transient(error):
            raise
        # A scan is best-effort, not an ownership renewal. Retry on its next
        # cadence so a transient failure cannot crowd out heartbeat/completion.
        _log("recovery_scan_failed", claim, error_type=type(error).__name__)
        return False
    return True


class _LeaseLost(Exception):
    pass


class _ExecutionFailure(Exception):
    def __init__(self, code: str, message: str, retryable: bool):
        self.code = code
        self.message = message
        self.retryable = retryable


class _Supervisor:
    def __init__(self, settings, repository, claim: dict[str, Any], claim_started: float):
        self.settings = settings
        self.repository = repository
        self.claim = claim
        self.execution = None
        self.messages: Queue = Queue()
        policy = claim["runtime_policy"]
        self.ttl = float(policy["lease_ttl_seconds"])
        self.interval = float(policy["heartbeat_interval_seconds"])
        self.margin = min(0.1, self.ttl * 0.05)
        now = time.monotonic()
        wall_now = datetime.now(timezone.utc)
        # DB time owns the lease. Request-start + TTL is a conservative local
        # bound without comparing the host's clock to the database's clock.
        self.lease_deadline = claim_started + self.ttl - self.margin
        self.next_heartbeat = now + min(self.interval, max(0, self.lease_deadline - now) / 2)
        self.started = now - max(0, (wall_now - _datetime(claim["started_at"])).total_seconds())
        hard_timeout = policy.get("attempt_hard_timeout_seconds")
        self.hard_deadline = self.started + hard_timeout if hard_timeout else None
        deadline = claim.get("deadline_at")
        self.task_deadline = now + (_datetime(deadline) - wall_now).total_seconds() if deadline else None
        first_started = claim.get("first_started_at", claim["started_at"])
        created = claim.get("created_at", claim["started_at"])
        self.warning_deadlines = {
            code: now + seconds - max(0, (wall_now - _datetime(origin)).total_seconds())
            for code, seconds, origin in (
                ("STEP_SOFT_WARNING", policy.get("step_soft_warning_seconds"), first_started),
                ("TASK_SOFT_WARNING", policy.get("task_soft_warning_seconds"), created),
            ) if seconds and origin
        }

    def _owned(self) -> None:
        if time.monotonic() >= self.lease_deadline:
            raise _LeaseLost("local_lease_expired")

    def _call(self, method: str, *args, **kwargs) -> bool | None:
        delay = 0.05
        while True:
            self._owned()
            if method != "fail":
                self._check_limits()
            # No more heartbeat retries for dead computations. Final event
            # processing can still use the remaining valid lease.
            if method == "heartbeat" and self.execution is not None and not self.execution.can_heartbeat():
                return None
            request_started = time.monotonic()
            try:
                accepted = getattr(self.repository, method)(self.claim, *args, **kwargs)
            except Exception as error:
                if not _transient(error):
                    code = str(getattr(error, "code", ""))
                    if code == "EXECUTOR_EXIT":
                        raise _ExecutionFailure(code, "Repository could not verify the executor process", True) from error
                    if code in {"ARTIFACT_UNAVAILABLE", "CHECKPOINT_INCOMPATIBLE", "INVALID_ARTIFACT"}:
                        raise _ExecutionFailure(code, "Repository rejected the executor artifact", False) from error
                    _log("repository_operation_failed", self.claim, operation=method, error_type=type(error).__name__)
                    raise _LeaseLost("ownership_unconfirmed") from error
                _log("database_retry", self.claim, operation=method, error_type=type(error).__name__)
                remaining = self.lease_deadline - time.monotonic()
                if remaining <= 0:
                    raise _LeaseLost("database_unavailable_until_lease_expiry") from error
                time.sleep(min(delay, remaining))
                delay = min(delay * 2, 0.5)
                continue
            if not accepted:
                raise _LeaseLost(f"{method}_refused")
            if method == "heartbeat":
                # A successful RPC's start time is conservative: renewal took
                # place no earlier than this. Wall-clock changes cannot extend it.
                self.lease_deadline = request_started + self.ttl - self.margin
                self.next_heartbeat = request_started + min(self.interval, (self.ttl - self.margin) / 2)
                self._owned()
                if self.execution:
                    self.execution.renew(self.claim["lease_expires_at"])
                _log("lease_renewed", self.claim)
            return True

    def _check_limits(self) -> None:
        now = time.monotonic()
        if self.task_deadline is not None and now >= self.task_deadline:
            raise _ExecutionFailure("TASK_DEADLINE", "Explicit task deadline reached", False)
        if self.hard_deadline is not None and now >= self.hard_deadline:
            raise _ExecutionFailure("ATTEMPT_TIMEOUT", "Explicit attempt hard timeout reached", False)

    def _stop_child(self) -> bool:
        if self.execution is None:
            if self.claim.get("execution_backend", "local") == "external":
                from .backends import stop_external
                receipt = stop_external(self.settings, self.claim)
                return bool(receipt and self.repository.record_external_exit(self.claim, receipt))
            return True
        confirmed = self.execution.stop()
        if confirmed and self.execution.external:
            try:
                return self.repository.record_external_exit(self.claim, self.execution.receipt)
            except Exception as error:
                if not _transient(error):
                    raise
                return False
        return confirmed

    def _fail(self, failure: _ExecutionFailure, confirmed: bool) -> None:
        self._call(
            "fail", failure.code, failure.message, failure.retryable,
            process_exit_confirmed=confirmed,
        )
        _log("attempt_failed", self.claim, error_code=failure.code, retryable=failure.retryable, process_exit_confirmed=confirmed)

    def _start(self) -> None:
        from .backends import start_execution
        self._owned()
        self._check_limits()
        try:
            self.execution = start_execution(self.settings, self.claim, self.messages)
        except OSError as error:
            raise _ExecutionFailure("EXECUTOR_EXIT", "Could not start executor subprocess", True) from error
        if not self.execution.external:
            self._call("record_executor", self.execution.pid)
        self._owned()
        try:
            self.execution.activate()
        except (OSError, ValueError) as error:
            raise _ExecutionFailure("EXECUTOR_EXIT", "Executor input pipe closed", True) from error
        _log("external_dispatch_started" if self.execution.external else "executor_started", self.claim, backend=self.claim.get("execution_backend", "local"), pid=self.execution.pid)

    def _supervise(self) -> None:
        result = None
        eof = False
        while True:
            self._owned()
            self._check_limits()
            exit_code = self.execution.poll()
            if self.execution.external and not self.execution.registered and (self.execution.can_heartbeat() or exit_code is not None):
                self._call("record_external_started")
                self.execution.registered = True
            if exit_code is not None and eof:
                if self.execution.external:
                    self._call("record_external_exit", self.execution.receipt)
                if exit_code == 0 and result is not None:
                    self._call("succeed", result)
                    _log("attempt_succeeded", self.claim, process_exit_confirmed=True)
                    return
                raise _ExecutionFailure("EXECUTOR_EXIT", "Executor exited without a successful result", True)
            # This loop is independent of executor progress. A sleeping or
            # CPU-bound child keeps its lease; a dead child does not.
            if exit_code is None and time.monotonic() >= self.next_heartbeat:
                self._call("heartbeat")
            for code, deadline in list(self.warning_deadlines.items()):
                if exit_code is None and time.monotonic() >= deadline:
                    self._call("warning", code, "Soft elapsed-time threshold reached; execution continues")
                    del self.warning_deadlines[code]
            try:
                event = self.messages.get(timeout=min(0.05, max(0, self.lease_deadline - time.monotonic())))
            except Empty:
                continue
            kind = event["type"]
            if kind == "_eof":
                eof = True
                continue
            if kind == "_protocol_error" or result is not None:
                raise _ExecutionFailure("INVALID_INPUT", "Invalid executor message protocol", False)
            if kind == "progress":
                progress = event.get("progress")
                if not isinstance(progress, dict):
                    raise _ExecutionFailure("INVALID_INPUT", "Invalid executor progress", False)
                self._call("progress", progress)
            elif kind in {"checkpoint", "result"}:
                artifact = event.get("artifact")
                if not isinstance(artifact, dict):
                    raise _ExecutionFailure("INVALID_INPUT", "Invalid executor artifact message", False)
                if kind == "checkpoint":
                    self._call("commit_checkpoint", artifact)
                else:
                    result = artifact
            elif kind == "error":
                code = event.get("error_code", "INVALID_INPUT")
                # Retryability is classification, not an arbitrary child flag.
                retryable = code in {"TRANSIENT_ERROR", "EXECUTOR_EXIT"} and event.get("retryable") is True
                raise _ExecutionFailure(str(code), str(event.get("message", "Executor failed"))[:1000], retryable)

    def run(self) -> None:
        try:
            self._start()
            self._supervise()
        except KeyboardInterrupt:
            confirmed = self._stop_child()
            try:
                self._fail(_ExecutionFailure("EXECUTOR_EXIT", "Worker interrupted; executor stop requested", True), confirmed)
            except (_LeaseLost, _ExecutionFailure):
                _log("shutdown_left_to_lease_recovery", self.claim, process_exit_confirmed=confirmed)
            raise
        except _ExecutionFailure as failure:
            confirmed = self._stop_child()
            try:
                self._fail(failure, confirmed)
            except (_LeaseLost, _ExecutionFailure):
                _log("failure_left_to_lease_recovery", self.claim, error_code=failure.code, process_exit_confirmed=confirmed)
        except _LeaseLost as lost:
            confirmed = self._stop_child()
            _log("lease_lost", self.claim, reason=str(lost), process_exit_confirmed=confirmed)
            if confirmed and hasattr(self.repository, "cancel_stopped"):
                try:
                    self.repository.cancel_stopped(self.claim, True)
                except Exception as error:
                    if not _transient(error):
                        raise
            # No authoritative write after local expiry or refused ownership.
        except Exception as error:
            confirmed = self._stop_child()
            _log("supervisor_error", self.claim, error_type=type(error).__name__, process_exit_confirmed=confirmed)
            try:
                self._fail(_ExecutionFailure("EXECUTOR_EXIT", "Supervisor could not complete execution", True), confirmed)
            except (_LeaseLost, _ExecutionFailure):
                pass
            raise
        finally:
            if self.execution is not None:
                self.execution.close()


class Worker:
    def __init__(self, settings, worker_id: str, repository=None):
        if repository is None:
            from .repository import Repository

            repository = Repository(settings)
        self.settings = settings
        self.worker_id = worker_id
        self.repository = repository
        self._stop = Event()
        self._maintenance = None
        self._scan_error = None

    def _scan(self):
        next_nodes = 0.0
        while not self._stop.is_set():
            try:
                _reconcile(self.repository)
                if self.settings.execution_nodes and time.monotonic() >= next_nodes:
                    self.repository.observe_nodes(stop=self._stop)
                    next_nodes = time.monotonic() + self.settings.node_probe_interval_seconds
            except Exception as error:
                _log("maintenance_failed", worker_id=self.worker_id, error_type=type(error).__name__)
                if not _transient(error):
                    self._scan_error = error
                    return
            self._stop.wait(self.settings.runtime_policy.coordinator_scan_interval_seconds)

    def close(self):
        self._stop.set()
        if self._maintenance:
            self._maintenance.join(timeout=2)

    def run_once(self) -> bool:
        if self._maintenance is None:
            self._maintenance = Thread(target=self._scan, name="worker-maintenance", daemon=True)
            self._maintenance.start()
        if self._scan_error is not None:
            raise self._scan_error
        # Reconciliation may be slow: it must not consume the new claim's TTL.
        started = time.monotonic()
        claim = self.repository.claim(self.worker_id)
        if claim is None:
            return False
        started = claim.pop("_lease_request_started", started)
        _log("attempt_claimed", claim)
        _Supervisor(self.settings, self.repository, claim, started).run()
        return True


def run_once(settings, worker_id: str) -> bool:
    """Claim and supervise one attempt; False means no work was available."""
    worker = Worker(settings, worker_id)
    try:
        return worker.run_once()
    finally:
        worker.close()


def main() -> int:
    from .config import Settings

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker-id", default=f"{socket.gethostname()}-{uuid4().hex[:12]}")
    arguments = parser.parse_args()
    settings = Settings.from_env()
    worker = Worker(settings, arguments.worker_id)
    def interrupt(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupt)
    _log("worker_started", worker_id=arguments.worker_id)
    try:
        while True:
            try:
                claimed = worker.run_once()
            except Exception as error:
                _log("worker_poll_failed", worker_id=arguments.worker_id, error_type=type(error).__name__)
                if not _transient(error):
                    raise
                claimed = False
            if not claimed:
                time.sleep(settings.poll_interval_seconds)
    except KeyboardInterrupt:
        _log("worker_stopped", worker_id=arguments.worker_id)
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
