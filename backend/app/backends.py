"""Execution transport only. The repository retains all authoritative decisions."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from queue import Queue
import subprocess
import sys
from threading import Event, Thread
import time
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, HTTPSHandler, ProxyHandler, Request, build_opener
import ssl
import os
from typing import Protocol


class ExecutionHandle(Protocol):
    """Nonblocking observation + separate I/O, never authoritative state writes."""
    external: bool
    pid: int | None
    receipt: dict | None
    def activate(self) -> None: ...
    def poll(self) -> int | None: ...
    def can_heartbeat(self) -> bool: ...
    def renew(self, expires_at) -> None: ...
    def stop(self) -> bool: ...
    def close(self) -> None: ...


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None  # A bound bearer must stay on the configured executor origin.


def _json(value):
    return json.dumps(value, default=lambda v: v.astimezone(timezone.utc).isoformat() if isinstance(v, datetime) else str(v), allow_nan=False)


def _read_messages(stream, messages):
    try:
        for line in stream:
            event = json.loads(line)
            if not isinstance(event, dict) or event.get("type") not in {"progress", "checkpoint", "result", "error"}:
                raise ValueError("Invalid computation event")
            messages.put(event)
    except (ValueError, TypeError, OSError, UnicodeError):
        messages.put({"type": "_protocol_error"})
    finally:
        messages.put({"type": "_eof"})


class LocalExecution:
    """Start suspended on stdin; register identity before activating computation."""
    external = False
    receipt = None

    def __init__(self, settings, claim, messages):
        self.claim, self.settings = claim, settings
        self.process = subprocess.Popen(
            [sys.executable, "-m", "app.executor", "--attempt-id", claim["attempt_id"], "--generation", str(claim["generation"])],
            cwd=Path(__file__).resolve().parent.parent, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, encoding="utf-8", bufsize=1,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            env={key: value for key, value in os.environ.items() if key in {
                "SYSTEMROOT", "SystemRoot", "WINDIR", "PATH", "TEMP", "TMP", "TMPDIR", "LANG", "LC_ALL", "PYTHONPATH"}},
        )
        self.pid = self.process.pid
        self.reader = Thread(target=_read_messages, args=(self.process.stdout, messages), daemon=True)
        self.reader.start()

    def activate(self):
        self.process.stdin.write(_json({"claim": self.claim, "artifact_root": str(self.settings.artifact_root)}) + "\n")
        self.process.stdin.close()

    def poll(self):
        return self.process.poll()

    def can_heartbeat(self):
        return self.poll() is None

    def renew(self, expires_at):
        pass  # A local child is supervised by the central process, not an RPC.

    def stop(self):
        try:
            if self.poll() is None:
                self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=2)
            return True
        except (OSError, subprocess.TimeoutExpired):
            return self.poll() is not None

    def close(self):
        if self.poll() is not None:
            self.reader.join(timeout=1)
            for stream in (self.process.stdin, self.process.stdout):
                if stream is not None:
                    stream.close()


class ExternalClient:
    def __init__(self, settings, binding, route=None):
        from .external_protocol import auth_headers
        self.origin, self.secret = settings.external_executor_url, settings.external_executor_secret.get_secret_value()
        handlers = [ProxyHandler({}), _NoRedirect()]
        if "node_id" in binding:
            from .nodes import resolve_node
            node = resolve_node(settings, binding["node_id"], route)
            if node.executor_id != binding["executor_id"]:
                raise ValueError("Execution node does not match the immutable executor binding")
            self.origin = node.url
            self.secret = settings.node_secrets[node.node_id].get_secret_value()
            if self.origin.startswith("https:"):
                handlers.append(HTTPSHandler(context=ssl.create_default_context(cafile=str(node.ca_file) if node.ca_file else None)))
        elif not self.origin or binding["executor_id"] != settings.external_executor_id:
            raise ValueError("Configured executor does not match the persisted binding")
        self.settings, self.binding = settings, binding
        self.state_id = route.get("state_id") if route else None
        self.headers = auth_headers(self.secret, binding)
        self.transport = build_opener(*handlers)

    def call(self, method, suffix="", body=None):
        from .external_protocol import validate_status
        return validate_status(self._request(method, "/v1/jobs/" + self.binding["job_id"] + suffix, body), self.binding)

    def _request(self, method, path, body=None, *, scope="job"):
        url = self.origin.rstrip("/") + path
        from .external_protocol import auth_headers
        headers = {**(self.headers if scope == "job" else auth_headers(self.secret, self.binding, scope=scope)), "Content-Type": "application/json"}
        request = Request(url, data=_json(body).encode() if body is not None else None, headers=headers, method=method)
        with self.transport.open(request, timeout=self.settings.external_rpc_timeout_seconds) as response:
            data = json.load(response)
        if self.state_id is not None and data.get("state_id") != self.state_id:
            raise ValueError("Executor state store changed; original ownership remains unconfirmed")
        return data

    def check_capabilities(self, claim):
        from .external_protocol import validate_capabilities
        data = self._request("GET", "/v1/capabilities", scope="capabilities")
        validate_capabilities(data, self.binding, claim)

    def inspect(self, storage_id, references, claim=None):
        data = self._request("POST", "/v1/capabilities", {"storage_id": storage_id, "references": references}, scope="capabilities")
        if claim is not None:
            from .external_protocol import validate_capabilities
            validate_capabilities(data, self.binding, claim)
        return data


class ExternalExecution:
    """Network polling runs independently of central lease supervision."""
    external = True
    pid = None

    def __init__(self, settings, claim, messages):
        from .external_protocol import binding_for
        self.client = ExternalClient(settings, binding_for(claim), claim.get("node_route"))
        self.claim, self.messages = claim, messages
        self.payload = claim["external_command"]
        self.expires_at = claim["lease_expires_at"]
        self.closed = Event()
        self.reader = None
        self.receipt = None
        self.exit_code = None
        self.last_alive = 0.0
        self.registered = False
        self.freshness = min(float(claim["runtime_policy"]["heartbeat_interval_seconds"]), float(claim["runtime_policy"]["lease_ttl_seconds"]) / 3)

    def activate(self):
        self.reader = Thread(target=self._poll_service, daemon=True)
        self.reader.start()

    def _poll_service(self):
        started, granted, cursor = False, None, 0
        while not self.closed.is_set():
            try:
                if not started:
                    try:
                        self.client.check_capabilities(self.payload)
                    except ValueError as error:
                        self.messages.put({"type": "error", "error_code": "INVALID_CONFIG", "message": str(error), "retryable": False})
                        return
                    self.client.call("PUT", body={"claim": self.payload, "lease_expires_at": self.expires_at})
                    started, granted = True, self.expires_at
                if granted != self.expires_at:
                    try:
                        self.client.call("POST", "/lease", {"lease_expires_at": self.expires_at})
                    except HTTPError as error:
                        # Completion can race the next grant. A terminal job
                        # rejects renewal, but its durable final events still
                        # must be read instead of retrying renew forever.
                        if error.code == 409:
                            granted = self.expires_at
                    except (URLError, TimeoutError, OSError):
                        # Renewal delivery and status observation are separate:
                        # even a failed grant must not hide a durable exit.
                        pass
                    else:
                        granted = self.expires_at
                state = self.client.call("GET", f"?after_seq={cursor}")
                for item in state["events"]:
                    if item["seq"] <= cursor:
                        raise ValueError("Non-increasing external event cursor")
                    self.messages.put(item["event"])
                    cursor = item["seq"]
                if state["exit_confirmed"]:
                    self.receipt = state
                    self.exit_code = state["returncode"] if type(state["returncode"]) is int else 1
                    self.messages.put({"type": "_eof"})
                    return
                if state["state"] == "RUNNING":
                    self.last_alive = time.monotonic()
            except HTTPError as error:
                if 400 <= error.code < 500:
                    self.messages.put({"type": "error", "error_code": "INVALID_CONFIG",
                        "message": f"External executor rejected protocol/command (HTTP {error.code})", "retryable": False})
                    return
                # A 5xx may follow a persisted start. Retry only this same ID.
            except (URLError, TimeoutError, OSError):
                # Ambiguous delivery retries the SAME persisted command/job ID.
                pass
            except ValueError:
                self.messages.put({"type": "_protocol_error"})
                return
            self.closed.wait(.05)

    def poll(self):
        return self.exit_code

    def can_heartbeat(self):
        return self.poll() is None and time.monotonic() - self.last_alive < self.freshness

    def renew(self, expires_at):
        self.expires_at = expires_at

    def stop(self):
        self.closed.set()
        try:
            state = self.client.call("POST", "/stop", {})
        except (URLError, TimeoutError, OSError, ValueError):
            return False
        if state["exit_confirmed"]:
            self.receipt = state
            return True
        return False

    def close(self):
        self.closed.set()
        if self.reader:
            self.reader.join(timeout=2)


def start_execution(settings, claim, messages: Queue) -> ExecutionHandle:
    kind = claim.get("execution_backend", "local")
    if kind == "external":
        return ExternalExecution(settings, claim, messages)
    if kind == "local":
        return LocalExecution(settings, claim, messages)
    raise ValueError("Unsupported execution backend")


def stop_external(settings, attempt):
    """Return a bound physical-exit receipt, never interpret HTTP success as exit."""
    from .external_protocol import binding_for
    claim = {**attempt, "attempt_id": str(attempt.get("id", attempt.get("attempt_id"))),
             "generation": attempt.get("fencing_token", attempt.get("generation"))}
    try:
        client = ExternalClient(settings, binding_for(claim), claim.get("node_route"))
        state = client.call("POST", "/stop", {})
        return state if state["exit_confirmed"] else None
    except (URLError, TimeoutError, OSError, ValueError, KeyError):
        return None
