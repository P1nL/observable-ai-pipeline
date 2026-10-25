"""Executor-owned checks: no central imports, fixtures, PostgreSQL, or HTTP harness.

Run this file directly with python -I from any working directory.
"""
import base64
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import hmac
import importlib.util
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from uuid import uuid4

import psutil

SERVICE = Path(__file__).resolve().parents[2] / "executors" / "standalone_mock.py"
sys.dont_write_bytecode = True
module_spec = importlib.util.spec_from_file_location("independent_executor_under_test", SERVICE)
runner = importlib.util.module_from_spec(module_spec)
module_spec.loader.exec_module(runner)
SECRET = "standalone-test-not-a-real-secret-1234567890"


def timestamp(seconds=0):
    return datetime.fromtimestamp(time.time() + seconds, timezone.utc).isoformat()


def headers(binding, scope=None):
    raw = json.dumps(binding, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()
    result = {"X-Execution-Protocol": "1", "Content-Type": "application/json",
              "X-Execution-Binding": base64.urlsafe_b64encode(raw).decode(),
              "Authorization": "Bearer " + hmac.new(SECRET.encode(), b"capabilities:" + raw if scope == "capabilities" else raw,
                                                    hashlib.sha256).hexdigest()}
    if scope is not None:
        result["X-Execution-Scope"] = scope
    return result


def command(key="normalize_asset", dependencies=(), checkpoint=None, task_id=None, attempt_number=1):
    job_id = str(uuid4())
    binding = {"executor_id": "independent-test", "job_id": job_id, "task_id": task_id or str(uuid4()),
               "step_id": str(uuid4()), "attempt_id": job_id, "generation": 1, "worker_id": "worker-standalone"}
    claim = {k: v for k, v in binding.items() if k != "job_id"}
    claim.update(external_job_id=job_id, execution_backend="external", step_key=key,
                 spec={"key": key, "processor": "mock", "processor_version": "independent-mock-v1",
                       "depends_on": [a["metadata"]["step_key"] for a in dependencies],
                       "parameters": {"duration_seconds": 0, "batch_duration_seconds": 0, "batch_count": 6,
                                      "frames_per_batch": 2, "fail_first_attempts": 0, "seed": 42},
                       "cache_enabled": False, "checkpoint_mode": "committed-batch-manifest" if key == "generate_frames" else "none",
                       "resource_kind": "mock-gpu" if key == "generate_frames" else "cpu"},
                 attempt_number=attempt_number, recovery_round=0, input={"kind": "synthetic", "seed": 42},
                 input_asset=None, compatibility_key=f"independent:{key}:opaque", dependencies=list(dependencies),
                 checkpoint=checkpoint, runtime_policy={"heartbeat_interval_seconds": 30, "lease_ttl_seconds": 300,
                     "coordinator_scan_interval_seconds": 1, "attempt_hard_timeout_seconds": None,
                     "task_deadline_seconds": None, "step_soft_warning_seconds": None, "task_soft_warning_seconds": None},
                 retry_policy={}, lease_expires_at=timestamp(30), started_at=timestamp(),
                 first_started_at=timestamp(), created_at=timestamp(), deadline_at=None,
                 resource_slot_id=1 if key == "generate_frames" else None)
    return binding, {"claim": claim, "lease_expires_at": timestamp(30)}


class StandaloneContractTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="standalone-contract-")
        self.directory = Path(self.temporary.name)
        self.engine = runner.Executor("independent-test", SECRET, self.directory / "artifacts", self.directory / "state")

    def tearDown(self):
        self.engine.close()
        self.temporary.cleanup()

    def rejection(self, status, call, *args):
        with self.assertRaises(runner.Rejection) as error:
            call(*args)
        self.assertEqual(error.exception.status, status, error.exception.detail)

    def finished(self, binding, timeout=8):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = self.engine.status(binding)
            if state["exit_confirmed"]:
                return state
            self.assertIsNone(state["returncode"])
            time.sleep(0.03)
        self.fail(f"No confirmed exit: {self.engine.status(binding)}")

    def result(self, key="normalize_asset", dependencies=(), **kwargs):
        binding, body = command(key, dependencies, **kwargs)
        self.engine.put(binding, body)
        state = self.finished(binding)
        self.assertEqual(state["returncode"], 0, state)
        self.assertEqual(state["events"][-1]["event"]["type"], "result")
        return state["events"][-1]["event"]["artifact"]

    def configure_node(self):
        self.engine.close()
        self.engine = runner.Executor("independent-test", SECRET, self.directory / "artifacts", self.directory / "node-state",
                                      node_id="node-a", mock_gpu_slots=("gpu-a", "gpu-b"))

    def node_command(self, resource="cpu", key="business_step"):
        binding, body = command(key)
        binding.update(node_id="node-a", resource_id=resource)
        body["claim"].update(node_id="node-a", resource_device_id=resource, artifact_storage_id="pipeline-artifacts",
                             executor_state_id=self.engine.state_id,
                             resource_slot_id=None if resource == "cpu" else 1)
        body["claim"]["spec"].update(processor="mock-step", checkpoint_mode="committed-batch-manifest",
                                      resource_kind="cpu" if resource == "cpu" else "mock-gpu",
                                      parameters={"duration_seconds": 30})
        return binding, body

    def test_node_binding_capacity_dedup_and_unknown_device_owner(self):
        self.configure_node()
        legacy, _ = command()
        self.rejection(403, self.engine.authenticate, headers(legacy))
        jobs = [self.node_command(resource) for resource in ("cpu", "cpu", "gpu-a", "gpu-b")]
        for binding, body in jobs:
            self.assertEqual(self.engine.authenticate(headers(binding), binding["job_id"]), binding)
            self.engine.put(binding, body)
        caps = self.engine.capabilities()
        self.assertEqual((caps["node_id"], caps["cpu_capacity"], caps["cpu_in_use"]), ("node-a", 2, 2))
        self.assertIn("node-binding-v1", caps["capabilities"])
        self.assertIn("shared-storage-inspection-v1", caps["capabilities"])
        self.assertEqual([slot["state"] for slot in caps["mock_gpu_slots"]], ["RUNNING", "RUNNING"])
        for resource in ("cpu", "gpu-a", "gpu-b"):
            binding, body = self.node_command(resource)
            self.rejection(409, self.engine.put, binding, body)
        for binding, body in jobs:
            self.assertEqual(self.engine.put(binding, body)["start_count"], 1)
        binding, _ = jobs[2]
        self.rejection(403, self.engine.authenticate, headers(dict(binding, node_id="node-b")))
        self.rejection(403, self.engine.authenticate, headers(dict(binding, resource_id="gpu-b")))
        self.engine.stop(binding, {})
        self.finished(binding)
        unknown, body = self.node_command("gpu-a")
        claim = runner.canonical(body["claim"])
        with self.engine.lock, self.engine.db:
            self.engine.db.execute("INSERT INTO jobs (job_id,binding,claim,command_fingerprint,lease,state,start_count,stop_requested,host) "
                "VALUES (?,?,?,?,?,'UNCERTAIN',0,1,?)", (unknown["job_id"], runner.canonical(unknown).decode(), claim.decode(),
                    hashlib.sha256(claim).hexdigest(), time.time() - 1, self.engine.host))
        slot = next(slot for slot in self.engine.capabilities()["mock_gpu_slots"] if slot["device_id"] == "gpu-a")
        self.assertEqual((slot["state"], slot["owner_attempt_id"]), ("QUARANTINED", unknown["attempt_id"]))
        self.assertEqual(self.engine.put(unknown, body)["state"], "UNCERTAIN")
        binding, body = self.node_command("gpu-a")
        self.rejection(409, self.engine.put, binding, body)

    def test_node_readonly_inspection_and_cross_version_generic_dependency(self):
        self.configure_node()
        _, producer = command("encode_package")
        batch = runner.write_artifact(self.engine.root, producer["claim"], "batch-0", {"batch": 0})
        refs = [{"index": 0, "relative_path": batch["relative_path"], "content_fingerprint": batch["content_fingerprint"]}]
        data = {"schema_version": 1, "source": "mock", "execution_resource": "cpu", "resource_kind": "cpu",
                "compatibility_key": "opaque-producer-key", "step_key": "encode_package", "processor_version": "1",
                "input": {"kind": "synthetic"}, "seed": 0, "dependencies": [], "completed_units": 1,
                "resumed_batches": 0, "total_batches": 1, "completed_batches": refs}
        output = runner.write_artifact(self.engine.root, producer["claim"], "result", data)
        checkpoint = runner.write_artifact(self.engine.root, producer["claim"], "checkpoint",
            {"schema_version": 1, "compatibility_key": "opaque-producer-key", "total_batches": 1,
             "completed_batches": refs}, "checkpoint")
        marker = {"storage_id": "pipeline-artifacts", "id": str(uuid4())}
        raw = runner.canonical(marker)
        marker_path = self.engine.root / "_system" / "storage-id.json"
        marker_path.parent.mkdir()
        marker_path.write_bytes(raw)
        probe = {"kind": "storage-probe", "relative_path": "_system/storage-id.json",
                 "content_fingerprint": hashlib.sha256(raw).hexdigest(), "metadata": marker}
        input_path = self.engine.root / "inputs" / "raw.bin"
        input_path.parent.mkdir()
        input_path.write_bytes(b"actual-input-bytes")
        input_ref = {"kind": "input", "relative_path": "inputs/raw.bin",
                     "content_fingerprint": hashlib.sha256(b"actual-input-bytes").hexdigest()}
        binding, _ = self.node_command()
        binding["resource_id"] = "inspection"
        request = {"storage_id": "pipeline-artifacts", "references": [probe, output, checkpoint, input_ref]}
        handler = object.__new__(runner.Handler)
        handler.server = SimpleNamespace(executor=self.engine)
        handler.command, handler.path = "POST", "/v1/capabilities"
        handler.headers = headers(binding, "capabilities")
        handler._body = lambda: request
        status, result = handler._dispatch()
        self.assertEqual(status, 200)
        self.assertTrue(result["references_readable"], result)
        self.assertEqual(self.engine.db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 0)
        handler.headers = headers(dict(binding, resource_id="cpu"))
        self.rejection(403, handler._dispatch)
        self.rejection(403, self.engine.authenticate, headers(binding), binding["job_id"])
        handler.headers = headers(binding, "capabilities")
        batch_path = self.engine.root / batch["relative_path"]
        original = batch_path.read_bytes()
        batch_path.write_bytes(b"tampered")
        self.assertEqual(handler._dispatch()[1]["inspection_error"], "ARTIFACT_UNAVAILABLE")
        batch_path.write_bytes(original)
        request["storage_id"] = "wrong-storage"
        self.assertFalse(handler._dispatch()[1]["references_readable"])
        request["storage_id"] = "pipeline-artifacts"
        binding, body = self.node_command(key="consume_business")
        body["claim"]["dependencies"] = [output]
        body["claim"]["spec"].update(depends_on=["encode_package"], parameters={"duration_seconds": 0, "batch_duration_seconds": 0})
        self.engine.put(binding, body)
        final = self.finished(binding)
        self.assertEqual(final["returncode"], 0, final)
        self.assertEqual(final["events"][-1]["event"]["artifact"]["metadata"]["dependencies"], [output["content_fingerprint"]])

    def test_node_state_identity_and_nonloopback_tls_gate(self):
        self.configure_node()
        self.engine.close()
        try:
            for node, storage in (("node-b", "pipeline-artifacts"), ("node-a", "other-storage")):
                with self.assertRaises(RuntimeError):
                    runner.Executor("independent-test", SECRET, self.directory / "artifacts", self.directory / "node-state",
                                    node_id=node, storage_id=storage)
        finally:
            self.engine = runner.Executor("independent-test", SECRET, self.directory / "artifacts", self.directory / "node-state",
                                          node_id="node-a", mock_gpu_slots=("gpu-a", "gpu-b"))
        result = subprocess.run([sys.executable, "-I", str(SERVICE), "--executor-id", "independent-test", "--node-id", "node-a",
            "--artifact-root", str(self.engine.root), "--state-dir", str(self.directory / "rejected-state"),
            "--port", "0", "--host", "0.0.0.0"], cwd=self.directory, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 2)
        self.assertIn(b"requires --tls-cert and --tls-key", result.stderr)
        self.assertFalse((self.directory / "rejected-state").exists())

    def test_capabilities_authentication_and_rejection_before_launch(self):
        binding, body = command()
        self.assertEqual(self.engine.authenticate(headers(binding)), binding)
        self.assertEqual(self.engine.capabilities()["processors"],
                         {"mock": ["independent-mock-v1"], "mock-step": ["independent-mock-v1"]})
        self.assertEqual(self.engine.capabilities()["input_kinds"], ["synthetic"])
        self.assertEqual(self.engine.db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 0)
        invalid = headers(binding)
        invalid["X-Execution-Protocol"] = "2"
        self.rejection(426, self.engine.authenticate, invalid)
        invalid = headers(binding)
        invalid["Authorization"] = "Bearer " + "0" * 64
        self.rejection(401, self.engine.authenticate, invalid)
        self.rejection(403, self.engine.authenticate, headers(binding), str(uuid4()))
        for bad in ("uploaded", "smoke"):
            modified = deepcopy(body)
            if bad == "uploaded":
                modified["claim"]["input"]["kind"] = bad
            else:
                modified["claim"]["spec"]["processor_version"] = bad
            self.rejection(422, self.engine.put, binding, modified)
        for key in ("command", "script", "artifact_root"):
            modified = deepcopy(body)
            modified["claim"][key] = "not allowed"
            self.rejection(422, self.engine.put, binding, modified)
        self.assertEqual(self.engine.db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 0)
        for raw in (b'{"x":NaN}', b'{"x":Infinity}', b'{"x":1e999}', b'{"x":0,"x":1}'):
            self.rejection(422, runner.read_json, raw)

    def test_capabilities_only_scope_cannot_authorize_any_job_route(self):
        binding, _ = command()
        readonly = headers(binding)
        raw = base64.urlsafe_b64decode(readonly["X-Execution-Binding"])
        readonly["X-Execution-Scope"] = "capabilities"
        readonly["Authorization"] = "Bearer " + hmac.new(SECRET.encode(), b"capabilities:" + raw, hashlib.sha256).hexdigest()
        handler = object.__new__(runner.Handler)
        handler.server = SimpleNamespace(executor=self.engine)

        def dispatch(method, path, credentials):
            handler.command, handler.path, handler.headers = method, path, credentials
            return handler._dispatch()

        status, capabilities = dispatch("GET", "/v1/capabilities", readonly)
        self.assertEqual(status, 200)
        self.assertEqual(capabilities["named_step_processors"], ["mock-step"])
        self.assertEqual(capabilities["processors"]["mock-step"], ["independent-mock-v1"])
        path = f"/v1/jobs/{binding['job_id']}"
        for method, suffix in (("GET", ""), ("PUT", ""), ("POST", "/lease"), ("POST", "/stop")):
            self.rejection(403, dispatch, method, path + suffix, readonly)
        no_scope = dict(readonly)
        no_scope.pop("X-Execution-Scope")
        self.rejection(401, dispatch, "GET", path, no_scope)
        wrong_domain = headers(binding)
        wrong_domain["X-Execution-Scope"] = "capabilities"
        self.rejection(401, dispatch, "GET", "/v1/capabilities", wrong_domain)
        self.assertEqual(dispatch("GET", "/v1/capabilities", headers(binding))[0], 200)
        self.assertEqual(self.engine.db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 0)
        self.assertEqual(list(self.engine.root.iterdir()), [])

    def test_dynamic_business_steps_checkpoint_resume_and_animation_name_collision(self):
        task = str(uuid4())

        def managed(key, dependencies=(), checkpoint=None, attempt=1, resource="cpu"):
            binding, body = command(key, dependencies, checkpoint=checkpoint, task_id=task, attempt_number=attempt)
            body["claim"]["spec"].update(processor="mock-step", parameters={},
                                          checkpoint_mode="committed-batch-manifest", resource_kind=resource)
            body["claim"]["resource_slot_id"] = 1 if resource == "mock-gpu" else None
            return binding, body

        invalid_binding, invalid_body = managed("../escape")
        self.rejection(422, self.engine.put, invalid_binding, invalid_body)
        legacy_binding, legacy_body = command("business_only")
        self.rejection(422, self.engine.put, legacy_binding, legacy_body)
        binding, body = managed("prepare_invoice-v2")
        parameters = {"duration_seconds": 0, "batch_duration_seconds": 0, "batch_count": 4,
                      "frames_per_batch": 2, "fail_first_attempts": 1}
        body["claim"]["spec"]["parameters"] = parameters
        self.engine.put(binding, body)
        failed = self.finished(binding)
        self.assertEqual(failed["returncode"], 1, failed)
        self.assertEqual(failed["events"][-1]["event"]["error_code"], "TRANSIENT_ERROR")
        checkpoint = [e["event"]["artifact"] for e in failed["events"] if e["event"]["type"] == "checkpoint"][-1]
        prefix = deepcopy(checkpoint["metadata"]["completed_batches"])
        self.assertEqual(len(prefix), 3)
        binding, body = managed("prepare_invoice-v2", checkpoint=checkpoint, attempt=2)
        body["claim"]["spec"]["parameters"] = dict(parameters)
        self.engine.put(binding, body)
        final = self.finished(binding)
        self.assertEqual(final["returncode"], 0, final)
        output = final["events"][-1]["event"]["artifact"]
        self.assertEqual(output["metadata"]["completed_batches"][:3], prefix)
        self.assertEqual(output["metadata"]["resumed_batches"], 3)
        self.assertEqual(output["metadata"]["resource_kind"], "cpu")
        for key, resource in (("encode_package", "mock-gpu"), ("publish_result", "cpu")):
            binding, body = managed(key, [output], resource=resource)
            predecessor = output["content_fingerprint"]
            self.engine.put(binding, body)
            final = self.finished(binding)
            self.assertEqual(final["returncode"], 0, final)
            output = final["events"][-1]["event"]["artifact"]
            data = runner.read_artifact(self.engine.root, output, "output")
            self.assertEqual(data["processor"], "mock-step")
            self.assertEqual(data["step_key"], key)
            self.assertEqual(data["resource_kind"], resource)
            self.assertEqual(data["dependencies"], [predecessor])
            self.assertEqual(data["total_batches"], 1)
            self.assertEqual(len(data["completed_batches"]), 1)
            self.assertNotIn("result_manifest", data)
            self.assertNotIn("publication", data)
        self.assertEqual(self.engine.db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 4)

    def test_one_launch_replay_frozen_binding_and_all_exit_events(self):
        binding, body = command()
        body["claim"]["spec"]["parameters"]["duration_seconds"] = 0.3
        initial = self.engine.put(binding, body)
        with self.engine.lock:
            row = self.engine._bound(binding)
            pid, lease = row["pid"], row["lease"]
        replay = deepcopy(body)
        replay["lease_expires_at"] = timestamp(60)
        self.assertEqual(self.engine.put(binding, replay)["start_count"], 1)
        with self.engine.lock:
            row = self.engine._bound(binding)
            self.assertEqual((row["pid"], row["lease"]), (pid, lease))
        conflicting = deepcopy(body)
        conflicting["claim"]["spec"]["parameters"]["seed"] += 1
        self.rejection(409, self.engine.put, binding, conflicting)
        other_binding = dict(binding, generation=2)
        self.rejection(403, self.engine.authenticate, headers(other_binding))
        state = self.finished(binding)
        self.assertEqual(initial["start_count"], 1)
        self.assertEqual(state["returncode"], 0, state)
        self.assertEqual([e["seq"] for e in state["events"]], [1, 2])
        self.assertEqual(state["events"][-1]["event"]["type"], "result")
        self.assertEqual(self.engine.status(binding, 1)["events"], state["events"][1:])
        self.assertEqual(self.engine.status(binding)["events"], state["events"])
        self.assertEqual(self.engine.status(binding, 2)["events"], [])
        self.rejection(409, self.engine.renew, binding, {"lease_expires_at": timestamp(120)})

    def test_six_real_json_steps_and_three_batch_resume(self):
        task = str(uuid4())
        normalized = self.result(task_id=task)
        segment = self.result("segment_subject", [normalized], task_id=task)
        motion = self.result("analyze_motion", [normalized], task_id=task)
        binding, body = command("generate_frames", [motion, segment], task_id=task)
        body["claim"]["spec"]["parameters"]["fail_first_attempts"] = 1
        self.engine.put(binding, body)
        failed = self.finished(binding)
        self.assertEqual(failed["returncode"], 1, failed)
        self.assertEqual(failed["events"][-1]["event"]["error_code"], "TRANSIENT_ERROR")
        checkpoints = [e["event"]["artifact"] for e in failed["events"] if e["event"]["type"] == "checkpoint"]
        self.assertEqual(len(checkpoints), 3)
        checkpoint = checkpoints[-1]
        original_prefix = deepcopy(checkpoint["metadata"]["completed_batches"])
        original_bytes = [(self.engine.root / ref["relative_path"]).read_bytes() for ref in original_prefix]
        # A later uncommitted candidate is never adopted by scanning the old directory.
        runner.write_artifact(self.engine.root, body["claim"], "batch-orphan", {"orphan": True})
        frames = self.result("generate_frames", [segment, motion], checkpoint=checkpoint, task_id=task, attempt_number=2)
        self.assertEqual(frames["metadata"]["resumed_batches"], 3)
        self.assertEqual(frames["metadata"]["completed_batches"][:3], original_prefix)
        self.assertEqual([(self.engine.root / r["relative_path"]).read_bytes() for r in original_prefix], original_bytes)
        encoded = self.result("encode_package", [frames], task_id=task)
        published = self.result("publish_result", [encoded], task_id=task)
        self.assertEqual(published["metadata"]["result_manifest"], encoded["metadata"]["result_manifest"])
        self.assertEqual(published["metadata"]["publication"], "local-only-mock")
        for artifact in (normalized, segment, motion, frames, encoded, published):
            raw = (self.engine.root / artifact["relative_path"]).read_bytes()
            self.assertEqual(hashlib.sha256(raw).hexdigest(), artifact["content_fingerprint"])
            self.assertEqual(json.loads(raw), artifact["metadata"])
            self.assertEqual(artifact["metadata"]["processor_version"], "independent-mock-v1")

    def test_omitted_parameters_default_without_mutating_claim(self):
        for key in ("normalize_asset", "generate_frames"):
            binding, body = command(key)
            body["claim"]["spec"]["parameters"] = {}
            if key == "generate_frames":
                body["claim"]["input"].pop("seed")
            frozen = runner.canonical(body["claim"])
            self.engine.put(binding, body)
            final = self.finished(binding)
            self.assertEqual(final["returncode"], 0, final)
            data = final["events"][-1]["event"]["artifact"]["metadata"]
            self.assertEqual(data["seed"], 0 if key == "generate_frames" else 42)
            if key == "generate_frames":
                self.assertEqual(data["total_batches"], 3)
                for ref in data["completed_batches"]:
                    self.assertEqual(len(runner.read_reference(self.engine.root, ref)["frames"]), 1)
            with self.engine.lock:
                row = self.engine._bound(binding)
                self.assertEqual(row["claim"].encode(), frozen)
                self.assertEqual(row["command_fingerprint"], hashlib.sha256(frozen).hexdigest())
            self.assertEqual(body["claim"]["spec"]["parameters"], {})
            self.assertEqual(self.engine.put(binding, body)["start_count"], 1)

    def test_tombstone_survives_restart_and_late_put(self):
        binding, body = command()
        state = self.engine.stop(binding, {})
        self.assertTrue(state["exit_confirmed"])
        self.assertEqual(state["start_count"], 0)
        self.engine.close()
        self.engine = runner.Executor("independent-test", SECRET, self.directory / "artifacts", self.directory / "state")
        self.rejection(409, self.engine.put, binding, body)
        self.assertEqual(self.engine.status(binding)["start_count"], 0)

    def test_expiring_lease_stops_real_sleeping_child(self):
        binding, body = command()
        body["claim"]["spec"]["parameters"]["duration_seconds"] = 30
        body["lease_expires_at"] = timestamp(0.6)
        self.engine.put(binding, body)
        with self.engine.lock:
            child = self.engine.live[binding["job_id"]]["process"]
        self.engine.renew(binding, {"lease_expires_at": timestamp(1.2)})
        time.sleep(0.7)
        self.assertFalse(self.engine.status(binding)["exit_confirmed"])
        final = self.finished(binding)
        self.assertIsNotNone(child.poll())
        self.assertNotEqual(final["returncode"], 0)
        self.assertFalse(any(e["event"]["type"] == "result" for e in final["events"]))
        self.rejection(409, self.engine.renew, binding, {"lease_expires_at": timestamp(60)})

    def test_zero_hard_limits_are_disabled(self):
        binding, body = command()
        claim = body["claim"]
        claim["runtime_policy"]["attempt_hard_timeout_seconds"] = 0
        claim["runtime_policy"]["task_deadline_seconds"] = 0
        claim["started_at"] = claim["first_started_at"] = timestamp(-60)
        claim["spec"]["parameters"]["duration_seconds"] = 0.2
        self.engine.put(binding, body)
        with self.engine.lock:
            self.assertIsNone(self.engine._bound(binding)["hard_deadline"])
        final = self.finished(binding)
        self.assertEqual(final["returncode"], 0, final)
        self.assertEqual(final["events"][-1]["event"]["type"], "result")

    def test_unknown_start_is_never_relaunched_or_confirmed(self):
        binding, body = command()
        claim = runner.canonical(body["claim"]).decode()
        with self.engine.lock, self.engine.db:
            self.engine.db.execute("INSERT INTO jobs (job_id,binding,claim,command_fingerprint,lease,state,start_count,host) "
                "VALUES (?,?,?,?,?,'STARTING',0,?)", (binding["job_id"], runner.canonical(binding).decode(), claim,
                    hashlib.sha256(claim.encode()).hexdigest(), time.time() + 30, self.engine.host))
        self.engine.close()
        self.engine = runner.Executor("independent-test", SECRET, self.directory / "artifacts", self.directory / "state")
        self.assertEqual(self.engine.put(binding, body)["state"], "UNCERTAIN")
        self.assertEqual(self.engine.stop(binding, {})["state"], "UNCERTAIN")
        state = self.engine.status(binding)
        self.assertFalse(state["exit_confirmed"])
        self.assertIsNone(state["returncode"])
        self.assertEqual(self.engine.live, {})

    def test_pid_reuse_does_not_kill_unrelated_process(self):
        binding, body = command()
        with self.engine.lock, self.engine.db:
            self.engine.db.execute("INSERT INTO jobs (job_id,binding,lease,state,start_count,stop_requested,pid,process_created,host) "
                "VALUES (?, ?, 0, 'UNCERTAIN', 1, 1, ?, ?, ?)",
                (binding["job_id"], runner.canonical(binding).decode(), os.getpid(),
                 psutil.Process().create_time() - 100, self.engine.host))
        state = self.finished(binding)
        self.assertIsNone(state["returncode"])
        self.assertTrue(psutil.Process().is_running())

    def test_restart_stops_known_compute_pid_with_unknown_returncode(self):
        binding, body = command()
        body["claim"]["spec"]["parameters"]["duration_seconds"] = 30
        self.engine.close()
        child = subprocess.Popen([runner.COMPUTE_PYTHON, "-I", str(SERVICE), "--compute"],
                                 cwd=self.directory, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL)
        try:
            child.stdin.write(runner.canonical({"claim": body["claim"], "artifact_root": str(self.directory / "artifacts")}))
            child.stdin.close()
            process = psutil.Process(child.pid)
            with sqlite3.connect(self.directory / "state" / "commands.sqlite3") as db:
                db.execute("INSERT INTO jobs (job_id,binding,claim,lease,state,start_count,pid,process_created,host) "
                    "VALUES (?,?,?,?,'RUNNING',1,?,?,?)", (binding["job_id"], runner.canonical(binding).decode(),
                        runner.canonical(body["claim"]).decode(), time.time() + 30,
                        child.pid, process.create_time(), socket.gethostname()))
            db.close()
            self.engine = runner.Executor("independent-test", SECRET, self.directory / "artifacts", self.directory / "state")
            final = self.finished(binding)
            if sys.platform.startswith("linux"):
                # Recovery must confirm exit without relying on the parent to reap it.
                self.assertEqual(process.status(), psutil.STATUS_ZOMBIE)
            child.wait(timeout=3)
            self.assertIsNone(final["returncode"])
            self.assertFalse(process.is_running())
            self.assertEqual(self.engine.put(binding, body)["start_count"], 1)
            self.assertEqual(self.engine.live, {})
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=3)

    def test_committed_integrity_and_path_rejection(self):
        artifact = self.result()
        binding, body = command("segment_subject", [artifact])
        changed = deepcopy(body)
        changed["claim"]["dependencies"][0]["metadata"]["seed"] += 1
        self.rejection(422, self.engine.put, binding, changed)
        (self.engine.root / artifact["relative_path"]).write_bytes(b'{}')
        self.rejection(422, self.engine.put, binding, body)
        for path in ("../escape.json", "C:/escape.json", "/escape.json", "a\\b.json"):
            self.rejection(422, runner.shared_path, self.engine.root, path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
