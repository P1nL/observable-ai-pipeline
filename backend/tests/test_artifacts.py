from __future__ import annotations

import hashlib
from contextlib import redirect_stdout
from io import StringIO
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from uuid import uuid4

from app.artifacts import read_checkpoint, validate_artifact, write_batch_artifact, write_json_artifact
from app.executor import execute


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.claim = {"task_id": str(uuid4()), "step_key": "generate_frames", "attempt_id": str(uuid4()), "compatibility_key": "compatible-v1"}

    def checkpoint(self):
        batch = write_batch_artifact(self.root, self.claim, 0, {"index": 0, "units": ["synthetic"]})
        manifest = {"schema_version": 1, "compatibility_key": self.claim["compatibility_key"], "total_batches": 2, "completed_batches": [batch]}
        return write_json_artifact(self.root, self.claim, "checkpoint", manifest)

    def test_immutable_attempt_scoped_files_and_direct_metadata(self):
        artifact = self.checkpoint()
        self.assertTrue(validate_artifact(self.root, artifact))
        path = self.root / artifact["relative_path"]
        self.assertEqual(json.loads(path.read_bytes()), artifact["metadata"])
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), artifact["content_fingerprint"])
        another = write_json_artifact(self.root, self.claim, "output", {"completed_units": 1})
        self.assertNotEqual(artifact["relative_path"], another["relative_path"])
        self.assertEqual(path.parts[-4:-1], (self.claim["task_id"], self.claim["step_key"], self.claim["attempt_id"]))
        self.assertFalse(list(path.parent.glob("*.tmp")))

    def test_publication_cannot_overwrite_an_existing_file(self):
        identifier = uuid4()
        with patch("app.artifacts.uuid4", return_value=identifier):
            first = write_json_artifact(self.root, self.claim, "output", {"value": 1})
            with self.assertRaises(FileExistsError):
                write_json_artifact(self.root, self.claim, "output", {"value": 2})
        self.assertEqual(json.loads((self.root / first["relative_path"]).read_bytes()), {"value": 1})
        self.assertTrue(validate_artifact(self.root, first))

    def test_checkpoint_recovery_requires_compatibility(self):
        checkpoint = self.checkpoint()
        recovered = read_checkpoint(self.root, checkpoint, "compatible-v1", total_batches=2)
        self.assertEqual(len(recovered["completed_batches"]), 1)
        with self.assertRaisesRegex(ValueError, "incompatible"):
            read_checkpoint(self.root, checkpoint, "other-version")
        with self.assertRaisesRegex(ValueError, "incompatible"):
            read_checkpoint(self.root, checkpoint, "compatible-v1", total_batches=3)

    def test_missing_committed_batch_is_not_silently_recomputed(self):
        checkpoint = self.checkpoint()
        batch = checkpoint["metadata"]["completed_batches"][0]
        (self.root / batch["relative_path"]).unlink()
        with self.assertRaisesRegex(ValueError, "unavailable"):
            validate_artifact(self.root, checkpoint)
        with self.assertRaises(ValueError):
            read_checkpoint(self.root, checkpoint, "compatible-v1")

    def test_corrupt_files_and_mutated_metadata_are_rejected(self):
        checkpoint = self.checkpoint()
        checkpoint["metadata"]["total_batches"] = 3
        with self.assertRaisesRegex(ValueError, "metadata"):
            validate_artifact(self.root, checkpoint)
        checkpoint = self.checkpoint()
        batch = checkpoint["metadata"]["completed_batches"][0]
        (self.root / batch["relative_path"]).write_text("changed", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "fingerprint"):
            validate_artifact(self.root, checkpoint)

    def test_escaping_artifact_and_batch_paths_are_rejected(self):
        for path in ("../outside.json", "/outside.json", "C:/outside.json", "a/../../outside.json", "a\\outside.json", "a/file.json:stream"):
            with self.subTest(path=path):
                artifact = write_json_artifact(self.root, self.claim, "output", {"value": 1})
                artifact["relative_path"] = path
                with self.assertRaises(ValueError):
                    validate_artifact(self.root, artifact)
        checkpoint = self.checkpoint()
        manifest = checkpoint["metadata"]
        manifest["completed_batches"][0]["relative_path"] = "../outside.json"
        escaped = write_json_artifact(self.root, self.claim, "checkpoint", manifest)
        with self.assertRaises(ValueError):
            validate_artifact(self.root, escaped)
        self.claim["step_key"] = "../outside"
        with self.assertRaises(ValueError):
            write_json_artifact(self.root, self.claim, "output", {"value": 1})

    def test_symlink_outside_root_is_rejected_when_supported(self):
        with TemporaryDirectory() as outside:
            try:
                (self.root / "escape").symlink_to(outside, target_is_directory=True)
            except (OSError, NotImplementedError):
                self.skipTest("Creating symlinks is not permitted on this host")
            artifact = write_json_artifact(self.root, self.claim, "output", {"value": 1})
            artifact["relative_path"] = "escape/file.json"
            with self.assertRaises(ValueError):
                validate_artifact(self.root, artifact)

    def test_batch_manifest_rejects_duplicate_or_out_of_order_indices(self):
        checkpoint = self.checkpoint()
        manifest = checkpoint["metadata"]
        manifest["completed_batches"][0]["index"] = 1
        invalid = write_json_artifact(self.root, self.claim, "checkpoint", manifest)
        with self.assertRaisesRegex(ValueError, "contiguous"):
            validate_artifact(self.root, invalid)

    def test_smoke_ignores_batch_controls_and_honors_its_duration(self):
        claim = {
            **self.claim, "attempt_number": 1, "input": {"seed": 42},
            "spec": {"processor_version": "1", "checkpoint_mode": "none", "parameters": {
                "duration_seconds": 5.5, "batch_count": 1, "batch_duration_seconds": 0,
            }},
        }
        stdout = StringIO()
        with redirect_stdout(stdout), patch("app.executor.time.sleep") as sleep:
            self.assertEqual(execute(claim, self.root), 0)
        sleep.assert_called_once_with(5.5)
        events = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual([event["type"] for event in events], ["progress", "result"])
        artifact = events[-1]["artifact"]
        self.assertEqual(artifact["metadata"]["compatibility_key"], claim["compatibility_key"])
        self.assertTrue(validate_artifact(self.root, artifact))

    def test_mock_step_v1_preserves_untagged_batch_bytes(self):
        claim = {**self.claim, "step_key": "work", "attempt_number": 1, "input": {"kind": "synthetic", "seed": 42},
                 "spec": {"processor": "mock-step", "processor_version": "1", "resource_kind": "cpu",
                          "checkpoint_mode": "committed-batch-manifest",
                          "parameters": {"batch_count": 1, "batch_duration_seconds": 0}}}
        stdout = StringIO()
        with redirect_stdout(stdout):
            self.assertEqual(execute(claim, self.root), 0)
        output = json.loads(stdout.getvalue().splitlines()[-1])["artifact"]["metadata"]
        self.assertNotIn("processor", output)
        batch = json.loads((self.root / output["completed_batches"][0]["relative_path"]).read_bytes())
        semantic = {"schema_version": 1, "source": "mock", "execution_resource": "cpu", "resource_kind": "cpu",
                    "compatibility_key": claim["compatibility_key"], "step_key": "work", "processor_version": "1",
                    "seed": 42, "input": claim["input"], "dependencies": [], "index": 0}
        content = json.dumps(semantic, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
        self.assertEqual(batch, {**semantic, "units": [hashlib.sha256(content + b":0").hexdigest()]})

    def test_full_checkpoint_recovery_reuses_batches_without_recomputation(self):
        claim = {
            **self.claim, "attempt_number": 1, "input": {"seed": 42},
            "spec": {"processor_version": "1", "checkpoint_mode": "committed-batch-manifest", "parameters": {
                "batch_count": 2, "batch_duration_seconds": 0, "fail_first_attempts": 1,
            }},
        }
        stdout = StringIO()
        with redirect_stdout(stdout), patch("app.executor.time.sleep"):
            self.assertEqual(execute(claim, self.root), 1)
        events = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual(events[-1]["error_code"], "TRANSIENT_ERROR")
        checkpoint = [event["artifact"] for event in events if event["type"] == "checkpoint"][-1]
        references = checkpoint["metadata"]["completed_batches"]
        resumed = {**claim, "attempt_id": str(uuid4()), "attempt_number": 2, "checkpoint": checkpoint}
        stdout = StringIO()
        with redirect_stdout(stdout), patch("app.executor.time.sleep", side_effect=AssertionError("No repeated delay")), patch("app.executor.write_batch_artifact", side_effect=AssertionError("No repeated batch")):
            self.assertEqual(execute(resumed, self.root), 0)
        artifact = json.loads(stdout.getvalue().splitlines()[-1])["artifact"]
        self.assertEqual(artifact["metadata"]["resumed_batches"], 2)
        self.assertEqual(artifact["metadata"]["completed_batches"], references)
        self.assertIn(resumed["attempt_id"], artifact["relative_path"])
        self.assertTrue(validate_artifact(self.root, artifact))
        batch_body = json.loads((self.root / references[0]["relative_path"]).read_bytes())
        for key in ("attempt_id", "duration_seconds", "batch_duration_seconds", "fail_first_attempts"):
            self.assertNotIn(key, batch_body)
            self.assertNotIn(key, artifact["metadata"])


if __name__ == "__main__":
    unittest.main()
