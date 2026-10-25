"""CPU-only MOCK computation. No database, lease renewal or child processes."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import sys
import time
from typing import Any

from .artifacts import read_checkpoint, validate_artifact, write_batch_artifact, write_json_artifact
from . import media


def _emit(kind: str, **payload: Any) -> None:
    print(json.dumps({"type": kind, **payload}, allow_nan=False), flush=True)


def _number(parameters: dict[str, Any], key: str, default: float) -> float:
    value = parameters.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError(f"{key} must be a finite nonnegative number")
    return float(value)


def _integer(parameters: dict[str, Any], key: str, default: int, *, minimum: int = 0) -> int:
    value = parameters.get(key, default)
    if type(value) is not int or value < minimum:
        raise ValueError(f"{key} must be an integer >= {minimum}")
    return value


def _reference(artifact: dict[str, Any]) -> dict[str, Any]:
    return {key: artifact[key] for key in ("id", "relative_path", "content_fingerprint")}


def _encoded_dependency(claim: dict[str, Any]) -> dict[str, Any]:
    dependencies = claim.get("dependencies", [])
    if len(dependencies) != 1 or dependencies[0]["metadata"].get("step_key") != "encode_package":
        raise ValueError("publish_result requires only its encode_package dependency")
    return dependencies[0]


def _publish_result(claim: dict[str, Any], output: dict[str, Any]) -> None:
    encoded = _encoded_dependency(claim)
    output["encoded_artifact"] = _reference(encoded)
    output["result_manifest"] = encoded["metadata"]["result_manifest"]
    if "preview" in encoded["metadata"]:
        output["preview"] = encoded["metadata"]["preview"]
    output["publication"] = "local-only-mock"


def _uploaded_frames(claim, root, semantic, normalized, image, duration, fail_first):
    parameters = claim["spec"].get("parameters", {})
    if claim["spec"].get("checkpoint_mode") != "committed-batch-manifest":
        raise ValueError("Uploaded frame generation requires committed batch checkpoints")
    total = _integer(parameters, "batch_count", 3, minimum=1)
    units = _integer(parameters, "frames_per_batch", 1, minimum=1)
    count = media.frame_budget(total, units, image.size)
    batch_duration = _number(parameters, "batch_duration_seconds", duration / total)
    batches = []
    if claim.get("checkpoint") is not None:
        try:
            checkpoint = read_checkpoint(root, claim["checkpoint"], claim["compatibility_key"], total_batches=total)
            if (checkpoint.get("media_pipeline") != media.MEDIA_PIPELINE
                    or checkpoint.get("normalized_asset") != normalized
                    or checkpoint.get("input_asset") != semantic["input_asset"]
                    or checkpoint.get("frames_per_batch") != units):
                raise ValueError("Checkpoint media input or frame configuration differs")
        except ValueError:
            _emit("error", error_code="ARTIFACT_UNAVAILABLE", message="The committed media checkpoint is unavailable or incompatible", retryable=False)
            return 1
        batches = list(checkpoint["completed_batches"])
    resumed = len(batches)

    def manifest():
        return {**semantic, "normalized_asset": normalized, "frames_per_batch": units,
                "total_batches": total, "completed_batches": list(batches)}

    def progress():
        _emit("progress", progress={"completed_units": len(batches) * units, "total_units": count,
                                    "resumed_batches": resumed, "percent": len(batches) * 100 / total})

    progress()
    for index in range(resumed, total):
        time.sleep(batch_duration)
        frames = []
        for offset in range(units):
            frame_index = index * units + offset
            frame = media.mock_frame(image, frame_index, count, semantic["seed"])
            frames.append({**media.write_image(root, claim, f"frame-{frame_index}", frame), "index": frame_index})
        batch = {**semantic, "normalized_asset": normalized, "index": index, "frames": frames}
        batches.append(write_batch_artifact(root, claim, index, batch))
        _emit("checkpoint", artifact=write_json_artifact(root, claim, "checkpoint", manifest()))
        progress()
    if claim["attempt_number"] <= fail_first:
        if resumed == total:
            _emit("checkpoint", artifact=write_json_artifact(root, claim, "checkpoint", manifest()))
        _emit("error", error_code="TRANSIENT_ERROR", message="Requested MOCK transient failure after media checkpoint", retryable=True)
        return 1
    output = {**manifest(), "completed_units": count, "resumed_batches": resumed}
    _emit("result", artifact=write_json_artifact(root, claim, "output", output))
    return 0


def _execute_uploaded(claim, root, semantic, duration, fail_first):
    semantic = {**semantic, "media_pipeline": media.MEDIA_PIPELINE, "input_asset": claim["input_asset"]}
    step = claim["step_key"]
    normalized = None
    image = None
    if step in {"generate_frames", "segment_subject", "analyze_motion"}:
        references = [item["metadata"]["normalized_asset"] for item in claim.get("dependencies", [])
                      if "normalized_asset" in item["metadata"]]
        if not references or any(ref != references[0] for ref in references):
            raise ValueError("Step requires an unambiguous normalized image dependency")
        normalized = references[0]
        image = media.read_image(root, normalized, normalized=True)
    if step == "generate_frames":
        return _uploaded_frames(claim, root, semantic, normalized, image, duration, fail_first)
    _emit("progress", progress={"completed_units": 0, "total_units": 1, "percent": 0})
    time.sleep(duration)
    if claim["attempt_number"] <= fail_first:
        _emit("error", error_code="TRANSIENT_ERROR", message="Requested MOCK transient failure", retryable=True)
        return 1
    output = {**semantic, "completed_units": 1, "resumed_batches": 0}
    if step == "normalize_asset":
        try:
            image = media.read_image(root, claim["input_asset"])
        except ValueError:
            _emit("error", error_code="ARTIFACT_UNAVAILABLE", message="The frozen uploaded input is unavailable", retryable=False)
            return 1
        image = media.normalize_image(image)
        output["normalized_asset"] = media.write_image(root, claim, "normalized", image)
    elif step in {"segment_subject", "analyze_motion"}:
        # These six-step adapters explicitly remain MOCK JSON, while retaining
        # the original normalize dependency for downstream real frame processing.
        output["normalized_asset"] = normalized
        output["mock_analysis"] = ({"subject_bounds": [0, 0, image.width, image.height]}
                                   if step == "segment_subject" else {"motion": "deterministic-translate-tint-fade"})
    elif step == "encode_package":
        dependencies = claim.get("dependencies", [])
        if len(dependencies) != 1 or dependencies[0]["metadata"].get("step_key") != "generate_frames":
            raise ValueError("Encoding requires its committed generate_frames output")
        generated = dependencies[0]["metadata"]
        if generated.get("media_pipeline") != media.MEDIA_PIPELINE:
            raise ValueError("Encoding an uploaded task requires real PNG frames")
        normalized = generated["normalized_asset"]
        media.frame_budget(generated["total_batches"], generated["frames_per_batch"],
                           (normalized["width"], normalized["height"]))
        frames = []
        for batch in generated["completed_batches"]:
            frames.extend(media.read_image(root, ref, normalized=True)
                          for ref in media.read_manifest(root, batch)["frames"])
        output["preview"] = media.write_gif(root, claim, frames)
        output["result_manifest"] = {"source": "mock", "format": "mock-animation-package-v1",
                                     "frame_manifests": [_reference(dependencies[0])],
                                     "input_id": claim["input_asset"]["id"], "frame_count": len(frames),
                                     "preview": output["preview"]}
    elif step == "publish_result":
        _publish_result(claim, output)
    else:
        raise ValueError("Unsupported uploaded-image step")
    _emit("result", artifact=write_json_artifact(root, claim, "output", output))
    return 0


def execute(claim: dict[str, Any], root: Path) -> int:
    spec = claim["spec"]
    parameters = spec.get("parameters", {})
    duration = _number(parameters, "duration_seconds", 0.05)
    fail_first = _integer(parameters, "fail_first_attempts", 0)
    for dependency in claim.get("dependencies", []):
        try:
            validate_artifact(root, dependency)
        except ValueError:
            _emit("error", error_code="ARTIFACT_UNAVAILABLE", message="A committed dependency is unavailable", retryable=False)
            return 1

    semantic = {
        "schema_version": 1,
        "source": "mock",
        "execution_resource": "cpu",
        "resource_kind": spec.get("resource_kind", "cpu"),
        "compatibility_key": claim["compatibility_key"],
        "step_key": claim["step_key"],
        "processor_version": spec["processor_version"],
        "seed": parameters.get("seed", claim.get("input", {}).get("seed", 0)),
        "input": claim.get("input", {}),
        "dependencies": [item["content_fingerprint"] for item in claim.get("dependencies", [])],
    }
    if claim.get("input_asset") is not None and spec.get("processor", "mock") == "mock":
        return _execute_uploaded(claim, root, semantic, duration, fail_first)
    batched = spec.get("checkpoint_mode", "none") == "committed-batch-manifest"
    if not batched:
        _emit("progress", progress={"completed_units": 0, "total_units": 1, "percent": 0})
        time.sleep(duration)
        if claim["attempt_number"] <= fail_first:
            _emit("error", error_code="TRANSIENT_ERROR", message="Requested synthetic transient failure", retryable=True)
            return 1
        output = {**semantic, "completed_units": 1, "resumed_batches": 0}
    else:
        total = _integer(parameters, "batch_count", 3, minimum=1)
        units = _integer(parameters, "frames_per_batch", 1, minimum=1)
        batch_duration = _number(parameters, "batch_duration_seconds", duration / total)
        batches = []
        if claim.get("checkpoint") is not None:
            try:
                checkpoint = read_checkpoint(root, claim["checkpoint"], claim["compatibility_key"], total_batches=total)
            except ValueError:
                _emit("error", error_code="ARTIFACT_UNAVAILABLE", message="The committed checkpoint is unavailable or incompatible", retryable=False)
                return 1
            batches = list(checkpoint["completed_batches"])
        resumed = len(batches)
        _emit("progress", progress={"completed_units": resumed * units, "total_units": total * units, "resumed_batches": resumed, "percent": resumed * 100 / total})
        for index in range(resumed, total):
            time.sleep(batch_duration)
            # Semantic bytes are independent of Attempt, timing, or injected
            # failures. The file path, not its content, isolates each Attempt.
            batch = {**semantic, "index": index}
            seed_bytes = json.dumps(batch, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
            batch["units"] = [hashlib.sha256(seed_bytes + f":{unit}".encode()).hexdigest() for unit in range(units)]
            batches.append(write_batch_artifact(root, claim, index, batch))
            manifest = {"schema_version": 1, "compatibility_key": claim["compatibility_key"], "total_batches": total, "completed_batches": list(batches)}
            _emit("checkpoint", artifact=write_json_artifact(root, claim, "checkpoint", manifest))
            _emit("progress", progress={"completed_units": len(batches) * units, "total_units": total * units, "resumed_batches": resumed, "percent": len(batches) * 100 / total})
        # Inject failure after the final durable checkpoint candidate. The
        # supervisor commits preceding messages before recording this failure.
        if claim["attempt_number"] <= fail_first:
            if resumed == total:
                manifest = {"schema_version": 1, "compatibility_key": claim["compatibility_key"], "total_batches": total, "completed_batches": list(batches)}
                _emit("checkpoint", artifact=write_json_artifact(root, claim, "checkpoint", manifest))
            _emit("error", error_code="TRANSIENT_ERROR", message="Requested synthetic transient failure after checkpoint", retryable=True)
            return 1
        output = {**semantic, "total_batches": total, "completed_batches": batches, "completed_units": total * units, "resumed_batches": resumed}
    if spec.get("processor", "mock") == "mock" and claim["step_key"] == "encode_package":
        output["result_manifest"] = {
            "source": "mock",
            "format": "mock-animation-package-v1",
            "frame_manifests": [
                {key: item[key] for key in ("id", "relative_path", "content_fingerprint")}
                for item in claim.get("dependencies", [])
            ],
        }
    if spec.get("processor", "mock") == "mock" and claim["step_key"] == "publish_result":
        _publish_result(claim, output)
    _emit("result", artifact=write_json_artifact(root, claim, "output", output))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Internal MOCK executor")
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument("--generation", type=int, required=True)
    arguments = parser.parse_args()
    try:
        request = json.loads(sys.stdin.readline())
        if arguments.attempt_id != request["claim"]["attempt_id"] or arguments.generation != request["claim"]["generation"] or arguments.generation < 1:
            raise ValueError("Executor identity differs from its immutable claim")
        return execute(request["claim"], Path(request["artifact_root"]))
    except (ValueError, KeyError, TypeError):
        _emit("error", error_code="INVALID_INPUT", message="Invalid immutable executor configuration", retryable=False)
    except OSError:
        _emit("error", error_code="TRANSIENT_ERROR", message="Executor storage operation failed", retryable=True)
    except Exception:
        _emit("error", error_code="EXECUTOR_ERROR", message="Executor computation failed", retryable=False)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
