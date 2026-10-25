"""Pure input/media checks. Real HTTP, database fencing and cancel are integrated separately."""
from __future__ import annotations

import asyncio
from contextlib import redirect_stdout
from copy import deepcopy
import hashlib
from io import BytesIO, StringIO
import json
from pathlib import Path
import struct
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID, uuid4
import zlib

from PIL import Image
import pytest

from app.artifacts import validate_artifact, write_batch_artifact, write_json_artifact
from app.domain import DomainError
from app.executor import execute
from app.inputs import build_input_router, read_upload, resolve_input, store_input
from app import media


class InputConnection:
    """Only the inputs table contract; no simulated HTTP/database acceptance."""
    def __init__(self):
        self.rows = {}
        self.result = None

    def execute(self, statement, parameters):
        if statement.startswith("INSERT"):
            row = dict(zip(("id", "relative_path", "content_fingerprint", "media_type", "width", "height", "byte_count"), parameters))
            self.rows[row["id"]] = row
        else:
            self.result = self.rows.get(str(parameters[0]))
        return self

    def fetchone(self):
        return self.result


def image_bytes(format="PNG", size=(48, 32)):
    image = Image.new("RGBA", size, (220, 60, 40, 230))
    for x in range(size[0] // 3, size[0] // 2):
        for y in range(size[1] // 3, size[1] // 2):
            image.putpixel((x, y), (40, 220, 110, 180))
    stream = BytesIO()
    (image.convert("RGB") if format == "JPEG" else image).save(stream, format=format)
    return stream.getvalue()


def header_png(width, height):
    # Header dimensions must be rejected before the decoder attempts the body.
    def chunk(kind, content):
        return struct.pack(">I", len(content)) + kind + content + struct.pack(">I", zlib.crc32(kind + content))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(b"\x00\x00\x00\x00")) + chunk(b"IEND", b""))


@pytest.fixture
def uploaded(tmp_path):
    conn = InputConnection()
    return conn, store_input(conn, image_bytes(), "image/png", tmp_path)


def claim_for(input_asset, step, dependencies=(), **parameters):
    return {"task_id": str(uuid4()), "attempt_id": str(uuid4()), "attempt_number": 1,
            "generation": 1, "step_key": step, "compatibility_key": "media-" + step,
            "input": {"kind": "uploaded", "input_id": input_asset["id"], "seed": 7},
            "input_asset": input_asset, "dependencies": list(dependencies),
            "spec": {"processor_version": "1", "parameters": {"duration_seconds": 0,
                     "batch_duration_seconds": 0, "batch_count": 2, "frames_per_batch": 2, **parameters},
                     "checkpoint_mode": "committed-batch-manifest" if step == "generate_frames" else "none"}}


def run(claim, root):
    stream = StringIO()
    with redirect_stdout(stream):
        code = execute(claim, root)
    return code, [json.loads(line) for line in stream.getvalue().splitlines()]


def normalized_claim(root, input_asset):
    claim = claim_for(input_asset, "normalize_asset")
    code, messages = run(claim, root)
    assert code == 0
    normalized = messages[-1]["artifact"]
    claim = claim_for(input_asset, "generate_frames", [normalized])
    claim["task_id"] = normalized["relative_path"].split("/")[0]
    return normalized, claim


@pytest.mark.parametrize("format,media_type,extension", [("PNG", "image/png", "png"), ("JPEG", "image/jpeg", "jpg")])
def test_input_storage_resolution_and_router_contract(tmp_path, format, media_type, extension):
    conn = InputConnection()
    content = image_bytes(format)
    item = store_input(conn, content, media_type, tmp_path)
    assert UUID(item["id"])
    assert item["relative_path"] == f"inputs/{item['id']}.{extension}"
    assert (tmp_path / item["relative_path"]).read_bytes() == content
    assert resolve_input(conn, item["id"], tmp_path) == item
    assert item["content_fingerprint"] == hashlib.sha256(content).hexdigest()
    assert (item["width"], item["height"], item["byte_count"]) == (48, 32, len(content))
    router = build_input_router(SimpleNamespace(artifact_root=tmp_path))
    assert {(route.path, tuple(sorted(route.methods))) for route in router.routes} == {
        ("/api/v1/inputs", ("POST",)), ("/api/v1/inputs/{input_id}", ("GET",)),
        ("/api/v1/inputs/{input_id}/content", ("GET",))}
    with patch("app.inputs.uuid4", return_value=UUID(item["id"])):
        with pytest.raises(FileExistsError):
            store_input(conn, image_bytes(format, (24, 24)), media_type, tmp_path)
    assert (tmp_path / item["relative_path"]).read_bytes() == content


def test_input_absence_corruption_dimensions_and_uuid_path(tmp_path, uploaded):
    conn, item = uploaded
    with pytest.raises(DomainError) as error:
        resolve_input(conn, uuid4(), tmp_path)
    assert error.value.http_status == 404
    original = conn.rows[item["id"]].copy()
    for key, value in (("relative_path", "../escape.png"), ("width", 49), ("byte_count", 1),
                       ("relative_path", f"inputs/{uuid4()}.png"), ("content_fingerprint", "0" * 64)):
        conn.rows[item["id"]] = {**original, key: value}
        with pytest.raises(DomainError) as error:
            resolve_input(conn, item["id"], tmp_path)
        assert error.value.http_status == 409
    conn.rows[item["id"]] = original
    (tmp_path / item["relative_path"]).write_bytes(b"corrupted")
    with pytest.raises(DomainError) as error:
        resolve_input(conn, item["id"], tmp_path)
    assert error.value.http_status == 409


@pytest.mark.parametrize("content,media_type", [
    (b"", "image/png"), (b"<svg/>", "image/png"), (b"<svg/>", "image/svg+xml"),
    (image_bytes("JPEG"), "image/png"), (image_bytes(), "image/jpeg"),
    (image_bytes("GIF"), "image/gif"), (b"x" * (media.MAX_UPLOAD_BYTES + 1), "image/png"),
], ids=["empty", "not-image", "svg", "jpeg-as-png", "png-as-jpeg", "gif", "over-8-mib"])
def test_invalid_and_mismatched_uploads(tmp_path, content, media_type):
    with pytest.raises(DomainError) as error:
        store_input(InputConnection(), content, media_type, tmp_path)
    assert error.value.http_status == 422
    assert not (tmp_path / "inputs").exists()


@pytest.mark.parametrize("size,message", [((8193, 1), "8192"), ((4001, 4000), "16 megapixels")])
def test_decode_bounds_are_checked_before_loading(size, message):
    with pytest.raises(ValueError, match=message):
        media.decode_image(header_png(*size), "image/png")


def test_animated_png_is_not_a_static_upload():
    stream = BytesIO()
    Image.new("RGB", (8, 8), "red").save(stream, format="PNG", save_all=True,
        append_images=[Image.new("RGB", (8, 8), "blue")], duration=80)
    with pytest.raises(ValueError, match="multi-frame"):
        media.decode_image(stream.getvalue(), "image/png")


def test_normalize_corrects_orientation_and_removes_upload_metadata():
    image = Image.new("RGB", (40, 20), "red")
    exif = Image.Exif()
    exif[274] = 6
    stream = BytesIO()
    image.save(stream, format="JPEG", exif=exif)
    normalized = media.normalize_image(media.decode_image(stream.getvalue(), "image/jpeg"))
    assert normalized.size == (20, 40)
    assert not normalized.info


def test_mock_frames_are_deterministic_for_the_same_source_and_seed():
    image = media.decode_image(image_bytes(), "image/png")
    first = media.mock_frame(image, 0, 10, 7)
    assert first.tobytes() == media.mock_frame(image, 0, 10, 7).tobytes()
    assert first.tobytes() != media.mock_frame(image, 0, 10, 8).tobytes()
    assert first.getchannel("A").tobytes() != image.getchannel("A").tobytes()


def test_request_stream_is_limited_incrementally_without_trusting_content_length():
    seen = []
    async def chunks():
        for index in range(11):
            seen.append(index)
            yield b"x" * (1024 * 1024)
    with pytest.raises(DomainError) as error:
        asyncio.run(read_upload(SimpleNamespace(stream=chunks)))
    assert error.value.http_status == 413
    assert seen == list(range(9))


def test_normalize_reads_the_source_and_reduces_it(tmp_path):
    conn = InputConnection()
    item = store_input(conn, image_bytes(size=(600, 300)), "image/png", tmp_path)
    claim = claim_for(item, "normalize_asset")
    code, events = run(claim, tmp_path)
    assert code == 0
    artifact = events[-1]["artifact"]
    assert validate_artifact(tmp_path, artifact)
    ref = artifact["metadata"]["normalized_asset"]
    actual = media.read_image(tmp_path, ref, normalized=True)
    assert actual.size == (256, 128)
    assert actual.tobytes() == media.normalize_image(media.read_image(tmp_path, item)).tobytes()
    assert ref["relative_path"].startswith(f"{claim['task_id']}/normalize_asset/{claim['attempt_id']}/")
    assert artifact["metadata"]["input_asset"] == item


def test_real_png_batches_and_decodable_gif_pipeline(tmp_path, uploaded):
    _, item = uploaded
    normalized, claim = normalized_claim(tmp_path, item)
    code, events = run(claim, tmp_path)
    assert code == 0
    generated = events[-1]["artifact"]
    assert validate_artifact(tmp_path, generated)
    frames = [frame for batch in generated["metadata"]["completed_batches"]
              for frame in media.read_manifest(tmp_path, batch)["frames"]]
    assert [frame["index"] for frame in frames] == list(range(4))
    assert len({frame["content_fingerprint"] for frame in frames}) > 1
    for frame in frames:
        assert media.read_image(tmp_path, frame, normalized=True).size == (48, 32)
        assert frame["relative_path"].startswith(f"{claim['task_id']}/generate_frames/{claim['attempt_id']}/")
    encode = claim_for(item, "encode_package", [generated])
    encode["task_id"] = claim["task_id"]
    code, events = run(encode, tmp_path)
    assert code == 0
    result = events[-1]["artifact"]
    assert validate_artifact(tmp_path, result)
    assert result["metadata"]["source"] == "mock"
    assert result["metadata"]["preview"]["media_type"] == "image/gif"
    assert result["metadata"]["result_manifest"]["frame_count"] == 4
    with Image.open(media.preview_path(tmp_path, result)) as gif:
        assert gif.format == "GIF" and 1 < gif.n_frames <= 4
        for index in range(gif.n_frames):
            gif.seek(index)
            gif.load()
    assert result["metadata"]["result_manifest"]["frame_manifests"][0]["id"] == generated["id"]
    assert normalized["metadata"]["normalized_asset"] == generated["metadata"]["normalized_asset"]
    # Preview association follows the committed JSON chain, not the consuming
    # Task's namespace. This is a validator contract, not six-step acceptance.
    published_claim = claim_for(item, "publish_result")
    published = write_json_artifact(tmp_path, published_claim, "output", {
        "media_pipeline": media.MEDIA_PIPELINE, "source": "mock", "step_key": "publish_result",
        "input_asset": item, "preview": result["metadata"]["preview"],
        "result_manifest": result["metadata"]["result_manifest"],
        "encoded_artifact": {key: result[key] for key in ("id", "relative_path", "content_fingerprint")},
    })
    assert validate_artifact(tmp_path, published)
    assert media.preview_path(tmp_path, published) == media.preview_path(tmp_path, result)


def test_media_checkpoint_recovers_only_committed_compatible_batches(tmp_path, uploaded):
    _, item = uploaded
    _, claim = normalized_claim(tmp_path, item)
    claim["spec"]["parameters"]["fail_first_attempts"] = 1
    code, events = run(claim, tmp_path)
    assert code == 1
    checkpoints = [event["artifact"] for event in events if event["type"] == "checkpoint"]
    # The caller supplies the committed prefix; no directory scan may adopt the
    # later candidate batch left by this interrupted Attempt.
    resumed = {**claim, "attempt_id": str(uuid4()), "attempt_number": 2, "checkpoint": checkpoints[0]}
    with patch("app.media.mock_frame", wraps=media.mock_frame) as generate:
        code, events = run(resumed, tmp_path)
    assert code == 0 and generate.call_count == 2
    output = events[-1]["artifact"]
    assert output["metadata"]["resumed_batches"] == 1
    assert output["metadata"]["completed_batches"][0] == checkpoints[0]["metadata"]["completed_batches"][0]
    assert validate_artifact(tmp_path, output)
    resumed["checkpoint"] = checkpoints[-1]
    with patch("app.media.mock_frame", side_effect=AssertionError("Cannot recompute committed frames")):
        assert run(resumed, tmp_path)[0] == 0
    resumed["compatibility_key"] = "different"
    code, events = run(resumed, tmp_path)
    assert code == 1 and events[-1]["error_code"] == "ARTIFACT_UNAVAILABLE"


def test_missing_or_corrupt_frames_invalidate_checkpoint_and_encoding(tmp_path, uploaded):
    _, item = uploaded
    _, claim = normalized_claim(tmp_path, item)
    _, events = run(claim, tmp_path)
    checkpoint = [event["artifact"] for event in events if event["type"] == "checkpoint"][-1]
    generated = events[-1]["artifact"]
    encode = claim_for(item, "encode_package", [generated])
    encode["task_id"] = claim["task_id"]
    _, encoded = run(encode, tmp_path)
    result = encoded[-1]["artifact"]
    batch_ref = generated["metadata"]["completed_batches"][0]
    batch = media.read_manifest(tmp_path, batch_ref)
    (tmp_path / batch["frames"][0]["relative_path"]).unlink()
    for artifact in (checkpoint, generated, result):
        with pytest.raises(ValueError):
            validate_artifact(tmp_path, artifact)
    resumed = {**claim, "attempt_id": str(uuid4()), "checkpoint": checkpoint}
    code, events = run(resumed, tmp_path)
    assert code == 1 and events[-1]["error_code"] == "ARTIFACT_UNAVAILABLE"


def test_matching_hash_does_not_turn_bad_media_into_a_valid_batch(tmp_path, uploaded):
    _, item = uploaded
    _, claim = normalized_claim(tmp_path, item)
    _, events = run(claim, tmp_path)
    generated = events[-1]["artifact"]
    manifest = deepcopy(generated["metadata"])
    batch = media.read_manifest(tmp_path, manifest["completed_batches"][0])
    frame = batch["frames"][0]
    (tmp_path / frame["relative_path"]).write_bytes(b"not an image")
    frame["content_fingerprint"] = hashlib.sha256(b"not an image").hexdigest()
    manifest["completed_batches"][0] = write_batch_artifact(tmp_path, claim, 0, batch)
    invalid = write_json_artifact(tmp_path, claim, "output", manifest)
    with pytest.raises(ValueError):
        validate_artifact(tmp_path, invalid)


def test_preview_requires_associated_gif_not_just_a_valid_hash(tmp_path, uploaded):
    _, item = uploaded
    normalized, claim = normalized_claim(tmp_path, item)
    # A PNG in the correct Attempt directory still cannot be a GIF preview.
    artifact = deepcopy(normalized)
    artifact["metadata"]["preview"] = {**artifact["metadata"]["normalized_asset"], "media_type": "image/gif"}
    with pytest.raises(ValueError, match="GIF"):
        media.preview_path(tmp_path, artifact)
    artifact["metadata"]["preview"]["relative_path"] = "../outside.gif"
    with pytest.raises(ValueError):
        media.preview_path(tmp_path, artifact)
    assert media.frame_budget(12, 10, (256, 256)) == 120
    with pytest.raises(ValueError, match="120"):
        media.frame_budget(11, 11, (256, 256))
    claim["spec"]["parameters"].update(batch_count=121, frames_per_batch=1)
    with pytest.raises(ValueError, match="120"):
        run(claim, tmp_path)
