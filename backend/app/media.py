"""Bounded local image processing; model behaviour remains deterministic CPU MOCK.

Media files are candidates, not downloadable artifacts. Their owning JSON must
first be accepted by the existing Attempt/lease fence.
"""
from __future__ import annotations

from io import BytesIO
import json
import math
from pathlib import Path, PurePosixPath
from typing import Any
import warnings

from PIL import Image, ImageEnhance, ImageOps, UnidentifiedImageError

from .artifacts import _read_verified, _safe_path, _write_file

MAX_UPLOAD_BYTES = 8 * 1024 * 1024
MAX_IMAGE_PIXELS = 16_000_000
MAX_IMAGE_EDGE = 8192
NORMALIZED_EDGE = 256
MAX_FRAMES = 120
MAX_PROCESSING_PIXELS = MAX_FRAMES * NORMALIZED_EDGE * NORMALIZED_EDGE
MEDIA_PIPELINE = "uploaded-image-v1"
STATIC_FORMATS = {"image/png": "PNG", "image/jpeg": "JPEG"}


def decode_image(content: bytes, media_type: str) -> Image.Image:
    """Validate headers before decoding, then return a detached RGBA image."""
    if not isinstance(media_type, str) or media_type not in STATIC_FORMATS:
        raise ValueError("Only static PNG and JPEG images are supported")
    if not content or len(content) > MAX_UPLOAD_BYTES:
        raise ValueError("Image must contain 1 byte to 8 MiB")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(BytesIO(content)) as image:
                if image.format != STATIC_FORMATS[media_type]:
                    raise ValueError("Content-Type must match the actual PNG/JPEG format")
                width, height = image.size
                if not (0 < width <= MAX_IMAGE_EDGE and 0 < height <= MAX_IMAGE_EDGE):
                    raise ValueError("Image edges must not exceed 8192 pixels")
                if width * height > MAX_IMAGE_PIXELS:
                    raise ValueError("Image must not exceed 16 megapixels")
                if getattr(image, "n_frames", 1) != 1:
                    raise ValueError("Animated or multi-frame inputs are not supported")
                image.verify()
            with Image.open(BytesIO(content)) as image:
                image.load()
                # Preserve EXIF for normalize's orientation correction, without
                # carrying arbitrary upload metadata into generated PNG files.
                result = image.convert("RGBA")
                result.info["exif"] = image.info.get("exif", b"")
                return result
    except (OSError, SyntaxError, UnidentifiedImageError,
            Image.DecompressionBombError, Image.DecompressionBombWarning) as error:
        raise ValueError("Image cannot be safely decoded") from error


def _media_bytes(root: Path, reference: dict[str, Any]) -> bytes:
    if not isinstance(reference, dict):
        raise ValueError("Invalid media reference")
    target = _safe_path(root, reference.get("relative_path"))
    try:
        if not 0 < target.stat().st_size <= MAX_UPLOAD_BYTES:
            raise ValueError("Media file exceeds the bounded size")
    except OSError as error:
        raise ValueError("Media file is unavailable") from error
    return _read_verified(root, reference)


def read_image(root: Path, reference: dict[str, Any], *, normalized: bool = False) -> Image.Image:
    image = decode_image(_media_bytes(root, reference), reference.get("media_type"))
    if (any(type(reference.get(key)) is not int for key in ("width", "height"))
            or (reference.get("width"), reference.get("height")) != image.size):
        raise ValueError("Media dimensions differ from their reference")
    if normalized and (reference.get("media_type") != "image/png" or max(image.size) > NORMALIZED_EDGE):
        raise ValueError("Normalized images and frames must be PNG at most 256 pixels per edge")
    return image


def normalize_image(image: Image.Image) -> Image.Image:
    image = ImageOps.exif_transpose(image)
    image.thumbnail((NORMALIZED_EDGE, NORMALIZED_EDGE), Image.Resampling.LANCZOS)
    result = image.convert("RGBA")
    result.info.clear()
    return result


def write_image(root: Path, claim: dict[str, Any], label: str, image: Image.Image) -> dict[str, Any]:
    if max(image.size) > NORMALIZED_EDGE or min(image.size) < 1:
        raise ValueError("Generated PNG dimensions exceed the processing bound")
    stream = BytesIO()
    image.save(stream, format="PNG")
    return _write_media(root, claim, label, stream.getvalue(), "png", "image/png", image.size)


def _write_media(root, claim, label, content, extension, media_type, size):
    if len(content) > MAX_UPLOAD_BYTES:
        raise ValueError("Generated media exceeds the file size bound")
    _, relative_path, fingerprint = _write_file(root, claim, label, content, extension)
    return {"relative_path": relative_path, "content_fingerprint": fingerprint,
            "media_type": media_type, "width": size[0], "height": size[1]}


def frame_budget(batch_count: int, frames_per_batch: int, size: tuple[int, int]) -> int:
    if type(batch_count) is not int or type(frames_per_batch) is not int or min(batch_count, frames_per_batch) < 1:
        raise ValueError("Frame batch counts must be positive integers")
    if len(size) != 2 or any(type(edge) is not int or edge < 1 for edge in size):
        raise ValueError("Frame dimensions must be positive integers")
    count = batch_count * frames_per_batch
    if count > MAX_FRAMES or max(size) > NORMALIZED_EDGE or min(size) < 1:
        raise ValueError("Uploaded-image tasks support at most 120 frames at 256 pixels per edge")
    if size[0] * size[1] * count > MAX_PROCESSING_PIXELS:
        raise ValueError("Frame processing exceeds the total pixel budget")
    return count


def mock_frame(image: Image.Image, index: int, count: int, seed: Any) -> Image.Image:
    """Move, tint and fade the actual input; no model or GPU is involved."""
    import hashlib

    phase = 2 * math.pi * (index / count + int(hashlib.sha256(str(seed).encode()).hexdigest()[:8], 16) / 2**32)
    tinted = ImageEnhance.Color(image).enhance(0.7 + 0.5 * (1 + math.sin(phase)))
    tinted = ImageEnhance.Brightness(tinted).enhance(0.85 + 0.15 * math.cos(phase))
    tinted.putalpha(tinted.getchannel("A").point(lambda alpha: round(alpha * (0.8 + 0.2 * math.cos(phase)))))
    canvas = Image.new("RGBA", image.size, (0, 0, 0, 0))
    dx = round(math.sin(phase) * max(1, image.width // 16))
    dy = round(math.cos(phase) * max(1, image.height // 24))
    canvas.alpha_composite(tinted, (dx, dy))
    return canvas


def write_gif(root: Path, claim: dict[str, Any], frames: list[Image.Image]) -> dict[str, Any]:
    if not frames:
        raise ValueError("Cannot encode an empty frame sequence")
    frame_budget(1, len(frames), frames[0].size)
    palettes = []
    for frame in frames:
        if frame.size != frames[0].size:
            raise ValueError("GIF frames must share their dimensions")
        background = Image.new("RGBA", frame.size, (20, 24, 36, 255))
        background.alpha_composite(frame)
        palettes.append(background.convert("RGB").quantize(colors=255))
    stream = BytesIO()
    palettes[0].save(stream, format="GIF", save_all=True, append_images=palettes[1:],
                     duration=80, loop=0, disposal=2, optimize=False)
    return _write_media(root, claim, "preview", stream.getvalue(), "gif", "image/gif", frames[0].size)


def _same_namespace(reference: dict, owner: dict) -> None:
    if PurePosixPath(reference.get("relative_path", "")).parent != PurePosixPath(owner.get("relative_path", "")).parent:
        raise ValueError("Media reference is outside its owning Attempt namespace")


def preview_path(root: Path, artifact: dict[str, Any]) -> Path:
    """Validate only an artifact's associated preview reference and actual GIF.

    Callers must first establish the JSON artifact is a committed Task reference.
    A publish_result may refer to its direct encode_package dependency's preview.
    """
    metadata = artifact.get("metadata", {})
    preview = metadata.get("preview")
    if not isinstance(preview, dict) or preview.get("media_type") != "image/gif":
        raise ValueError("Artifact has no GIF preview reference")
    owner = artifact
    if metadata.get("step_key") == "publish_result":
        owner = metadata.get("encoded_artifact", {})
        encoded = read_manifest(root, owner)
        if encoded.get("step_key") != "encode_package" or encoded.get("preview") != preview:
            raise ValueError("Preview does not belong to the encoded dependency")
    _same_namespace(preview, owner)
    content = _media_bytes(root, preview)
    try:
        with Image.open(BytesIO(content)) as image:
            if image.format != "GIF":
                raise ValueError("Preview is not a GIF")
            frame_budget(1, image.n_frames, image.size)
            for index in range(image.n_frames):
                image.seek(index)
                if max(image.size) > NORMALIZED_EDGE:
                    raise ValueError("GIF frame exceeds the processing bound")
                image.load()
    except (OSError, SyntaxError) as error:
        raise ValueError("GIF preview cannot be decoded") from error
    return _safe_path(root, preview["relative_path"])


def read_manifest(root: Path, reference: dict[str, Any]) -> dict[str, Any]:
    try:
        manifest = json.loads(_read_verified(root, reference))
    except (ValueError, UnicodeError) as error:
        raise ValueError("Referenced media manifest is unavailable") from error
    if not isinstance(manifest, dict):
        raise ValueError("Invalid media manifest")
    return manifest


def validate_frame_batch(root: Path, manifest: dict, reference: dict, content: bytes) -> None:
    try:
        batch = json.loads(content)
    except (ValueError, UnicodeError) as error:
        raise ValueError("Frame batch is not JSON") from error
    units = manifest.get("frames_per_batch")
    normalized = manifest.get("normalized_asset", {})
    if not isinstance(normalized, dict):
        raise ValueError("Missing normalized image reference")
    frame_budget(manifest["total_batches"], units, (normalized.get("width", 0), normalized.get("height", 0)))
    if not isinstance(batch, dict) or batch.get("index") != reference["index"] or batch.get("compatibility_key") != manifest["compatibility_key"]:
        raise ValueError("Frame batch does not match its committed manifest")
    if batch.get("normalized_asset") != normalized or batch.get("input_asset") != manifest.get("input_asset"):
        raise ValueError("Frame batch input differs from its manifest")
    frames = batch.get("frames")
    if not isinstance(frames, list) or len(frames) != units:
        raise ValueError("Missing frames in the committed batch")
    paths = set()
    for offset, frame in enumerate(frames):
        if not isinstance(frame, dict) or frame.get("index") != reference["index"] * units + offset:
            raise ValueError("Frame indices must form the committed ordered batch")
        _same_namespace(frame, reference)
        image = read_image(root, frame, normalized=True)
        if image.size != (normalized["width"], normalized["height"]):
            raise ValueError("Frame dimensions differ from the normalized input")
        if frame["relative_path"] in paths:
            raise ValueError("Duplicate frame reference")
        paths.add(frame["relative_path"])


def validate_media_references(root: Path, artifact: dict, manifest: dict) -> None:
    """Extend JSON availability to every media file it actually references."""
    if "normalized_asset" in manifest:
        normalized = manifest["normalized_asset"]
        read_image(root, normalized, normalized=True)
        if manifest.get("step_key") == "normalize_asset":
            _same_namespace(normalized, artifact)
    if "preview" in manifest:
        preview_path(root, artifact)
    if manifest.get("media_pipeline") != MEDIA_PIPELINE:
        return
    from .artifacts import validate_artifact

    step = manifest.get("step_key")
    if step in {"normalize_asset", "generate_frames", "segment_subject", "analyze_motion"} and "normalized_asset" not in manifest:
        raise ValueError("Media step is missing its normalized input/output reference")
    if step == "generate_frames" and "completed_batches" not in manifest:
        raise ValueError("Frame output is missing its committed batches")
    if step in {"encode_package", "publish_result"}:
        if "preview" not in manifest or manifest.get("result_manifest", {}).get("preview") != manifest["preview"]:
            raise ValueError("Result manifest is missing its associated preview")
    if manifest.get("step_key") == "encode_package":
        refs = manifest.get("result_manifest", {}).get("frame_manifests")
        if not isinstance(refs, list) or len(refs) != 1:
            raise ValueError("Missing frame manifest for GIF output")
        for ref in refs:
            upstream = read_manifest(root, ref)
            if upstream.get("step_key") != "generate_frames" or upstream.get("media_pipeline") != MEDIA_PIPELINE:
                raise ValueError("GIF dependency is not an uploaded-image frame manifest")
            # Logical upload IDs/paths may differ for byte-identical inputs in
            # a later committed reuse chain; content, not Task namespace, binds it.
            if (upstream.get("input_asset", {}).get("content_fingerprint")
                    != manifest.get("input_asset", {}).get("content_fingerprint")):
                raise ValueError("GIF input differs from its frame dependency")
            validate_artifact(root, {**ref, "kind": "output", "metadata": upstream})
    elif manifest.get("step_key") == "publish_result":
        ref = manifest.get("encoded_artifact", {})
        upstream = read_manifest(root, ref)
        if upstream.get("step_key") != "encode_package" or upstream.get("result_manifest") != manifest.get("result_manifest"):
            raise ValueError("Published result differs from its encoded dependency")
        validate_artifact(root, {**ref, "kind": "output", "metadata": upstream})
