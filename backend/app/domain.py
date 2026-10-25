"""Validated submission and immutable execution-snapshot contracts."""

from __future__ import annotations

import json
import math
from enum import StrEnum
from typing import TYPE_CHECKING, Annotated, Literal, Self
from uuid import UUID

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_validator,
    model_validator,
)

if TYPE_CHECKING:
    from .config import Settings


class TaskStatus(StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELING = "CANCELING"
    CANCELED = "CANCELED"


class StepStatus(StrEnum):
    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    CACHED = "CACHED"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"
    CANCELED = "CANCELED"


class AttemptStatus(StrEnum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    LOST = "LOST"
    CANCELED = "CANCELED"


class MonitorAcknowledgement(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: AttemptStatus
    resource_cleanup_state: Literal["CPU_PROCESS_UNCONFIRMED", "QUARANTINED"]
    stop_requested_at: AwareDatetime | None


class ErrorCode(StrEnum):
    TRANSIENT_ERROR = "TRANSIENT_ERROR"
    EXECUTOR_EXIT = "EXECUTOR_EXIT"
    INVALID_INPUT = "INVALID_INPUT"
    INVALID_CONFIG = "INVALID_CONFIG"
    OOM = "OOM"
    ATTEMPT_TIMEOUT = "ATTEMPT_TIMEOUT"
    TASK_DEADLINE = "TASK_DEADLINE"
    LEASE_LOST = "LEASE_LOST"
    ARTIFACT_UNAVAILABLE = "ARTIFACT_UNAVAILABLE"
    CHECKPOINT_INCOMPATIBLE = "CHECKPOINT_INCOMPATIBLE"

    @property
    def retryable(self) -> bool:
        return self in {
            ErrorCode.TRANSIENT_ERROR,
            ErrorCode.EXECUTOR_EXIT,
            ErrorCode.LEASE_LOST,
        }


def is_retryable_error(code: ErrorCode | str) -> bool:
    """Classify known execution failures; unknown errors are not auto-retried."""
    try:
        return ErrorCode(code).retryable
    except ValueError:
        return False


class DomainError(Exception):
    def __init__(
        self, code: ErrorCode | str, message: str, http_status: int = 409
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


PositiveSeconds = Annotated[float, Field(gt=0, allow_inf_nan=False)]
NonNegativeSeconds = Annotated[float, Field(ge=0, allow_inf_nan=False)]


def _zero_disables(value: float | None) -> float | None:
    return None if value == 0 else value


OptionalSeconds = Annotated[
    NonNegativeSeconds | None, AfterValidator(_zero_disables)
]


class RuntimePolicy(_Contract):
    heartbeat_interval_seconds: PositiveSeconds = 30
    lease_ttl_seconds: PositiveSeconds = 300
    coordinator_scan_interval_seconds: PositiveSeconds = 5
    attempt_hard_timeout_seconds: OptionalSeconds = None
    task_deadline_seconds: OptionalSeconds = None
    step_soft_warning_seconds: OptionalSeconds = 21600
    task_soft_warning_seconds: OptionalSeconds = 86400

    @model_validator(mode="after")
    def heartbeat_fits_lease(self) -> Self:
        if self.heartbeat_interval_seconds >= self.lease_ttl_seconds:
            raise ValueError("heartbeat_interval_seconds must be less than lease_ttl_seconds")
        return self


class RetryPolicy(_Contract):
    max_attempts_per_recovery_round: int = Field(default=3, ge=1)
    initial_delay_seconds: NonNegativeSeconds = 10
    max_delay_seconds: NonNegativeSeconds = 300
    jitter: bool = True

    @model_validator(mode="after")
    def delay_fits_cap(self) -> Self:
        if self.initial_delay_seconds > self.max_delay_seconds:
            raise ValueError("initial_delay_seconds must not exceed max_delay_seconds")
        return self


class PolicyOverrides(_Contract):
    """Only execution limits/warnings may be changed by a submission."""

    attempt_hard_timeout_seconds: OptionalSeconds = None
    task_deadline_seconds: OptionalSeconds = None
    step_soft_warning_seconds: OptionalSeconds = None
    task_soft_warning_seconds: OptionalSeconds = None


class MockOptions(_Contract):
    duration_seconds: NonNegativeSeconds = 1.0
    batch_count: int = Field(default=4, gt=0, le=1000)
    frames_per_batch: int = Field(default=10, gt=0, le=1000)
    batch_duration_seconds: NonNegativeSeconds = 0.5
    branch_duration_seconds: NonNegativeSeconds = 0.5
    fail_first_attempts: int = Field(default=0, ge=0)
    fail_step: Literal["normalize_asset", "segment_subject", "analyze_motion", "generate_frames", "encode_package", "publish_result"] = "generate_frames"


MAX_INPUT_BYTES = 64 * 1024
ProcessorVersion = Annotated[str, Field(min_length=1, max_length=64)]


class SubmitTask(_Contract):
    # Legacy values are decoded only for stored-key replay and internal tests;
    # Repository.submit enforces admission before creating any new task.
    pipeline_id: Literal["smoke-v1", "animation-v1", "animation-dag-v2", "managed-v1"] = Field(
        default="animation-dag-v2",
        description="Use animation-dag-v2 for the example or managed-v1 for business-registered sequential steps. Retired versions are replay-only.",
        json_schema_extra={"enum": ["animation-dag-v2", "managed-v1"]},
    )
    input: dict[str, JsonValue] = Field(
        default_factory=lambda: {"kind": "synthetic", "seed": 42}
    )
    mock: MockOptions = Field(default_factory=MockOptions)
    policy: PolicyOverrides = Field(default_factory=PolicyOverrides)
    retry_policy: RetryPolicy | None = None
    cache_enabled: bool = False
    bypass_cache: bool = False
    generation_tag: str = Field(default="", max_length=64)
    cache_scope: Literal["single-user-demo"] = "single-user-demo"
    processor_version: ProcessorVersion = "1"
    execution_backend: Literal["local", "external"] = "local"

    @field_validator("input", mode="before")
    @classmethod
    def bounded_synthetic_input(cls, value: object) -> object:
        if not isinstance(value, dict) or value.get("kind") not in ("synthetic", "uploaded"):
            raise ValueError("input.kind must be synthetic or uploaded")
        if value.get("kind") == "uploaded":
            if set(value) - {"kind", "input_id", "seed"}:
                raise ValueError("uploaded input accepts only input_id and seed, never filesystem paths")
            try:
                value = {**value, "input_id": str(UUID(str(value["input_id"])))}
            except (ValueError, KeyError, TypeError) as error:
                raise ValueError("uploaded input requires a valid input_id") from error
            seed = value.get("seed", 42)
            if type(seed) is not int or not 0 <= seed <= 4294967295:
                raise ValueError("uploaded seed must be a nonnegative 32-bit integer")
            value["seed"] = seed

        # Do not let Python-only values (or non-string object keys) be silently
        # coerced into JSON. The executor accepts data, never paths or code.
        pending: list[tuple[object, int]] = [(value, 0)]
        while pending:
            item, depth = pending.pop()
            if depth > 64:
                raise ValueError("input JSON nesting must not exceed 64 levels")
            if isinstance(item, dict):
                if any(not isinstance(key, str) for key in item):
                    raise ValueError("input JSON object keys must be strings")
                pending.extend((child, depth + 1) for child in item.values())
            elif isinstance(item, list):
                pending.extend((child, depth + 1) for child in item)
            elif item is None or isinstance(item, (str, bool, int)):
                continue
            elif isinstance(item, float) and math.isfinite(item):
                continue
            else:
                raise ValueError("input must contain only finite JSON values")
        try:
            encoded = json.dumps(
                value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
            ).encode("utf-8")
        except (ValueError, TypeError, RecursionError, UnicodeError) as exc:
            raise ValueError("input must be valid UTF-8 JSON") from exc
        if len(encoded) > MAX_INPUT_BYTES:
            raise ValueError(f"input JSON must not exceed {MAX_INPUT_BYTES} bytes")
        return value

    @model_validator(mode="after")
    def bounded_uploaded_render(self):
        if self.pipeline_id == "managed-v1" and self.input.get("kind") != "synthetic":
            raise ValueError("managed-v1 currently supports synthetic JSON input")
        if self.input.get("kind") == "uploaded" and self.mock.batch_count * self.mock.frames_per_batch > 120:
            raise ValueError("uploaded image preview is limited to 120 frames")
        return self


class StepSpec(_Contract):
    key: str = Field(min_length=1, max_length=64)
    processor: Literal["mock", "mock-step"] = "mock"
    processor_version: ProcessorVersion = "1"
    depends_on: list[str] = Field(default_factory=list)
    parameters: dict[str, JsonValue] = Field(default_factory=dict)
    cache_enabled: bool = False
    checkpoint_mode: Literal["none", "committed-batch-manifest"] = "none"
    resource_kind: Literal["cpu", "mock-gpu"] = "cpu"


BusinessKey = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")]


class ManagedParameters(_Contract):
    duration_seconds: NonNegativeSeconds = .05
    batch_count: int = Field(default=1, ge=1, le=1000)
    frames_per_batch: int = Field(default=1, ge=1, le=1000)
    batch_duration_seconds: NonNegativeSeconds = .1
    fail_first_attempts: int = Field(default=0, ge=0)


class RegisterStep(_Contract):
    key: BusinessKey
    after_step: BusinessKey | None = None
    processor: Literal["mock-step"] = "mock-step"
    processor_version: Literal["1", "independent-mock-v1"] = "1"
    input: dict[str, JsonValue] | None = None
    parameters: ManagedParameters = Field(default_factory=ManagedParameters)
    resource_kind: Literal["cpu", "mock-gpu"] = "cpu"

    @field_validator("input", mode="before")
    @classmethod
    def synthetic_step_input(cls, value):
        if value is None:
            return None
        if not isinstance(value, dict) or value.get("kind") != "synthetic":
            raise ValueError("Registered mock-step input must be synthetic JSON")
        return SubmitTask(input=value).input


class FinishTask(_Contract):
    last_step: BusinessKey | None = None


def validate_dependencies(steps: list[StepSpec]) -> None:
    remaining = {step.key: set(step.depends_on) for step in steps}
    if not steps or len(remaining) != len(steps):
        raise ValueError("Step keys must be nonempty and unique")
    if any(dependency not in remaining for needs in remaining.values() for dependency in needs):
        raise ValueError("Step dependency does not exist")
    completed = set()
    while remaining:
        ready = {key for key, needs in remaining.items() if needs <= completed}
        if not ready:
            raise ValueError("Step graph contains a cycle")
        completed.update(ready)
        remaining = {key: needs for key, needs in remaining.items() if key not in ready}


def build_steps(request: SubmitTask) -> list[StepSpec]:
    """Build approved, versioned graphs; never execute arbitrary user code."""
    common = {
        "processor_version": request.processor_version,
        "cache_enabled": request.cache_enabled,
    }
    if request.pipeline_id == "managed-v1":
        return []  # Business registrations are separately frozen rows, not a graph preset.
    if request.pipeline_id == "smoke-v1":
        return [
            StepSpec(
                key="mock",
                parameters={
                    "duration_seconds": request.mock.duration_seconds,
                    "batch_count": 1,
                    "batch_duration_seconds": 0,
                    "fail_first_attempts": request.mock.fail_first_attempts,
                },
                **common,
            )
        ]

    steps: list[StepSpec] = []
    graph = (
        ("normalize_asset", []),
        ("generate_frames", ["normalize_asset"]),
        ("encode_package", ["generate_frames"]),
    )
    if request.pipeline_id == "animation-dag-v2":
        graph = (("normalize_asset", []), ("segment_subject", ["normalize_asset"]),
                 ("analyze_motion", ["normalize_asset"]), ("generate_frames", ["segment_subject", "analyze_motion"]),
                 ("encode_package", ["generate_frames"]), ("publish_result", ["encode_package"]))
    for key, depends_on in graph:
        generate = key == "generate_frames"
        steps.append(
            StepSpec(
                key=key,
                depends_on=depends_on,
                parameters={
                    "duration_seconds": 0 if generate else request.mock.branch_duration_seconds if key in ("segment_subject", "analyze_motion") else 0.05,
                    "batch_count": request.mock.batch_count if generate else 1,
                    "batch_duration_seconds": (
                        request.mock.batch_duration_seconds if generate else 0
                    ),
                    **({"frames_per_batch": request.mock.frames_per_batch} if generate else {}),
                    "fail_first_attempts": (
                        request.mock.fail_first_attempts
                        if key == request.mock.fail_step
                        else 0
                    ),
                },
                checkpoint_mode="committed-batch-manifest" if generate else "none",
                resource_kind="mock-gpu" if generate else "cpu",
                **{**common, "cache_enabled": request.cache_enabled and key != "publish_result"},
            )
        )
    validate_dependencies(steps)
    return steps


def freeze_config(request: SubmitTask, settings: Settings) -> dict:
    """Make an independent JSON snapshot; never re-read process configuration."""
    policy = settings.runtime_policy.model_dump(mode="json")
    policy.update(request.policy.model_dump(mode="json", exclude_unset=True))
    return {
        "execution_schema_version": 2,
        "runtime_policy": RuntimePolicy.model_validate(policy).model_dump(mode="json"),
        "retry_policy": (request.retry_policy or settings.retry_policy).model_dump(
            mode="json"
        ),
        "request": request.model_dump(mode="json"),
        "steps": [step.model_dump(mode="json") for step in build_steps(request)],
        "resource_policy": {"gpu_provider": settings.gpu_provider, "mock_gpu_slots": settings.mock_gpu_slots, "cpu_max_concurrency": settings.cpu_max_concurrency},
        "execution_backend": ({"kind": "external", "node_ids": [n.node_id for n in settings.execution_nodes], "storage_id": settings.artifact_storage_id}
            if request.execution_backend == "external" and settings.execution_nodes else
            {"kind": request.execution_backend, "executor_id": settings.external_executor_id if request.execution_backend == "external" else None}),
    }


class TaskAccepted(BaseModel):
    id: str
    status: TaskStatus = TaskStatus.QUEUED

    @field_validator("id", mode="before")
    @classmethod
    def stringify_uuid(cls, value: object) -> object:
        return str(value) if isinstance(value, UUID) else value
