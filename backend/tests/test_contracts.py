"""Focused contracts: bounded submissions, safe policy merging and snapshots."""

import json
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.config import Settings
from app.domain import (
    MAX_INPUT_BYTES,
    AttemptStatus,
    DomainError,
    ErrorCode,
    MockOptions,
    PolicyOverrides,
    RetryPolicy,
    RuntimePolicy,
    StepSpec,
    StepStatus,
    SubmitTask,
    TaskAccepted,
    TaskStatus,
    build_steps,
    freeze_config,
    is_retryable_error,
)


def test_statuses_match_spec_exactly():
    assert set(TaskStatus) == {
        "QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELING", "CANCELED"
    }
    assert set(StepStatus) == {
        "PENDING", "READY", "RUNNING", "RETRY_WAIT", "SUCCEEDED", "CACHED",
        "FAILED", "BLOCKED", "CANCELED",
    }
    assert set(AttemptStatus) == {
        "RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "LOST", "CANCELED"
    }


def test_error_classification_and_api_error_contract():
    assert set(ErrorCode) == {
        "TRANSIENT_ERROR", "EXECUTOR_EXIT", "INVALID_INPUT", "INVALID_CONFIG",
        "OOM", "ATTEMPT_TIMEOUT", "TASK_DEADLINE", "LEASE_LOST",
        "ARTIFACT_UNAVAILABLE", "CHECKPOINT_INCOMPATIBLE",
    }
    retryable = {"TRANSIENT_ERROR", "EXECUTOR_EXIT", "LEASE_LOST"}
    for code in ErrorCode:
        assert code.retryable == (code in retryable)
        assert is_retryable_error(code.value) == code.retryable
    assert not is_retryable_error("UNKNOWN")

    error = DomainError(ErrorCode.LEASE_LOST, "lease expired")
    assert error.code == "LEASE_LOST"
    assert error.message == str(error) == "lease expired"
    assert error.http_status == 409
    assert DomainError("NOT_FOUND", "missing", http_status=404).http_status == 404


def test_long_running_defaults_do_not_limit_execution():
    assert RuntimePolicy().model_dump() == {
        "heartbeat_interval_seconds": 30,
        "lease_ttl_seconds": 300,
        "coordinator_scan_interval_seconds": 5,
        "attempt_hard_timeout_seconds": None,
        "task_deadline_seconds": None,
        "step_soft_warning_seconds": 21600,
        "task_soft_warning_seconds": 86400,
    }
    assert RetryPolicy().model_dump() == {
        "max_attempts_per_recovery_round": 3,
        "initial_delay_seconds": 10,
        "max_delay_seconds": 300,
        "jitter": True,
    }


LIMIT_FIELDS = (
    "attempt_hard_timeout_seconds", "task_deadline_seconds",
    "step_soft_warning_seconds", "task_soft_warning_seconds",
)
TIMING_FIELDS = (
    "heartbeat_interval_seconds", "lease_ttl_seconds",
    "coordinator_scan_interval_seconds",
)


@pytest.mark.parametrize("disabled", [0, "0", None])
def test_zero_and_null_disable_only_execution_limits(disabled):
    values = dict.fromkeys(LIMIT_FIELDS, disabled)
    assert all(getattr(RuntimePolicy(**values), field) is None for field in LIMIT_FIELDS)
    assert PolicyOverrides(**values).model_dump() == dict.fromkeys(LIMIT_FIELDS)
    for field in TIMING_FIELDS:
        with pytest.raises(ValidationError):
            RuntimePolicy(**{field: disabled})


@pytest.mark.parametrize("invalid", [-1, float("inf"), float("nan"), float("-inf")])
def test_runtime_and_request_limits_reject_negative_and_nonfinite_values(invalid):
    for field in (*TIMING_FIELDS, *LIMIT_FIELDS):
        with pytest.raises(ValidationError):
            RuntimePolicy(**{field: invalid})
    for field in LIMIT_FIELDS:
        with pytest.raises(ValidationError):
            PolicyOverrides(**{field: invalid})


@pytest.mark.parametrize("heartbeat", [300, 301])
def test_heartbeat_must_be_strictly_less_than_lease(heartbeat):
    with pytest.raises(ValidationError, match="less than"):
        RuntimePolicy(heartbeat_interval_seconds=heartbeat)


def test_retry_budget_and_backoff_are_validated():
    for overrides in (
        {"max_attempts_per_recovery_round": 0},
        {"initial_delay_seconds": -1},
        {"max_delay_seconds": float("inf")},
        {"initial_delay_seconds": 2, "max_delay_seconds": 1},
        {"unknown": True},
    ):
        with pytest.raises(ValidationError):
            RetryPolicy(**overrides)


def test_settings_defaults_and_paths_are_explicit():
    settings = Settings.from_env({})
    assert settings.profile == "long-running"
    assert settings.database_url == "postgresql://pipeline:pipeline@127.0.0.1:55432/pipeline"
    assert settings.artifact_root == Path(".runtime/artifacts").resolve()
    assert settings.poll_interval_seconds == 0.2
    assert not settings.enable_test_faults
    assert settings.runtime_policy == RuntimePolicy()
    assert settings.retry_policy == RetryPolicy()


def test_fast_recovery_requires_explicit_selection():
    settings = Settings.from_env({"PIPELINE_PROFILE": "fast-recovery"})
    assert settings.runtime_policy.heartbeat_interval_seconds == 0.25
    assert settings.runtime_policy.lease_ttl_seconds == 1.5
    assert settings.runtime_policy.coordinator_scan_interval_seconds == 0.2
    assert settings.runtime_policy.attempt_hard_timeout_seconds is None
    assert settings.runtime_policy.task_deadline_seconds is None
    assert settings.retry_policy.initial_delay_seconds == 0.1
    assert settings.retry_policy.max_delay_seconds == 0.5
    assert Settings(profile="fast-recovery") == settings
    assert Settings().runtime_policy.heartbeat_interval_seconds == 30


def test_environment_values_override_profile_and_support_template_names():
    settings = Settings.from_env({
        "PIPELINE_PROFILE": "fast-recovery",
        "DATABASE_URL": "postgresql://example/pipeline",
        "ARTIFACT_ROOT": ".runtime/custom-artifacts",
        "WORKER_HEARTBEAT_INTERVAL_SECONDS": "2",
        "WORKER_LEASE_TTL_SECONDS": "12",
        "COORDINATOR_SCAN_INTERVAL_SECONDS": "1",
        "ATTEMPT_HARD_TIMEOUT_SECONDS": "20",
        "TASK_DEADLINE_SECONDS": "0",
        "STEP_SOFT_WARNING_SECONDS": "0",
        "TASK_SOFT_WARNING_SECONDS": "100",
        "MAX_ATTEMPTS_PER_RECOVERY_ROUND": "4",
        "RETRY_INITIAL_DELAY_SECONDS": "1",
        "RETRY_MAX_DELAY_SECONDS": "2",
        "RETRY_JITTER": "false",
        "PIPELINE_POLL_INTERVAL_SECONDS": "0.1",
        "ENABLE_TEST_FAULTS": "true",
        "GPU_PROVIDER": "mock",
        "MOCK_GPU_SLOTS": "0",
    })
    assert settings.database_url == "postgresql://example/pipeline"
    assert settings.artifact_root == Path(".runtime/custom-artifacts").resolve()
    assert settings.runtime_policy.model_dump() == {
        "heartbeat_interval_seconds": 2,
        "lease_ttl_seconds": 12,
        "coordinator_scan_interval_seconds": 1,
        "attempt_hard_timeout_seconds": 20,
        "task_deadline_seconds": None,
        "step_soft_warning_seconds": None,
        "task_soft_warning_seconds": 100,
    }
    assert settings.retry_policy == RetryPolicy(
        max_attempts_per_recovery_round=4, initial_delay_seconds=1,
        max_delay_seconds=2, jitter=False,
    )
    assert settings.poll_interval_seconds == 0.1
    assert settings.enable_test_faults
    assert settings.gpu_provider == "mock"
    assert settings.mock_gpu_slots == 0


def test_from_env_reads_process_environment_only_when_no_mapping_supplied(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://process/pipeline")
    assert Settings.from_env().database_url == "postgresql://process/pipeline"
    assert Settings.from_env({}).database_url == Settings().database_url


@pytest.mark.parametrize("overrides", [
    {"PIPELINE_PROFILE": "unknown"},
    {"WORKER_HEARTBEAT_INTERVAL_SECONDS": "300"},
    {"PIPELINE_POLL_INTERVAL_SECONDS": "0"},
    {"TASK_DEADLINE_SECONDS": "nan"},
    {"GPU_PROVIDER": "cuda"},
])
def test_invalid_environment_does_not_silently_fall_back(overrides):
    with pytest.raises(ValidationError):
        Settings.from_env(overrides)


def test_submission_defaults_are_independent():
    first, second = SubmitTask(), SubmitTask()
    assert first.input == {"kind": "synthetic", "seed": 42}
    assert first.pipeline_id == "animation-dag-v2"
    public_pipeline = SubmitTask.model_json_schema()["properties"]["pipeline_id"]
    assert public_pipeline["enum"] == ["animation-dag-v2", "managed-v1"]
    assert public_pipeline["default"] == "animation-dag-v2"
    assert first.cache_scope == "single-user-demo"
    assert not first.cache_enabled
    assert first.processor_version == "1"
    assert first.mock == MockOptions()
    first.input["seed"] = 0
    first.mock.batch_count = 2
    assert second.input["seed"] == 42
    assert second.mock.batch_count == 4


@pytest.mark.parametrize("invalid_input", [
    {"kind": "upload", "path": "private.dat"},
    {"seed": 42},
    {"kind": "synthetic", "value": float("nan")},
    {"kind": "synthetic", "value": float("inf")},
    {"kind": "synthetic", "value": b"not JSON"},
    {"kind": "synthetic", "value": (1, 2)},
    {"kind": "synthetic", 1: "not a string key"},
])
def test_input_is_synthetic_finite_json_only(invalid_input):
    with pytest.raises(ValidationError):
        SubmitTask(input=invalid_input)


def test_input_size_is_bounded_in_utf8_bytes():
    minimal = {"kind": "synthetic", "payload": ""}
    overhead = len(json.dumps(minimal, separators=(",", ":")).encode("utf-8"))
    at_limit = {**minimal, "payload": "a" * (MAX_INPUT_BYTES - overhead)}
    assert SubmitTask(input=at_limit).input == at_limit
    with pytest.raises(ValidationError, match="bytes"):
        SubmitTask(input={**at_limit, "payload": at_limit["payload"] + "a"})
    with pytest.raises(ValidationError, match="bytes"):
        SubmitTask(input={**minimal, "payload": "界" * (MAX_INPUT_BYTES // 2)})


def test_cyclic_or_excessively_nested_python_input_is_rejected():
    cyclic = {"kind": "synthetic"}
    cyclic["self"] = cyclic
    with pytest.raises(ValidationError, match="nesting"):
        SubmitTask(input=cyclic)


@pytest.mark.parametrize("overrides", [
    {"batch_count": 0}, {"batch_count": 1001},
    {"duration_seconds": -1}, {"duration_seconds": float("inf")},
    {"batch_duration_seconds": float("nan")}, {"fail_first_attempts": -1},
    {"fail_step": "execute-code"},
])
def test_mock_options_are_bounded(overrides):
    with pytest.raises(ValidationError):
        MockOptions(**overrides)


def test_mock_zero_durations_and_batch_upper_bound_are_valid():
    options = MockOptions(duration_seconds=0, batch_duration_seconds=0, batch_count=1000)
    assert options.duration_seconds == options.batch_duration_seconds == 0
    # Fault injection permission belongs to the API, not the data contract.
    assert SubmitTask(mock={"fail_first_attempts": 1}).mock.fail_first_attempts == 1


@pytest.mark.parametrize("overrides", [
    {"pipeline_id": "custom"}, {"cache_scope": "other-user"},
    {"processor_version": ""}, {"processor_version": "x" * 65},
    {"gpu": True}, {"runtime_policy": {"lease_ttl_seconds": 1}},
    {"policy": {"heartbeat_interval_seconds": 1}},
    {"policy": {"lease_ttl_seconds": 1}},
    {"policy": {"coordinator_scan_interval_seconds": 1}},
])
def test_submission_cannot_override_resource_timing_or_escape_fixed_scope(overrides):
    with pytest.raises(ValidationError):
        SubmitTask(**overrides)
    with pytest.raises(ValidationError):
        RuntimePolicy(arbitrary_timeout=1)


def test_step_spec_is_mock_only_and_has_no_gpu_request():
    step = StepSpec(key="mock")
    assert step.processor == "mock"
    assert step.checkpoint_mode == "none"
    with pytest.raises(ValidationError):
        StepSpec(key="mock", gpu=True)
    with pytest.raises(ValidationError):
        StepSpec(key="mock", processor="arbitrary-executable")


def test_smoke_pipeline_uses_one_noncheckpointed_step():
    request = SubmitTask(
        pipeline_id="smoke-v1",
        mock={"duration_seconds": 2, "batch_count": 100, "fail_first_attempts": 1},
        processor_version="v2", cache_enabled=False,
    )
    steps = build_steps(request)
    assert len(steps) == 1
    step = steps[0]
    assert step.key == "mock"
    assert step.depends_on == []
    assert step.parameters == {
        "duration_seconds": 2, "batch_count": 1,
        "batch_duration_seconds": 0, "fail_first_attempts": 1,
    }
    assert step.checkpoint_mode == "none"
    assert step.processor_version == "v2"
    assert not step.cache_enabled


@pytest.mark.parametrize("fail_step", ["normalize_asset", "generate_frames", "encode_package"])
def test_recovery_pipeline_dependencies_checkpoints_and_targeted_faults(fail_step):
    request = SubmitTask(
        pipeline_id="animation-v1", processor_version="2",
        mock={"batch_count": 7, "batch_duration_seconds": 0.25,
              "duration_seconds": 99, "fail_first_attempts": 2, "fail_step": fail_step},
    )
    steps = build_steps(request)
    assert [step.key for step in steps] == ["normalize_asset", "generate_frames", "encode_package"]
    assert [step.depends_on for step in steps] == [[], ["normalize_asset"], ["generate_frames"]]
    assert [step.checkpoint_mode for step in steps] == [
        "none", "committed-batch-manifest", "none"
    ]
    for step in steps:
        assert step.processor == "mock"
        assert step.processor_version == "2"
        assert not step.cache_enabled
        assert step.parameters["fail_first_attempts"] == (2 if step.key == fail_step else 0)
    assert steps[1].parameters["duration_seconds"] == 0
    assert steps[1].parameters["batch_count"] == 7
    assert steps[1].parameters["batch_duration_seconds"] == 0.25
    for step in (steps[0], steps[2]):
        assert step.parameters["duration_seconds"] == 0.05
        assert step.parameters["batch_count"] == 1
        assert step.parameters["batch_duration_seconds"] == 0
    assert all(not step.cache_enabled for step in build_steps(
        SubmitTask(pipeline_id="animation-v1", cache_enabled=False)
    ))


def test_freeze_distinguishes_omitted_limits_from_explicit_null_and_zero():
    settings = Settings(runtime_policy={
        "attempt_hard_timeout_seconds": 60, "task_deadline_seconds": 120,
    })
    omitted = freeze_config(SubmitTask(), settings)["runtime_policy"]
    assert omitted["attempt_hard_timeout_seconds"] == 60
    assert omitted["task_deadline_seconds"] == 120
    assert omitted["step_soft_warning_seconds"] == 21600

    explicit = freeze_config(SubmitTask(policy={
        "attempt_hard_timeout_seconds": None, "task_deadline_seconds": 0,
        "step_soft_warning_seconds": 3, "task_soft_warning_seconds": 0,
    }), settings)["runtime_policy"]
    assert explicit == {
        **omitted, "attempt_hard_timeout_seconds": None, "task_deadline_seconds": None,
        "step_soft_warning_seconds": 3, "task_soft_warning_seconds": None,
    }


def test_freeze_is_a_json_snapshot_with_effective_retry_policy(monkeypatch):
    settings = Settings.from_env({"PIPELINE_PROFILE": "fast-recovery"})
    request = SubmitTask(pipeline_id="animation-v1")
    snapshot = freeze_config(request, settings)
    assert set(snapshot) == {"execution_schema_version", "runtime_policy", "retry_policy", "request", "steps", "resource_policy", "execution_backend"}
    assert snapshot["execution_backend"] == {"kind": "local", "executor_id": None}
    assert snapshot["execution_schema_version"] == 2
    assert snapshot["resource_policy"] == {"gpu_provider": "mock", "mock_gpu_slots": 1, "cpu_max_concurrency": 2}
    assert json.loads(json.dumps(snapshot, allow_nan=False)) == snapshot
    assert snapshot["request"] == request.model_dump(mode="json")
    assert snapshot["steps"] == [step.model_dump(mode="json") for step in build_steps(request)]
    assert snapshot["retry_policy"]["initial_delay_seconds"] == 0.1

    request.input["seed"] = 0
    request.mock.batch_count = 1
    settings.runtime_policy.lease_ttl_seconds = 999
    settings.retry_policy.initial_delay_seconds = 0.4
    monkeypatch.setenv("WORKER_LEASE_TTL_SECONDS", "888")
    assert snapshot["request"]["input"]["seed"] == 42
    assert snapshot["steps"][1]["parameters"]["batch_count"] == 4
    assert snapshot["runtime_policy"]["lease_ttl_seconds"] == 1.5
    assert snapshot["retry_policy"]["initial_delay_seconds"] == 0.1

    override = SubmitTask(retry_policy={
        "max_attempts_per_recovery_round": 1, "initial_delay_seconds": 0,
        "max_delay_seconds": 0, "jitter": False,
    })
    assert freeze_config(override, settings)["retry_policy"] == override.retry_policy.model_dump()


def test_task_accepted_accepts_database_uuid_and_serializes_enum():
    task_id = uuid4()
    assert TaskAccepted(id=task_id, status=TaskStatus.QUEUED).model_dump(mode="json") == {
        "id": str(task_id), "status": "QUEUED"
    }
