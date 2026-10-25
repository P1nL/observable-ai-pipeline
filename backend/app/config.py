"""Explicit environment loading; fast recovery is always opt-in."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator
from urllib.parse import urlsplit
import json
import re

from .domain import PositiveSeconds, RetryPolicy, RuntimePolicy


class ExecutionNode(BaseModel):
    """One explicitly configured target, not discovery or a runnable plugin."""
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    node_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
    executor_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
    url: str
    secret_env: str = Field(pattern=r"^[A-Z][A-Z0-9_]{0,127}$")
    accepting: bool = True
    cpu_capacity: int = Field(default=2, ge=0, le=64)
    mock_gpu_slots: list[str] = Field(default_factory=lambda: ["gpu-0"], max_length=64)
    processors: dict[str, list[str]] = Field(default_factory=lambda: {"mock": ["1"], "mock-step": ["1"]})
    storage_id: str = Field(default="pipeline-artifacts", pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
    ca_file: Path | None = Field(default=None, exclude=True)

    @model_validator(mode="after")
    def controlled_target(self):
        address = urlsplit(self.url)
        if (not address.hostname or address.username or address.password or address.path not in ("", "/")
                or address.query or address.fragment or address.scheme not in {"http", "https"}):
            raise ValueError("Execution node requires an explicit HTTP(S) origin")
        _ = address.port
        if address.scheme == "http" and address.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("Non-loopback execution nodes require verified HTTPS")
        if self.node_id == "legacy":
            raise ValueError("legacy is reserved for historical execution state")
        if len(set(self.mock_gpu_slots)) != len(self.mock_gpu_slots) or any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", v) or v in {"cpu", "inspection"} for v in self.mock_gpu_slots):
            raise ValueError("Mock devices must be distinct safe names, not cpu/inspection")
        if not self.processors or any(k not in {"mock", "mock-step"} or not versions or any(not isinstance(v, str) or not 1 <= len(v) <= 64 for v in versions) for k, versions in self.processors.items()):
            raise ValueError("Only preinstalled mock/mock-step processor versions may be configured")
        if self.ca_file is not None and not self.ca_file.is_file():
            raise ValueError("Configured CA file does not exist")
        self.url = self.url.rstrip("/")
        return self

    def route(self):
        return {"node_id": self.node_id, "executor_id": self.executor_id, "url": self.url, "storage_id": self.storage_id}


class Settings(BaseModel):
    model_config = ConfigDict(
        extra="forbid", allow_inf_nan=False, validate_default=True
    )

    database_url: str = Field(
        default="postgresql://pipeline:pipeline@127.0.0.1:55432/pipeline", min_length=1
    )
    artifact_root: Path = Path(".runtime/artifacts")
    profile: Literal["long-running", "fast-recovery"] = "long-running"
    runtime_policy: RuntimePolicy = Field(default_factory=RuntimePolicy)
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    poll_interval_seconds: PositiveSeconds = 0.2
    enable_test_faults: bool = False
    gpu_provider: Literal["mock"] = "mock"
    mock_gpu_slots: int = Field(default=1, ge=0, le=1)
    cpu_max_concurrency: int = Field(default=2, ge=1, le=4)
    external_executor_url: str = ""
    external_executor_id: str = Field(default="executor-one", pattern=r"^[A-Za-z0-9_-]{1,64}$")
    external_executor_secret: SecretStr = SecretStr("")
    external_rpc_timeout_seconds: float = Field(default=.5, gt=0, le=2)
    execution_nodes: list[ExecutionNode] = Field(default_factory=list, max_length=32)
    node_secrets: dict[str, SecretStr] = Field(default_factory=dict, exclude=True, repr=False)
    artifact_storage_id: str = Field(default="pipeline-artifacts", pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
    node_probe_interval_seconds: PositiveSeconds = 5

    @model_validator(mode="after")
    def nodes_have_distinct_credentials(self):
        if len({n.node_id for n in self.execution_nodes}) != len(self.execution_nodes) or len({n.executor_id for n in self.execution_nodes}) != len(self.execution_nodes):
            raise ValueError("Node and executor identities must be unique")
        secrets = [self.node_secrets.get(n.node_id, SecretStr("")).get_secret_value() for n in self.execution_nodes]
        if any(len(secret) < 32 for secret in secrets) or len(set(secrets)) != len(secrets):
            raise ValueError("Each node requires its own distinct secret of at least 32 characters")
        return self

    @model_validator(mode="after")
    def external_endpoint_is_explicit_local_service(self):
        if self.external_executor_url:
            url = urlsplit(self.external_executor_url)
            if url.scheme != "http" or url.hostname not in {"127.0.0.1", "localhost", "::1"} or url.username or url.password or url.path not in {"", "/"} or url.query or url.fragment:
                raise ValueError("First external backend requires a configured loopback HTTP origin")
            if len(self.external_executor_secret.get_secret_value()) < 32:
                raise ValueError("External executor requires a secret of at least 32 characters")
        return self

    @model_validator(mode="before")
    @classmethod
    def apply_explicit_profile(cls, value: object) -> object:
        if not isinstance(value, dict) or value.get("profile") != "fast-recovery":
            return value
        values = dict(value)
        defaults = {
            "runtime_policy": {
                "heartbeat_interval_seconds": 0.25,
                "lease_ttl_seconds": 1.5,
                "coordinator_scan_interval_seconds": 0.2,
            },
            "retry_policy": {
                "initial_delay_seconds": 0.1,
                "max_delay_seconds": 0.5,
            },
        }
        for field, profile_defaults in defaults.items():
            explicit = values.get(field, {})
            if isinstance(explicit, dict):
                values[field] = {**profile_defaults, **explicit}
        return values

    @field_validator("artifact_root")
    @classmethod
    def resolve_artifact_root(cls, value: Path) -> Path:
        return value.expanduser().resolve()

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        """Read only the supplied mapping or os.environ, never a .env file."""
        source = os.environ if env is None else env
        values: dict = {}
        for variable, field in {
            "DATABASE_URL": "database_url",
            "ARTIFACT_ROOT": "artifact_root",
            "PIPELINE_PROFILE": "profile",
            "PIPELINE_POLL_INTERVAL_SECONDS": "poll_interval_seconds",
            "ENABLE_TEST_FAULTS": "enable_test_faults",
            "GPU_PROVIDER": "gpu_provider",
            "MOCK_GPU_SLOTS": "mock_gpu_slots",
            "CPU_MAX_CONCURRENCY": "cpu_max_concurrency",
            "EXTERNAL_EXECUTOR_URL": "external_executor_url",
            "EXTERNAL_EXECUTOR_ID": "external_executor_id",
            "EXTERNAL_EXECUTOR_SECRET": "external_executor_secret",
            "EXTERNAL_RPC_TIMEOUT_SECONDS": "external_rpc_timeout_seconds",
            "ARTIFACT_STORAGE_ID": "artifact_storage_id",
            "NODE_PROBE_INTERVAL_SECONDS": "node_probe_interval_seconds",
        }.items():
            if variable in source:
                values[field] = source[variable]

        if "EXTERNAL_EXECUTOR_SECRET" in source:
            values["external_executor_secret"] = SecretStr(source["EXTERNAL_EXECUTOR_SECRET"])
        if "EXECUTION_NODES_JSON" in source:
            nodes = json.loads(source["EXECUTION_NODES_JSON"])
            if not isinstance(nodes, list):
                raise ValueError("EXECUTION_NODES_JSON must be an array")
            # Resolve credentials from the supplied mapping, never a global fallback.
            parsed = [ExecutionNode.model_validate(node) for node in nodes]
            values["execution_nodes"] = parsed
            values["node_secrets"] = {node.node_id: SecretStr(source.get(node.secret_env, "")) for node in parsed}

        for field, variable_fields in {
            "runtime_policy": {
                "WORKER_HEARTBEAT_INTERVAL_SECONDS": "heartbeat_interval_seconds",
                "WORKER_LEASE_TTL_SECONDS": "lease_ttl_seconds",
                "COORDINATOR_SCAN_INTERVAL_SECONDS": "coordinator_scan_interval_seconds",
                "ATTEMPT_HARD_TIMEOUT_SECONDS": "attempt_hard_timeout_seconds",
                "TASK_DEADLINE_SECONDS": "task_deadline_seconds",
                "STEP_SOFT_WARNING_SECONDS": "step_soft_warning_seconds",
                "TASK_SOFT_WARNING_SECONDS": "task_soft_warning_seconds",
            },
            "retry_policy": {
                "MAX_ATTEMPTS_PER_RECOVERY_ROUND": "max_attempts_per_recovery_round",
                "RETRY_INITIAL_DELAY_SECONDS": "initial_delay_seconds",
                "RETRY_MAX_DELAY_SECONDS": "max_delay_seconds",
                "RETRY_JITTER": "jitter",
            },
        }.items():
            explicit = {
                name: source[variable]
                for variable, name in variable_fields.items()
                if variable in source
            }
            if explicit:
                values[field] = explicit
        return cls.model_validate(values)
