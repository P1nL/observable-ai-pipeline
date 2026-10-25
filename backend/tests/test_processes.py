from contextlib import contextmanager
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

import psutil
import pytest

from app import processes


@contextmanager
def child_process(arguments):
    process = subprocess.Popen(
        [sys.executable, *arguments],
        cwd=Path(__file__).resolve().parents[1],
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        text=True,
    )
    try:
        yield process
    finally:
        # Popen owns this child, independently of the helpers under test.
        if process.poll() is None:
            process.kill()
        process.wait(timeout=3)
        process.stdin.close()


@pytest.fixture
def executor():
    claim = {"attempt_id": str(uuid4()), "generation": 7}
    with child_process([
        "-m", "app.executor", "--attempt-id", claim["attempt_id"],
        "--generation", str(claim["generation"]),
    ]) as process:
        yield process, claim


def test_real_executor_identity_and_bounded_stop(executor):
    process, claim = executor
    identity = processes.identify_executor(process.pid, claim)
    assert identity == {
        "pid": process.pid,
        "created_at": psutil.Process(process.pid).create_time(),
        "host": processes.current_host(),
    }
    assert processes.executor_state(**identity) == "RUNNING"
    assert processes.stop_executor(**identity)
    process.wait(timeout=3)
    assert processes.executor_state(**identity) == "EXITED"
    assert processes.stop_executor(**identity)


@pytest.mark.parametrize("mismatch", [{"attempt_id": "different-attempt"}, {"generation": 8}])
def test_identification_rejects_wrong_attempt_or_generation(executor, mismatch):
    process, claim = executor
    with pytest.raises(ValueError):
        processes.identify_executor(process.pid, {**claim, **mismatch})
    assert process.poll() is None


def test_python_command_containing_executor_arguments_is_not_executor():
    claim = {"attempt_id": str(uuid4()), "generation": 1}
    with child_process([
        "-c", "import sys; sys.stdin.read()", "-m", "app.executor",
        "--attempt-id", claim["attempt_id"], "--generation", "1",
    ]) as process:
        with pytest.raises(ValueError):
            processes.identify_executor(process.pid, claim)
        assert process.poll() is None


def test_reused_pid_means_original_exited_without_stopping_successor(executor):
    process, claim = executor
    identity = processes.identify_executor(process.pid, claim)
    original = {**identity, "created_at": identity["created_at"] - 1}
    assert processes.executor_state(**original) == "EXITED"
    assert processes.stop_executor(**original)
    assert process.poll() is None
    assert processes.executor_state(**identity) == "RUNNING"


@pytest.mark.parametrize("unknown", [
    {"host": None}, {"host": "not-this-host"}, {"pid": None}, {"created_at": None},
])
def test_unknown_identity_never_stops_live_process(executor, unknown):
    process, claim = executor
    identity = processes.identify_executor(process.pid, claim)
    unknown_identity = {**identity, **unknown}
    assert processes.executor_state(**unknown_identity) == "UNKNOWN"
    assert not processes.stop_executor(**unknown_identity)
    assert process.poll() is None


def test_access_denied_is_unknown_and_not_confirmed_cleanup(executor, monkeypatch):
    process, claim = executor
    identity = processes.identify_executor(process.pid, claim)

    def denied(*args, **kwargs):
        raise psutil.AccessDenied(process.pid)

    with monkeypatch.context() as patch:
        patch.setattr(processes.psutil, "Process", denied)
        with pytest.raises(ValueError):
            processes.identify_executor(process.pid, claim)
        assert processes.executor_state(**identity) == "UNKNOWN"
        assert not processes.stop_executor(**identity)
    assert process.poll() is None
