"""Local executor identity and bounded, PID-reuse-safe orphan cleanup."""

from __future__ import annotations

import math
from pathlib import Path
import socket
import sys

import psutil


def current_host() -> str:
    return socket.gethostname()


def identify_executor(pid: int, claim: dict) -> dict:
    """Bind a live Python executor to this immutable Attempt/generation."""
    try:
        if type(pid) is not int or pid <= 0:
            raise ValueError("Invalid executor PID")
        attempt_id = claim["attempt_id"]
        generation = claim["generation"]
        if not isinstance(attempt_id, str) or not attempt_id or type(generation) is not int or generation < 1:
            raise ValueError("Invalid executor claim identity")
        process = psutil.Process(pid)
        created_at = process.create_time()
        command = process.cmdline()
        if command[1:3] != ["-m", "app.executor"]:
            raise ValueError("Process is not an executor module")
        for flag, expected in (("--attempt-id", attempt_id), ("--generation", str(generation))):
            if command.count(flag) != 1:
                raise ValueError("Missing or ambiguous executor identity flag")
            index = command.index(flag)
            if index + 1 >= len(command) or command[index + 1] != expected:
                raise ValueError("Executor claim identity does not match")
        # Windows may expose either our venv redirector or the base interpreter.
        # Both paths are known locally; argv[0] alone is not executable proof.
        if Path(process.exe()).resolve() not in {
            Path(sys.executable).resolve(), Path(psutil.Process().exe()).resolve(),
        }:
            raise ValueError("Executor Python executable does not match")
        if not process.is_running() or process.status() == psutil.STATUS_ZOMBIE:
            raise ValueError("Executor already exited")
        host = current_host()
        if not host:
            raise ValueError("Unknown executor host")
        return {"pid": pid, "created_at": created_at, "host": host}
    except (psutil.Error, OSError, KeyError, TypeError) as error:
        raise ValueError("Executor identity could not be verified") from error


def _inspect_executor(pid, created_at, host) -> tuple[str, psutil.Process | None]:
    try:
        if (
            type(pid) is not int or pid <= 0
            or type(created_at) not in (int, float)
            or not math.isfinite(created_at) or created_at <= 0
            or not isinstance(host, str) or not host or host != current_host()
        ):
            return "UNKNOWN", None
        process = psutil.Process(pid)
        if process.create_time() != created_at or not process.is_running():
            return "EXITED", None
        if process.status() == psutil.STATUS_ZOMBIE:
            return "EXITED", None
        return "RUNNING", process
    except (psutil.NoSuchProcess, psutil.ZombieProcess):
        return "EXITED", None
    except (psutil.Error, OSError, ValueError):
        return "UNKNOWN", None


def executor_state(pid: int | None, created_at: float | None, host: str | None) -> str:
    """Report the original process, never a reused PID or an unknown host."""
    state, _ = _inspect_executor(pid, created_at, host)
    return state


def stop_executor(pid, created_at, host, timeout: float = .2) -> bool:
    """Stop only the matching local process; uncertainty is not cleanup."""
    state, process = _inspect_executor(pid, created_at, host)
    if state != "RUNNING":
        return state == "EXITED"
    try:
        # Keep the same Process instance: psutil checks PID reuse again before
        # terminate/kill instead of signaling whichever process owns the PID now.
        process.terminate()
        try:
            process.wait(timeout=timeout)
        except psutil.TimeoutExpired:
            process.kill()
            process.wait(timeout=timeout)
    except (psutil.NoSuchProcess, psutil.ZombieProcess):
        return True
    except (psutil.Error, OSError):
        pass
    return executor_state(pid, created_at, host) == "EXITED"
