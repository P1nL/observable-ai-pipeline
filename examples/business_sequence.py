"""Small business-side adapter: register next work from durable history, then close.

No central imports, subprocess computation, lease calls or schedule duplication.
Run again with the same business-id after this client stops; the Worker continues
already authorized work and the adapter reads history before adding another step.
"""
import argparse
import json
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class BusinessClient:
    def __init__(self, base):
        self.base = base.rstrip("/")

    def request(self, method, path, body=None, key=None):
        headers = {"Content-Type": "application/json"}
        if key:
            headers["Idempotency-Key"] = key
        encoded = json.dumps(body).encode() if body is not None else None
        for retry in range(6):
            try:
                with urlopen(Request(self.base + path, data=encoded, headers=headers, method=method), timeout=5) as response:
                    return json.load(response)
            except HTTPError as error:
                if error.code < 500:
                    raise RuntimeError(error.read().decode("utf-8")) from error
            except (URLError, TimeoutError):
                pass
            # Ambiguous writes always reuse the same request and idempotency key.
            time.sleep(min(.25 * 2**retry, 2))
        raise RuntimeError("Service unavailable; rerun with the same business-id")

    def history(self, task_id):
        return self.request("GET", f"/api/v1/tasks/{task_id}")

    def register(self, task_id, ordinal, previous, version):
        return self.request("POST", f"/api/v1/tasks/{task_id}/steps", {
            "key": f"business-{ordinal}", "after_step": previous,
            "processor": "mock-step", "processor_version": version,
            "input": {"kind": "synthetic", "seed": ordinal},
            "parameters": {"batch_count": min(ordinal, 3), "batch_duration_seconds": .1},
        }, f"step-{ordinal}")

    def finish(self, task_id, last):
        return self.request("POST", f"/api/v1/tasks/{task_id}/finish", {"last_step": last}, "business-finish")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--business-id", required=True, help="Stable create idempotency key; reuse after client interruption")
    parser.add_argument("--steps", type=int, default=3, help="Example business stopping rule; not sent as a total to the center")
    parser.add_argument("--backend", choices=("local", "external"), default="local")
    parser.add_argument("--processor-version", default="1")
    args = parser.parse_args()
    if not 1 <= args.steps <= 100:
        parser.error("Example steps must be 1..100")
    client = BusinessClient(args.url)
    accepted = client.request("POST", "/api/v1/tasks", {
        "pipeline_id": "managed-v1", "execution_backend": args.backend,
        "cache_enabled": False, "bypass_cache": True,
    }, args.business_id)
    task_id = accepted["id"]
    print(json.dumps({"task_id": task_id}), flush=True)
    while True:
        task = client.history(task_id)
        steps = task["steps"]
        if task["status"] == "SUCCEEDED":
            print(json.dumps({"task_id": task_id, "status": task["status"], "registered_steps": len(steps), "output_refs": task["output_refs"]}), flush=True)
            return
        if task["status"] in ("FAILED", "CANCELING", "CANCELED"):
            raise RuntimeError(f"Task is {task['status']}; inspect history/resume explicitly before continuing")
        if task["registration_closed"] or any(s["status"] != "SUCCEEDED" for s in steps):
            time.sleep(.2)
            continue
        # The decision belongs here, not to the execution manager. A real business
        # can inspect these committed output refs and choose a different next step.
        tail = steps[-1]["step_key"] if steps else None
        if len(steps) >= args.steps:
            client.finish(task_id, tail)
        else:
            client.register(task_id, len(steps) + 1, tail, args.processor_version)


if __name__ == "__main__":
    main()
