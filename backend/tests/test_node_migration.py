"""Upgrade actual pre-node tables without rewriting old ownership or files."""
import json
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

from psycopg import sql
from psycopg.types.json import Jsonb
import pytest

from app.artifacts import write_json_artifact
from app.db import connect, migrate
from app.domain import SubmitTask, freeze_config
from app.repository import Repository
from conftest import ROOT

pytestmark = pytest.mark.integration


def test_node_migration_preserves_legacy_quarantine_history_and_artifact(engine):
    schema = "old_nodes_" + uuid4().hex
    url = urlsplit(engine.settings.database_url)
    query = dict(parse_qsl(url.query)); query["options"] = "-csearch_path=" + schema
    settings = engine.settings.model_copy(update={"database_url": urlunsplit(url._replace(query=urlencode(query)))})
    with connect(engine.settings) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        with connect(settings) as conn:
            conn.execute("CREATE TABLE schema_migrations(version text PRIMARY KEY,applied_at timestamptz NOT NULL DEFAULT clock_timestamp())")
            for path in sorted((ROOT / "backend" / "migrations").glob("*.sql")):
                if path.name >= "007_":
                    continue
                conn.execute(path.read_text(encoding="utf-8"))
                conn.execute("INSERT INTO schema_migrations(version) VALUES (%s)", (path.name,))
            task_id, success, pending, last, producer, lost = [str(uuid4()) for _ in range(6)]
            snapshot = freeze_config(SubmitTask(pipeline_id="animation-v1"), settings)
            snapshot.pop("execution_backend"); snapshot["request"].pop("execution_backend")
            conn.execute("INSERT INTO tasks(id,request_fingerprint,pipeline_id,config_snapshot,status) VALUES (%s,'legacy','animation-v1',%s,'RUNNING')", (task_id, Jsonb(snapshot)))
            for identity, spec, status in zip((success, pending, last), snapshot["steps"], ("SUCCEEDED", "RETRY_WAIT", "PENDING")):
                conn.execute("INSERT INTO steps(id,task_id,step_key,spec,depends_on,status,execution_generation) VALUES (%s,%s,%s,%s,%s,%s,1)",
                    (identity, task_id, spec["key"], Jsonb(spec), Jsonb(spec["depends_on"]), status))
            conn.execute("""INSERT INTO step_attempts(id,step_id,attempt_number,recovery_round,worker_id,fencing_token,status,lease_expires_at,process_exit_confirmed_at)
                VALUES (%s,%s,1,0,'old',1,'SUCCEEDED',clock_timestamp()-interval '5 second',clock_timestamp())""", (producer, success))
            conn.execute("""INSERT INTO step_attempts(id,step_id,attempt_number,recovery_round,worker_id,fencing_token,status,lease_expires_at,resource_slot_id,resource_cleanup_state)
                VALUES (%s,%s,1,0,'old',1,'LOST',clock_timestamp()-interval '5 second',1,'QUARANTINED')""", (lost, pending))
            conn.execute("UPDATE resource_slots SET state='QUARANTINED',owner_attempt_id=%s,owner_generation=1 WHERE id=1", (lost,))
            artifact = write_json_artifact(settings.artifact_root, {"task_id": task_id, "step_key": "normalize_asset", "attempt_id": producer}, "output",
                                           {"source": "mock", "compatibility_key": "historical", "step_key": "normalize_asset"})
            conn.execute("INSERT INTO artifacts(id,task_id,step_id,kind,relative_path,content_fingerprint,producer_attempt_id,metadata) VALUES (%s,%s,%s,'output',%s,%s,%s,%s)",
                (artifact["id"], task_id, success, artifact["relative_path"], artifact["content_fingerprint"], producer, Jsonb(artifact["metadata"])))
            conn.execute("UPDATE steps SET output_refs=%s WHERE id=%s", (Jsonb([artifact["id"]]), success))
            before = conn.execute("SELECT lease_expires_at FROM step_attempts WHERE id=%s", (lost,)).fetchone()
        original_bytes = (settings.artifact_root / artifact["relative_path"]).read_bytes()
        migrate(settings); migrate(settings)
        repo = Repository(settings)
        repo.sync_nodes(force=True)
        repo.reconcile_resources()  # Missing physical identity must remain unknown.
        with connect(settings) as conn:
            slot = conn.execute("SELECT * FROM resource_slots WHERE id=1").fetchone()
            assert slot["state"] == "QUARANTINED" and str(slot["owner_attempt_id"]) == lost
            assert slot["node_id"] == "legacy" and slot["device_id"] == "gpu-0"
            row = conn.execute("SELECT * FROM step_attempts WHERE id=%s", (lost,)).fetchone()
            assert row["lease_expires_at"] == before["lease_expires_at"] and row["process_exit_confirmed_at"] is None
            assert row["node_id"] == "legacy" and row["resource_device_id"] == "gpu-0"
            assert conn.execute("SELECT config_snapshot FROM tasks WHERE id=%s", (task_id,)).fetchone()["config_snapshot"] == snapshot
            assert conn.execute("SELECT count(*) AS n FROM step_attempts").fetchone()["n"] == 2
            assert conn.execute("SELECT output_refs FROM steps WHERE id=%s", (success,)).fetchone()["output_refs"] == [artifact["id"]]
        assert (settings.artifact_root / artifact["relative_path"]).read_bytes() == original_bytes
    finally:
        with connect(engine.settings) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
