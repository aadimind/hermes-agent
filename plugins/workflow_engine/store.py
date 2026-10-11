"""Small SQLite store with explicit, durable workflow state transitions."""
from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any


class WorkflowStateError(RuntimeError):
    """Raised when a run or node transition would be unsafe or ambiguous."""


class WorkflowStore:
    def __init__(self, path: str | Path):
        requested_path = str(path)
        self._uri = requested_path == ":memory:"
        self.path = f"file:workflow_engine_{uuid.uuid4().hex}?mode=memory&cache=shared" if self._uri else requested_path
        self._anchor = sqlite3.connect(self.path, uri=True, isolation_level=None) if self._uri else None
        if not self._uri:
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def _connection(self):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None, uri=self._uri)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=10000")
        db.execute("PRAGMA foreign_keys=ON")
        try:
            yield db
        finally:
            db.close()

    def _initialize(self) -> None:
        with self._connection() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS workflow_runs (
                    run_id TEXT PRIMARY KEY,
                    workflow_id TEXT NOT NULL,
                    graph_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
                );
                CREATE TABLE IF NOT EXISTS workflow_nodes (
                    run_id TEXT NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
                    node_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    output_json TEXT,
                    error TEXT,
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                    PRIMARY KEY (run_id, node_id)
                );
            """)

    def create_run(self, run_id: str, graph: dict[str, Any]) -> dict[str, Any]:
        graph_json = json.dumps(graph, sort_keys=True, separators=(",", ":"))
        with self._connection() as db:
            db.execute("INSERT OR IGNORE INTO workflow_runs(run_id,workflow_id,graph_json,status) VALUES(?,?,?,'pending')",
                       (run_id, graph["id"], graph_json))
            row = db.execute("SELECT workflow_id,graph_json FROM workflow_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None or row["workflow_id"] != graph["id"] or row["graph_json"] != graph_json:
                raise WorkflowStateError("run_id already belongs to a different workflow document")
            return self.get_run(run_id)

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._connection() as db:
            row = db.execute("SELECT * FROM workflow_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                return None
            nodes = db.execute("SELECT * FROM workflow_nodes WHERE run_id=? ORDER BY rowid", (run_id,)).fetchall()
        result = dict(row)
        result["graph"] = json.loads(result.pop("graph_json"))
        result["nodes"] = {item["node_id"]: self._decode_node(item) for item in nodes}
        return result

    @staticmethod
    def _decode_node(row: sqlite3.Row) -> dict[str, Any]:
        item = dict(row)
        item["output"] = json.loads(item.pop("output_json")) if item.get("output_json") is not None else None
        return item

    def get_node(self, run_id: str, node_id: str) -> dict[str, Any] | None:
        with self._connection() as db:
            row = db.execute("SELECT * FROM workflow_nodes WHERE run_id=? AND node_id=?", (run_id,node_id)).fetchone()
        return self._decode_node(row) if row else None

    def set_run_status(self, run_id: str, status: str, error: str | None = None) -> None:
        with self._connection() as db:
            cur = db.execute("UPDATE workflow_runs SET status=?,error=?,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE run_id=?",
                             (status,error,run_id))
            if cur.rowcount != 1:
                raise WorkflowStateError(f"unknown run: {run_id}")

    def start_node(self, run_id: str, node_id: str, idempotency_key: str) -> dict[str, Any] | None:
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM workflow_nodes WHERE run_id=? AND node_id=?", (run_id,node_id)).fetchone()
            if row and row["status"] == "succeeded":
                db.commit()
                return self._decode_node(row)
            if row and row["status"] in {"running", "failed", "waiting_approval"}:
                db.rollback()
                raise WorkflowStateError(f"node {node_id} is {row['status']}; explicit recovery is required")
            db.execute("INSERT INTO workflow_nodes(run_id,node_id,status,idempotency_key) VALUES(?,?, 'running',?) "
                       "ON CONFLICT(run_id,node_id) DO UPDATE SET status='running',idempotency_key=excluded.idempotency_key,error=NULL,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')",
                       (run_id,node_id,idempotency_key))
            db.commit()
        return None

    def set_node_status(self, run_id: str, node_id: str, status: str, idempotency_key: str,
                        output: Any = None, error: str | None = None) -> None:
        output_json = json.dumps(output, sort_keys=True, separators=(",", ":"), allow_nan=False) if output is not None else None
        with self._connection() as db:
            db.execute("INSERT INTO workflow_nodes(run_id,node_id,status,idempotency_key,output_json,error) VALUES(?,?,?,?,?,?) "
                       "ON CONFLICT(run_id,node_id) DO UPDATE SET status=excluded.status,idempotency_key=excluded.idempotency_key,output_json=excluded.output_json,error=excluded.error,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')",
                       (run_id,node_id,status,idempotency_key,output_json,error))

    def complete_node(self, run_id: str, node_id: str, output: Any) -> None:
        with self._connection() as db:
            row = db.execute("SELECT idempotency_key,status FROM workflow_nodes WHERE run_id=? AND node_id=?", (run_id,node_id)).fetchone()
        if row is None or row["status"] != "running":
            raise WorkflowStateError(f"node {node_id} is not running")
        self.set_node_status(run_id,node_id,"succeeded",row["idempotency_key"],output=output)

    def recover_node(self, run_id: str, node_id: str, *, acknowledge_possible_side_effects: bool) -> None:
        """Reset an interrupted/failed node only after explicit operator review.

        A handler may have performed an external side effect before stopping. Keep
        the original idempotency key so cooperating destinations can deduplicate replay.
        """
        if acknowledge_possible_side_effects is not True:
            raise WorkflowStateError("recovery requires acknowledgement of possible external side effects")
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            run = db.execute("SELECT status FROM workflow_runs WHERE run_id=?", (run_id,)).fetchone()
            node = db.execute("SELECT status FROM workflow_nodes WHERE run_id=? AND node_id=?", (run_id, node_id)).fetchone()
            if run is None or node is None:
                db.rollback()
                raise WorkflowStateError("unknown run or node")
            if run["status"] not in {"failed", "running", "paused"}:
                db.rollback()
                raise WorkflowStateError(f"run status {run['status']} cannot be recovered")
            if node["status"] not in {"running", "failed"}:
                db.rollback()
                raise WorkflowStateError(f"node status {node['status']} is not recoverable")
            db.execute("UPDATE workflow_nodes SET status='pending',output_json=NULL,error=NULL,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE run_id=? AND node_id=?", (run_id, node_id))
            db.execute("UPDATE workflow_runs SET status='paused',error=NULL,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE run_id=?", (run_id,))
            db.commit()

    def fail_node(self, run_id: str, node_id: str, error: str) -> None:
        with self._connection() as db:
            row = db.execute("SELECT idempotency_key FROM workflow_nodes WHERE run_id=? AND node_id=?", (run_id,node_id)).fetchone()
        if row is None:
            raise WorkflowStateError(f"unknown node: {node_id}")
        self.set_node_status(run_id,node_id,"failed",row["idempotency_key"],error=error)
