"""Deterministic, handler-driven DAG execution with durable checkpoints."""
from __future__ import annotations

import hashlib
import json
from typing import Any, Callable

from plugins.workflow_engine.graph import topological_order, validate_workflow
from plugins.workflow_engine.store import WorkflowStateError, WorkflowStore

NodeHandler = Callable[[dict[str, Any], dict[str, Any], str], Any]


class WorkflowRuntime:
    """Execute validated graphs; no implicit shell, network, or model execution is provided."""
    def __init__(self, store: WorkflowStore, handlers: dict[str, NodeHandler] | None = None):
        self.store = store
        self.handlers = dict(handlers or {})

    def _get_run_or_raise(self, run_id: str) -> dict[str, Any]:
        result = self.store.get_run(run_id)
        if result is None:
            raise WorkflowStateError(f"run disappeared during execution: {run_id}")
        return result

    def recover_node(self, run_id: str, node_id: str, *, acknowledge_possible_side_effects: bool = False, acknowledge_run_stopped: bool = False) -> None:
        """Authorize replay of a failed/interrupted node after operator review."""
        self.store.recover_node(run_id, node_id, acknowledge_possible_side_effects=acknowledge_possible_side_effects, acknowledge_run_stopped=acknowledge_run_stopped)

    def run(self, document: dict[str, Any], run_id: str, *, approvals: dict[str, bool] | None = None) -> dict[str, Any]:
        if not isinstance(run_id, str) or not run_id.strip():
            raise WorkflowStateError("run_id must be a non-empty string")
        graph = validate_workflow(document)
        approval_nodes = {node["id"] for node in graph["nodes"] if node["type"] == "approval"}
        if approvals is not None:
            if not isinstance(approvals, dict) or any(key not in approval_nodes for key in approvals):
                raise WorkflowStateError("approvals must only reference approval nodes in this workflow")
            if any(type(value) is not bool for value in approvals.values()):
                raise WorkflowStateError("approval values must be booleans")
        run = self.store.create_run(run_id, graph)
        if run["status"] == "succeeded":
            return run
        if run["status"] in {"failed", "cancelled"}:
            raise WorkflowStateError(f"run is {run['status']}; explicit recovery is required")
        approvals = approvals or {}
        nodes = {node["id"]: node for node in graph["nodes"]}
        predecessors: dict[str, list[str]] = {node_id: [] for node_id in nodes}
        for edge in graph["edges"]:
            predecessors[edge["target"]].append(edge["source"])
        self.store.set_run_status(run_id, "running")
        for node_id in topological_order(graph):
            node = nodes[node_id]
            existing = self.store.get_node(run_id, node_id)
            if existing and existing["status"] == "succeeded":
                continue
            inputs = {}
            for parent_id in predecessors[node_id]:
                parent = self.store.get_node(run_id, parent_id)
                if not parent or parent["status"] != "succeeded":
                    self.store.set_run_status(run_id, "paused", f"waiting for predecessor {parent_id}")
                    return self._get_run_or_raise(run_id)
                inputs[parent_id] = parent["output"]
            idempotency_key = hashlib.sha256(json.dumps([run_id, node_id], separators=(",", ":")).encode()).hexdigest()
            if node["type"] == "approval":
                if node_id not in approvals:
                    self.store.set_node_status(run_id,node_id,"waiting_approval",idempotency_key)
                    self.store.set_run_status(run_id,"paused",f"approval required: {node_id}")
                    return self._get_run_or_raise(run_id)
                if approvals[node_id] is not True:
                    self.store.set_node_status(run_id,node_id,"failed",idempotency_key,error="approval rejected")
                    self.store.set_run_status(run_id,"failed",f"approval rejected: {node_id}")
                    return self._get_run_or_raise(run_id)
                self.store.set_node_status(run_id,node_id,"succeeded",idempotency_key,output={"approved": True})
                continue
            if node["type"] == "end":
                self.store.set_node_status(run_id,node_id,"succeeded",idempotency_key,output={"completed": True})
                continue
            handler = self.handlers.get(node["type"])
            if handler is None:
                self.store.set_run_status(run_id,"paused",f"no handler registered for node type: {node['type']}")
                return self._get_run_or_raise(run_id)
            cached = self.store.start_node(run_id,node_id,idempotency_key)
            if cached is not None:
                continue
            try:
                output = handler(dict(node.get("config") or {}), inputs, idempotency_key)
                # Fail before recording success if a handler returns non-JSON data.
                json.dumps(output, sort_keys=True, allow_nan=False)
                self.store.complete_node(run_id,node_id,output)
            except Exception as exc:  # health: allow BLE001 -- persist failures from caller-supplied handlers.
                try:
                    self.store.fail_node(run_id,node_id,f"{type(exc).__name__}: {exc}")
                finally:
                    self.store.set_run_status(run_id,"failed",f"node {node_id} failed")
                return self._get_run_or_raise(run_id)
        self.store.set_run_status(run_id,"succeeded")
        return self._get_run_or_raise(run_id)
