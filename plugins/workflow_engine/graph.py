"""Strict validation and deterministic ordering for workflow graph documents."""

from __future__ import annotations

from collections import defaultdict, deque
import copy
import math
from typing import Any

SCHEMA_VERSION = 1
_ALLOWED_NODE_TYPES = frozenset({"agent", "approval", "transform", "end"})
_ALLOWED_NODE_KEYS = frozenset({"id", "type", "name", "config"})
_ALLOWED_EDGE_KEYS = frozenset({"source", "target"})
_ALLOWED_ROOT_KEYS = frozenset({"schema_version", "id", "name", "nodes", "edges"})
_FORBIDDEN_CONFIG_KEYS = frozenset({"argv", "shell", "command", "exec", "executable", "module", "import", "url"})


def _validate_config(value: Any, path: str) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str):
                raise WorkflowValidationError(f"{path} keys must be strings")
            if key.strip().lower() in _FORBIDDEN_CONFIG_KEYS:
                raise WorkflowValidationError(f"{path} contains unsupported key: {key}")
            _validate_config(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _validate_config(child, f"{path}[{index}]")
    elif isinstance(value, float) and not math.isfinite(value):
        raise WorkflowValidationError(f"{path} contains a non-finite number")
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise WorkflowValidationError(f"{path} contains a non-JSON value")


class WorkflowValidationError(ValueError):
    """Raised when a workflow document is malformed or unsafe to execute."""


def _validate_nodes(nodes: list[Any]) -> tuple[list[dict[str, Any]], set[str]]:
    normalized_nodes: list[dict[str, Any]] = []
    node_ids: set[str] = set()
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            raise WorkflowValidationError(f"nodes[{index}] must be an object")
        unknown = set(node) - _ALLOWED_NODE_KEYS
        if unknown:
            raise WorkflowValidationError(f"nodes[{index}] has unsupported fields: {', '.join(sorted(unknown))}")
        node_id, node_type = node.get("id"), node.get("type")
        if not isinstance(node_id, str) or not node_id.strip():
            raise WorkflowValidationError(f"nodes[{index}].id must be a non-empty string")
        node_id = node_id.strip()
        if node_id in node_ids:
            raise WorkflowValidationError(f"duplicate node id: {node_id}")
        if not isinstance(node_type, str) or node_type not in _ALLOWED_NODE_TYPES:
            raise WorkflowValidationError(f"node {node_id}: unsupported type {node_type!r}")
        config = node.get("config", {})
        if not isinstance(config, dict):
            raise WorkflowValidationError(f"node {node_id}.config must be an object")
        _validate_config(config, f"node {node_id}.config")
        normalized = {"id": node_id, "type": node_type, "config": copy.deepcopy(config)}
        if "name" in node:
            if not isinstance(node["name"], str):
                raise WorkflowValidationError(f"node {node_id}.name must be a string")
            normalized["name"] = node["name"]
        node_ids.add(node_id)
        normalized_nodes.append(normalized)
    return normalized_nodes, node_ids

def _validate_edges(edges: list[Any], node_ids: set[str]) -> list[dict[str, str]]:
    normalized_edges: list[dict[str, str]] = []
    seen_edges: set[tuple[str, str]] = set()
    for index, edge in enumerate(edges):
        if not isinstance(edge, dict):
            raise WorkflowValidationError(f"edges[{index}] must be an object")
        unknown = set(edge) - _ALLOWED_EDGE_KEYS
        if unknown:
            raise WorkflowValidationError(f"edges[{index}] has unsupported fields: {', '.join(sorted(unknown))}")
        source, target = edge.get("source"), edge.get("target")
        if not isinstance(source, str) or not isinstance(target, str) or source not in node_ids or target not in node_ids:
            raise WorkflowValidationError(f"edges[{index}] references an unknown node")
        if source == target:
            raise WorkflowValidationError(f"self-loop is not allowed: {source}")
        pair = (source, target)
        if pair in seen_edges:
            raise WorkflowValidationError(f"duplicate edge: {source} -> {target}")
        seen_edges.add(pair)
        normalized_edges.append({"source": source, "target": target})
    return normalized_edges

def validate_workflow(document: Any) -> dict[str, Any]:
    """Validate and return a normalized copy of a workflow document.

    The schema intentionally has no shell/argv, arbitrary expression, import, or URL fields.
    Runtime integrations must implement node handlers explicitly.
    """
    if not isinstance(document, dict):
        raise WorkflowValidationError("workflow must be an object")
    if any(not isinstance(key, str) for key in document):
        raise WorkflowValidationError("workflow field names must be strings")
    unknown_root = set(document) - _ALLOWED_ROOT_KEYS
    if unknown_root:
        raise WorkflowValidationError(f"unknown workflow fields: {', '.join(sorted(unknown_root))}")
    if type(document.get("schema_version")) is not int or document.get("schema_version") != SCHEMA_VERSION:
        raise WorkflowValidationError(f"schema_version must be {SCHEMA_VERSION}")
    workflow_id = document.get("id")
    name = document.get("name")
    if not isinstance(workflow_id, str) or not workflow_id.strip():
        raise WorkflowValidationError("id must be a non-empty string")
    if not isinstance(name, str) or not name.strip():
        raise WorkflowValidationError("name must be a non-empty string")
    nodes = document.get("nodes")
    edges = document.get("edges")
    if not isinstance(nodes, list) or not nodes:
        raise WorkflowValidationError("nodes must be a non-empty list")
    if not isinstance(edges, list):
        raise WorkflowValidationError("edges must be a list")

    normalized_nodes, node_ids = _validate_nodes(nodes)
    normalized_edges = _validate_edges(edges, node_ids)
    normalized = {"schema_version": SCHEMA_VERSION, "id": workflow_id.strip(), "name": name.strip(),
                  "nodes": normalized_nodes, "edges": normalized_edges}
    topological_order(normalized)
    return normalized


def topological_order(document: dict[str, Any]) -> list[str]:
    """Return a stable topological order, preserving node declaration order for ties."""
    nodes = document.get("nodes")
    edges = document.get("edges")
    if not isinstance(nodes, list) or not isinstance(edges, list):
        raise WorkflowValidationError("nodes and edges must be lists")
    order_index: dict[str, int] = {}
    adjacency: dict[str, list[str]] = defaultdict(list)
    indegree: dict[str, int] = {}
    for index, node in enumerate(nodes):
        if not isinstance(node, dict) or not isinstance(node.get("id"), str):
            raise WorkflowValidationError(f"nodes[{index}] must have a string id")
        node_id = node["id"]
        if node_id in indegree:
            raise WorkflowValidationError(f"duplicate node id: {node_id}")
        order_index[node_id] = index
        indegree[node_id] = 0
    seen_edges: set[tuple[str, str]] = set()
    for index, edge in enumerate(edges):
        if not isinstance(edge, dict):
            raise WorkflowValidationError(f"edges[{index}] must be an object")
        source, target = edge.get("source"), edge.get("target")
        if not isinstance(source, str) or not isinstance(target, str) or source not in indegree or target not in indegree:
            raise WorkflowValidationError(f"edges[{index}] references an unknown node")
        if source == target:
            raise WorkflowValidationError(f"self-loop is not allowed: {source}")
        pair = (source, target)
        if pair in seen_edges:
            raise WorkflowValidationError(f"duplicate edge: {source} -> {target}")
        seen_edges.add(pair)
        adjacency[source].append(target)
        indegree[target] += 1

    ready = [(order_index[node_id], node_id) for node_id, degree in indegree.items() if degree == 0]
    ready.sort()
    queue = deque(ready)
    result: list[str] = []
    while queue:
        _, current = queue.popleft()
        result.append(current)
        for target in sorted(adjacency[current], key=order_index.__getitem__):
            indegree[target] -= 1
            if indegree[target] == 0:
                ready_items = list(queue)
                ready_items.append((order_index[target], target))
                ready_items.sort()
                queue = deque(ready_items)
    if len(result) != len(indegree):
        cyclic = sorted(node_id for node_id, degree in indegree.items() if degree > 0)
        raise WorkflowValidationError(f"workflow contains a cycle involving: {', '.join(cyclic)}")
    return result
