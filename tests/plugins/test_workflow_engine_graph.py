"""Contract tests for the isolated workflow graph validator."""

import pytest
import math

from plugins.workflow_engine.graph import WorkflowValidationError, topological_order, validate_workflow


def graph(nodes=None, edges=None):
    return {"schema_version": 1, "id": "sample", "name": "Sample",
            "nodes": nodes if nodes is not None else [
                {"id": "start", "type": "transform"}, {"id": "review", "type": "approval"},
                {"id": "finish", "type": "end"}],
            "edges": edges if edges is not None else [
                {"source": "start", "target": "review"}, {"source": "review", "target": "finish"}]}


def test_valid_graph_is_normalized_and_ordered():
    normalized = validate_workflow(graph())
    assert topological_order(normalized) == ["start", "review", "finish"]
    assert normalized["schema_version"] == 1


def test_independent_nodes_keep_declaration_order():
    document = graph(nodes=[{"id": "z", "type": "end"}, {"id": "a", "type": "end"},
                            {"id": "m", "type": "end"}], edges=[])
    assert topological_order(validate_workflow(document)) == ["z", "a", "m"]


@pytest.mark.parametrize("document, message", [
    ([], "workflow must be an object"), ({"unexpected": True}, "unknown workflow fields"),
    (graph(nodes=[{"id": "x", "type": "shell"}], edges=[]), "unsupported type"),
    (graph(nodes=[{"id": "x", "type": "end"}, {"id": "x", "type": "end"}], edges=[]), "duplicate node id"),
    (graph(edges=[{"source": "missing", "target": "finish"}]), "unknown node"),
    (graph(edges=[{"source": "start", "target": "start"}]), "self-loop"),
    (graph(edges=[{"source": "start", "target": "review"}, {"source": "review", "target": "start"}]), "cycle"),
    (graph(nodes=[{"id": "x", "type": "end", "argv": ["sh", "-c", "unsafe"]}], edges=[]), "unsupported fields"),
    (graph(nodes=[{"id": "x", "type": "transform", "config": {"nested": {"command": "unsafe"}}}], edges=[]), "unsupported key"),
    (graph(nodes=[{"id": "x", "type": ["end"]}], edges=[]), "unsupported type"),
    (graph(edges=[{"source": [], "target": "finish"}]), "unknown node"),
])
def test_invalid_graphs_fail_closed(document, message):
    with pytest.raises(WorkflowValidationError, match=message):
        validate_workflow(document)


def test_duplicate_edges_are_rejected():
    document = graph(edges=[{"source": "start", "target": "review"}, {"source": "start", "target": "review"}])
    with pytest.raises(WorkflowValidationError, match="duplicate edge"):
        validate_workflow(document)


def test_boolean_schema_version_is_not_integer_version_one():
    document = graph()
    document["schema_version"] = True
    with pytest.raises(WorkflowValidationError, match="schema_version"):
        validate_workflow(document)


def test_schema_version_must_be_supported():
    document = graph()
    document["schema_version"] = 2
    with pytest.raises(WorkflowValidationError, match="schema_version"):
        validate_workflow(document)


def test_non_finite_config_numbers_are_rejected():
    document = graph(nodes=[{"id": "x", "type": "transform", "config": {"value": math.nan}}], edges=[])
    with pytest.raises(WorkflowValidationError, match="non-finite number"):
        validate_workflow(document)
