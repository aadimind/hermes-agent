"""Runtime contract tests: checkpointing, deterministic ordering and approvals."""
import pytest

from plugins.workflow_engine.runtime import WorkflowRuntime
from plugins.workflow_engine.store import WorkflowStateError, WorkflowStore


def graph():
    return {"schema_version": 1, "id": "demo", "name": "Demo",
            "nodes": [{"id": "one", "type": "transform", "config": {"value": 2}},
                      {"id": "review", "type": "approval", "config": {}},
                      {"id": "end", "type": "end", "config": {}}],
            "edges": [{"source": "one", "target": "review"}, {"source": "review", "target": "end"}]}


def test_approval_pauses_and_resume_reuses_completed_checkpoint(tmp_path):
    calls = []
    runtime = WorkflowRuntime(WorkflowStore(tmp_path / "workflow.sqlite"), {
        "transform": lambda config, inputs, key: calls.append(key) or {"value": config["value"] + 1}
    })
    first = runtime.run(graph(), "run-1")
    assert first["status"] == "paused"
    assert first["nodes"]["one"]["status"] == "succeeded"
    assert first["nodes"]["review"]["status"] == "waiting_approval"
    second = runtime.run(graph(), "run-1", approvals={"review": True})
    assert second["status"] == "succeeded"
    assert second["nodes"]["one"]["output"] == {"value": 3}
    assert len(calls) == 1


def test_same_run_id_cannot_be_reused_for_changed_graph(tmp_path):
    runtime = WorkflowRuntime(WorkflowStore(tmp_path / "workflow.sqlite"), {"transform": lambda c, i, k: {}})
    runtime.run(graph(), "run-2")
    changed = graph()
    changed["name"] = "Different graph document"
    with pytest.raises(WorkflowStateError, match="different workflow document"):
        runtime.run(changed, "run-2")


def test_failed_node_is_not_silently_retried(tmp_path):
    runtime = WorkflowRuntime(WorkflowStore(tmp_path / "workflow.sqlite"), {
        "transform": lambda config, inputs, key: (_ for _ in ()).throw(RuntimeError("external failure"))
    })
    first = runtime.run(graph(), "run-3")
    assert first["status"] == "failed"
    with pytest.raises(WorkflowStateError, match="explicit recovery"):
        runtime.run(graph(), "run-3")
