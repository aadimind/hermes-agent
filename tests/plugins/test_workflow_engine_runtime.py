"""Runtime contract tests: checkpointing, deterministic ordering and approvals."""
import hashlib

import pytest

from plugins.workflow_engine.graph import validate_workflow
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


def test_recovery_requires_explicit_side_effect_acknowledgement(tmp_path):
    runtime = WorkflowRuntime(WorkflowStore(tmp_path / "workflow.sqlite"), {
        "transform": lambda config, inputs, key: (_ for _ in ()).throw(RuntimeError("interrupted side effect"))
    })
    first = runtime.run(graph(), "run-recovery")
    assert first["status"] == "failed"
    with pytest.raises(WorkflowStateError, match="acknowledgement"):
        runtime.recover_node("run-recovery", "one")
    runtime.handlers["transform"] = lambda config, inputs, key: {"recovered": True}
    runtime.recover_node("run-recovery", "one", acknowledge_possible_side_effects=True, acknowledge_run_stopped=True)
    second = runtime.run(graph(), "run-recovery")
    assert second["status"] == "paused"
    assert second["nodes"]["one"]["output"] == {"recovered": True}
    assert second["nodes"]["review"]["status"] == "waiting_approval"


def test_interrupted_running_node_can_be_recovered_explicitly(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite")
    runtime = WorkflowRuntime(store, {"transform": lambda config, inputs, key: {"ok": True}})
    store.create_run("run-interrupted", validate_workflow(graph()))
    store.set_run_status("run-interrupted", "running")
    store.start_node("run-interrupted", "one", hashlib.sha256(b"run-interrupted:one").hexdigest())
    runtime.recover_node("run-interrupted", "one", acknowledge_possible_side_effects=True, acknowledge_run_stopped=True)
    result = runtime.run(graph(), "run-interrupted")
    assert result["status"] == "paused"
    assert result["nodes"]["one"]["status"] == "succeeded"


def test_missing_handler_pauses_and_can_resume_when_registered(tmp_path):
    runtime = WorkflowRuntime(WorkflowStore(tmp_path / "workflow.sqlite"))
    first = runtime.run(graph(), "run-missing-handler")
    assert first["status"] == "paused"
    assert "no handler registered" in first["error"]
    runtime.handlers["transform"] = lambda config, inputs, key: {"ready": True}
    second = runtime.run(graph(), "run-missing-handler")
    assert second["status"] == "paused"
    assert second["nodes"]["one"]["output"] == {"ready": True}


def test_store_rejects_illegal_run_and_node_transitions(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite")
    store.create_run("run-transitions", validate_workflow(graph()))
    with pytest.raises(WorkflowStateError, match="invalid run transition"):
        store.set_run_status("run-transitions", "succeeded")
    store.set_run_status("run-transitions", "running")
    store.start_node("run-transitions", "one", "key-one")
    with pytest.raises(WorkflowStateError, match="invalid node transition"):
        store.set_node_status("run-transitions", "one", "waiting_approval", "key-one")
    store.fail_node("run-transitions", "one", "failed")
    with pytest.raises(WorkflowStateError, match="invalid node transition"):
        store.set_node_status("run-transitions", "one", "succeeded", "key-one", output={"unsafe": False})


def test_store_rejects_starting_node_when_run_is_not_running(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite")
    store.create_run("run-not-running", validate_workflow(graph()))
    with pytest.raises(WorkflowStateError, match="cannot start a node"):
        store.start_node("run-not-running", "one", "key-one")


def test_recovery_requires_confirmation_that_runner_has_stopped(tmp_path):
    runtime = WorkflowRuntime(WorkflowStore(tmp_path / "workflow.sqlite"), {
        "transform": lambda config, inputs, key: (_ for _ in ()).throw(RuntimeError("failure"))
    })
    runtime.run(graph(), "run-recovery-guard")
    with pytest.raises(WorkflowStateError, match="original runner has stopped"):
        runtime.recover_node("run-recovery-guard", "one", acknowledge_possible_side_effects=True)


@pytest.mark.parametrize("approvals", [
    {"not-an-approval-node": True},
    {"review": 1},
    {"review": "yes"},
])
def test_approval_inputs_are_validated(approvals, tmp_path):
    runtime = WorkflowRuntime(WorkflowStore(tmp_path / "workflow.sqlite"), {
        "transform": lambda config, inputs, key: {"ok": True}
    })
    with pytest.raises(WorkflowStateError, match="approvals|approval values"):
        runtime.run(graph(), "run-invalid-approval-" + str(len(approvals)) + str(type(next(iter(approvals.values())))), approvals=approvals)


def test_store_rejects_node_not_in_workflow(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite")
    store.create_run("run-unknown-node", validate_workflow(graph()))
    store.set_run_status("run-unknown-node", "running")
    with pytest.raises(WorkflowStateError, match="not part of the stored workflow"):
        store.start_node("run-unknown-node", "ghost", "ghost-key")
