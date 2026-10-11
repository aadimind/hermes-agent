"""Contract tests for workflow engine CLI registration."""
from argparse import ArgumentParser
import json

from plugins.workflow_engine import register
from plugins.workflow_engine.cli import register_cli, workflow_engine_command

def test_register_exposes_hermes_command():
    captured = {}
    class Context:
        def register_cli_command(self, **kwargs):
            captured.update(kwargs)
    register(Context())
    assert captured["name"] == "workflow-engine"
    assert callable(captured["setup_fn"])
    assert callable(captured["handler_fn"])

def test_cli_validate_and_plan(tmp_path, capsys):
    path = tmp_path / "workflow.json"
    path.write_text(json.dumps({"schema_version": 1, "id": "demo", "name": "Demo", "nodes": [{"id": "a", "type": "end"}, {"id": "b", "type": "end"}], "edges": []}), encoding="utf-8")
    parser = ArgumentParser()
    register_cli(parser)
    assert workflow_engine_command(parser.parse_args(["validate", str(path)])) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True
    assert workflow_engine_command(parser.parse_args(["plan", str(path)])) == 0
    assert json.loads(capsys.readouterr().out)["execution_order"] == ["a", "b"]

def test_invalid_json_returns_error(tmp_path, capsys):
    path = tmp_path / "invalid.json"
    path.write_text("{", encoding="utf-8")
    parser = ArgumentParser()
    register_cli(parser)
    assert workflow_engine_command(parser.parse_args(["validate", str(path)])) == 1
    assert "Workflow error:" in capsys.readouterr().out
