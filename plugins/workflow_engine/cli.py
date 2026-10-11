"""Operator CLI for validating and inspecting workflow plans."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from plugins.workflow_engine.graph import topological_order, validate_workflow


def register_cli(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="workflow_engine_action", required=True)
    validate = actions.add_parser("validate", help="validate a workflow JSON document")
    validate.add_argument("path", help="path to workflow JSON")
    plan = actions.add_parser("plan", help="print deterministic execution order")
    plan.add_argument("path", help="path to workflow JSON")


def workflow_engine_command(args: argparse.Namespace) -> int:
    try:
        document = json.loads(Path(args.path).read_text(encoding="utf-8-sig"))
        graph = validate_workflow(document)
        if args.workflow_engine_action == "validate":
            print(json.dumps({"ok": True, "workflow_id": graph["id"], "nodes": len(graph["nodes"]), "edges": len(graph["edges"])}, indent=2))
        elif args.workflow_engine_action == "plan":
            print(json.dumps({"workflow_id": graph["id"], "execution_order": topological_order(graph)}, indent=2))
        else:
            print(f"Unknown action: {args.workflow_engine_action}")
            return 2
        return 0
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"Workflow error: {exc}")
        return 1


def register(ctx) -> None:
    ctx.register_cli_command(
        name="workflow-engine",
        help="Validate and inspect deterministic workflow graphs",
        setup_fn=register_cli,
        handler_fn=workflow_engine_command,
        description="Validate a workflow document or print its stable topological execution plan.",
    )
