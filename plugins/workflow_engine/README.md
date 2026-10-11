# Workflow Engine (experimental)

This package is an isolated first-party foundation for deterministic Hermes workflow execution. It does not patch Hermes core and does not execute arbitrary commands.

## Current scope

- Strict versioned workflow document validation.
- Deterministic topological ordering (ties preserve document node order).
- Fail-closed validation for duplicate IDs/edges, unknown endpoints, cycles, unknown fields, and unsupported node types.
- No shell/argv field, arbitrary expression evaluator, network client, or implicit subprocess execution.

## Workflow document

```json

{
  "schema_version": 1,
  "id": "paper-review",
  "name": "Research paper review",
  "nodes": [
    {"id": "extract", "type": "agent", "config": {"task": "Extract claims"}},
    {"id": "approve", "type": "approval", "config": {}},
    {"id": "publish", "type": "end", "config": {}}
  ],
  "edges": [
    {"source": "extract", "target": "approve"},
    {"source": "approve", "target": "publish"}
  ]
}
```

The graph module validates structure only. It does not imply that `agent`, `approval`, or other nodes can run until a runtime handler explicitly implements their semantics. Integrations must supply handlers and must treat external side effects as at-least-once unless the destination supports an idempotency key.

## Development

Run the targeted tests with:

```bash
pytest tests/plugins/test_workflow_engine_graph.py
```

A handler-driven runtime, SQLite checkpoint store, approval pause/resume, and targeted tests are included. Event/webhook/cron triggers, cancellation controls, a visual editor, built-in Hermes agent-session adapters, and full Desktop end-to-end integration are not yet implemented.
