"""Deterministic workflow primitives for Hermes Agent.

This package deliberately has no core imports, network client, shell executor, or model calls.
Integrations supply explicit handlers to the runtime.
"""

from plugins.workflow_engine.graph import WorkflowValidationError, validate_workflow, topological_order
from plugins.workflow_engine.cli import register

__all__ = ["WorkflowValidationError", "register", "topological_order", "validate_workflow"]
