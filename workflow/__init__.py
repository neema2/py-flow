"""
Workflow orchestration — durable workflows with pluggable backends.

User API::

    from workflow import WorkflowEngine, create_engine

    engine = create_engine("demo")   # resolves alias → creates backend
    engine.launch()
    engine.workflow(my_fn, ...)

Platform API lives in ``workflow.admin``.
"""

from workflow.engine import WorkflowEngine, WorkflowStatus
from workflow.factory import create_engine

__all__ = ["WorkflowEngine", "WorkflowStatus", "create_engine"]
