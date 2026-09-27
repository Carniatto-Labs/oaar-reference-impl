import contextvars
from contextlib import contextmanager

OPERATION_ID_ATTR = "agent.operation.id"

_current_operation: contextvars.ContextVar = contextvars.ContextVar(
    "oaar_current_operation", default=None
)


@contextmanager
def operation(operation_id: str):
    if not operation_id or not str(operation_id).strip():
        raise ValueError("logical operation id must be a non-empty application-assigned value")
    token = _current_operation.set(str(operation_id))
    try:
        yield str(operation_id)
    finally:
        _current_operation.reset(token)


def current_operation():
    return _current_operation.get()
