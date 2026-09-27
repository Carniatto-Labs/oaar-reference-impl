import time

from opentelemetry import trace

from langgraph.checkpoint.postgres import PostgresSaver

from oaar_impl.spans import (
    ATTR_CHECKPOINT_DURATION_MS,
    ATTR_CHECKPOINT_ID,
    ATTR_CHECKPOINT_OUTCOME,
    SemanticViolation,
    checkpoint_span,
    emit_lifecycle,
    emit_state_transition,
)

RESUME_CHECKPOINT_ATTR = "agent.runtime.resume.checkpoint.id"


def _version_number(value) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        head = value.split(".", 1)[0]
        if head.isdigit():
            return int(head)
    return None


def _max_state_version(versions) -> int:
    numbers = [
        n for n in (_version_number(v) for v in (versions or {}).values()) if n is not None
    ]
    return max(numbers) if numbers else 0


class OtelCheckpointer(PostgresSaver):
    def __init__(self, conn, *, backend: str = "postgres"):
        super().__init__(conn)
        self._backend = backend
        self._last_step: dict = {}
        self.saved_checkpoints: list = []
        self.restored_checkpoints: list = []

    @staticmethod
    def _thread_key(config) -> str:
        configurable = (config or {}).get("configurable") or {}
        return f"{configurable.get('thread_id', '')}:{configurable.get('checkpoint_ns', '')}"

    @staticmethod
    def _thread_id(config) -> str:
        return ((config or {}).get("configurable") or {}).get("thread_id", "")

    def put(self, config, checkpoint, metadata, new_versions):
        metadata = metadata or {}
        checkpoint = checkpoint or {}
        checkpoint_id = checkpoint.get("id", "")
        thread_id = self._thread_id(config)
        thread_key = self._thread_key(config)
        state_version = _max_state_version(checkpoint.get("channel_versions"))
        writes = metadata.get("writes") or {}
        step = metadata.get("step")
        changed_keys = sorted(writes.keys()) or sorted((new_versions or {}).keys())
        started = time.monotonic()
        with checkpoint_span(
            "save", checkpoint_id, thread_id, state_version, self._backend
        ) as span:
            result = super().put(config, checkpoint, metadata, new_versions)
            span.set_attribute(
                ATTR_CHECKPOINT_DURATION_MS, (time.monotonic() - started) * 1000
            )
            span.set_attribute(ATTR_CHECKPOINT_OUTCOME, "saved")
        self.saved_checkpoints.append(
            {
                "thread_id": thread_id,
                "checkpoint_id": checkpoint_id,
                "state_version": state_version,
                "step": step,
            }
        )
        previous_step = self._last_step.get(thread_key)
        if previous_step is not None and step is not None:
            try:
                emit_state_transition(
                    f"step:{previous_step}",
                    f"step:{step}",
                    changed_keys,
                    {"writes": {k: type(v).__name__ for k, v in writes.items()}},
                    reason=str(metadata.get("source", "superstep")),
                    actor="runtime",
                    state_version=state_version,
                )
                emit_lifecycle(
                    "agent.run.checkpointed",
                    reason="superstep checkpoint",
                    actor="runtime",
                    state_version=state_version,
                )
            except SemanticViolation:
                pass
        self._last_step[thread_key] = step
        return result

    def get_tuple(self, config):
        started = time.monotonic()
        tuple_ = super().get_tuple(config)
        if tuple_ is None:
            return None
        checkpoint = tuple_.checkpoint or {}
        checkpoint_id = checkpoint.get("id", "")
        thread_id = self._thread_id(config)
        thread_key = self._thread_key(config)
        state_version = _max_state_version(checkpoint.get("channel_versions"))
        with checkpoint_span(
            "restore", checkpoint_id, thread_id, state_version, self._backend
        ) as span:
            span.set_attribute(
                ATTR_CHECKPOINT_DURATION_MS, (time.monotonic() - started) * 1000
            )
            span.set_attribute(ATTR_CHECKPOINT_OUTCOME, "restored")
        metadata = tuple_.metadata or {}
        self.restored_checkpoints.append(
            {
                "thread_id": thread_id,
                "checkpoint_id": checkpoint_id,
                "state_version": state_version,
                "step": metadata.get("step"),
            }
        )
        self._last_step[thread_key] = metadata.get("step")
        current = trace.get_current_span()
        if current and current.get_span_context().is_valid:
            current.set_attribute(RESUME_CHECKPOINT_ATTR, checkpoint_id)
            try:
                emit_lifecycle(
                    "agent.run.resumed",
                    reason="checkpoint_restore",
                    actor="runtime",
                    state_version=state_version,
                )
            except SemanticViolation:
                pass
        return tuple_
