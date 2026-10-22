"""Exactly-once accounting for business outcomes per logical operation.

The three-level outcome model (execution / artifact / business) makes business outcomes
application-level facts that can arrive *after* every run of the operation has ended. That
creates a specific failure mode: several runs of the same logical operation each report an
outcome, so a naive counter counts the operation once per attempt and inflates the rate.

The contract implemented here is:

  * one terminal value per (subject, definition, definition_version);
  * non-terminal values (``pending``, ``unknown``) never count and never block a later
    terminal value;
  * a repeated terminal value is a duplicate (history keeps it, the denominator does not);
  * a *different* terminal value on an already-resolved subject is a conflict: the first
    value stays as the recorded one and the conflicting attempt is flagged, never silently
    overwritten.

This class is the durable-outcome store boundary of the runtime. The in-process
implementation below is what the reference scenarios exercise; a production runtime backs
the same interface with a unique index, e.g.

    UNIQUE (subject, definition, definition_version)

so that the guarantee survives process restarts, replays and concurrent workers.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

TERMINAL_VALUES = frozenset({"accepted", "declined"})


@dataclass(frozen=True)
class OutcomeResult:
    """What happened when an outcome was offered to the registry."""

    value: str
    recorded: bool
    prior_value: str | None = None
    conflict: bool = False
    terminal: bool = True

    @property
    def duplicate(self) -> bool:
        """A terminal value offered for an operation that already has one (same value)."""
        return self.terminal and not self.recorded and not self.conflict


class OutcomeRegistry:
    def __init__(self):
        self._resolved: dict[tuple[str, str, str], str] = {}
        self._lock = threading.Lock()

    @staticmethod
    def key(subject: str, definition: str, definition_version: str) -> tuple[str, str, str]:
        return (subject, definition, definition_version)

    def resolve(
        self, subject: str, definition: str, definition_version: str, value: str
    ) -> OutcomeResult:
        if value not in TERMINAL_VALUES:
            return OutcomeResult(value=value, recorded=False, terminal=False)
        key = self.key(subject, definition, definition_version)
        with self._lock:
            prior = self._resolved.get(key)
            if prior is None:
                self._resolved[key] = value
                return OutcomeResult(value=value, recorded=True)
            return OutcomeResult(
                value=value,
                recorded=False,
                prior_value=prior,
                conflict=prior != value,
            )

    def recorded_value(self, subject: str, definition: str, definition_version: str):
        return self._resolved.get(self.key(subject, definition, definition_version))


_default_registry = OutcomeRegistry()


def default_registry() -> OutcomeRegistry:
    return _default_registry
