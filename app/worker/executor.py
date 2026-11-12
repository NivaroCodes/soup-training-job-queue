"""Deterministic simulated training job executor with cooperative cancellation."""

from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Callable
from typing import Any


class RetryableJobError(Exception):
    """Raised when a job fails due to a transient/retryable condition."""


class NonRetryableJobError(Exception):
    """Raised when a job fails due to a permanent/non-retryable condition."""


class JobCancelledError(Exception):
    """Raised when cooperative cancellation is observed at a safe checkpoint."""


class LeaseLostError(Exception):
    """Raised when the background heartbeat observes lease loss or preemption."""


class SimulatedJobExecutor:
    """Executes deterministic simulated training steps and produces an effect payload."""

    @staticmethod
    def execute(
        payload: dict[str, Any],
        *,
        cancel_event: threading.Event,
        lease_lost_event: threading.Event,
        on_checkpoint: Callable[[int, int], None] | None = None,
    ) -> dict[str, Any]:
        """Run deterministic training steps and return measurable effect metrics."""
        steps = int(payload.get("steps", 3))
        if steps < 1:
            raise NonRetryableJobError("Payload 'steps' must be >= 1.")

        step_duration_ms = float(payload.get("step_duration_ms", 10.0))
        simulate_failure = payload.get("simulate_failure")

        canonical_payload = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        payload_sha256 = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()

        final_loss = 1.0
        for step in range(1, steps + 1):
            if lease_lost_event.is_set():
                raise LeaseLostError("Worker lost lease during execution.")
            if cancel_event.is_set():
                raise JobCancelledError("Cooperative cancellation requested.")

            if step_duration_ms > 0:
                # Wait is immediately interrupted if cancel_event is set.
                if cancel_event.wait(timeout=step_duration_ms / 1000.0):
                    if lease_lost_event.is_set():
                        raise LeaseLostError("Worker lost lease during execution.")
                    raise JobCancelledError("Cooperative cancellation requested.")

            final_loss = round(1.0 / (step + 1), 6)
            if on_checkpoint is not None:
                on_checkpoint(step, steps)

        if lease_lost_event.is_set():
            raise LeaseLostError("Worker lost lease before completion.")

        if simulate_failure == "non_retryable":
            raise NonRetryableJobError("Simulated non-retryable training failure.")
        if simulate_failure == "retryable":
            raise RetryableJobError("Simulated retryable training failure.")

        if cancel_event.is_set():
            raise JobCancelledError("Cooperative cancellation requested.")

        return {
            "model": payload.get("model", "default"),
            "steps_completed": steps,
            "final_loss": final_loss,
            "payload_sha256": payload_sha256,
        }
