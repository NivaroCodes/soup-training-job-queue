"""Request and response schemas for the training job queue API."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.db.models import JobStatus


class JobCreateRequest(BaseModel):
    """Request payload for creating a training job."""

    idempotency_key: str | None = Field(
        default=None,
        max_length=255,
        description="Optional body idempotency key; may also be passed via Idempotency-Key header.",
    )
    payload: dict[str, Any] = Field(
        ...,
        description="Training job specification payload.",
    )
    max_attempts: int = Field(
        default=3,
        ge=1,
        le=100,
        description="Maximum execution attempts before terminal failure.",
    )

    @field_validator("idempotency_key")
    @classmethod
    def _validate_idempotency_key(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("idempotency_key must not be blank.")
        return stripped


class ExecutionEffectResponse(BaseModel):
    """Committed measurable database side effect for a succeeded job."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    worker_id: str
    fencing_token: int
    effect_payload: dict[str, Any]
    created_at: datetime


class JobResponse(BaseModel):
    """Serialized representation of a training job."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    idempotency_key: str
    request_hash: str
    status: JobStatus
    payload: dict[str, Any]
    attempt: int
    max_attempts: int
    fencing_token: int
    worker_id: str | None
    lease_expires_at: datetime | None
    last_heartbeat_at: datetime | None
    cancel_requested: bool
    cancel_requested_at: datetime | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    cancelled_at: datetime | None
    finished_at: datetime | None
    execution_effect: ExecutionEffectResponse | None = None


class JobListResponse(BaseModel):
    """Paginated list of training jobs."""

    items: list[JobResponse]
    total: int
    limit: int
    offset: int
