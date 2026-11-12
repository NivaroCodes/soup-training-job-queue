"""REST API endpoints for training job management."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, status
from sqlalchemy.orm import Session

from app.api.dependencies import get_db
from app.db.models import JobStatus
from app.schemas.jobs import JobCreateRequest, JobListResponse, JobResponse
from app.services.job_service import (
    IdempotencyConflictError,
    JobNotFoundError,
    JobService,
)

router = APIRouter(prefix="/jobs", tags=["jobs"])


def _resolve_idempotency_key(
    header_key: str | None,
    body_key: str | None,
) -> str:
    normalized_header = header_key.strip() if header_key is not None else None
    if normalized_header == "":
        normalized_header = None

    if normalized_header is None and body_key is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Idempotency key is required via Idempotency-Key header or idempotency_key field.",
        )
    if normalized_header is not None and body_key is not None and normalized_header != body_key:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Idempotency-Key header does not match idempotency_key in request body.",
        )

    resolved = normalized_header or body_key
    assert resolved is not None
    if len(resolved) > 255:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Idempotency key must not exceed 255 characters.",
        )
    return resolved


@router.post(
    "",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_job(
    request_body: JobCreateRequest,
    response: Response,
    db: Annotated[Session, Depends(get_db)],
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> JobResponse:
    """Create a training job idempotently."""
    resolved_key = _resolve_idempotency_key(
        idempotency_key,
        request_body.idempotency_key,
    )
    try:
        job, created = JobService.create_job(
            db,
            idempotency_key=resolved_key,
            payload=request_body.payload,
            max_attempts=request_body.max_attempts,
        )
    except IdempotencyConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc

    response.status_code = (
        status.HTTP_201_CREATED if created else status.HTTP_200_OK
    )
    return JobResponse.model_validate(job)


@router.get(
    "",
    response_model=JobListResponse,
)
def list_jobs(
    db: Annotated[Session, Depends(get_db)],
    job_status: Annotated[JobStatus | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> JobListResponse:
    """List training jobs with deterministic ordering and offset/limit pagination."""
    items, total = JobService.list_jobs(
        db,
        status=job_status,
        limit=limit,
        offset=offset,
    )
    return JobListResponse(
        items=[JobResponse.model_validate(item) for item in items],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/{job_id}",
    response_model=JobResponse,
)
def get_job(
    job_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
) -> JobResponse:
    """Retrieve a single training job by ID."""
    try:
        job = JobService.get_job(db, job_id)
    except JobNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    return JobResponse.model_validate(job)


@router.post(
    "/{job_id}/cancel",
    response_model=JobResponse,
)
def cancel_job(
    job_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
) -> JobResponse:
    """Cancel a queued job, request cancellation on a running job, or return terminal state."""
    try:
        job = JobService.cancel_job(db, job_id)
    except JobNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    return JobResponse.model_validate(job)
