from __future__ import annotations

from fastapi import APIRouter, Query

from app.core.errors import ConflictError, NotFoundError
from app.seismic.schemas import ComputeRequest, EventCreate, EventPatch, ObservationCreate, TaskFailure
from app.seismic.service import DEFAULT_LEASE_SECONDS, SeismicService

router = APIRouter(prefix="/api/seismic", tags=["地震科学计算"])


def service() -> SeismicService:
    return SeismicService()


@router.post("/events", status_code=201)
def create_event(payload: EventCreate):
    try:
        return service().create_event(payload.model_dump(), actor=payload.source)
    except Exception as exc:
        if "UNIQUE" in str(exc).upper():
            raise ConflictError("external_id 已存在") from exc
        raise


@router.get("/events/{event_id}")
def get_event(event_id: int, include_observations: bool = Query(True)):
    value = service().get_event(event_id, include_observations)
    if value is None:
        raise NotFoundError("事件不存在")
    return value


@router.patch("/events/{event_id}")
def patch_event(event_id: int, payload: EventPatch):
    try:
        return service().patch_event(event_id, payload.model_dump(exclude_unset=True), actor="operator")
    except KeyError as exc:
        raise NotFoundError("事件不存在") from exc


@router.post("/events/{event_id}/observations", status_code=201)
def add_observation(event_id: int, payload: ObservationCreate):
    try:
        return service().add_observation(event_id, payload.model_dump(), actor="station")
    except KeyError as exc:
        raise NotFoundError("事件不存在") from exc


@router.post("/events/{event_id}/computations", status_code=202)
def enqueue(event_id: int, payload: ComputeRequest):
    try:
        task, deduped = service().enqueue_computation(
            event_id,
            payload.model_version,
            payload.grid_step_km,
            payload.radius_km,
            payload.requested_by,
            max_attempts=payload.max_attempts,
        )
    except NotFoundError:
        raise
    task["deduped"] = deduped
    return task


@router.get("/computations")
def list_computations(
    status: str | None = Query(default=None, pattern="^(queued|leased|retry|done|failed)$"),
    limit: int = Query(default=100, ge=1, le=500),
):
    return {"tasks": service().list_tasks(status=status, limit=limit)}


@router.post("/computations/recover")
def recover(grace_seconds: int = Query(default=0, ge=0, le=86400)):
    return service().recover_stale_leases(grace_seconds=grace_seconds)


@router.post("/computations/claim")
def claim(worker_id: str = Query(..., min_length=1), lease_seconds: int = Query(default=DEFAULT_LEASE_SECONDS, ge=1, le=86400)):
    task = service().claim_task(worker_id, lease_seconds=lease_seconds)
    return {"task": task}


@router.post("/computations/{task_id}/fail")
def fail(task_id: int, payload: TaskFailure):
    return service().fail_task(task_id, payload.worker_id, payload.error_message, retry_seconds=payload.retry_seconds)


@router.post("/computations/{task_id}/retry", status_code=202)
def retry(task_id: int, requested_by: str = Query("operator", min_length=1, max_length=80)):
    return service().retry_task(task_id, requested_by=requested_by)


@router.post("/computations/{task_id}/calculate")
def calculate(task_id: int, worker_id: str = Query(..., min_length=1)):
    return service().calculate_task(task_id, worker_id)


@router.get("/computations/{task_id}/verify")
def verify(task_id: int):
    return service().verify_task(task_id)


@router.get("/computations/{task_id}")
def get_computation(task_id: int):
    return service().get_task(task_id)
