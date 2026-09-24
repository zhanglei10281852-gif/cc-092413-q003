from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Response

from app.core.errors import ConflictError
from app.seismic.schemas import ComputeRequest, EventCreate, EventPatch, ObservationCreate, TaskFail
from app.seismic.service import SeismicService

router = APIRouter(prefix="/api/seismic", tags=["地震科学计算"])


def service() -> SeismicService:
    return SeismicService()


@router.post("/events", status_code=201)
def create_event(payload: EventCreate):
    try:
        return service().create_event(payload.model_dump(), actor=payload.source)
    except Exception as exc:
        if "UNIQUE" in str(exc).upper():
            raise HTTPException(status_code=409, detail="external_id 已存在") from exc
        raise


@router.get("/events/{event_id}")
def get_event(event_id: int, include_observations: bool = Query(True)):
    value = service().get_event(event_id, include_observations)
    if value is None:
        raise HTTPException(status_code=404, detail="事件不存在")
    return value


@router.patch("/events/{event_id}")
def patch_event(event_id: int, payload: EventPatch):
    try:
        return service().patch_event(event_id, payload.model_dump(exclude_unset=True), actor="operator")
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="事件不存在") from exc


@router.post("/events/{event_id}/observations", status_code=201)
def add_observation(event_id: int, payload: ObservationCreate):
    try:
        return service().add_observation(event_id, payload.model_dump(), actor="station")
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="事件不存在") from exc


@router.post("/events/{event_id}/computations", status_code=202)
def enqueue(response: Response, event_id: int, payload: ComputeRequest):
    try:
        task, created = service().enqueue_computation(event_id, payload.model_version, payload.grid_step_km, payload.radius_km, payload.requested_by)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="事件不存在") from exc
    # 幂等提交：重复请求命中同一行，用响应头标明这是重放而非新建
    if not created:
        response.headers["X-Idempotent-Replay"] = "1"
    return task


# 静态路径须在 {task_id} 路径之前注册，避免被当作整数解析
@router.post("/computations/recover")
def recover():
    """重启恢复：把所有过期 leased 任务退回 retry 队列。"""
    return {"recovered": service().recover_expired_leases()}


@router.get("/computations")
def list_computations(
    event_id: int | None = Query(None),
    status_filter: str | None = Query(None, alias="status", pattern="^(queued|leased|retry|done|failed)$"),
    limit: int = Query(100, ge=1, le=500),
):
    return {"tasks": service().list_tasks(event_id=event_id, status=status_filter, limit=limit)}


@router.post("/computations/claim")
def claim(worker_id: str = Query(..., min_length=1), lease_seconds: int = Query(60, ge=1, le=3600)):
    return {"task": service().claim_task(worker_id, lease_seconds=lease_seconds)}


@router.post("/computations/{task_id}/calculate")
def calculate(task_id: int, worker_id: str = Query(..., min_length=1)):
    try:
        return service().calculate_task(task_id, worker_id)
    except KeyError as exc:
        raise HTTPException(status_code=409, detail="任务不属于该工作者或不存在") from exc


@router.post("/computations/{task_id}/fail")
def fail(task_id: int, payload: TaskFail):
    try:
        return service().fail_task(task_id, payload.worker_id, payload.error_message, retry_seconds=payload.retry_seconds)
    except KeyError as exc:
        raise HTTPException(status_code=409, detail="任务不属于该工作者或不存在") from exc


@router.post("/computations/{task_id}/retry")
def retry(task_id: int):
    try:
        return service().retry_task(task_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="任务不存在") from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=exc.message) from exc


@router.get("/computations/{task_id}")
def get_computation(task_id: int):
    row = service().get_task(task_id)
    if row is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    return row


@router.get("/computations/{task_id}/receipt")
def receipt(task_id: int):
    try:
        return service().receipt(task_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="任务不存在") from exc


@router.get("/computations/{task_id}/verify")
def verify(task_id: int, include_grid: bool = Query(False, description="是否在响应中回传完整网格坐标")):
    try:
        return service().verify_result(task_id, include_grid=include_grid)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="任务不存在") from exc
