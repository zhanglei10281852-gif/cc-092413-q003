from __future__ import annotations

import argparse
import json
import time
from typing import Any

from app.seismic.service import DEFAULT_LEASE_SECONDS, SeismicService


def _emit(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def _service() -> SeismicService:
    return SeismicService()


def command_submit(args: argparse.Namespace) -> int:
    task, created = _service().enqueue_computation(args.event_id, args.model_version, args.grid_step_km, args.radius_km, args.requested_by)
    _emit({"created": created, "task": task})
    return 0


def command_list(args: argparse.Namespace) -> int:
    tasks = _service().list_tasks(event_id=args.event_id, status=args.status, limit=args.limit)
    _emit({"tasks": tasks, "count": len(tasks)})
    return 0


def command_status(args: argparse.Namespace) -> int:
    service = _service()
    task = service.get_task(args.task_id)
    if task is None:
        _emit({"error": "task_not_found", "task_id": args.task_id})
        return 2
    _emit(service.receipt(args.task_id))
    return 0


def command_receipt(args: argparse.Namespace) -> int:
    try:
        _emit(_service().receipt(args.task_id))
    except KeyError:
        _emit({"error": "task_not_found", "task_id": args.task_id})
        return 2
    return 0


def command_verify(args: argparse.Namespace) -> int:
    try:
        report = _service().verify_result(args.task_id, include_grid=args.include_grid)
    except KeyError:
        _emit({"error": "task_not_found", "task_id": args.task_id})
        return 2
    _emit(report)
    return 0 if report["verified"] else 1


def command_claim(args: argparse.Namespace) -> int:
    task = _service().claim_task(args.worker_id, lease_seconds=args.lease_seconds)
    _emit({"task": task})
    return 0 if task else 3


def command_calculate(args: argparse.Namespace) -> int:
    try:
        _emit(_service().calculate_task(args.task_id, args.worker_id))
    except KeyError:
        _emit({"error": "task_not_owned", "task_id": args.task_id, "worker_id": args.worker_id})
        return 2
    return 0


def command_fail(args: argparse.Namespace) -> int:
    try:
        _emit(_service().fail_task(args.task_id, args.worker_id, args.error, retry_seconds=args.retry_seconds))
    except KeyError:
        _emit({"error": "task_not_owned", "task_id": args.task_id, "worker_id": args.worker_id})
        return 2
    return 0


def command_retry(args: argparse.Namespace) -> int:
    try:
        _emit(_service().retry_task(args.task_id))
    except KeyError:
        _emit({"error": "task_not_found", "task_id": args.task_id})
        return 2
    return 0


def command_recover(args: argparse.Namespace) -> int:
    count = _service().recover_expired_leases()
    _emit({"recovered": count})
    return 0


def command_worker(args: argparse.Namespace) -> int:
    """离线工作者：启动先回收过期租约，再循环领取-计算-回执，队列空后退出。"""
    service = _service()
    recovered = service.recover_expired_leases()
    processed: list[int] = []
    failed: list[dict[str, Any]] = []
    idle_rounds = 0
    while idle_rounds < args.max_idle_rounds:
        task = service.claim_task(args.worker_id, lease_seconds=args.lease_seconds)
        if task is None:
            idle_rounds += 1
            time.sleep(args.poll_interval)
            continue
        try:
            service.calculate_task(task["id"], args.worker_id)
            processed.append(task["id"])
        except Exception as exc:  # 计算失败必须显式上报，交由退避重试
            message = f"{type(exc).__name__}: {exc}"
            service.fail_task(task["id"], args.worker_id, message)
            failed.append({"task_id": task["id"], "error": message})
        idle_rounds = 0
    _emit({"worker_id": args.worker_id, "recovered_at_start": recovered, "processed": processed, "failed": failed})
    return 0


def command_seed(args: argparse.Namespace) -> int:
    """插入一次演示地震与若干台站观测，并提交一个计算任务，便于离线全流程验证。"""
    service = _service()
    event = service.create_event(
        {
            "external_id": args.external_id,
            "origin_time": "2026-09-24T08:15:00+00:00",
            "latitude": 30.10,
            "longitude": 103.20,
            "depth_km": 12.0,
            "magnitude": 5.8,
            "magnitude_type": "ML",
            "source": "demo",
        },
        actor="demo",
    )
    event_id = event["id"]
    for station, pga, pgv, distance in (("SC01", 0.82, 21.4, 18), ("SC02", 0.41, 12.0, 34), ("SC03", 0.18, 6.7, 61)):
        service.add_observation(
            event_id,
            {
                "station_code": station,
                "channel": "HNZ",
                "observed_at": "2026-09-24T08:15:03+00:00",
                "pga": pga,
                "pgv": pgv,
                "distance_km": distance,
            },
            actor="demo",
        )
    task, created = service.enqueue_computation(event_id, args.model_version, args.grid_step_km, args.radius_km, "demo")
    _emit({"event_id": event_id, "created": created, "task": task})
    return 0


def add_subparsers(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("seismic-submit", help="提交烈度网格计算任务（重复提交返回同一任务）")
    parser.add_argument("--event-id", type=int, required=True)
    parser.add_argument("--model-version", default="gmpe-2026.1")
    parser.add_argument("--grid-step-km", type=float, default=10.0)
    parser.add_argument("--radius-km", type=float, default=100.0)
    parser.add_argument("--requested-by", default="cli")
    parser.set_defaults(func=command_submit)

    parser = subparsers.add_parser("seismic-list", help="列出计算任务")
    parser.add_argument("--event-id", type=int)
    parser.add_argument("--status", choices=["queued", "leased", "retry", "done", "failed"])
    parser.add_argument("--limit", type=int, default=100)
    parser.set_defaults(func=command_list)

    parser = subparsers.add_parser("seismic-status", help="查看任务状态回执")
    parser.add_argument("task_id", type=int)
    parser.set_defaults(func=command_status)

    parser = subparsers.add_parser("seismic-receipt", help="查看任务完成回执")
    parser.add_argument("task_id", type=int)
    parser.set_defaults(func=command_receipt)

    parser = subparsers.add_parser("seismic-verify", help="校验已完成任务结果的 sha256 与元数据")
    parser.add_argument("task_id", type=int)
    parser.add_argument("--include-grid", action="store_true", help="同时输出完整网格坐标")
    parser.set_defaults(func=command_verify)

    parser = subparsers.add_parser("seismic-claim", help="手动领取一个任务（租约内可模拟崩溃恢复）")
    parser.add_argument("--worker-id", default=f"cli-{int(time.time())}")
    parser.add_argument("--lease-seconds", type=int, default=DEFAULT_LEASE_SECONDS)
    parser.set_defaults(func=command_claim)

    parser = subparsers.add_parser("seismic-calculate", help="对已领取的任务执行计算并完成")
    parser.add_argument("task_id", type=int)
    parser.add_argument("--worker-id", required=True)
    parser.set_defaults(func=command_calculate)

    parser = subparsers.add_parser("seismic-fail", help="上报任务失败（按退避重试或终态失败）")
    parser.add_argument("task_id", type=int)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--error", required=True)
    parser.add_argument("--retry-seconds", type=int)
    parser.set_defaults(func=command_fail)

    parser = subparsers.add_parser("seismic-retry", help="人工重新排队失败任务")
    parser.add_argument("task_id", type=int)
    parser.set_defaults(func=command_retry)

    parser = subparsers.add_parser("seismic-recover", help="重启恢复：回收所有过期租约")
    parser.set_defaults(func=command_recover)

    parser = subparsers.add_parser("seismic-worker", help="离线工作者：回收过期租约并排空任务队列")
    parser.add_argument("--worker-id", default=f"worker-{int(time.time())}")
    parser.add_argument("--lease-seconds", type=int, default=DEFAULT_LEASE_SECONDS)
    parser.add_argument("--poll-interval", type=float, default=0.2)
    parser.add_argument("--max-idle-rounds", type=int, default=2)
    parser.set_defaults(func=command_worker)

    parser = subparsers.add_parser("seismic-seed", help="插入演示地震、台站观测并提交计算任务")
    parser.add_argument("--external-id", default="EQ-DEMO-001")
    parser.add_argument("--model-version", default="gmpe-2026.1")
    parser.add_argument("--grid-step-km", type=float, default=20.0)
    parser.add_argument("--radius-km", type=float, default=60.0)
    parser.set_defaults(func=command_seed)
