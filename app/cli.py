from __future__ import annotations

import argparse
import json
import sys

from fastapi.testclient import TestClient

from app.database import database_path, get_connection, init_db
from app.main import app
from app.seismic.service import SeismicService, ensure_schema as ensure_seismic_schema


def _print(value: dict | list) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def command_init() -> int:
    init_db()
    ensure_seismic_schema()
    print(json.dumps({"database": str(database_path()), "status": "initialized"}, ensure_ascii=False))
    return 0


def command_check() -> int:
    init_db()
    connection = get_connection()
    result = {
        "database": str(database_path()),
        "integrity": connection.execute("PRAGMA integrity_check").fetchone()[0],
        "foreign_keys": connection.execute("PRAGMA foreign_keys").fetchone()[0],
        "journal_mode": connection.execute("PRAGMA journal_mode").fetchone()[0],
        "tables": connection.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0],
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["integrity"] == "ok" and result["foreign_keys"] == 1 else 1


def command_smoke() -> int:
    with TestClient(app) as client:
        root = client.get("/")
        health = client.get("/api/system/health")
    result = {"root": root.json(), "health": health.json(), "status_codes": [root.status_code, health.status_code]}
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status_codes"] == [200, 200] else 1


# ----------------------------------------------------------------- 地震任务

def _seismic_service() -> SeismicService:
    init_db()
    ensure_seismic_schema()
    return SeismicService()


def command_seismic_list(args: argparse.Namespace) -> int:
    _print({"tasks": _seismic_service().list_tasks(status=args.status, limit=args.limit)})
    return 0


def command_seismic_status(args: argparse.Namespace) -> int:
    _print(_seismic_service().get_task(args.task_id))
    return 0


def command_seismic_verify(args: argparse.Namespace) -> int:
    report = _seismic_service().verify_task(args.task_id)
    _print(report)
    return 0 if report["valid"] else 2


def command_seismic_recover(args: argparse.Namespace) -> int:
    _print(_seismic_service().recover_stale_leases(grace_seconds=args.grace_seconds))
    return 0


def command_seismic_enqueue(args: argparse.Namespace) -> int:
    task, deduped = _seismic_service().enqueue_computation(
        args.event_id, args.model_version, args.grid_step_km, args.radius_km, args.requested_by
    )
    task["deduped"] = deduped
    _print(task)
    return 0


def command_seismic_claim(args: argparse.Namespace) -> int:
    task = _seismic_service().claim_task(args.worker_id, lease_seconds=args.lease_seconds)
    if task is None:
        _print({"task": None, "message": "没有可领取的任务"})
        return 1
    _print(task)
    return 0


def command_seismic_run(args: argparse.Namespace) -> int:
    service = _seismic_service()
    task = service.claim_task(args.worker_id, lease_seconds=args.lease_seconds)
    if task is None:
        _print({"message": "没有可领取的任务"})
        return 1
    done = service.calculate_task(task["id"], args.worker_id)
    report = service.verify_task(done["id"])
    _print({"task": done, "verification": report})
    return 0 if report["valid"] else 2


def command_seismic_fail(args: argparse.Namespace) -> int:
    _print(_seismic_service().fail_task(args.task_id, args.worker_id, args.error_message, retry_seconds=args.retry_seconds))
    return 0


def command_seismic_retry(args: argparse.Namespace) -> int:
    _print(_seismic_service().retry_task(args.task_id))
    return 0


def command_seismic_demo(args: argparse.Namespace) -> int:
    """离线端到端演示：建事件、加观测、提交、重复提交去重、领取执行、校验、重启恢复。"""
    service = _seismic_service()
    suffix = args.label
    event = service.create_event(
        {
            "external_id": f"EQ-DEMO-{suffix}",
            "origin_time": "2026-09-24T12:00:00+00:00",
            "latitude": 30.1,
            "longitude": 103.2,
            "depth_km": 12.0,
            "magnitude": 5.8,
            "magnitude_type": "ML",
            "source": "demo",
        },
        actor="demo",
    )
    event_id = event["id"]
    for code, pga, pgv, distance in (("SC01", 0.8, 2.1, 18), ("SC02", 0.4, 1.2, 42)):
        service.add_observation(
            event_id,
            {
                "station_code": code,
                "channel": "HNZ",
                "observed_at": f"2026-09-24T12:00:0{distance % 10}+00:00",
                "pga": pga,
                "pgv": pgv,
                "distance_km": distance,
            },
            actor="demo",
        )
    first, deduped_first = service.enqueue_computation(event_id, "gmpe-2026.1", 20, 60, "demo")
    second, deduped_second = service.enqueue_computation(event_id, "gmpe-2026.1", 20, 60, "demo")
    task = service.claim_task("demo-worker", lease_seconds=args.lease_seconds)
    done = service.calculate_task(task["id"], "demo-worker")
    verification = service.verify_task(done["id"])
    # 模拟工作者崩溃：领取后不完成，直接用新进程视角回收过期租约。
    crashed, _ = service.enqueue_computation(event_id, "gmpe-2026.1", 40, 60, "demo")
    leased = service.claim_task("crashed-worker", lease_seconds=0)
    recovery = service.recover_stale_leases(grace_seconds=0)
    reclaimed = service.claim_task("restart-worker", lease_seconds=60)
    _print(
        {
            "event_id": event_id,
            "submit": {"task_id": first["id"], "deduped_on_first": deduped_first},
            "duplicate_submit": {"same_task_id": second["id"], "deduped": deduped_second},
            "completed_task_id": done["id"],
            "result_meta": {
                "model_version": json.loads(done["result_json"])["model_version"],
                "input_digest": json.loads(done["result_json"])["input_digest"],
                "grid_order": json.loads(done["result_json"])["grid"]["order"],
                "shape": json.loads(done["result_json"])["grid"]["shape"],
                "count": json.loads(done["result_json"])["count"],
                "checksum": done["result_checksum"],
            },
            "verification": verification,
            "restart_recovery": {"crashed_task_id": leased["id"], "recovery": recovery, "reclaimed_task_id": reclaimed["id"]},
        }
    )
    return 0 if verification["valid"] and recovery["recovered"] == 1 else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="township-service", description="乡镇政务协同服务维护入口")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init-db", help="初始化 SQLite 数据库")
    subparsers.add_parser("check-db", help="检查数据库完整性")
    subparsers.add_parser("smoke", help="执行本地 API 冒烟检查")

    seismic = subparsers.add_parser("seismic", help="地震烈度网格计算任务（离线）")
    seismic_sub = seismic.add_subparsers(dest="seismic_command", required=True)

    p_list = seismic_sub.add_parser("list", help="列出计算任务")
    p_list.add_argument("--status", choices=["queued", "leased", "retry", "done", "failed"])
    p_list.add_argument("--limit", type=int, default=100)
    p_list.set_defaults(func=command_seismic_list)

    p_status = seismic_sub.add_parser("status", help="查看任务状态与回执")
    p_status.add_argument("task_id", type=int)
    p_status.set_defaults(func=command_seismic_status)

    p_verify = seismic_sub.add_parser("verify", help="校验结果校验和、模型版本与坐标顺序")
    p_verify.add_argument("task_id", type=int)
    p_verify.set_defaults(func=command_seismic_verify)

    p_recover = seismic_sub.add_parser("recover", help="回收过期租约（重启恢复）")
    p_recover.add_argument("--grace-seconds", dest="grace_seconds", type=int, default=0)
    p_recover.set_defaults(func=command_seismic_recover)

    p_enqueue = seismic_sub.add_parser("enqueue", help="提交计算任务")
    p_enqueue.add_argument("event_id", type=int)
    p_enqueue.add_argument("--model-version", dest="model_version", default="gmpe-2026.1")
    p_enqueue.add_argument("--grid-step-km", dest="grid_step_km", type=float, default=10)
    p_enqueue.add_argument("--radius-km", dest="radius_km", type=float, default=100)
    p_enqueue.add_argument("--requested-by", dest="requested_by", default="cli")
    p_enqueue.set_defaults(func=command_seismic_enqueue)

    p_claim = seismic_sub.add_parser("claim", help="领取一个任务")
    p_claim.add_argument("worker_id")
    p_claim.add_argument("--lease-seconds", dest="lease_seconds", type=int, default=60)
    p_claim.set_defaults(func=command_seismic_claim)

    p_run = seismic_sub.add_parser("run", help="领取并同步执行一个任务")
    p_run.add_argument("worker_id")
    p_run.add_argument("--lease-seconds", dest="lease_seconds", type=int, default=60)
    p_run.set_defaults(func=command_seismic_run)

    p_fail = seismic_sub.add_parser("fail", help="上报任务失败（可重试）")
    p_fail.add_argument("task_id", type=int)
    p_fail.add_argument("worker_id")
    p_fail.add_argument("error_message")
    p_fail.add_argument("--retry-seconds", dest="retry_seconds", type=int)
    p_fail.set_defaults(func=command_seismic_fail)

    p_retry = seismic_sub.add_parser("retry", help="将 failed 任务重新排队")
    p_retry.add_argument("task_id", type=int)
    p_retry.set_defaults(func=command_seismic_retry)

    p_demo = seismic_sub.add_parser("demo", help="端到端演示（去重、校验、重启恢复）")
    p_demo.add_argument("--label", default="001")
    p_demo.add_argument("--lease-seconds", dest="lease_seconds", type=int, default=60)
    p_demo.set_defaults(func=command_seismic_demo)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "seismic":
        return args.func(args)
    return {"init-db": command_init, "check-db": command_check, "smoke": command_smoke}[args.command]()


if __name__ == "__main__":
    sys.exit(main())
