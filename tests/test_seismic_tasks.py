from __future__ import annotations

import json
import sqlite3

import pytest

from app.core.errors import ConflictError
from app.database import close_connection, database_path
from app.seismic.service import GRID_ORDER, SeismicService


def _make_event(client, external_id: str = "EQ-TASK-001") -> int:
    response = client.post(
        "/api/seismic/events",
        json={
            "external_id": external_id,
            "origin_time": "2026-09-24T12:00:00+00:00",
            "latitude": 30.1,
            "longitude": 103.2,
            "depth_km": 12.0,
            "magnitude": 5.8,
            "magnitude_type": "ML",
            "source": "test",
        },
    )
    assert response.status_code == 201, response.text
    event_id = response.json()["id"]
    observation = client.post(
        f"/api/seismic/events/{event_id}/observations",
        json={
            "station_code": "SC01",
            "channel": "HNZ",
            "observed_at": "2026-09-24T12:00:03+00:00",
            "pga": 0.8,
            "pgv": 2.1,
            "distance_km": 18,
        },
    )
    assert observation.status_code == 201, observation.text
    return event_id


def test_duplicate_submission_returns_same_task(client):
    event_id = _make_event(client)
    body = {"model_version": "gmpe-2026.1", "grid_step_km": 20, "radius_km": 40, "requested_by": "test"}
    first = client.post(f"/api/seismic/events/{event_id}/computations", json=body)
    second = client.post(f"/api/seismic/events/{event_id}/computations", json=body)
    assert first.status_code == second.status_code == 202
    assert first.json()["id"] == second.json()["id"]
    assert first.json()["task_key"] == second.json()["task_key"]
    assert first.json()["deduped"] is False
    assert second.json()["deduped"] is True


def test_different_model_version_creates_separate_task(client):
    event_id = _make_event(client, "EQ-TASK-002")
    old = client.post(
        f"/api/seismic/events/{event_id}/computations",
        json={"model_version": "gmpe-2025.9", "grid_step_km": 20, "radius_km": 40},
    )
    new = client.post(
        f"/api/seismic/events/{event_id}/computations",
        json={"model_version": "gmpe-2026.1", "grid_step_km": 20, "radius_km": 40},
    )
    assert old.json()["id"] != new.json()["id"]
    assert old.json()["deduped"] is False and new.json()["deduped"] is False


def test_result_carries_model_version_input_summary_grid_order_and_checksum(client):
    event_id = _make_event(client, "EQ-TASK-003")
    queued = client.post(
        f"/api/seismic/events/{event_id}/computations",
        json={"model_version": "gmpe-2026.1", "grid_step_km": 20, "radius_km": 40},
    ).json()
    claimed = client.post("/api/seismic/computations/claim?worker_id=w1").json()["task"]
    assert claimed["id"] == queued["id"]
    assert claimed["attempts"] == 1
    done = client.post(f"/api/seismic/computations/{queued['id']}/calculate?worker_id=w1").json()
    result = json.loads(done["result_json"])

    assert result["model_version"] == "gmpe-2026.1"
    assert result["input_digest"] == done["input_digest"]
    assert result["input_summary"]["magnitude"] == 5.8
    assert result["input_summary"]["depth_km"] == 12.0
    assert result["input_summary"]["station_codes"] == ["SC01"]
    assert result["grid"]["order"] == GRID_ORDER
    assert result["grid"]["shape"] == [5, 5]
    assert result["count"] == 25
    assert len(result["points"]) == 25
    # 坐标顺序：纬度外循环升序、经度内循环升序。
    lats = [point["latitude"] for point in result["points"]]
    lons = [point["longitude"] for point in result["points"]]
    assert lats == sorted(lats)
    assert lons[:5] == sorted(lons[:5])
    assert done["result_checksum"] == result["checksum"]

    report = client.get(f"/api/seismic/computations/{done['id']}/verify").json()
    assert report["valid"] is True
    assert report["checks"] == {
        "checksum": True,
        "model_version": True,
        "input_digest": True,
        "grid_order": True,
        "point_count": True,
    }


def test_tampered_result_fails_verification(client):
    event_id = _make_event(client, "EQ-TASK-004")
    client.post(
        f"/api/seismic/events/{event_id}/computations",
        json={"model_version": "gmpe-2026.1", "grid_step_km": 20, "radius_km": 40},
    )
    task = client.post("/api/seismic/computations/claim?worker_id=w1").json()["task"]
    done = client.post(f"/api/seismic/computations/{task['id']}/calculate?worker_id=w1").json()

    # 外部篡改结果内容但不更新校验和。
    connection = SeismicService().connection
    result = json.loads(done["result_json"])
    result["points"][0]["intensity"] = 99.9
    connection.execute(
        "UPDATE seismic_computations SET result_json=? WHERE id=?",
        (json.dumps(result), done["id"]),
    )
    report = client.get(f"/api/seismic/computations/{done['id']}/verify").json()
    assert report["valid"] is False
    assert report["checks"]["checksum"] is False


def test_failure_retries_with_backoff_then_failed_and_manual_retry(client):
    event_id = _make_event(client, "EQ-TASK-005")
    queued = client.post(
        f"/api/seismic/events/{event_id}/computations",
        json={"model_version": "m", "grid_step_km": 20, "radius_km": 40, "max_attempts": 2},
    ).json()

    # 第一次领取并失败，立即重试（retry_seconds=0）。
    first = client.post("/api/seismic/computations/claim?worker_id=w1").json()["task"]
    assert first["id"] == queued["id"]
    failed = client.post(
        f"/api/seismic/computations/{first['id']}/fail",
        json={"worker_id": "w1", "error_message": "boom", "retry_seconds": 0},
    ).json()
    assert failed["status"] == "retry"
    assert failed["attempts"] == 1
    assert failed["error_message"] == "boom"

    # 其他工作者不能替别人上报失败。
    forbidden = client.post(
        f"/api/seismic/computations/{first['id']}/fail",
        json={"worker_id": "intruder", "error_message": "x"},
    )
    assert forbidden.status_code == 409

    # 第二次失败后达到 max_attempts，任务终止为 failed。
    second = client.post("/api/seismic/computations/claim?worker_id=w2").json()["task"]
    assert second["attempts"] == 2
    terminal = client.post(
        f"/api/seismic/computations/{second['id']}/fail",
        json={"worker_id": "w2", "error_message": "boom again", "retry_seconds": 0},
    ).json()
    assert terminal["status"] == "failed"

    # failed 任务不会再被领取，人工重试后重新入队并清零计数。
    assert client.post("/api/seismic/computations/claim?worker_id=w3").json()["task"] is None
    retried = client.post(f"/api/seismic/computations/{second['id']}/retry?requested_by=ops").json()
    assert retried["status"] == "queued"
    assert retried["attempts"] == 0
    reclaimed = client.post("/api/seismic/computations/claim?worker_id=w3").json()["task"]
    assert reclaimed["id"] == second["id"]


def test_backoff_makes_task_unavailable_until_delay(client):
    event_id = _make_event(client, "EQ-TASK-006")
    client.post(
        f"/api/seismic/events/{event_id}/computations",
        json={"model_version": "m", "grid_step_km": 20, "radius_km": 40},
    )
    task = client.post("/api/seismic/computations/claim?worker_id=w1").json()["task"]
    client.post(
        f"/api/seismic/computations/{task['id']}/fail",
        json={"worker_id": "w1", "error_message": "transient", "retry_seconds": 3600},
    )
    # 退避窗口内领取不到任何任务。
    assert client.post("/api/seismic/computations/claim?worker_id=w2").json()["task"] is None


def test_stale_lease_is_recovered_after_restart(client):
    event_id = _make_event(client, "EQ-TASK-007")
    client.post(
        f"/api/seismic/events/{event_id}/computations",
        json={"model_version": "m", "grid_step_km": 20, "radius_km": 40},
    )
    # 工作者领取后崩溃：直接用 0 秒租约模拟租约立即过期。
    crashed = SeismicService().claim_task("crashed", lease_seconds=0)
    assert crashed["status"] == "leased"
    leased_id = crashed["id"]

    # 模拟服务重启：打开一个全新的数据库连接执行恢复。
    close_connection()
    fresh = sqlite3.connect(database_path())
    fresh.row_factory = sqlite3.Row
    restarted = SeismicService(fresh)
    report = restarted.recover_stale_leases(grace_seconds=0)
    assert report == {"recovered": 1, "task_ids": [leased_id]}
    reclaimed = restarted.claim_task("restart-worker", lease_seconds=60)
    assert reclaimed is not None
    assert reclaimed["id"] == leased_id
    assert reclaimed["attempts"] == 2
    fresh.close()

    # claim 自身也会顺带回收过期租约，无需先调 recover。
    close_connection()
    fresh2 = sqlite3.connect(database_path())
    fresh2.row_factory = sqlite3.Row
    svc2 = SeismicService(fresh2)
    svc2.fail_task(reclaimed["id"], "restart-worker", "again", retry_seconds=0)
    leased2 = svc2.claim_task("crashed-again", lease_seconds=0)
    assert leased2["status"] == "leased"
    auto = svc2.claim_task("next-worker", lease_seconds=0)
    assert auto is not None and auto["id"] == leased2["id"]
    fresh2.close()


def test_completion_rejects_newer_model_version_overwrite(client):
    event_id = _make_event(client, "EQ-TASK-008")
    client.post(
        f"/api/seismic/events/{event_id}/computations",
        json={"model_version": "gmpe-2026.1", "grid_step_km": 20, "radius_km": 40},
    )
    service = SeismicService()
    task = service.claim_task("w1", lease_seconds=60)

    # 旧工作者回传旧模型版本的结果，必须被拒绝且任务不变成 done。
    stale_result = {"model_version": "gmpe-2025.9", "input_digest": task["input_digest"], "points": []}
    with pytest.raises(ConflictError):
        service.complete_task(task["id"], "w1", stale_result)
    row = service.get_task(task["id"])
    assert row["status"] == "leased"
    assert row["result_checksum"] == ""

    # 输入摘要不匹配同样拒绝。
    with pytest.raises(ConflictError):
        service.complete_task(
            task["id"],
            "w1",
            {"model_version": "gmpe-2026.1", "input_digest": "deadbeef", "points": []},
        )

    # 正确完成后再次完成是幂等冲突，不会覆盖既有回执。
    done = service.calculate_task(task["id"], "w1")
    assert done["status"] == "done"
    with pytest.raises(ConflictError):
        service.complete_task(task["id"], "w1", json.loads(done["result_json"]))


def test_status_and_list_endpoints(client):
    event_id = _make_event(client, "EQ-TASK-009")
    queued = client.post(
        f"/api/seismic/events/{event_id}/computations",
        json={"model_version": "m", "grid_step_km": 20, "radius_km": 40},
    ).json()
    by_id = client.get(f"/api/seismic/computations/{queued['id']}")
    assert by_id.status_code == 200
    assert by_id.json()["status"] == "queued"

    listing = client.get("/api/seismic/computations?status=queued")
    assert any(item["id"] == queued["id"] for item in listing.json()["tasks"])
    done_listing = client.get("/api/seismic/computations?status=done")
    assert all(item["status"] == "done" for item in done_listing.json()["tasks"])
