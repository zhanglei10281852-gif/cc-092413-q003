from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.core.clock import FrozenClock
from app.seismic.service import SeismicService


def event_payload(external_id: str = "EQ-TEST-001"):
    return {
        "external_id": external_id,
        "origin_time": "2026-09-24T12:00:00+00:00",
        "latitude": 30.1,
        "longitude": 103.2,
        "depth_km": 12.0,
        "magnitude": 5.8,
        "magnitude_type": "ML",
        "source": "test",
    }


def _create_ready_task(client, external_id="EQ-TEST-001", model_version="test-1", step=20, radius=20):
    event_id = client.post("/api/seismic/events", json=event_payload(external_id)).json()["id"]
    obs = client.post(
        f"/api/seismic/events/{event_id}/observations",
        json={"station_code": "SC01", "channel": "HNZ", "observed_at": "2026-09-24T12:00:03+00:00", "pga": 0.8, "pgv": 2.1, "distance_km": 18},
    )
    assert obs.status_code == 201
    task = client.post(
        f"/api/seismic/events/{event_id}/computations",
        json={"model_version": model_version, "grid_step_km": step, "radius_km": radius, "requested_by": "test"},
    )
    assert task.status_code == 202
    return event_id, task.json()


def test_seismic_event_observation_and_computation(client):
    event_id, task_row = _create_ready_task(client)
    claimed = client.post("/api/seismic/computations/claim?worker_id=test-worker")
    assert claimed.status_code == 200
    task_id = claimed.json()["task"]["id"]
    result = client.post(f"/api/seismic/computations/{task_id}/calculate?worker_id=test-worker")
    assert result.status_code == 200
    assert result.json()["status"] == "done"
    assert result.json()["result_json"]


def test_duplicate_observation_is_idempotent(client):
    event = client.post("/api/seismic/events", json={**event_payload(), "external_id": "EQ-TEST-002"}).json()
    payload = {"station_code": "SC02", "channel": "HNZ", "observed_at": "2026-09-24T12:00:03+00:00", "pga": 0.8, "distance_km": 18}
    first = client.post(f"/api/seismic/events/{event['id']}/observations", json=payload)
    second = client.post(f"/api/seismic/events/{event['id']}/observations", json=payload)
    assert first.status_code == second.status_code == 201
    assert first.json()["id"] == second.json()["id"]


def test_duplicate_submission_returns_same_task_and_header(client):
    _, first = _create_ready_task(client, "EQ-TEST-010")
    second = client.post(
        f"/api/seismic/events/{first['event_id']}/computations",
        json={"model_version": "test-1", "grid_step_km": 20, "radius_km": 20, "requested_by": "test"},
    )
    assert second.status_code == 202
    assert second.json()["id"] == first["id"]
    assert second.json()["task_key"] == first["task_key"]
    assert second.headers["x-idempotent-replay"] == "1"
    # 完成后重复提交仍然只返回同一行，不产生第二份数据
    claimed = client.post("/api/seismic/computations/claim?worker_id=w1").json()["task"]
    client.post(f"/api/seismic/computations/{claimed['id']}/calculate?worker_id=w1")
    third = client.post(
        f"/api/seismic/events/{first['event_id']}/computations",
        json={"model_version": "test-1", "grid_step_km": 20, "radius_km": 20, "requested_by": "test"},
    )
    assert third.json()["id"] == first["id"]
    assert third.json()["status"] == "done"
    listing = client.get(f"/api/seismic/computations?event_id={first['event_id']}").json()["tasks"]
    assert len(listing) == 1


def test_result_envelope_carries_model_digest_and_grid_order_and_verifies(client):
    _, task_row = _create_ready_task(client, "EQ-TEST-020")
    claimed = client.post("/api/seismic/computations/claim?worker_id=w1").json()["task"]
    done = client.post(f"/api/seismic/computations/{claimed['id']}/calculate?worker_id=w1").json()
    assert done["result_checksum"]
    assert done["completed_at"]

    import json

    result = json.loads(done["result_json"])
    assert result["model_version"] == "test-1"
    assert result["input_digest"] == task_row["input_digest"]
    assert result["input_summary"]["observation_count"] == 1
    grid = result["grid"]
    assert grid["axis_order"] == "lat,lon"
    assert grid["scan_order"] == "lat-asc/lon-asc"
    assert grid["shape"][0] * grid["shape"][1] == result["count"]
    # 行主序：每个纬度带内经度单调不减
    rows = grid["shape"][1]
    for i in range(grid["shape"][0]):
        band = grid["points"][i * rows : (i + 1) * rows]
        assert [p["lon"] for p in band] == sorted(p["lon"] for p in band)

    verify = client.get(f"/api/seismic/computations/{claimed['id']}/verify?include_grid=true")
    assert verify.status_code == 200
    body = verify.json()
    assert body["verified"] is True
    assert body["axis_order"] == "lat,lon"
    assert body["scan_order"] == "lat-asc/lon-asc"
    assert body["stored_checksum"] == body["actual_checksum"] == done["result_checksum"]
    assert body["shape"] == grid["shape"]

    receipt = client.get(f"/api/seismic/computations/{claimed['id']}/receipt").json()
    assert receipt["status"] == "done"
    assert receipt["result_count"] == result["count"]
    assert receipt["model_version"] == "test-1"


def test_new_model_version_creates_independent_task_and_keeps_old_result(client):
    event_id, old_task = _create_ready_task(client, "EQ-TEST-030", model_version="model-v1")
    claimed = client.post("/api/seismic/computations/claim?worker_id=w1").json()["task"]
    old_done = client.post(f"/api/seismic/computations/{claimed['id']}/calculate?worker_id=w1").json()

    new_task = client.post(
        f"/api/seismic/events/{event_id}/computations",
        json={"model_version": "model-v2", "grid_step_km": 20, "radius_km": 20, "requested_by": "test"},
    ).json()
    assert new_task["id"] != old_task["id"]
    assert new_task["task_key"] != old_task["task_key"]
    claimed2 = client.post("/api/seismic/computations/claim?worker_id=w2").json()["task"]
    new_done = client.post(f"/api/seismic/computations/{claimed2['id']}/calculate?worker_id=w2").json()

    # 新旧版本互不覆盖，各自校验通过
    old = client.get(f"/api/seismic/computations/{old_task['id']}").json()
    assert old["status"] == "done"
    assert client.get(f"/api/seismic/computations/{old_task['id']}/verify").json()["verified"] is True
    assert client.get(f"/api/seismic/computations/{new_done['id']}/verify").json()["verified"] is True
    import json

    assert json.loads(old["result_json"])["model_version"] == "model-v1"
    assert json.loads(new_done["result_json"])["model_version"] == "model-v2"


def test_failure_retries_with_backoff_then_fails_permanently(client):
    _, task_row = _create_ready_task(client, "EQ-TEST-040")
    task_id = task_row["id"]
    # 超过最大尝试次数后进入终态 failed：max_attempts 默认 5
    for attempt in range(1, 6):
        claimed = client.post("/api/seismic/computations/claim?worker_id=w1").json()["task"]
        assert claimed["attempts"] == attempt
        failed = client.post(
            f"/api/seismic/computations/{task_id}/fail",
            json={"worker_id": "w1", "error_message": f"boom {attempt}", "retry_seconds": 0},
        )
        assert failed.status_code == 200
    final = client.get(f"/api/seismic/computations/{task_id}").json()
    assert final["status"] == "failed"
    assert final["attempts"] == 5
    assert "boom 5" in final["error_message"]

    # 终态失败后人工 retry：重新排队、重置预算，成功完成
    retried = client.post(f"/api/seismic/computations/{task_id}/retry")
    assert retried.status_code == 200
    assert retried.json()["status"] == "queued"
    claimed = client.post("/api/seismic/computations/claim?worker_id=w2").json()["task"]
    assert claimed["attempts"] == 1
    done = client.post(f"/api/seismic/computations/{task_id}/calculate?worker_id=w2")
    assert done.status_code == 200
    assert done.json()["status"] == "done"
    assert done.json()["error_message"] == ""


def test_retry_is_refused_for_done_task(client):
    _, _ = _create_ready_task(client, "EQ-TEST-050")
    claimed = client.post("/api/seismic/computations/claim?worker_id=w1").json()["task"]
    client.post(f"/api/seismic/computations/{claimed['id']}/calculate?worker_id=w1")
    refused = client.post(f"/api/seismic/computations/{claimed['id']}/retry")
    assert refused.status_code == 409


def test_lease_expiry_is_recovered_after_restart(client):
    clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=UTC))
    service = SeismicService(clock=clock)
    _, task_row = _create_ready_task(client, "EQ-TEST-060")
    task_id = task_row["id"]

    # 工作者 A 领取后崩溃，租约 60 秒
    leased = service.claim_task("worker-A", lease_seconds=60)
    assert leased and leased["lease_owner"] == "worker-A"
    # 服务重启：先调用恢复接口（此时租约未过期，不应回收）
    assert service.recover_expired_leases() == 0
    assert service.get_task(task_id)["status"] == "leased"

    # 时间越过租约到期点（模拟进程死亡很久后重启）
    clock.advance(seconds=61)
    assert service.recover_expired_leases() == 1
    row = service.get_task(task_id)
    assert row["status"] == "retry"
    assert row["lease_owner"] == ""

    # B 重新领取并完成；A 的迟到完成回执必须被拒绝
    reclaimed = service.claim_task("worker-B", lease_seconds=60)
    assert reclaimed["attempts"] == 2
    with pytest.raises(KeyError):
        service.calculate_task(task_id, "worker-A")
    done = service.calculate_task(task_id, "worker-B")
    assert done["status"] == "done"
    assert client.get(f"/api/seismic/computations/{task_id}/verify").json()["verified"] is True


def test_claim_skips_tasks_not_yet_available(client):
    clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=UTC))
    service = SeismicService(clock=clock)
    _, task_row = _create_ready_task(client, "EQ-TEST-070")
    leased = service.claim_task("w1", lease_seconds=60)
    failed = service.fail_task(leased["id"], "w1", "transient", retry_seconds=30)
    assert failed["status"] == "retry"
    assert service.claim_task("w1") is None
    clock.advance(seconds=31)
    reclaimed = service.claim_task("w1")
    assert reclaimed is not None and reclaimed["id"] == task_row["id"]


def test_resubmit_failed_task_requeues_same_row(client):
    _, task_row = _create_ready_task(client, "EQ-TEST-080")
    task_id = task_row["id"]
    for _ in range(5):
        claimed = client.post("/api/seismic/computations/claim?worker_id=w1").json()["task"]
        client.post(f"/api/seismic/computations/{task_id}/fail", json={"worker_id": "w1", "error_message": "x", "retry_seconds": 0})
    assert client.get(f"/api/seismic/computations/{task_id}").json()["status"] == "failed"
    # 以原始请求重新提交：同一行复活，不新增任务
    resubmitted = client.post(
        f"/api/seismic/events/{task_row['event_id']}/computations",
        json={"model_version": "test-1", "grid_step_km": 20, "radius_km": 20, "requested_by": "test"},
    ).json()
    assert resubmitted["id"] == task_id
    assert resubmitted["status"] == "queued"
    assert resubmitted["attempts"] == 0


def test_recover_endpoint_and_listing(client):
    _, task_row = _create_ready_task(client, "EQ-TEST-090")
    recovered = client.post("/api/seismic/computations/recover")
    assert recovered.status_code == 200
    assert recovered.json() == {"recovered": 0}
    listing = client.get("/api/seismic/computations?status=queued").json()["tasks"]
    assert [t["id"] for t in listing] == [task_row["id"]]
    assert client.get("/api/seismic/computations?status=done").json()["tasks"] == []
