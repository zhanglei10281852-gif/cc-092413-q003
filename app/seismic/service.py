from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

from app.core.errors import ConflictError, NotFoundError
from app.database import get_connection


# 烈度网格结果的坐标排列顺序：纬度外循环升序，经度内循环升序（行优先）。
GRID_ORDER = "lat_asc_lon_asc"
DEFAULT_LEASE_SECONDS = 60
DEFAULT_MAX_ATTEMPTS = 5

SCHEMA = """
CREATE TABLE IF NOT EXISTS seismic_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT NOT NULL UNIQUE,
    origin_time TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    depth_km REAL NOT NULL,
    magnitude REAL NOT NULL,
    magnitude_type TEXT NOT NULL,
    source TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft' CHECK(status IN ('draft','review','published','archived')),
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS seismic_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL REFERENCES seismic_events(id) ON DELETE RESTRICT,
    station_code TEXT NOT NULL,
    channel TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    pga REAL,
    pgv REAL,
    distance_km REAL NOT NULL,
    quality_score REAL NOT NULL DEFAULT 0,
    quality_status TEXT NOT NULL DEFAULT 'pending',
    quality_reason TEXT NOT NULL DEFAULT '',
    source_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(event_id, station_code, channel, observed_at)
);
CREATE TABLE IF NOT EXISTS seismic_computations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL REFERENCES seismic_events(id) ON DELETE RESTRICT,
    task_key TEXT NOT NULL UNIQUE,
    model_version TEXT NOT NULL,
    input_digest TEXT NOT NULL,
    grid_step_km REAL NOT NULL,
    radius_km REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','leased','retry','done','failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    available_at TEXT NOT NULL DEFAULT '',
    lease_owner TEXT NOT NULL DEFAULT '',
    lease_until TEXT NOT NULL DEFAULT '',
    result_json TEXT NOT NULL DEFAULT '{}',
    result_checksum TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    completed_at TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS seismic_event_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    before_json TEXT NOT NULL DEFAULT '{}',
    after_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_seismic_obs_event ON seismic_observations(event_id, observed_at);
CREATE INDEX IF NOT EXISTS idx_seismic_tasks_status ON seismic_computations(status, created_at);
"""

# 旧版本库的增量列：离线重启后自动补齐，不破坏既有数据。
_MIGRATIONS = (
    ("max_attempts", "ALTER TABLE seismic_computations ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 5"),
    ("available_at", "ALTER TABLE seismic_computations ADD COLUMN available_at TEXT NOT NULL DEFAULT ''"),
    ("result_checksum", "ALTER TABLE seismic_computations ADD COLUMN result_checksum TEXT NOT NULL DEFAULT ''"),
    ("completed_at", "ALTER TABLE seismic_computations ADD COLUMN completed_at TEXT NOT NULL DEFAULT ''"),
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime) -> str:
    return value.isoformat(timespec="seconds")


def ensure_schema(connection: sqlite3.Connection | None = None) -> None:
    connection = connection or get_connection()
    connection.executescript(SCHEMA)
    existing = {row["name"] for row in connection.execute("PRAGMA table_info(seismic_computations)")}
    for column, statement in _MIGRATIONS:
        if column not in existing:
            connection.execute(statement)


def _row(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


def _event_digest(event: sqlite3.Row, observations: list[sqlite3.Row]) -> str:
    payload = {
        "event": dict(event),
        "observations": [dict(item) for item in observations],
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def _canonical_digest(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    return hashlib.sha256(blob).hexdigest()


def _quality(observation: dict[str, Any]) -> tuple[float, str, str]:
    reasons: list[str] = []
    score = 1.0
    if observation.get("pga") is None and observation.get("pgv") is None:
        score = 0.0
        reasons.append("缺少峰值指标")
    if observation.get("pga") is not None and observation["pga"] > 20:
        score -= 0.6
        reasons.append("PGA 超出量程")
    if observation.get("pgv") is not None and observation["pgv"] > 300:
        score -= 0.4
        reasons.append("PGV 超出量程")
    if observation.get("distance_km", 0) == 0:
        score -= 0.2
        reasons.append("距离为零")
    score = max(0.0, min(1.0, round(score, 3)))
    status = "accepted" if score >= 0.6 else "rejected"
    return score, status, "、".join(reasons) if reasons else "通过基础质量检查"


@dataclass(frozen=True)
class GridPoint:
    latitude: float
    longitude: float
    intensity: float


class SeismicService:
    """事件、观测和烈度网格计算任务的事务服务。

    任务状态机：queued/retry -> leased -> done | failed；
    leased 任务租约过期后可被安全回收重新领取（崩溃/重启恢复）。
    """

    def __init__(self, connection: sqlite3.Connection | None = None):
        self.connection = connection or get_connection()
        ensure_schema(self.connection)

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        connection = self.connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield connection
        except Exception:
            connection.rollback()
            raise
        else:
            connection.commit()

    # ------------------------------------------------------------------ 事件

    def create_event(self, payload: dict[str, Any], actor: str = "system") -> dict[str, Any]:
        now = _stamp(_now())
        with self._tx() as connection:
            cursor = connection.execute(
                "INSERT INTO seismic_events(external_id,origin_time,latitude,longitude,depth_km,magnitude,magnitude_type,source,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (payload["external_id"], payload["origin_time"], payload["latitude"], payload["longitude"], payload["depth_km"], payload["magnitude"], payload["magnitude_type"], payload["source"], now, now),
            )
            event_id = cursor.lastrowid
            connection.execute("INSERT INTO seismic_event_audit(event_id,action,actor,after_json,created_at) VALUES(?,?,?,?,?)", (event_id, "create", actor, json.dumps(payload, ensure_ascii=False), now))
            return _row(connection.execute("SELECT * FROM seismic_events WHERE id=?", (event_id,)).fetchone()) or {}

    def get_event(self, event_id: int, include_observations: bool = True) -> dict[str, Any] | None:
        event = self.connection.execute("SELECT * FROM seismic_events WHERE id=?", (event_id,)).fetchone()
        if event is None:
            return None
        result = _row(event) or {}
        if include_observations:
            rows = self.connection.execute("SELECT * FROM seismic_observations WHERE event_id=? ORDER BY observed_at, id", (event_id,)).fetchall()
            result["observations"] = [dict(item) for item in rows]
        return result

    def patch_event(self, event_id: int, payload: dict[str, Any], actor: str = "system") -> dict[str, Any]:
        with self._tx() as connection:
            current = connection.execute("SELECT * FROM seismic_events WHERE id=?", (event_id,)).fetchone()
            if current is None:
                raise KeyError("event_not_found")
            before = dict(current)
            values = {key: value for key, value in payload.items() if key in {"depth_km", "magnitude", "magnitude_type", "status"} and value is not None}
            if not values:
                return before
            assignments = ", ".join(f"{key}=?" for key in values)
            now = _stamp(_now())
            connection.execute(f"UPDATE seismic_events SET {assignments}, version=version+1, updated_at=? WHERE id=?", (*values.values(), now, event_id))
            after = connection.execute("SELECT * FROM seismic_events WHERE id=?", (event_id,)).fetchone()
            connection.execute("INSERT INTO seismic_event_audit(event_id,action,actor,before_json,after_json,created_at) VALUES(?,?,?,?,?,?)", (event_id, "patch:" + payload.get("reason", ""), actor, json.dumps(before, ensure_ascii=False), json.dumps(dict(after), ensure_ascii=False), now))
            return dict(after)

    def add_observation(self, event_id: int, payload: dict[str, Any], actor: str = "system") -> dict[str, Any]:
        event = self.connection.execute("SELECT id FROM seismic_events WHERE id=?", (event_id,)).fetchone()
        if event is None:
            raise KeyError("event_not_found")
        quality_score, quality_status, quality_reason = _quality(payload)
        now = _stamp(_now())
        source_hash = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        with self._tx() as connection:
            try:
                cursor = connection.execute(
                    "INSERT INTO seismic_observations(event_id,station_code,channel,observed_at,pga,pgv,distance_km,quality_score,quality_status,quality_reason,source_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (event_id, payload["station_code"], payload["channel"], payload["observed_at"], payload.get("pga"), payload.get("pgv"), payload["distance_km"], quality_score, quality_status, quality_reason, source_hash, now),
                )
            except sqlite3.IntegrityError:
                existing = connection.execute("SELECT * FROM seismic_observations WHERE event_id=? AND station_code=? AND channel=? AND observed_at=?", (event_id, payload["station_code"], payload["channel"], payload["observed_at"])).fetchone()
                return dict(existing) if existing else {}
            return dict(connection.execute("SELECT * FROM seismic_observations WHERE id=?", (cursor.lastrowid,)).fetchone())

    # ------------------------------------------------------------------ 网格

    def _grid(self, event: sqlite3.Row, observations: list[sqlite3.Row], step: float, radius: float) -> tuple[list[GridPoint], tuple[int, int]]:
        """按 GRID_ORDER 生成网格：纬度外循环升序、经度内循环升序。"""
        center_lat, center_lon = float(event["latitude"]), float(event["longitude"])
        radius_deg = radius / 111.0
        count = max(1, int(math.floor((radius * 2) / step)))
        # 简化 GMPE 深度衰减项：震源越深，地表烈度估计越低。
        depth_term = -0.004 * float(event["depth_km"])
        accepted = [item for item in observations if item["quality_status"] == "accepted"]
        points: list[GridPoint] = []
        for lat_index in range(count + 1):
            lat = center_lat - radius_deg + lat_index * (step / 111.0)
            for lon_index in range(count + 1):
                lon = center_lon - radius_deg + lon_index * (step / 111.0) / max(0.2, math.cos(math.radians(lat)))
                values = []
                for item in accepted:
                    distance = math.hypot((lat - center_lat) * 111, (lon - center_lon) * 111 * max(0.2, math.cos(math.radians(lat))))
                    weight = 1 / max(1, abs(distance - float(item["distance_km"])))
                    estimate = (
                        float(event["magnitude"])
                        - math.log10(max(1, float(item["distance_km"])))
                        + depth_term
                        + (float(item["pga"] or 0) * 0.01)
                    )
                    values.append((estimate * weight, weight))
                if values:
                    intensity = sum(value for value, _ in values) / sum(weight for _, weight in values)
                else:
                    intensity = float(event["magnitude"]) - 1 + depth_term
                points.append(GridPoint(round(lat, 6), round(lon, 6), round(intensity, 3)))
        return points, (count + 1, count + 1)

    # ------------------------------------------------------------------ 任务

    def _load_event_inputs(self, connection: sqlite3.Connection, event_id: int) -> tuple[sqlite3.Row, list[sqlite3.Row]]:
        event = connection.execute("SELECT * FROM seismic_events WHERE id=?", (event_id,)).fetchone()
        if event is None:
            raise NotFoundError("事件不存在")
        observations = connection.execute(
            "SELECT * FROM seismic_observations WHERE event_id=? ORDER BY id", (event_id,)
        ).fetchall()
        return event, observations

    def enqueue_computation(
        self,
        event_id: int,
        model_version: str,
        grid_step_km: float,
        radius_km: float,
        requested_by: str,
        *,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> tuple[dict[str, Any], bool]:
        """提交计算任务。相同输入摘要+模型版本+网格参数返回同一任务（幂等）。"""
        event, observations = self._load_event_inputs(self.connection, event_id)
        digest = _event_digest(event, observations)
        # 统一转 float，避免 20（int）与 20.0（命令行 float）生成不同任务键。
        grid_step_km = float(grid_step_km)
        radius_km = float(radius_km)
        task_key = _canonical_digest(
            {
                "event_id": event_id,
                "input_digest": digest,
                "model_version": model_version,
                "grid_step_km": grid_step_km,
                "radius_km": radius_km,
            }
        )
        now = _stamp(_now())
        with self._tx() as connection:
            existing = connection.execute("SELECT * FROM seismic_computations WHERE task_key=?", (task_key,)).fetchone()
            if existing:
                return dict(existing), True
            cursor = connection.execute(
                "INSERT INTO seismic_computations(event_id,task_key,model_version,input_digest,grid_step_km,radius_km,"
                "status,max_attempts,available_at,created_at,updated_at) VALUES(?,?,?,?,?,?, 'queued',?,?,?,?)",
                (event_id, task_key, model_version, digest, grid_step_km, radius_km, max_attempts, now, now, now),
            )
            connection.execute(
                "INSERT INTO seismic_event_audit(event_id,action,actor,after_json,created_at) VALUES(?,?,?,?,?)",
                (event_id, "compute.enqueue", requested_by,
                 json.dumps({"task_key": task_key, "model_version": model_version}, ensure_ascii=False), now),
            )
            task = connection.execute("SELECT * FROM seismic_computations WHERE id=?", (cursor.lastrowid,)).fetchone()
            return dict(task), False

    def list_tasks(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if status:
            rows = self.connection.execute(
                "SELECT * FROM seismic_computations WHERE status=? ORDER BY id DESC LIMIT ?", (status, limit)
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM seismic_computations ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(row) for row in rows]

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM seismic_computations WHERE id=?", (task_id,)).fetchone()
        if row is None:
            raise NotFoundError("任务不存在")
        return dict(row)

    def recover_stale_leases(self, *, grace_seconds: int = 0) -> dict[str, Any]:
        """回收租约过期的 leased 任务（工作者崩溃或服务重启后的恢复入口）。

        grace_seconds 为到期后的额外宽限时间；lease_until 早于 now-grace 即回收。
        """
        moment = _now()
        threshold = _stamp(moment - timedelta(seconds=grace_seconds))
        now = _stamp(moment)
        with self._tx() as connection:
            stale = connection.execute(
                "SELECT id FROM seismic_computations WHERE status='leased' AND lease_until<=? ORDER BY id",
                (threshold,),
            ).fetchall()
            ids = [row["id"] for row in stale]
            if ids:
                connection.execute(
                    "UPDATE seismic_computations SET status='retry', lease_owner='', lease_until='', "
                    "available_at=?, updated_at=? WHERE status='leased' AND lease_until<=?",
                    (now, now, threshold),
                )
        return {"recovered": len(ids), "task_ids": ids}

    def claim_task(self, worker_id: str, *, lease_seconds: int = DEFAULT_LEASE_SECONDS) -> dict[str, Any] | None:
        """领取一个可执行任务；领取前自动回收已到期租约（lease_until <= 当前时刻）。"""
        moment = _now()
        now = _stamp(moment)
        lease_until = _stamp(moment + timedelta(seconds=lease_seconds))
        with self._tx() as connection:
            connection.execute(
                "UPDATE seismic_computations SET status='retry', lease_owner='', lease_until='', updated_at=? "
                "WHERE status='leased' AND lease_until<=?",
                (now, now),
            )
            task = connection.execute(
                "SELECT * FROM seismic_computations WHERE status IN ('queued','retry') "
                "AND (available_at='' OR available_at<=?) ORDER BY created_at,id LIMIT 1",
                (now,),
            ).fetchone()
            if task is None:
                return None
            cursor = connection.execute(
                "UPDATE seismic_computations SET status='leased', attempts=attempts+1, lease_owner=?, "
                "lease_until=?, updated_at=? WHERE id=? AND status IN ('queued','retry') "
                "AND (available_at='' OR available_at<=?)",
                (worker_id, lease_until, now, task["id"], now),
            )
            if cursor.rowcount != 1:
                return None
            return dict(connection.execute("SELECT * FROM seismic_computations WHERE id=?", (task["id"],)).fetchone())

    def fail_task(
        self,
        task_id: int,
        worker_id: str,
        error_message: str,
        *,
        retry_seconds: int | None = None,
    ) -> dict[str, Any]:
        """上报失败：未超过 max_attempts 回到 retry（指数退避），否则置 failed。"""
        moment = _now()
        now = _stamp(moment)
        with self._tx() as connection:
            task = connection.execute("SELECT * FROM seismic_computations WHERE id=?", (task_id,)).fetchone()
            if task is None or task["status"] != "leased" or task["lease_owner"] != worker_id:
                raise ConflictError("任务不存在或未被该工作者持有")
            if retry_seconds is None:
                retry_seconds = min(60, 5 * (2 ** max(0, task["attempts"] - 1)))
            give_up = task["attempts"] >= task["max_attempts"]
            if give_up:
                status, available_at = "failed", now
            else:
                status, available_at = "retry", _stamp(moment + timedelta(seconds=retry_seconds))
            connection.execute(
                "UPDATE seismic_computations SET status=?, error_message=?, available_at=?, lease_owner='', "
                "lease_until='', updated_at=? WHERE id=?",
                (status, error_message[:1000], available_at, now, task_id),
            )
            return dict(connection.execute("SELECT * FROM seismic_computations WHERE id=?", (task_id,)).fetchone())

    def retry_task(self, task_id: int, *, requested_by: str = "operator") -> dict[str, Any]:
        """人工将 failed 任务重新排队（重置尝试计数与退避）。"""
        now = _stamp(_now())
        with self._tx() as connection:
            task = connection.execute("SELECT * FROM seismic_computations WHERE id=?", (task_id,)).fetchone()
            if task is None:
                raise NotFoundError("任务不存在")
            if task["status"] not in {"failed", "retry"}:
                raise ConflictError(f"状态为 {task['status']} 的任务不能重新排队")
            connection.execute(
                "UPDATE seismic_computations SET status='queued', attempts=0, available_at=?, error_message='', "
                "lease_owner='', lease_until='', updated_at=? WHERE id=?",
                (now, now, task_id),
            )
            connection.execute(
                "INSERT INTO seismic_event_audit(event_id,action,actor,after_json,created_at) VALUES(?,?,?,?,?)",
                (task["event_id"], "compute.retry", requested_by,
                 json.dumps({"task_key": task["task_key"]}, ensure_ascii=False), now),
            )
            return dict(connection.execute("SELECT * FROM seismic_computations WHERE id=?", (task_id,)).fetchone())

    def _build_result(self, task: sqlite3.Row, event: sqlite3.Row, observations: list[sqlite3.Row]) -> dict[str, Any]:
        points, shape = self._grid(event, observations, task["grid_step_km"], task["radius_km"])
        stations = sorted({item["station_code"] for item in observations})
        accepted = sum(1 for item in observations if item["quality_status"] == "accepted")
        result = {
            "model_version": task["model_version"],
            "input_digest": task["input_digest"],
            "input_summary": {
                "event_external_id": event["external_id"],
                "magnitude": float(event["magnitude"]),
                "magnitude_type": event["magnitude_type"],
                "depth_km": float(event["depth_km"]),
                "latitude": float(event["latitude"]),
                "longitude": float(event["longitude"]),
                "observation_count": len(observations),
                "accepted_count": accepted,
                "rejected_count": len(observations) - accepted,
                "station_codes": stations,
            },
            "grid": {
                "step_km": task["grid_step_km"],
                "radius_km": task["radius_km"],
                "order": GRID_ORDER,
                "shape": list(shape),
            },
            "points": [point.__dict__ for point in points],
            "count": len(points),
        }
        result["checksum"] = _result_checksum(result)
        return result

    def complete_task(self, task_id: int, worker_id: str, result: dict[str, Any]) -> dict[str, Any]:
        """写入完成回执。结果必须声明与任务行一致的模型版本与输入摘要，否则拒绝落盘，
        避免旧工作者用旧模型结果覆盖较新模型版本的产物。"""
        now = _stamp(_now())
        with self._tx() as connection:
            task = connection.execute("SELECT * FROM seismic_computations WHERE id=?", (task_id,)).fetchone()
            if task is None or task["status"] != "leased" or task["lease_owner"] != worker_id:
                raise ConflictError("任务不存在或未被该工作者持有")
            if result.get("model_version") != task["model_version"]:
                raise ConflictError(
                    "结果模型版本与任务不匹配",
                    context={"task_model_version": task["model_version"], "result_model_version": result.get("model_version")},
                )
            if result.get("input_digest") != task["input_digest"]:
                raise ConflictError("结果输入摘要与任务不匹配")
            checksum = result.get("checksum") or _result_checksum(result)
            if _result_checksum(result) != checksum:
                raise ConflictError("结果校验和不正确")
            connection.execute(
                "UPDATE seismic_computations SET status='done', result_json=?, result_checksum=?, "
                "completed_at=?, error_message='', lease_owner='', lease_until='', updated_at=? "
                "WHERE id=? AND model_version=?",
                (json.dumps(result, ensure_ascii=False), checksum, now, now, task_id, task["model_version"]),
            )
            return dict(connection.execute("SELECT * FROM seismic_computations WHERE id=?", (task_id,)).fetchone())

    def calculate_task(self, task_id: int, worker_id: str) -> dict[str, Any]:
        task = self.get_task(task_id)
        if task["status"] != "leased" or task["lease_owner"] != worker_id:
            raise ConflictError("任务不存在或未被该工作者持有")
        event, observations = self._load_event_inputs(self.connection, task["event_id"])
        task_row = self.connection.execute("SELECT * FROM seismic_computations WHERE id=?", (task_id,)).fetchone()
        result = self._build_result(task_row, event, observations)
        return self.complete_task(task_id, worker_id, result)

    def verify_task(self, task_id: int) -> dict[str, Any]:
        """重算结果校验和并核对模型版本、输入摘要与坐标顺序。"""
        task = self.get_task(task_id)
        if task["status"] != "done":
            return {"task_id": task_id, "status": task["status"], "valid": False, "reason": "not_done"}
        result = json.loads(task["result_json"] or "{}")
        stored = task["result_checksum"]
        recomputed = _result_checksum(result)
        checks = {
            "checksum": stored == recomputed and bool(stored),
            "model_version": result.get("model_version") == task["model_version"],
            "input_digest": result.get("input_digest") == task["input_digest"],
            "grid_order": result.get("grid", {}).get("order") == GRID_ORDER,
            "point_count": result.get("count") == len(result.get("points", [])),
        }
        return {
            "task_id": task_id,
            "status": "done",
            "valid": all(checks.values()),
            "checks": checks,
            "stored_checksum": stored,
            "recomputed_checksum": recomputed,
            "completed_at": task["completed_at"],
        }


def _result_checksum(result: dict[str, Any]) -> str:
    """对模型版本、输入摘要、网格坐标顺序与全部格点做规范化哈希。"""
    material = {
        "model_version": result.get("model_version"),
        "input_digest": result.get("input_digest"),
        "grid_order": result.get("grid", {}).get("order"),
        "grid_shape": result.get("grid", {}).get("shape"),
        "points": result.get("points", []),
    }
    return _canonical_digest(material)
