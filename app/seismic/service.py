from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError
from app.database import get_connection, transaction


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
    last_failure_at TEXT NOT NULL DEFAULT '',
    completed_at TEXT NOT NULL DEFAULT '',
    result_json TEXT NOT NULL DEFAULT '{}',
    result_checksum TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
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
CREATE INDEX IF NOT EXISTS idx_seismic_tasks_ready ON seismic_computations(status, available_at);
CREATE INDEX IF NOT EXISTS idx_seismic_tasks_event ON seismic_computations(event_id);
"""

# 旧库增量升级：列名 -> 列定义
_ADDED_COLUMNS = {
    "max_attempts": "INTEGER NOT NULL DEFAULT 5",
    "available_at": "TEXT NOT NULL DEFAULT ''",
    "last_failure_at": "TEXT NOT NULL DEFAULT ''",
    "completed_at": "TEXT NOT NULL DEFAULT ''",
    "result_checksum": "TEXT NOT NULL DEFAULT ''",
}

DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_LEASE_SECONDS = 60
RESULT_SCHEMA = "seismic-intensity-grid/v1"


def _now(clock: Clock | None = None) -> str:
    return to_storage((clock or SystemClock()).now())


def ensure_schema() -> None:
    connection = get_connection()
    connection.executescript(SCHEMA)
    existing = {row["name"] for row in connection.execute("PRAGMA table_info(seismic_computations)").fetchall()}
    for name, definition in _ADDED_COLUMNS.items():
        if name not in existing:
            connection.execute(f"ALTER TABLE seismic_computations ADD COLUMN {name} {definition}")


def _row(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row else None


def _canonical(payload: Any) -> str:
    """规范化 JSON：键排序、无空白，保证同输入同字节，可跨进程复算。"""
    return json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _event_digest(event: sqlite3.Row, observations: list[sqlite3.Row]) -> str:
    payload = {
        "event": dict(event),
        "observations": [dict(item) for item in observations],
    }
    return _sha256_text(_canonical(payload))


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
    lat: float
    lon: float
    intensity: float


class SeismicService:
    """事件、观测和烈度网格计算任务的事务服务。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None):
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        ensure_schema()

    # ------------------------------------------------------------------ 事件

    def create_event(self, payload: dict[str, Any], actor: str = "system") -> dict[str, Any]:
        now = _now(self.clock)
        with transaction(immediate=True) as connection:
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
        with transaction(immediate=True) as connection:
            current = connection.execute("SELECT * FROM seismic_events WHERE id=?", (event_id,)).fetchone()
            if current is None:
                raise KeyError("event_not_found")
            before = dict(current)
            values = {key: value for key, value in payload.items() if key in {"depth_km", "magnitude", "magnitude_type", "status"} and value is not None}
            if not values:
                return before
            assignments = ", ".join(f"{key}=?" for key in values)
            now = _now(self.clock)
            connection.execute(f"UPDATE seismic_events SET {assignments}, version=version+1, updated_at=? WHERE id=?", (*values.values(), now, event_id))
            after = connection.execute("SELECT * FROM seismic_events WHERE id=?", (event_id,)).fetchone()
            connection.execute("INSERT INTO seismic_event_audit(event_id,action,actor,before_json,after_json,created_at) VALUES(?,?,?,?,?,?)", (event_id, "patch:" + payload.get("reason", ""), actor, json.dumps(before, ensure_ascii=False), json.dumps(dict(after), ensure_ascii=False), now))
            return dict(after)

    def add_observation(self, event_id: int, payload: dict[str, Any], actor: str = "system") -> dict[str, Any]:
        event = self.connection.execute("SELECT id FROM seismic_events WHERE id=?", (event_id,)).fetchone()
        if event is None:
            raise KeyError("event_not_found")
        quality_score, quality_status, quality_reason = _quality(payload)
        now = _now(self.clock)
        source_hash = _sha256_text(json.dumps(payload, sort_keys=True))
        with transaction(immediate=True) as connection:
            try:
                cursor = connection.execute(
                    "INSERT INTO seismic_observations(event_id,station_code,channel,observed_at,pga,pgv,distance_km,quality_score,quality_status,quality_reason,source_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (event_id, payload["station_code"], payload["channel"], payload["observed_at"], payload.get("pga"), payload.get("pgv"), payload["distance_km"], quality_score, quality_status, quality_reason, source_hash, now),
                )
            except sqlite3.IntegrityError:
                existing = connection.execute("SELECT * FROM seismic_observations WHERE event_id=? AND station_code=? AND channel=? AND observed_at=?", (event_id, payload["station_code"], payload["channel"], payload["observed_at"])).fetchone()
                return dict(existing) if existing else {}
            return dict(connection.execute("SELECT * FROM seismic_observations WHERE id=?", (cursor.lastrowid,)).fetchone())

    # ------------------------------------------------------------- 网格计算

    def _grid(self, event: sqlite3.Row, observations: list[sqlite3.Row], step: float, radius: float) -> tuple[list[GridPoint], int, int]:
        """按 lat 升序（外层）、lon 升序（内层）的行主序生成网格。"""
        center_lat, center_lon = float(event["latitude"]), float(event["longitude"])
        radius_deg = radius / 111.0
        count = max(1, int(math.floor((radius * 2) / step)))
        points: list[GridPoint] = []
        accepted = [item for item in observations if item["quality_status"] == "accepted"]
        for lat_index in range(count + 1):
            lat = center_lat - radius_deg + lat_index * (step / 111.0)
            for lon_index in range(count + 1):
                lon = center_lon - radius_deg + lon_index * (step / 111.0) / max(0.2, math.cos(math.radians(lat))) / max(0.2, math.cos(math.radians(lat)))
                values = []
                for item in accepted:
                    distance = math.hypot((lat - center_lat) * 111, (lon - center_lon) * 111 * max(0.2, math.cos(math.radians(lat))))
                    weight = 1 / max(1, abs(distance - float(item["distance_km"])))
                    estimate = float(event["magnitude"]) - math.log10(max(1, float(item["distance_km"]))) + (float(item["pga"] or 0) * 0.01)
                    values.append((estimate * weight, weight))
                intensity = round(sum(value for value, _ in values) / sum(weight for _, weight in values), 3) if values else round(float(event["magnitude"]) - 1, 3)
                points.append(GridPoint(round(lat, 6), round(lon, 6), intensity))
        axis_count = count + 1
        return points, axis_count, axis_count

    def _build_result(self, task: sqlite3.Row, event: sqlite3.Row, observations: list[sqlite3.Row]) -> dict[str, Any]:
        points, lat_count, lon_count = self._grid(event, observations, task["grid_step_km"], task["radius_km"])
        accepted_count = sum(1 for item in observations if item["quality_status"] == "accepted")
        return {
            "result_schema": RESULT_SCHEMA,
            "model_version": task["model_version"],
            "parameters": {
                "grid_step_km": task["grid_step_km"],
                "radius_km": task["radius_km"],
            },
            "input_digest": task["input_digest"],
            "input_summary": {
                "event_external_id": event["external_id"],
                "origin_time": event["origin_time"],
                "latitude": event["latitude"],
                "longitude": event["longitude"],
                "depth_km": event["depth_km"],
                "magnitude": event["magnitude"],
                "magnitude_type": event["magnitude_type"],
                "observation_count": len(observations),
                "accepted_observation_count": accepted_count,
            },
            "grid": {
                "crs": "EPSG:4326",
                "axis_order": "lat,lon",
                "scan_order": "lat-asc/lon-asc",
                "origin": "southwest",
                "shape": [lat_count, lon_count],
                "points": [{"lat": point.lat, "lon": point.lon, "intensity": point.intensity} for point in points],
            },
            "count": len(points),
            "computed_at": _now(self.clock),
        }

    # ------------------------------------------------------------- 任务流转

    @staticmethod
    def _task_key(event_id: int, digest: str, model_version: str, grid_step_km: float, radius_km: float) -> str:
        # 规范化 JSON，避免浮点字符串拼接和字段顺序造成的键漂移
        payload = {
            "event_id": event_id,
            "input_digest": digest,
            "model_version": model_version,
            "grid_step_km": grid_step_km,
            "radius_km": radius_km,
        }
        return _sha256_text(_canonical(payload))

    def enqueue_computation(self, event_id: int, model_version: str, grid_step_km: float, radius_km: float, requested_by: str) -> tuple[dict[str, Any], bool]:
        """提交任务。返回 (任务行, 是否新建)；相同输入永远命中同一行。"""
        event = self.connection.execute("SELECT * FROM seismic_events WHERE id=?", (event_id,)).fetchone()
        if event is None:
            raise KeyError("event_not_found")
        observations = self.connection.execute("SELECT * FROM seismic_observations WHERE event_id=? ORDER BY id", (event_id,)).fetchall()
        digest = _event_digest(event, observations)
        task_key = self._task_key(event_id, digest, model_version, grid_step_km, radius_km)
        now = _now(self.clock)
        with transaction(immediate=True) as connection:
            existing = connection.execute("SELECT * FROM seismic_computations WHERE task_key=?", (task_key,)).fetchone()
            if existing:
                row = dict(existing)
                # 终态失败后再次提交同一请求：幂式重新排队，仍然只有一行
                if row["status"] == "failed":
                    connection.execute(
                        "UPDATE seismic_computations SET status='queued', attempts=0, available_at='', lease_owner='', lease_until='', "
                        "error_message='', updated_at=? WHERE id=? AND status='failed'",
                        (now, row["id"]),
                    )
                    row = dict(connection.execute("SELECT * FROM seismic_computations WHERE id=?", (row["id"],)).fetchone())
                    connection.execute("INSERT INTO seismic_event_audit(event_id,action,actor,after_json,created_at) VALUES(?,?,?,?,?)", (event_id, "compute.requeue", requested_by, json.dumps({"task_key": task_key}, ensure_ascii=False), now))
                return row, False
            cursor = connection.execute(
                "INSERT INTO seismic_computations(event_id,task_key,model_version,input_digest,grid_step_km,radius_km,available_at,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (event_id, task_key, model_version, digest, grid_step_km, radius_km, "", now, now),
            )
            connection.execute("INSERT INTO seismic_event_audit(event_id,action,actor,after_json,created_at) VALUES(?,?,?,?,?)", (event_id, "compute.enqueue", requested_by, json.dumps({"task_key": task_key, "model_version": model_version}, ensure_ascii=False), now))
            return dict(connection.execute("SELECT * FROM seismic_computations WHERE id=?", (cursor.lastrowid,)).fetchone()), True

    def recover_expired_leases(self) -> int:
        """把租约过期（工作者崩溃或服务重启）的任务安全退回重试队列。返回回收数量。"""
        now = _now(self.clock)
        with transaction(immediate=True) as connection:
            cursor = connection.execute(
                "UPDATE seismic_computations SET status='retry', lease_owner='', lease_until='', updated_at=?, "
                "error_message=CASE WHEN error_message='' THEN '租约过期，工作者未上报结果' ELSE error_message END "
                "WHERE status='leased' AND (lease_until='' OR lease_until<=?)",
                (now, now),
            )
            return cursor.rowcount

    def claim_task(self, worker_id: str, *, lease_seconds: int = DEFAULT_LEASE_SECONDS) -> dict[str, Any] | None:
        now = self.clock.now()
        now_s = to_storage(now)
        lease_until = to_storage(now + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            # 顺带回收过期租约（重启恢复路径）
            connection.execute(
                "UPDATE seismic_computations SET status='retry', lease_owner='', lease_until='', updated_at=?, "
                "error_message=CASE WHEN error_message='' THEN '租约过期，工作者未上报结果' ELSE error_message END "
                "WHERE status='leased' AND (lease_until='' OR lease_until<=?)",
                (now_s, now_s),
            )
            task = connection.execute(
                "SELECT * FROM seismic_computations WHERE status IN ('queued','retry') AND (available_at='' OR available_at<=?) "
                "ORDER BY CASE status WHEN 'queued' THEN 0 ELSE 1 END, available_at, id LIMIT 1",
                (now_s,),
            ).fetchone()
            if task is None:
                return None
            cursor = connection.execute(
                "UPDATE seismic_computations SET status='leased', attempts=attempts+1, lease_owner=?, lease_until=?, updated_at=? "
                "WHERE id=? AND status IN ('queued','retry') AND (available_at='' OR available_at<=?)",
                (worker_id, lease_until, now_s, task["id"], now_s),
            )
            if cursor.rowcount != 1:
                return None
            return dict(connection.execute("SELECT * FROM seismic_computations WHERE id=?", (task["id"],)).fetchone())

    def fail_task(self, task_id: int, worker_id: str, error_message: str, *, retry_seconds: int | None = None) -> dict[str, Any]:
        """工作者上报失败：未超次数则按退避重新排队，超过 max_attempts 进入终态 failed。"""
        now = self.clock.now()
        now_s = to_storage(now)
        with transaction(immediate=True) as connection:
            task = connection.execute("SELECT * FROM seismic_computations WHERE id=?", (task_id,)).fetchone()
            if task is None:
                raise KeyError("task_not_found")
            if task["status"] != "leased" or task["lease_owner"] != worker_id:
                raise KeyError("task_not_owned")
            attempts = task["attempts"]
            if attempts >= task["max_attempts"]:
                status, available_at = "failed", ""
            else:
                status = "retry"
                if retry_seconds is None:
                    retry_seconds = min(300, 5 * (2 ** (attempts - 1)))
                available_at = to_storage(now + timedelta(seconds=max(0, retry_seconds)))
            connection.execute(
                "UPDATE seismic_computations SET status=?, available_at=?, lease_owner='', lease_until='', "
                "last_failure_at=?, error_message=?, updated_at=? WHERE id=?",
                (status, available_at, now_s, error_message[:1000], now_s, task_id),
            )
            return dict(connection.execute("SELECT * FROM seismic_computations WHERE id=?", (task_id,)).fetchone())

    def retry_task(self, task_id: int) -> dict[str, Any]:
        """人工把终态失败/退避中的任务重新置为可领取。done 不允许重试，避免覆盖结果。"""
        now_s = _now(self.clock)
        with transaction(immediate=True) as connection:
            task = connection.execute("SELECT * FROM seismic_computations WHERE id=?", (task_id,)).fetchone()
            if task is None:
                raise KeyError("task_not_found")
            if task["status"] == "done":
                raise ConflictError("任务已完成，不允许重试覆盖既有结果")
            if task["status"] in ("failed", "retry"):
                connection.execute(
                    "UPDATE seismic_computations SET status='queued', attempts=0, available_at='', lease_owner='', lease_until='', "
                    "error_message='', updated_at=? WHERE id=?",
                    (now_s, task_id),
                )
            return dict(connection.execute("SELECT * FROM seismic_computations WHERE id=?", (task_id,)).fetchone())

    def complete_task(self, task_id: int, worker_id: str, result: dict[str, Any]) -> dict[str, Any]:
        """写入完成回执：仅当前租约持有者可完成；done 行永不被二次覆盖。"""
        with transaction(immediate=True) as connection:
            task = connection.execute("SELECT * FROM seismic_computations WHERE id=?", (task_id,)).fetchone()
            if task is None:
                raise KeyError("task_not_found")
            if task["status"] != "leased" or task["lease_owner"] != worker_id:
                raise KeyError("task_not_owned")
            if result.get("model_version") not in (None, task["model_version"]):
                raise ConflictError("结果模型版本与任务不一致，拒绝写入")
            if result.get("input_digest") not in (None, task["input_digest"]):
                raise ConflictError("结果输入摘要与任务不一致，拒绝写入")
            serialized = json.dumps(result, sort_keys=True, ensure_ascii=False)
            checksum = _sha256_text(_canonical(result))
            now_s = _now(self.clock)
            connection.execute(
                "UPDATE seismic_computations SET status='done', result_json=?, result_checksum=?, completed_at=?, "
                "lease_owner='', lease_until='', error_message='', updated_at=? WHERE id=? AND status='leased'",
                (serialized, checksum, now_s, now_s, task_id),
            )
            return dict(connection.execute("SELECT * FROM seismic_computations WHERE id=?", (task_id,)).fetchone())

    def calculate_task(self, task_id: int, worker_id: str) -> dict[str, Any]:
        task = self.connection.execute("SELECT * FROM seismic_computations WHERE id=?", (task_id,)).fetchone()
        if task is None:
            raise KeyError("task_not_found")
        if task["status"] != "leased" or task["lease_owner"] != worker_id:
            raise KeyError("task_not_owned")
        event = self.connection.execute("SELECT * FROM seismic_events WHERE id=?", (task["event_id"],)).fetchone()
        observations = self.connection.execute("SELECT * FROM seismic_observations WHERE event_id=? ORDER BY id", (task["event_id"],)).fetchall()
        result = self._build_result(task, event, observations)
        return self.complete_task(task_id, worker_id, result)

    # ------------------------------------------------------------- 查询/校验

    def get_task(self, task_id: int) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM seismic_computations WHERE id=?", (task_id,)).fetchone()
        return _row(row)

    def get_task_by_key(self, task_key: str) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM seismic_computations WHERE task_key=?", (task_key,)).fetchone()
        return _row(row)

    def list_tasks(self, *, event_id: int | None = None, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if event_id is not None:
            clauses.append("event_id=?")
            params.append(event_id)
        if status:
            clauses.append("status=?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(max(1, min(limit, 500)))
        rows = self.connection.execute(
            f"SELECT id,event_id,task_key,model_version,input_digest,grid_step_km,radius_km,status,attempts,max_attempts,"
            f"available_at,lease_owner,lease_until,last_failure_at,completed_at,result_checksum,error_message,created_at,updated_at "
            f"FROM seismic_computations{where} ORDER BY id DESC LIMIT ?",
            params,
        ).fetchall()
        return [dict(row) for row in rows]

    def receipt(self, task_id: int) -> dict[str, Any]:
        task = self.get_task(task_id)
        if task is None:
            raise KeyError("task_not_found")
        result_count = 0
        if task["status"] == "done" and task["result_json"]:
            try:
                result_count = int(json.loads(task["result_json"]).get("count", 0))
            except (ValueError, TypeError):
                result_count = 0
        return {
            "task_id": task["id"],
            "task_key": task["task_key"],
            "event_id": task["event_id"],
            "status": task["status"],
            "model_version": task["model_version"],
            "parameters": {"grid_step_km": task["grid_step_km"], "radius_km": task["radius_km"]},
            "input_digest": task["input_digest"],
            "attempts": task["attempts"],
            "max_attempts": task["max_attempts"],
            "lease_owner": task["lease_owner"],
            "available_at": task["available_at"],
            "created_at": task["created_at"],
            "last_failure_at": task["last_failure_at"],
            "completed_at": task["completed_at"],
            "result_count": result_count,
            "result_checksum": task["result_checksum"],
            "error_message": task["error_message"],
        }

    def verify_result(self, task_id: int, *, include_grid: bool = False) -> dict[str, Any]:
        """用存档结果重新计算 sha256，与完成时的回执校验和比对。"""
        task = self.get_task(task_id)
        if task is None:
            raise KeyError("task_not_found")
        if task["status"] != "done":
            report = {"task_id": task_id, "status": task["status"], "verified": False, "reason": "任务未完成", "stored_checksum": task["result_checksum"], "actual_checksum": ""}
            return report
        try:
            payload = json.loads(task["result_json"] or "{}")
        except ValueError:
            payload = None
        actual = _sha256_text(_canonical(payload)) if payload is not None else ""
        stored = task["result_checksum"]
        grid = payload.get("grid") if payload else None
        report = {
            "task_id": task_id,
            "status": "done",
            "verified": bool(stored) and stored == actual,
            "stored_checksum": stored,
            "actual_checksum": actual,
            "model_version": payload.get("model_version") if payload else None,
            "input_digest": payload.get("input_digest") if payload else None,
            "axis_order": grid.get("axis_order") if grid else None,
            "scan_order": grid.get("scan_order") if grid else None,
            "shape": grid.get("shape") if grid else None,
        }
        if include_grid:
            report["grid"] = grid
        return report
