"""共用保留边界：保留策略与生命周期清理作业唯一的保留边界来源。

两处入口（保留策略核查、历史清理作业）过去各自读取保留天数/最低版本与
快照时间并分别计算，本模块把这套读取计算收拢为一份**共用保留边界结果
数据**，并约束：

- 一次执行只能照同一时点处理：边界首次核定时固定 ``decision_time``，
  中断恢复、并发重试都复用该时点，不得按新时间重算处理范围；
- 边界结果（含逐快照核查结论）落库后不可变，中断后过去的核查结果仍可回看；
- 释放登记幂等：并发恢复和重复清理对同一快照只登记一次释放；
- 恢复时允许按当前时间复算做对比，但新到期对象不得混入原作业范围，冲突
  结论只追加到保留策略审计台账。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.store import store

BOUNDARY_TABLE = "retention_boundary"
AUDIT_TABLE = "retention_audit"
CATALOG_TABLE = "retention_snapshot_catalog"


# ------------------------------------------------------------------
# 时间与快照解析
# ------------------------------------------------------------------

def normalize_decision_time(value: Any = None) -> datetime:
    """把外部传入的判断时点统一为朴素 UTC datetime；缺省取当前 UTC 时间。"""
    if value is None:
        return datetime.now(timezone.utc).replace(tzinfo=None)
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    text = str(value).strip()
    for suffix in ("Z", "+00:00", "+0000"):
        text = text.replace(suffix, "")
    text = text.split("+")[0]
    parsed = datetime.fromisoformat(text)
    return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed


def to_iso_z(value: datetime) -> str:
    return value.replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_snapshot_time(raw: Any, fallback: datetime) -> datetime:
    """与历史 retention_checker 相同的容错解析：无法解析时按未来时间处理。"""
    text = str(raw or "")
    for suffix in ("Z", "+00:00", "+0000"):
        text = text.replace(suffix, "")
    text = text.split("+")[0]
    try:
        return datetime.fromisoformat(text)
    except (ValueError, TypeError):
        return fallback


# ------------------------------------------------------------------
# 纯计算：共用保留边界
# ------------------------------------------------------------------

def build_retention_boundary(
    snapshots: list[dict[str, Any]],
    retention_days: int = 30,
    min_versions: int = 5,
    decision_time: Any = None,
) -> dict[str, Any]:
    """按保留天数与最低版本数核定保留边界。

    输出同时包含历史 ``retention_checker`` 的全部键值（数值口径不变）和
    逐快照核查结论，调用方无需再自行读取快照时间重复计算。
    """
    decision_at = normalize_decision_time(decision_time)
    cutoff = decision_at - timedelta(days=retention_days)

    details: list[dict[str, Any]] = []
    expired_rows: list[dict[str, Any]] = []
    valid_rows: list[dict[str, Any]] = []

    for index, snap in enumerate(snapshots, start=1):
        snapshot_id = snap.get("snapshot_id") or f"snapshot-{index}"
        created_at = str(snap.get("created_at", ""))
        created = _parse_snapshot_time(created_at, cutoff + timedelta(days=1))
        is_expired = created < cutoff
        row = {
            "snapshot_id": snapshot_id,
            "created_at": created_at,
            "created_naive": created,
            "is_expired": is_expired,
        }
        details.append(row)
        (expired_rows if is_expired else valid_rows).append(row)

    # 最低版本保护：过期快照中最新的 min_versions 个仍须保留，
    # 与历史 can_clean = max(0, expired_count - min_versions) 口径完全一致。
    expired_sorted = sorted(expired_rows, key=lambda row: row["created_naive"])
    protected_rows = expired_sorted[-min_versions:] if min_versions > 0 else []
    protected_ids = {row["snapshot_id"] for row in protected_rows}

    cleanable_rows = [
        row for row in expired_sorted if row["snapshot_id"] not in protected_ids
    ]
    cleanable_ids = [row["snapshot_id"] for row in cleanable_rows]
    protected_ids_list = [row["snapshot_id"] for row in protected_rows]
    expired_ids = [row["snapshot_id"] for row in expired_sorted]
    valid_ids = [row["snapshot_id"] for row in valid_rows]

    snapshot_verdicts = []
    for row in sorted(details, key=lambda item: item["created_naive"]):
        if not row["is_expired"]:
            verdict = "保留-未过期"
        elif row["snapshot_id"] in protected_ids:
            verdict = "保护-最低版本"
        else:
            verdict = "可清理"
        snapshot_verdicts.append({
            "snapshot_id": row["snapshot_id"],
            "created_at": row["created_at"],
            "is_expired": row["is_expired"],
            "verdict": verdict,
        })

    can_clean = len(cleanable_ids)
    return {
        # 历史 retention_checker 键值，口径保持不变
        "total_snapshots": len(snapshots),
        "expired_count": len(expired_rows),
        "valid_count": len(valid_rows),
        "min_versions_required": min_versions,
        "retention_days": retention_days,
        "can_clean_count": can_clean,
        "should_clean": can_clean > 0,
        # 共用保留边界结果数据
        "decision_time": to_iso_z(decision_at),
        "cutoff_time": to_iso_z(cutoff),
        "expired_snapshot_ids": expired_ids,
        "valid_snapshot_ids": valid_ids,
        "protected_snapshot_ids": protected_ids_list,
        "cleanable_snapshot_ids": cleanable_ids,
        "retained_snapshot_ids": valid_ids + protected_ids_list,
        "snapshot_verdicts": snapshot_verdicts,
        "snapshot_catalog": [dict(snap) for snap in snapshots],
    }


class RetentionBoundaryRegistry:
    """保留边界结果数据与审计台账的登记处（内存实现，接口等价于持久层）。"""

    # -- 快照目录：场景化重演时登记，缺省回退到各服务历史样例 -------------
    def register_snapshot_catalog(
        self, scope_key: str, snapshots: list[dict[str, Any]],
    ) -> dict[str, Any]:
        rows = store.rows(CATALOG_TABLE)
        record = {
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "scope_key": scope_key,
            "snapshots": [dict(snap) for snap in snapshots],
            "updated_at": to_iso_z(normalize_decision_time()),
        }
        rows.append(record)
        return record

    def get_snapshot_catalog(self, scope_key: str) -> list[dict[str, Any]] | None:
        for row in store.rows(CATALOG_TABLE):
            if row.get("scope_key") == scope_key:
                return [dict(snap) for snap in row.get("snapshots", [])]
        return None

    # -- 边界核定：固定判断时点，重复进入只复用不重算 ---------------------
    def get_or_create_boundary(
        self,
        scope_key: str,
        snapshots: list[dict[str, Any]],
        retention_days: int,
        min_versions: int,
        *,
        now: Any = None,
        source: str = "",
        job_ref: str = "",
        policy_ref: str = "",
        table: str = "",
    ) -> tuple[dict[str, Any], bool]:
        existing = self.find_boundary_by_scope(scope_key)
        if existing is not None:
            # 一次执行只能照同一时点处理：忽略新传入的 now，复用固定边界。
            return existing, False

        decision_at = normalize_decision_time(now)
        boundary = build_retention_boundary(
            snapshots,
            retention_days=retention_days,
            min_versions=min_versions,
            decision_time=decision_at,
        )
        rows = store.rows(BOUNDARY_TABLE)
        boundary_id = f"RB-{len(rows) + 1:04d}"
        record = {
            "id": len(rows) + 1,
            "boundary_id": boundary_id,
            "scope_key": scope_key,
            "source": source,
            "job_ref": job_ref,
            "policy_ref": policy_ref,
            "table": table,
            "status": "fixed",
            "retention_days": retention_days,
            "min_versions_required": min_versions,
            "decision_time": boundary["decision_time"],
            "cutoff_time": boundary["cutoff_time"],
            "total_snapshots": boundary["total_snapshots"],
            "expired_count": boundary["expired_count"],
            "valid_count": boundary["valid_count"],
            "can_clean_count": boundary["can_clean_count"],
            "should_clean": boundary["should_clean"],
            "expired_snapshot_ids": boundary["expired_snapshot_ids"],
            "valid_snapshot_ids": boundary["valid_snapshot_ids"],
            "protected_snapshot_ids": boundary["protected_snapshot_ids"],
            "cleanable_snapshot_ids": boundary["cleanable_snapshot_ids"],
            "retained_snapshot_ids": boundary["retained_snapshot_ids"],
            "snapshot_verdicts": boundary["snapshot_verdicts"],
            "snapshot_catalog": boundary["snapshot_catalog"],
            "released_snapshot_ids": [],
            "release_count": 0,
            "release_attempts": [],
            "conflict_recorded": False,
            "created_at": to_iso_z(decision_at),
        }
        rows.append(record)

        self.append_audit(
            boundary=record,
            event="边界核定",
            conclusion=(
                f"采用判断时点 {record['decision_time']}（截止线 {record['cutoff_time']}）："
                f"共 {record['total_snapshots']} 个快照，过期 {record['expired_count']} 个、"
                f"未过期 {record['valid_count']} 个，最低保留 {min_versions} 个版本，"
                f"核定可清理范围 {record['can_clean_count']} 个："
                f"{record['cleanable_snapshot_ids'] or '无'}。"
            ),
        )
        return record, True

    def list_boundaries(self, scope_key: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(BOUNDARY_TABLE)
        if scope_key is None:
            return list(rows)
        return [row for row in rows if row.get("scope_key") == scope_key]

    def find_boundary_by_scope(self, scope_key: str) -> dict[str, Any] | None:
        for row in store.rows(BOUNDARY_TABLE):
            if row.get("scope_key") == scope_key:
                return row
        return None

    def get_boundary(self, boundary_id: str) -> dict[str, Any] | None:
        for row in store.rows(BOUNDARY_TABLE):
            if row.get("boundary_id") == boundary_id:
                return row
        return None

    # -- 恢复复算：只对比，不改原边界，新到期对象不混入 -------------------
    def recheck_at_current_time(
        self, boundary: dict[str, Any], now: Any,
    ) -> dict[str, Any]:
        current = build_retention_boundary(
            boundary["snapshot_catalog"],
            retention_days=int(boundary["retention_days"]),
            min_versions=int(boundary["min_versions_required"]),
            decision_time=normalize_decision_time(now),
        )
        original_verdict = {
            item["snapshot_id"]: item for item in boundary["snapshot_verdicts"]
        }
        changed: list[dict[str, Any]] = []
        for item in current["snapshot_verdicts"]:
            old = original_verdict.get(item["snapshot_id"])
            if old and old["verdict"] != item["verdict"]:
                changed.append({
                    "snapshot_id": item["snapshot_id"],
                    "original_verdict": old["verdict"],
                    "current_verdict": item["verdict"],
                    "original_is_expired": old["is_expired"],
                    "current_is_expired": item["is_expired"],
                })

        newly_due = [
            item["snapshot_id"]
            for item in changed
            if not item["original_is_expired"] and item["current_is_expired"]
        ]
        # 只登记一次冲突结论；原边界与清理范围保持不变。
        if changed and not boundary.get("conflict_recorded"):
            boundary["conflict_recorded"] = True
            disposition = "；".join(
                f"{item['snapshot_id']} 原核查结论为「{item['original_verdict']}」，"
                f"新时点条件下变为「{item['current_verdict']}」，按原判断时点保留、不混入本次清理"
                for item in changed
            )
            self.append_audit(
                boundary=boundary,
                event="时点冲突",
                conclusion=(
                    f"本次采用判断时点 {boundary['decision_time']}（不采用恢复时点 "
                    f"{current['decision_time']}）。{disposition}。"
                    f"新到期对象 {newly_due or '无'} 不得混入原作业范围。"
                ),
                extra={"changed_snapshots": changed, "newly_due_snapshot_ids": newly_due},
            )
        return {
            "recheck_time": current["decision_time"],
            "changed_snapshots": changed,
            "newly_due_snapshot_ids": newly_due,
            "applied_decision_time": boundary["decision_time"],
            "cleanable_snapshot_ids": list(boundary["cleanable_snapshot_ids"]),
        }

    # -- 释放登记：幂等，同一快照只登记一次释放 ---------------------------
    def register_release(
        self,
        boundary: dict[str, Any],
        snapshot_ids: list[str],
        *,
        attempt_ref: str,
        now: Any = None,
    ) -> dict[str, Any]:
        cleanable = set(boundary["cleanable_snapshot_ids"])
        already = set(boundary["released_snapshot_ids"])
        requested = list(snapshot_ids)

        rejected = [sid for sid in requested if sid not in cleanable]
        newly_released = [
            sid for sid in requested if sid in cleanable and sid not in already
        ]
        duplicate = [sid for sid in requested if sid in already]

        boundary["released_snapshot_ids"] = list(already) + newly_released
        boundary["release_count"] = len(boundary["released_snapshot_ids"])
        attempt = {
            "attempt_ref": attempt_ref,
            "registered_at": to_iso_z(normalize_decision_time(now)),
            "requested_snapshot_ids": requested,
            "newly_released_snapshot_ids": newly_released,
            "duplicate_snapshot_ids": duplicate,
            "rejected_snapshot_ids": rejected,
        }
        boundary["release_attempts"].append(attempt)

        self.append_audit(
            boundary=boundary,
            event="释放登记",
            conclusion=(
                f"尝试 {attempt_ref}：请求释放 {len(requested)} 个，"
                f"本次新释放 {len(newly_released)} 个（{newly_released or '无'}），"
                f"重复登记 {len(duplicate)} 个（{duplicate or '无'}），"
                f"越界拒绝 {len(rejected)} 个；累计释放 {boundary['release_count']} 个，"
                "同一快照只登记一次释放。"
            ),
            extra={"attempt": attempt},
        )
        return attempt

    # -- 审计台账（append-only） ------------------------------------------
    def append_audit(
        self,
        *,
        boundary: dict[str, Any],
        event: str,
        conclusion: str,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        rows = store.rows(AUDIT_TABLE)
        record = {
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "audit_id": f"RA-{len(rows) + 1:04d}",
            "event": event,
            "conclusion": conclusion,
            "boundary_id": boundary.get("boundary_id"),
            "scope_key": boundary.get("scope_key"),
            "source": boundary.get("source"),
            "job_ref": boundary.get("job_ref"),
            "policy_ref": boundary.get("policy_ref"),
            "table": boundary.get("table"),
            "decision_time": boundary.get("decision_time"),
            "cutoff_time": boundary.get("cutoff_time"),
            "retention_days": boundary.get("retention_days"),
            "min_versions_required": boundary.get("min_versions_required"),
            "cleanable_snapshot_ids": list(boundary.get("cleanable_snapshot_ids", [])),
            "released_snapshot_ids": list(boundary.get("released_snapshot_ids", [])),
            "created_at": to_iso_z(normalize_decision_time()),
        }
        if extra:
            record.update(extra)
        rows.append(record)
        return record

    def list_audit(
        self,
        *,
        scope_key: str | None = None,
        boundary_id: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = store.rows(AUDIT_TABLE)
        result = rows
        if scope_key is not None:
            result = [row for row in result if row.get("scope_key") == scope_key]
        if boundary_id is not None:
            result = [row for row in result if row.get("boundary_id") == boundary_id]
        return list(result)


registry = RetentionBoundaryRegistry()
