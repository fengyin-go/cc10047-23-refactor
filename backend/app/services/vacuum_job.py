"""历史清理（生命周期）领域服务。

清理作业不再自行估算清理范围，而是按共用保留边界结果数据核定实际处理范围：

- 首次执行核定保留边界并固定判断时点；中断/并发恢复复用同一时点与边界，
  一次执行只能照同一时点处理；
- 恢复时若已跨过部分快照到期时间，仅按当前时间复算做对比并在保留策略
  审计台账登记时点冲突，原核查结果（含逐快照结论）保持可回看，新到期
  对象不得混入原作业；
- 释放登记幂等：并发恢复和重复清理对同一快照只登记一次释放；
- 清理结论回写保留策略审计台账，作业入口仍展示原作业与全部后续尝试。

旧 domain_clean_history 的入参与返回键保持不变，新数据通过附加键返回。
"""
from __future__ import annotations

from typing import Any

from app.retention_boundary import registry
from app.services.base import BaseModuleService
from app.store import store


class VacuumJobService(BaseModuleService):
    MODULE = "vacuum_job"
    ENTITY = "清理作业"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["作业标识", "目标湖表", "保留版本", "清理范围", "预览大小", "执行状态", "清理时间", "释放空间"]
    REQUIRED_FIELDS = ["作业标识", "目标湖表", "保留版本"]
    STATUS_ORDER = ["待执行", "清理中", "已完成", "清理异常"]
    ACTION_RULES = {"执行清理": "待执行", "取消清理": "清理中"}
    NEGATIVE_ACTIONS = []

    def _safe_int(self, val: Any, default: int = 0) -> int:
        try: return int(val)
        except (ValueError, TypeError): return default

    def _legacy_snapshots(self, entry_id: int) -> list[dict[str, Any]]:
        """重构前保留策略侧内置样例的同构回退，保证无目录登记时行为可解释。"""
        return [
            {"snapshot_id": f"v{entry_id}-{i}", "created_at": "2024-10-10T00:00:00Z"}
            for i in range(1, 8)
        ] + [
            {"snapshot_id": f"v{entry_id}-{i}", "created_at": "2024-11-20T00:00:00Z"}
            for i in range(8, 10)
        ]

    def _load_snapshots(self, scope_key: str, entry_id: int) -> list[dict[str, Any]]:
        catalog = registry.get_snapshot_catalog(scope_key)
        if catalog is not None:
            return catalog
        return self._legacy_snapshots(entry_id)

    def domain_clean_history(
        self,
        entry_id: int,
        *,
        now: Any = None,
        attempt_ref: str = "",
        retention_days: int | None = None,
    ) -> dict[str, Any]:
        """按共用保留边界执行一次清理，返回标准化处理记录。

        流程：读取作业配置→读取共用快照目录→核定/复用共用保留边界（固定
        判断时点）→按边界 cleanable 范围登记幂等释放→结论回写审计台账。
        中断后以新 now 恢复会触发时点冲突复算，但处理范围仍按原边界执行。
        """
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"清理作业 {entry_id} 不存在"}

        scope_key = f"vacuum_job:{entry_id}"
        min_versions = self._safe_int(entry.get("保留版本"), 5)
        days = self._safe_int(
            entry.get("保留天数"),
            retention_days if retention_days is not None else 30,
        )
        snapshots = self._load_snapshots(scope_key, entry_id)

        boundary, created = registry.get_or_create_boundary(
            scope_key,
            snapshots,
            days,
            min_versions,
            now=now,
            source="vacuum_job",
            job_ref=str(entry.get("作业标识")),
            table=str(entry.get("目标湖表")),
        )

        recheck: dict[str, Any] = {}
        if not created and now is not None:
            # 中断/并发恢复：按当前时间复算对比，登记冲突但不动原边界与范围。
            recheck = registry.recheck_at_current_time(boundary, now)

        # 实际处理范围严格按共用边界核定，新到期对象不会混入。
        cleanable = list(boundary["cleanable_snapshot_ids"])
        attempt = attempt_ref or f"{entry.get('作业标识')}-attempt-{len(boundary['release_attempts']) + 1}"
        release = registry.register_release(
            boundary, cleanable, attempt_ref=attempt, now=now,
        )

        registry.append_audit(
            boundary=boundary,
            event="清理结论",
            conclusion=(
                f"作业 {entry.get('作业标识')} 采用判断时点 {boundary['decision_time']} 完成清理尝试"
                f" {attempt}：实际处理范围 {len(cleanable)} 个快照（{cleanable or '无'}），"
                f"本次新释放 {len(release['newly_released_snapshot_ids'])} 个、"
                f"重复登记 {len(release['duplicate_snapshot_ids'])} 个，"
                f"累计释放 {boundary['release_count']} 个；同一快照只登记一次释放。"
                + (
                    f" 恢复时点 {recheck.get('recheck_time')} 与原判断时点存在冲突，"
                    f"已按原时点处理，新到期对象 {recheck.get('newly_due_snapshot_ids') or '无'} 不混入。"
                    if recheck else ""
                )
            ),
            extra={"attempt_ref": attempt},
        )

        # 作业入口仍展示原作业与后续尝试（尝试记录挂在作业行上）。
        attempts = entry.setdefault("清理尝试", [])
        attempts.append({
            "attempt_ref": attempt,
            "boundary_id": boundary["boundary_id"],
            "decision_time": boundary["decision_time"],
            "cleanable_snapshot_ids": cleanable,
            "newly_released_snapshot_ids": release["newly_released_snapshot_ids"],
            "duplicate_snapshot_ids": release["duplicate_snapshot_ids"],
            "recheck": recheck,
        })

        # 旧返回键与口径全部保留（files_cleaned 等历史常量不变）。
        return {
            "ok": True,
            "job_id": entry.get("作业标识"),
            "table": entry.get("目标湖表"),
            "retained_versions": min_versions,
            "files_cleaned": 42,
            "reclaimed_bytes": self._safe_int(entry.get("释放空间"), 0) * 1024 * 1024,
            "clean_range": entry.get("清理范围"),
            "duration_seconds": 12,
            # 共用保留边界结果数据（附加键）
            "boundary_id": boundary["boundary_id"],
            "scope_key": boundary["scope_key"],
            "decision_time": boundary["decision_time"],
            "cutoff_time": boundary["cutoff_time"],
            "cleanable_snapshot_ids": cleanable,
            "released_snapshot_ids": list(boundary["released_snapshot_ids"]),
            "release_count": boundary["release_count"],
            "newly_released_snapshot_ids": release["newly_released_snapshot_ids"],
            "duplicate_snapshot_ids": release["duplicate_snapshot_ids"],
            "attempt_ref": attempt,
            "recheck": recheck,
        }

    # -- 共用保留边界与审计台账查询入口 -----------------------------------
    def get_job_boundary(self, entry_id: int) -> dict[str, Any] | None:
        return registry.find_boundary_by_scope(f"vacuum_job:{entry_id}")

    def get_job_detail(self, entry_id: int) -> dict[str, Any]:
        """清理详情：原作业数据 + 共用边界 + 全部尝试 + 审计台账。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"清理作业 {entry_id} 不存在"}
        boundary = self.get_job_boundary(entry_id)
        return {
            "ok": True,
            "job": entry,
            "boundary": boundary,
            "attempts": entry.get("清理尝试", []),
            "audit": registry.list_audit(scope_key=f"vacuum_job:{entry_id}"),
        }
