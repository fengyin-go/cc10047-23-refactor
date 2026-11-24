"""保留策略领域服务。

保留天数与快照时间的读取计算不再在本服务内单独完成，统一走共用保留边界
（app.retention_boundary），历史清理作业消费同一份边界结果数据。旧合法
参数 domain_check_retention 的输入与返回键保持不变。
"""
from __future__ import annotations

from typing import Any

from app.retention_boundary import registry
from app.services.base import BaseModuleService
from app.store import store


class RetentionPolicyService(BaseModuleService):
    MODULE = "retention_policy"
    ENTITY = "保留规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则标识", "目标湖表", "保留天数", "清理方式", "豁免标记", "上次执行", "执行结果", "下次执行"]
    REQUIRED_FIELDS = ["规则标识", "目标湖表", "保留天数"]
    STATUS_ORDER = ["待生效", "已生效", "已暂停", "已过期"]
    ACTION_RULES = {"启用规则": "待生效", "暂停规则": "已生效"}
    NEGATIVE_ACTIONS = []

    def _safe_int(self, val: Any, default: int = 0) -> int:
        try: return int(val)
        except (ValueError, TypeError): return default

    def _legacy_snapshots(self, entry_id: int) -> list[dict[str, Any]]:
        """重构前内置的历史样例快照：目录未登记时原样回退，保证旧输出不变。"""
        return [
            {"snapshot_id": f"s{entry_id}-{i}", "created_at": "2024-10-10T00:00:00Z"}
            for i in range(1, 8)
        ] + [
            {"snapshot_id": f"s{entry_id}-{i}", "created_at": "2024-11-20T00:00:00Z"}
            for i in range(8, 10)
        ]

    def _load_snapshots(self, scope_key: str, entry_id: int) -> list[dict[str, Any]]:
        catalog = registry.get_snapshot_catalog(scope_key)
        if catalog is not None:
            return catalog
        return self._legacy_snapshots(entry_id)

    def domain_check_retention(self, entry_id: int, *, now: Any = None) -> dict[str, Any]:
        """按共用保留边界核查当前规则，联动状态并产出标准化记录。

        处理流程：读取配置→读取共用快照目录→核定/复用共用保留边界→
        写入审计台账。旧入参与旧返回键（含 retention_checker 七项）保持不变，
        边界与审计信息以附加键返回。一次核查固定判断时点，重复进入只复用。
        """
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"保留规则 {entry_id} 不存在"}

        scope_key = f"retention_policy:{entry_id}"
        retention_days = self._safe_int(entry.get("保留天数"), 30)
        # 保留策略入口沿用历史最低版本口径 5，可由扩展参数覆盖。
        min_versions = self._safe_int(entry.get("最低版本数"), 5)
        snapshots = self._load_snapshots(scope_key, entry_id)

        boundary, _created = registry.get_or_create_boundary(
            scope_key,
            snapshots,
            retention_days,
            min_versions,
            now=now,
            source="retention_policy",
            policy_ref=str(entry.get("规则标识")),
            table=str(entry.get("目标湖表")),
        )
        registry.append_audit(
            boundary=boundary,
            event="策略核查",
            conclusion=(
                f"规则 {entry.get('规则标识')} 按判断时点 {boundary['decision_time']} 核查："
                f"过期 {boundary['expired_count']} 个、未过期 {boundary['valid_count']} 个，"
                f"可清理 {boundary['can_clean_count']} 个，保留快照 "
                f"{boundary['retained_snapshot_ids']}。"
            ),
        )

        # 旧返回结构：rule_id/table + retention_checker 七项，键与口径不变。
        legacy = {
            "total_snapshots": boundary["total_snapshots"],
            "expired_count": boundary["expired_count"],
            "valid_count": boundary["valid_count"],
            "min_versions_required": boundary["min_versions_required"],
            "retention_days": boundary["retention_days"],
            "can_clean_count": boundary["can_clean_count"],
            "should_clean": boundary["should_clean"],
        }
        return {
            "ok": True,
            "rule_id": entry.get("规则标识"),
            "table": entry.get("目标湖表"),
            **legacy,
            # 共用保留边界结果数据（附加，不影响旧消费方）
            "boundary_id": boundary["boundary_id"],
            "scope_key": boundary["scope_key"],
            "decision_time": boundary["decision_time"],
            "cutoff_time": boundary["cutoff_time"],
            "cleanable_snapshot_ids": boundary["cleanable_snapshot_ids"],
            "protected_snapshot_ids": boundary["protected_snapshot_ids"],
            "snapshot_verdicts": boundary["snapshot_verdicts"],
        }

    # -- 共用保留边界与审计台账查询入口 -----------------------------------
    def list_boundaries(self) -> list[dict[str, Any]]:
        return registry.list_boundaries()

    def list_audit_ledger(self) -> list[dict[str, Any]]:
        return registry.list_audit()
