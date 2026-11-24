"""保留策略与生命周期清理的共用保留边界接口。

通用资源路由负责作业/规则的登记与状态流转；本路由承载重构后的保留边界
核定、清理执行、清理详情、审计台账与快照目录重演能力，两个入口
（保留策略、历史清理）共用同一份保留边界结果数据。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.retention_boundary import registry
from app.services import service_for

router = APIRouter(prefix="/api/lifecycle", tags=["保留边界与生命周期"])


class CatalogPayload(BaseModel):
    scope_key: str
    snapshots: list[dict[str, Any]] = Field(default_factory=list)


@router.post("/catalog")
def register_catalog(payload: CatalogPayload) -> dict[str, Any]:
    """登记某一作用域（作业/规则）的快照目录，供历史样例重演使用。"""
    record = registry.register_snapshot_catalog(payload.scope_key, payload.snapshots)
    return {"ok": True, "scope_key": payload.scope_key, "catalog_id": record["id"]}


@router.post("/retention-policies/{entry_id}/check")
def check_retention(entry_id: int, now: str | None = Query(default=None)) -> dict[str, Any]:
    service = service_for("retention_policy")
    result = service.domain_check_retention(entry_id, now=now)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("message"))
    return result


@router.post("/vacuum-jobs/{entry_id}/clean")
def clean_history(
    entry_id: int,
    now: str | None = Query(default=None),
    attempt_ref: str | None = Query(default=None),
    retention_days: int | None = Query(default=None),
) -> dict[str, Any]:
    service = service_for("vacuum_job")
    result = service.domain_clean_history(
        entry_id,
        now=now,
        attempt_ref=attempt_ref or "",
        retention_days=retention_days,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("message"))
    return result


@router.get("/vacuum-jobs/{entry_id}/detail")
def vacuum_detail(entry_id: int) -> dict[str, Any]:
    """刷新清理详情：原作业、共用保留边界、全部尝试与审计台账。"""
    return service_for("vacuum_job").get_job_detail(entry_id)


@router.get("/boundaries")
def list_boundaries(scope_key: str | None = Query(default=None)) -> dict[str, Any]:
    items = registry.list_boundaries(scope_key)
    return {"items": items, "total": len(items)}


@router.get("/boundaries/{boundary_id}")
def get_boundary(boundary_id: str) -> dict[str, Any]:
    boundary = registry.get_boundary(boundary_id)
    if boundary is None:
        raise HTTPException(status_code=404, detail=f"保留边界 {boundary_id} 不存在")
    return boundary


@router.get("/audit")
def list_audit(
    scope_key: str | None = Query(default=None),
    boundary_id: str | None = Query(default=None),
) -> dict[str, Any]:
    """保留策略审计台账：边界核定、策略核查、时点冲突、释放登记、清理结论。"""
    items = registry.list_audit(scope_key=scope_key, boundary_id=boundary_id)
    return {"items": items, "total": len(items)}
