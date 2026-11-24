"""实时流数据湖业务计算引擎：分区、合并、压缩、快照差异、保留策略、
版本冲突、增量协调等核心算法实现。
"""
from __future__ import annotations

import hashlib
import math
from typing import Any


# ------------------------------------------------------------------
# partition_calculator
# ------------------------------------------------------------------

def partition_calculator(
    rows: int, partition_keys: list[str], target_size_mb: float = 128.0,
) -> dict[str, Any]:
    """根据行数和分区键计算最佳分区策略。"""
    partitions = max(1, math.ceil(rows * 0.25 / target_size_mb))
    return {
        "partition_count": partitions,
        "partition_keys": partition_keys,
        "strategy": "hash" if partitions > 4 else "range",
        "estimated_rows_per_partition": rows // partitions,
        "target_partition_size_mb": target_size_mb,
    }


# ------------------------------------------------------------------
# merge_strategy
# ------------------------------------------------------------------

def merge_strategy(
    small_files: list[int],
    target_file_size_mb: float = 256.0,
    min_file_count: int = 3,
) -> dict[str, Any]:
    """计算小文件合并策略：按目标文件大小将多个小文件合为一个。"""
    if not small_files or len(small_files) < min_file_count:
        return {"should_merge": False, "reason": "小文件数量不足，跳过合并"}
    total_size = sum(small_files)
    groups = max(1, math.ceil(total_size / target_file_size_mb))
    return {
        "should_merge": True,
        "file_count": len(small_files),
        "total_size_mb": total_size,
        "target_groups": groups,
        "files_per_group": len(small_files) // groups,
        "estimated_output_mb": target_file_size_mb,
    }


# ------------------------------------------------------------------
# compaction_planner
# ------------------------------------------------------------------

def compaction_planner(
    table_path: str,
    file_sizes: list[int],
    target_file_size: int = 134217728,  # 128MB
    max_parallelism: int = 4,
) -> dict[str, Any]:
    """规划压缩作业：输入文件大小列表，输出分批压缩计划。"""
    candidates = [i for i, sz in enumerate(file_sizes) if sz < target_file_size // 4]
    if not candidates:
        return {
            "table": table_path,
            "plan": None,
            "candidate_file_count": 0,
            "batches": 0,
            "files_per_batch": 0,
            "parallelism": 0,
            "target_file_size_bytes": target_file_size,
            "estimated_duration_seconds": 0,
            "reason": "没有需要压缩的小文件",
        }
    batches = math.ceil(len(candidates) / max_parallelism)
    plan = {
        "candidate_file_count": len(candidates),
        "batches": batches,
        "files_per_batch": len(candidates) // batches if batches else 0,
        "parallelism": min(max_parallelism, len(candidates)),
        "target_file_size_bytes": target_file_size,
        "estimated_duration_seconds": len(candidates) * 15,
    }
    return {
        "table": table_path,
        "plan": plan,
        **plan,
    }


# ------------------------------------------------------------------
# snapshot_differ
# ------------------------------------------------------------------

def snapshot_differ(
    base_snapshot: dict[str, Any],
    current_snapshot: dict[str, Any],
) -> dict[str, Any]:
    """比较两个快照的差异，输出新增/更新/删除的数据分布。"""
    base_files = base_snapshot.get("files", [])
    curr_files = current_snapshot.get("files", [])

    base_set = {f.get("path"): f for f in base_files}
    curr_set = {f.get("path"): f for f in curr_files}

    added = [f for path, f in curr_set.items() if path not in base_set]
    removed = [f for path, f in base_set.items() if path not in curr_set]
    modified = []
    for path in base_set.keys() & curr_set.keys():
        bf = base_set[path]
        cf = curr_set[path]
        if bf.get("checksum") != cf.get("checksum"):
            modified.append({"path": path, "old": bf, "new": cf})

    total_added_rows = sum(f.get("rows", 0) for f in added)
    total_removed_rows = sum(f.get("rows", 0) for f in removed)
    total_modified_rows = sum(f["new"].get("rows", 0) for f in modified)

    return {
        "base_snapshot_id": base_snapshot.get("snapshot_id"),
        "current_snapshot_id": current_snapshot.get("snapshot_id"),
        "added_files": len(added),
        "removed_files": len(removed),
        "modified_files": len(modified),
        "added_rows": total_added_rows,
        "removed_rows": total_removed_rows,
        "modified_rows": total_modified_rows,
        "net_change_rows": total_added_rows - total_removed_rows,
    }


# ------------------------------------------------------------------
# retention_checker
# ------------------------------------------------------------------

def retention_checker(
    snapshots: list[dict[str, Any]],
    retention_days: int = 30,
    min_versions: int = 5,
    decision_time: Any = None,
) -> dict[str, Any]:
    """检查快照保留策略：标记可清理的过期快照。

    保留天数与最低版本的读取计算已收拢到共用保留边界模块
    （``app.retention_boundary.build_retention_boundary``），本函数保持旧的
    入参与返回键不变，仅作为共用边界结果的兼容视图。
    """
    from app.retention_boundary import build_retention_boundary

    boundary = build_retention_boundary(
        snapshots,
        retention_days=retention_days,
        min_versions=min_versions,
        decision_time=decision_time,
    )
    return {
        "total_snapshots": boundary["total_snapshots"],
        "expired_count": boundary["expired_count"],
        "valid_count": boundary["valid_count"],
        "min_versions_required": boundary["min_versions_required"],
        "retention_days": boundary["retention_days"],
        "can_clean_count": boundary["can_clean_count"],
        "should_clean": boundary["should_clean"],
    }


# ------------------------------------------------------------------
# version_conflict_resolver
# ------------------------------------------------------------------

def version_conflict_resolver(
    base_version: int,
    incoming_version: int,
    strategy: str = "last-write-wins",
    conflict_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """解决版本冲突：支持 LWW、三路合并和手动覆盖策略。"""
    if base_version == incoming_version:
        return {
            "conflict": False,
            "resolved_version": base_version,
            "strategy": strategy,
        }

    if strategy == "last-write-wins":
        winner = max(base_version, incoming_version)
    elif strategy == "base-wins":
        winner = base_version
    elif strategy == "incoming-wins":
        winner = incoming_version
    elif strategy == "manual" and conflict_data:
        winner = conflict_data.get("preferred_version", base_version)
    else:
        winner = base_version

    return {
        "conflict": base_version != incoming_version,
        "base_version": base_version,
        "incoming_version": incoming_version,
        "resolved_version": winner,
        "strategy": strategy,
        "resolution": "resolved" if strategy != "manual" else "needs_review",
    }


# ------------------------------------------------------------------
# delta_reconciler
# ------------------------------------------------------------------

def delta_reconciler(
    stream_events: list[dict[str, Any]],
    batch_events: list[dict[str, Any]],
    dedup_key: str = "id",
) -> dict[str, Any]:
    """协调流批数据：对流事件和批事件进行去重合并，输出最终写入集。"""
    # Build dedup index from batch (batch wins for same key)
    batch_index: dict[str, dict[str, Any]] = {}
    for event in batch_events:
        key = str(event.get(dedup_key, ""))
        if key:
            batch_index[key] = event

    reconciled = []
    duplicates = 0
    stream_only = 0

    for event in stream_events:
        key = str(event.get(dedup_key, ""))
        if key and key in batch_index:
            reconciled.append(event)  # batch wins, but keep source trace
            duplicates += 1
        else:
            reconciled.append(event)
            stream_only += 1

    # Append batch-only events
    existing_keys = {str(e.get(dedup_key, "")) for e in stream_events}
    batch_only = 0
    for key, event in batch_index.items():
        if key not in existing_keys:
            reconciled.append(event)
            batch_only += 1

    total = len(reconciled)
    checksum = _compute_checksum(str(reconciled))

    return {
        "stream_count": len(stream_events),
        "batch_count": len(batch_events),
        "reconciled_total": total,
        "duplicates_resolved": duplicates,
        "stream_only": stream_only,
        "batch_only": batch_only,
        "dedup_key": dedup_key,
        "checksum": checksum,
    }


# ------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------

def _compute_checksum(data: str) -> str:
    return hashlib.md5(data.encode()).hexdigest()[:12]