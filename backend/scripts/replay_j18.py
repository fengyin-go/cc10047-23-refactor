"""J18 历史样例重演脚本（保留 30 天 + 最低 3 个版本）。

运行（在 backend 目录下）：
    .venv/bin/python -I scripts/replay_j18.py

剧情：
- T0（2026-11-10 08:00）首次查看 J18 时 S4 仍有效，执行首次清理；
- 暂停期间跨过 S4 到期时间，次日 T1（2026-11-11 08:00）才恢复；
- 恢复时旧核查结果与新时间条件冲突，审计台账说明采用时点与 S4 处置；
- 刷新清理详情与审计台账，核对 S4 范围、采用时点及重试后的释放次数。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.retention_boundary import registry  # noqa: E402
from app.services import service_for  # noqa: E402
from app.store import store  # noqa: E402

T0 = "2026-11-10T08:00:00Z"
T1 = "2026-11-11T08:00:00Z"

SNAPSHOTS = [
    {"snapshot_id": "S0", "created_at": "2026-08-15T00:00:00Z"},
    {"snapshot_id": "S1", "created_at": "2026-09-01T00:00:00Z"},
    {"snapshot_id": "S2", "created_at": "2026-09-15T00:00:00Z"},
    {"snapshot_id": "S3", "created_at": "2026-10-05T00:00:00Z"},
    # T0 截止线 2026-10-11 08:00：S4 当晚 20:00 才生成 → T0 未过期；
    # T1 截止线 2026-10-12 08:00 → S4 已过期（暂停期间跨过到期）。
    {"snapshot_id": "S4", "created_at": "2026-10-11T20:00:00Z"},
    {"snapshot_id": "S5", "created_at": "2026-10-25T00:00:00Z"},
    {"snapshot_id": "S6", "created_at": "2026-11-01T00:00:00Z"},
    {"snapshot_id": "S7", "created_at": "2026-11-08T00:00:00Z"},
]


def pretty(data: object) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)


def main() -> int:
    # 隔离的内存台账，避免与其它样例数据串扰
    store._tables["retention_boundary"] = []
    store._tables["retention_audit"] = []
    store._tables["retention_snapshot_catalog"] = []

    vacuum = service_for("vacuum_job")
    entry, missing = vacuum.create_entry({
        "作业标识": "J18",
        "目标湖表": "lakehouse.dwd.events",
        "保留版本": "3",
        "清理范围": "快照 S0-S7",
        "预览大小": "512",
    })
    assert not missing
    job_id = entry["id"]
    scope_key = f"vacuum_job:{job_id}"
    registry.register_snapshot_catalog(scope_key, SNAPSHOTS)

    print("=" * 72)
    print(f"J18 重演：保留 30 天 / 最低 3 版本，首次查看时点 T0={T0}")
    print("=" * 72)

    first = vacuum.domain_clean_history(
        job_id, now=T0, attempt_ref="J18-attempt-1", retention_days=30,
    )
    print("\n[1] T0 首次查看并执行清理")
    print(f"    采用判断时点 : {first['decision_time']}")
    print(f"    截止线       : {first['cutoff_time']}")
    print(f"    可清理范围   : {first['cleanable_snapshot_ids']}")
    print(f"    新释放       : {first['newly_released_snapshot_ids']}")
    print(f"    累计释放次数 : {first['release_count']}")

    print("\n[2] 暂停跨天，次日 T1 恢复（S4 已跨过到期时间）")
    resumed = vacuum.domain_clean_history(
        job_id, now=T1, attempt_ref="J18-attempt-2", retention_days=30,
    )
    print(f"    采用判断时点 : {resumed['decision_time']}（保持 T0，不按 T1 重算）")
    print(f"    实际处理范围 : {resumed['cleanable_snapshot_ids']}（S4 未混入）")
    print(f"    新到期对象   : {resumed['recheck']['newly_due_snapshot_ids']}")
    print(f"    本次新释放   : {resumed['newly_released_snapshot_ids']}")
    print(f"    重复登记     : {resumed['duplicate_snapshot_ids']}")
    print(f"    累计释放次数 : {resumed['release_count']}（重试后仍为 1）")

    print("\n[3] 并发恢复再触发一次重复清理")
    concurrent = vacuum.domain_clean_history(
        job_id, now=T1, attempt_ref="J18-attempt-3", retention_days=30,
    )
    print(f"    本次新释放   : {concurrent['newly_released_snapshot_ids']}")
    print(f"    累计释放次数 : {concurrent['release_count']}（同一快照只释放一次）")

    print("\n[4] 刷新清理详情（原作业 + 共用边界 + 全部尝试）")
    detail = vacuum.get_job_detail(job_id)
    boundary = detail["boundary"]
    print(f"    作业         : {detail['job']['作业标识']} / {detail['job']['目标湖表']}")
    print(f"    边界编号     : {boundary['boundary_id']}")
    print(f"    判断时点     : {boundary['decision_time']}")
    print(f"    释放清单     : {boundary['released_snapshot_ids']}")
    print("    逐快照核查结论（中断前旧核查结果，仍可回看）:")
    for verdict in boundary["snapshot_verdicts"]:
        marker = "  <-- 暂停期间跨过到期，按原时点仍保留" if verdict["snapshot_id"] == "S4" else ""
        print(f"      {verdict['snapshot_id']}  {verdict['created_at']}  {verdict['verdict']}{marker}")
    print("    尝试记录:")
    for attempt in detail["attempts"]:
        print(f"      {attempt['attempt_ref']} @decision={attempt['decision_time']} "
              f"新释放={attempt['newly_released_snapshot_ids']} 重复={attempt['duplicate_snapshot_ids']}")

    print("\n[5] 刷新保留策略审计台账")
    for row in detail["audit"]:
        print(f"    [{row['audit_id']}] {row['event']} @采用时点 {row['decision_time']}")
        print(f"        {row['conclusion']}")

    # 核对断言
    assert first["cleanable_snapshot_ids"] == ["S0"]
    assert resumed["decision_time"] == T0
    assert "S4" not in resumed["cleanable_snapshot_ids"]
    assert resumed["recheck"]["newly_due_snapshot_ids"] == ["S4"]
    assert concurrent["release_count"] == 1
    s4_verdict = next(v for v in boundary["snapshot_verdicts"] if v["snapshot_id"] == "S4")
    assert s4_verdict["verdict"] == "保留-未过期"
    conflict = [r for r in detail["audit"] if r["event"] == "时点冲突"]
    assert len(conflict) == 1 and "S4" in conflict[0]["conclusion"]

    print("\n全部核对通过：S4 未混入原作业范围，采用时点固定为 T0，重试后释放次数为 1。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
