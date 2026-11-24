"""共用保留边界重构测试。

覆盖：
1. 旧合法参数的输入输出保持不变（retention_checker / 两个 domain_* 方法）；
2. J18 重演：保留 30 天 + 最低 3 个版本，T0 首次查看时 S4 仍有效，暂停跨天
   到次日 T1 恢复，旧核查结果与新时间条件冲突；
3. 一次执行只照同一时点处理，中断后核查结果可回看；
4. 并发恢复与重复清理只登记一次释放，后续到期对象（S4）不混入原作业；
5. 清理结论回写保留策略审计台账，作业入口仍展示原作业与全部尝试。
"""
from __future__ import annotations

import sys
sys.path.insert(0, '.')

from fastapi.testclient import TestClient

from app.compute import retention_checker
from app.main import app
from app.retention_boundary import build_retention_boundary, registry
from app.services import service_for
from app.store import store

client = TestClient(app)

T0 = "2026-11-10T08:00:00Z"   # J18 首次查看/首次清理判断时点
T1 = "2026-11-11T08:00:00Z"   # 次日恢复时点（暂停期间跨过 S4 到期）

J18_SNAPSHOTS = [
    # 截止线 T0=2026-10-11 08:00，T1=2026-10-12 08:00
    {"snapshot_id": "S0", "created_at": "2026-08-15T00:00:00Z"},  # 两时点均过期
    {"snapshot_id": "S1", "created_at": "2026-09-01T00:00:00Z"},  # 两时点均过期
    {"snapshot_id": "S2", "created_at": "2026-09-15T00:00:00Z"},  # 两时点均过期
    {"snapshot_id": "S3", "created_at": "2026-10-05T00:00:00Z"},  # 两时点均过期
    {"snapshot_id": "S4", "created_at": "2026-10-11T20:00:00Z"},  # T0 未过期、T1 已过期
    {"snapshot_id": "S5", "created_at": "2026-10-25T00:00:00Z"},  # 两时点均未过期
    {"snapshot_id": "S6", "created_at": "2026-11-01T00:00:00Z"},
    {"snapshot_id": "S7", "created_at": "2026-11-08T00:00:00Z"},
]


def _reset_boundary_tables() -> None:
    store._tables["retention_boundary"] = []
    store._tables["retention_audit"] = []
    store._tables["retention_snapshot_catalog"] = []


def _create_j18() -> tuple[int, str]:
    svc = service_for("vacuum_job")
    entry, missing = svc.create_entry({
        "作业标识": "J18",
        "目标湖表": "lakehouse.dwd.events",
        "保留版本": "3",
        "清理范围": "快照 S0-S7",
        "预览大小": "512",
    })
    assert not missing
    scope_key = f"vacuum_job:{entry['id']}"
    registry.register_snapshot_catalog(scope_key, J18_SNAPSHOTS)
    return entry["id"], scope_key


class TestLegacyContract:
    def test_retention_checker_keys_unchanged(self):
        snaps = [{"snapshot_id": f"s{i}", "created_at": "2024-10-10T00:00:00Z"} for i in range(9)]
        result = retention_checker(snaps, retention_days=30)
        assert set(result) == {
            "total_snapshots", "expired_count", "valid_count",
            "min_versions_required", "retention_days",
            "can_clean_count", "should_clean",
        }
        assert result["retention_days"] == 30
        assert result["min_versions_required"] == 5
        assert result["can_clean_count"] == max(0, result["expired_count"] - 5)

    def test_clean_history_legacy_output_unchanged(self):
        _reset_boundary_tables()
        result = service_for("vacuum_job").domain_clean_history(1)
        for key in ("job_id", "table", "retained_versions", "files_cleaned",
                    "reclaimed_bytes", "clean_range", "duration_seconds"):
            assert key in result
        assert result["files_cleaned"] == 42
        assert result["duration_seconds"] == 12
        assert result["retained_versions"] == 5

    def test_check_retention_legacy_output_unchanged(self):
        _reset_boundary_tables()
        result = service_for("retention_policy").domain_check_retention(1)
        assert result["ok"] is True
        assert "should_clean" in result and "can_clean_count" in result
        assert result["rule_id"] == "保留策略样例1"
        # 历史样例：7 个 2024-10-10 + 2 个 2024-11-20，相对今天全部过期
        assert result["total_snapshots"] == 9
        assert result["expired_count"] == 9


class TestJ18Replay:
    def test_s4_valid_at_t0_and_expires_at_t1(self):
        """先重演保留 30 天/最低 3 版本的纯边界：S4 的跨期事实。"""
        boundary_t0 = build_retention_boundary(
            J18_SNAPSHOTS, retention_days=30, min_versions=3, decision_time=T0,
        )
        verdict_t0 = {v["snapshot_id"]: v["verdict"] for v in boundary_t0["snapshot_verdicts"]}
        assert verdict_t0["S4"] == "保留-未过期"
        assert "S4" not in boundary_t0["cleanable_snapshot_ids"]
        # T0：S0-S3 过期 4 个，最低保留 3 个 → 仅最早的 S0 可清理
        assert boundary_t0["cleanable_snapshot_ids"] == ["S0"]
        assert boundary_t0["decision_time"] == T0

        boundary_t1 = build_retention_boundary(
            J18_SNAPSHOTS, retention_days=30, min_versions=3, decision_time=T1,
        )
        verdict_t1 = {v["snapshot_id"]: v["verdict"] for v in boundary_t1["snapshot_verdicts"]}
        assert verdict_t1["S4"] != "保留-未过期"
        assert "S4" in boundary_t1["expired_snapshot_ids"]

    def test_full_interrupted_job_keeps_t0_scope_and_single_release(self):
        _reset_boundary_tables()
        job_id, scope_key = _create_j18()
        svc = service_for("vacuum_job")

        # 首次查看并执行：T0 核定边界，S4 仍有效
        first = svc.domain_clean_history(job_id, now=T0, attempt_ref="J18-attempt-1", retention_days=30)
        assert first["decision_time"] == T0
        assert first["cleanable_snapshot_ids"] == ["S0"]
        assert first["release_count"] == 1
        assert first["newly_released_snapshot_ids"] == ["S0"]
        assert "S4" not in first["cleanable_snapshot_ids"]

        boundary = registry.find_boundary_by_scope(scope_key)
        verdicts = {v["snapshot_id"]: v for v in boundary["snapshot_verdicts"]}
        assert verdicts["S4"]["verdict"] == "保留-未过期"  # 中断后过去的核查结果可回看

        # 暂停跨天，次日 T1 恢复：S4 已到期，但只能照 T0 时点处理
        resumed = svc.domain_clean_history(job_id, now=T1, attempt_ref="J18-attempt-2", retention_days=30)
        assert resumed["decision_time"] == T0                      # 采用时点不变
        assert resumed["cleanable_snapshot_ids"] == ["S0"]         # 处理范围不变
        assert "S4" not in resumed["cleanable_snapshot_ids"]       # 新到期对象不混入
        assert resumed["newly_released_snapshot_ids"] == []        # 无重复释放
        assert resumed["duplicate_snapshot_ids"] == ["S0"]
        assert resumed["release_count"] == 1                       # 重试后释放次数仍为 1
        assert resumed["recheck"]["newly_due_snapshot_ids"] == ["S4"]
        assert resumed["recheck"]["applied_decision_time"] == T0

        # 并发恢复：再来一次重复清理，仍然只登记一次释放，冲突只记一次
        concurrent = svc.domain_clean_history(job_id, now=T1, attempt_ref="J18-attempt-3", retention_days=30)
        assert concurrent["release_count"] == 1
        assert concurrent["newly_released_snapshot_ids"] == []
        assert concurrent["duplicate_snapshot_ids"] == ["S0"]

        # 边界结果数据始终不可变：S4 的历史核查结论仍是 T0 的“未过期”
        boundary2 = registry.find_boundary_by_scope(scope_key)
        verdicts2 = {v["snapshot_id"]: v for v in boundary2["snapshot_verdicts"]}
        assert verdicts2["S4"]["verdict"] == "保留-未过期"
        assert boundary2["decision_time"] == T0
        assert boundary2["released_snapshot_ids"] == ["S0"]
        assert boundary2["release_count"] == 1
        assert len(boundary2["release_attempts"]) == 3

        # 审计台账：说明采用时点与 S4 处置；冲突事件只出现一次
        audit = registry.list_audit(scope_key=scope_key)
        events = [row["event"] for row in audit]
        assert events.count("时点冲突") == 1
        assert events.count("边界核定") == 1
        assert events.count("释放登记") == 3
        assert events.count("清理结论") == 3
        conflict_row = next(row for row in audit if row["event"] == "时点冲突")
        assert conflict_row["decision_time"] == T0
        assert "S4" in conflict_row["conclusion"]
        assert "不混入" in conflict_row["conclusion"]
        for row in audit:
            assert row["decision_time"] == T0  # 台账每条都写明采用时点

        # 作业入口仍展示原作业与后续尝试
        detail = svc.get_job_detail(job_id)
        assert detail["ok"] is True
        assert detail["job"]["作业标识"] == "J18"
        assert len(detail["attempts"]) == 3
        assert [a["attempt_ref"] for a in detail["attempts"]] == [
            "J18-attempt-1", "J18-attempt-2", "J18-attempt-3",
        ]
        assert len(detail["audit"]) == len(audit)

    def test_concurrent_first_runs_share_one_fixed_boundary(self):
        """并发恢复场景：同一 now 下两次进入，边界只核定一次、释放只登记一次。"""
        _reset_boundary_tables()
        job_id, _ = _create_j18()
        svc = service_for("vacuum_job")
        r1 = svc.domain_clean_history(job_id, now=T0, attempt_ref="c-1", retention_days=30)
        r2 = svc.domain_clean_history(job_id, now=T0, attempt_ref="c-2", retention_days=30)
        assert r1["boundary_id"] == r2["boundary_id"]
        assert r1["decision_time"] == r2["decision_time"] == T0
        assert r1["release_count"] == 1 and r2["release_count"] == 1
        assert r2["newly_released_snapshot_ids"] == []
        assert registry.list_boundaries().__len__() == 1


class TestPolicySideBoundary:
    def test_policy_check_fixes_decision_time_and_appends_audit(self):
        _reset_boundary_tables()
        svc = service_for("retention_policy")
        r1 = svc.domain_check_retention(1, now=T0)
        r2 = svc.domain_check_retention(1, now=T1)  # 次日再查
        assert r1["boundary_id"] == r2["boundary_id"]
        assert r2["decision_time"] == T0             # 复用同一时点
        audit = registry.list_audit(scope_key="retention_policy:1")
        assert any(row["event"] == "边界核定" for row in audit)
        assert sum(1 for row in audit if row["event"] == "策略核查") == 2
        for row in audit:
            assert row["decision_time"] == T0


class TestLifecycleHttpEndpoints:
    def test_http_replay_detail_and_ledger(self):
        _reset_boundary_tables()
        job_id, scope_key = _create_j18()

        resp = client.post(f"/api/lifecycle/vacuum-jobs/{job_id}/clean",
                           params={"now": T0, "attempt_ref": "http-1", "retention_days": 30})
        assert resp.status_code == 200
        assert resp.json()["cleanable_snapshot_ids"] == ["S0"]

        resp = client.post(f"/api/lifecycle/vacuum-jobs/{job_id}/clean",
                           params={"now": T1, "attempt_ref": "http-2", "retention_days": 30})
        assert resp.status_code == 200
        body = resp.json()
        assert body["decision_time"] == T0
        assert body["release_count"] == 1

        # 刷新清理详情
        resp = client.get(f"/api/lifecycle/vacuum-jobs/{job_id}/detail")
        assert resp.status_code == 200
        detail = resp.json()
        assert detail["boundary"]["snapshot_verdicts"]
        assert len(detail["attempts"]) == 2

        # 刷新审计台账
        resp = client.get("/api/lifecycle/audit", params={"scope_key": scope_key})
        assert resp.status_code == 200
        ledger = resp.json()["items"]
        assert {row["event"] for row in ledger} >= {
            "边界核定", "时点冲突", "释放登记", "清理结论",
        }
        s4_rows = [row for row in ledger if "S4" in row["conclusion"]]
        assert s4_rows and all(row["decision_time"] == T0 for row in s4_rows)

        # 共用保留边界结果数据可单独回看
        resp = client.get("/api/lifecycle/boundaries", params={"scope_key": scope_key})
        assert resp.status_code == 200
        assert resp.json()["total"] == 1
