"""数据版本领域服务：管理数据快照版本，与 data_delivery、subscriber、schema_diff、govern_report 联动。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.store import store


class DataVersionService(BaseModuleService):
    MODULE = "data_version"
    ENTITY = "数据快照"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["版本编号", "数据来源", "行数统计", "版本标签", "Schema摘要", "创建原因", "依赖版本", "创建时间", "缓存版本", "分区清单"]
    REQUIRED_FIELDS = ["版本编号", "数据来源", "行数统计"]
    STATUS_ORDER = ["创建中", "可用", "标注中", "已废弃"]
    ACTION_RULES = {"创建快照": "可用", "标注版本": "标注中", "废弃版本": "已废弃"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str, remark: str | None = None):
        """创建快照时同步投递管道和订阅方；标注版本时联动治理报告；废弃版本时审计。"""
        entry, message = super().run_action(entry_id, action, remark)
        if entry is None:
            return entry, message
        if action == "创建快照":
            self._sync_delivery_and_subscribers(entry)
            self._log_version_event(entry, "created")
            message += "，已同步投递管道和订阅方"
        elif action == "标注版本":
            self._create_govern_report(entry)
            message += "，已联动治理报告"
        elif action == "废弃版本":
            self._audit_obsolescence(entry)
            message += "，已记录审计日志"
        return entry, message

    def _sync_delivery_and_subscribers(self, version: dict):
        """通知投递管道和订阅方版本已可用。"""
        source = version.get("数据来源", "")
        for delivery in store.rows("data_delivery"):
            if delivery.get("数据来源") == source:
                delivery["status"] = "投递中"
                delivery["pending"] = True
        for sub in store.rows("subscriber"):
            if sub.get("数据主题") == source:
                sub["消费状态"] = "待消费"
                sub["最后推送"] = "2026-11-17"

    def _log_version_event(self, version: dict, event: str):
        log_rows = store.rows("call_log")
        vid = str(version["id"]).zfill(4)
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-VER-{vid}",
            "接口端点": version.get("数据来源", ""),
            "请求参数": f"version_event:{event}",
            "响应状态": "200",
            "响应耗时(ms)": 30,
            "返回行数": version.get("行数统计", 0),
            "错误信息": "",
            "调用时间": "2026-11-17",
            "status": "成功", "pending": False, "abnormal": False,
        })

    def _create_govern_report(self, version: dict):
        gov_rows = store.rows("govern_report")
        vid = str(version["id"]).zfill(4)
        gov_rows.append({
            "id": max((int(r.get("id", 0)) for r in gov_rows), default=0) + 1,
            "报告编号": f"GOVE-VER-{vid}",
            "评估范围": f"数据版本标注:{version.get('数据来源', '')}",
            "合规指标": "版本标注规范",
            "当前得分": 90.0,
            "改进事项": f"版本{vid}已标注为{version.get('版本标签', '')}",
            "评估周期": "2026-Q4",
            "负责人": "数据标注组",
            "生成时间": "2026-11-17",
            "status": "已生成", "pending": False, "abnormal": False,
        })

    def _audit_obsolescence(self, version: dict):
        audit_rows = store.rows("pipeline_audit")
        vid = str(version["id"]).zfill(4)
        audit_rows.append({
            "id": max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1,
            "记录编号": f"PIPE-VER-{vid}",
            "管道名称": f"data_version:{vid}",
            "操作人": "system",
            "操作类型": "废弃",
            "变更摘要": f"版本{vid}({version.get('版本标签', '')})已废弃",
            "操作时间": "2026-11-17",
            "来源节点": "data_version",
            "审计结论": "已记录",
            "status": "已记录", "pending": False, "abnormal": False,
        })

    def check_version_lineage(self, source: str) -> dict:
        """跨模块版本谱系检查，兼容历史完整结果：只统计能追溯分区与缓存版本的完整快照。"""
        versions = [v for v in store.rows("data_version") if v.get("数据来源") == source]
        deliveries = [d for d in store.rows("data_delivery") if d.get("数据来源") == source]
        subscribers = [s for s in store.rows("subscriber") if s.get("数据主题") == source]
        complete = [
            v for v in versions
            if self._complete_partitions(v)
        ]
        stats = []
        for version in complete:
            partitions = self._complete_partitions(version)
            try:
                rows = int(version.get("行数统计", 0))
            except (TypeError, ValueError):
                rows = 0
            stats.append({
                "版本编号": version.get("版本编号"),
                "缓存版本": version.get("缓存版本") or "",
                "partitions": sorted(set(partitions)),
                "partition_count": len(set(partitions)),
                "rows": rows,
            })
        return {
            "source": source,
            "versions": len(versions),
            "active_versions": sum(1 for v in versions if v.get("status") == "可用"),
            "complete_versions": len(complete),
            "version_stats": stats,
            "delivery_pipelines": len(deliveries),
            "subscribers": len(subscribers),
        }

    @staticmethod
    def _complete_partitions(version: dict) -> list[str]:
        """完整快照的分区口径：新版看分区清单，历史完整结果从分区策略推断。"""
        raw = str(version.get("分区清单") or version.get("缓存分区") or "")
        partitions: list[str] = []
        for item in raw.replace("|", "、").replace(",", "、").replace("，", "、").split("、"):
            name = item.strip()
            if name and name not in partitions:
                partitions.append(name)
        if partitions:
            return partitions
        if not str(version.get("缓存版本", "")).strip():
            policy = str(version.get("依赖版本") or "")
            if "partitions=" in policy:
                segment = policy.split("partitions=", 1)[1].split(";")[0]
                for item in segment.replace("|", "、").replace(",", "、").split("、"):
                    name = item.strip()
                    if name and name not in partitions:
                        partitions.append(name)
            elif str(version.get("版本标签", "")) == "delivery_complete":
                partitions = ["p0", "p1"]
        return partitions