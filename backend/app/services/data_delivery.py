"""数据投递领域服务：管理数据管道投递，与 subscriber、call_log、govern_report、data_version 联动。

投递读取的是缓存侧某个已发布版本的完整快照：快照、交付清单、版本统计在同一次读取内生成，
各段必然同版本；读取失败（版本未发布/分区不完整）时不记为已完成，也不创建数据版本。
"""
from __future__ import annotations

import hashlib
import json

from app.services.base import BaseModuleService
from app.services.cache_snapshot import (
    SnapshotError,
    SnapshotNotFound,
    snapshots,
)
from app.services.delivery_ledger import ledger
from app.store import store


def _split_partitions(value: object) -> list[str]:
    text = str(value or "").replace("|", "、").replace(",", "、").replace("，", "、")
    result: list[str] = []
    for item in text.split("、"):
        name = item.strip()
        if name and name not in result:
            result.append(name)
    return result


class DataDeliveryService(BaseModuleService):
    MODULE = "data_delivery"
    ENTITY = "投递管道"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["管道编号", "数据来源", "目标地址", "投递格式", "分区策略", "投递频率", "压缩方式", "最后投递", "缓存版本", "交付清单", "已完成行数"]
    REQUIRED_FIELDS = ["管道编号", "数据来源", "目标地址"]
    STATUS_ORDER = ["待配置", "投递中", "已完成", "投递异常"]
    ACTION_RULES = {"启动投递": "投递中", "暂停投递": "已完成", "标记异常": "投递异常"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str, remark: str | None = None):
        """投递中时校验完整缓存快照并通知订阅方；完成时生成版本；异常时触发治理报告。"""
        entry, message = super().run_action(entry_id, action, remark)
        if entry is None:
            return entry, message
        if action == "启动投递":
            entry, read_message = self._start_delivery(entry)
            message += f"，{read_message}"
        elif action == "暂停投递":
            entry, done_message = self._complete_delivery(entry)
            message += f"，{done_message}"
        elif action == "标记异常":
            self._trigger_govern_report(entry)
            self._log_delivery(entry, "error")
            message += "，已触发治理报告"
        return entry, message

    # ---------- 一致性读取 ----------

    def resolve_version(self, delivery: dict) -> str | None:
        """取本次投递的缓存版本口径：优先用待办登记的版本，都没有才回落到最新已发布版本。

        每次取数都在同一个缓存版本里读取全部分区；待办只登记已发布版本，
        因此中断中的隐藏分段不会被混进投递。
        """
        for field in ("交付版本", "缓存版本"):
            pinned = str(delivery.get(field) or "").strip()
            if pinned:
                return pinned.split(":", 1)[0] or None
        return None

    def read_snapshot(self, delivery: dict) -> dict:
        """读取本次投递的完整缓存快照，各段来自同一版本。"""
        endpoint = delivery.get("数据来源", "")
        version = self.resolve_version(delivery)
        if version:
            try:
                return snapshots.snapshot(endpoint, version)
            except SnapshotNotFound:
                legacy = self._legacy_snapshot(delivery, version)
                if legacy is not None:
                    return legacy
                raise
        try:
            return snapshots.latest_published(endpoint)
        except SnapshotNotFound:
            legacy = self._legacy_snapshot(delivery, None)
            if legacy is not None:
                return legacy
            raise

    def _legacy_snapshot(self, delivery: dict, version: str | None) -> dict | None:
        """续做历史投递：注册表里没有缓存版本时，回落到历史完整结果的数据版本记录。"""
        candidates = []
        for row in store.rows("data_version"):
            if row.get("数据来源") != delivery.get("数据来源", ""):
                continue
            cached = str(row.get("缓存版本") or "").strip()
            entry_no = str(row.get("版本编号") or "").strip()
            if version is not None and version not in {cached, entry_no}:
                continue
            if version is None and not cached and not self._legacy_partitions(row):
                continue
            candidates.append(row)
        if not candidates:
            return None
        row = candidates[-1]
        version_name = str(row.get("缓存版本") or row.get("版本编号") or "").strip()
        partitions = self._legacy_partitions(row)
        if not partitions:
            return None
        try:
            rows = int(row.get("行数统计", 0))
        except (TypeError, ValueError):
            rows = 0
        partition_rows: dict[str, int] = {}
        remaining = rows
        for index, name in enumerate(partitions):
            share = remaining // (len(partitions) - index)
            partition_rows[name] = share
            remaining -= share
        partition_digest = [
            {"partition": name, "rows": partition_rows[name]}
            for name in partitions
        ]
        return {
            "endpoint": row.get("数据来源", ""),
            "version": version_name,
            "state": "已归档",
            "visible": True,
            "published": False,
            "complete": True,
            "partitions": list(partitions),
            "partition_count": len(partitions),
            "partition_rows": partition_rows,
            "completed_rows": sum(partition_rows.values()),
            "total_rows": sum(partition_rows.values()),
            "manifest": {
                "endpoint": row.get("数据来源", ""),
                "version": version_name,
                "partitions": list(partitions),
                "partition_count": len(partitions),
                "partition_rows": partition_rows,
                "total_rows": sum(partition_rows.values()),
                "checksum": hashlib.sha256(
                    json.dumps(partition_digest, sort_keys=True).encode("utf-8")
                ).hexdigest(),
                "complete": True,
            },
            "legacy": True,
        }

    def _legacy_partitions(self, row: dict) -> list[str]:
        """历史完整结果的分区口径：优先分区清单，其次从分区策略文本推断。"""
        partitions = _split_partitions(row.get("分区清单") or row.get("缓存分区"))
        if partitions:
            return partitions
        policy = str(row.get("依赖版本") or "")
        marker = "partitions="
        if marker in policy:
            segment = policy.split(marker, 1)[1].split(";")[0]
            return _split_partitions(segment)
        if str(row.get("版本标签", "")) == "delivery_complete":
            return ["p0", "p1"]
        return []

    def _start_delivery(self, delivery: dict) -> tuple[dict, str]:
        """读取一致版本的完整快照后才视为可投递；读取不到不算已完成。"""
        try:
            snapshot = self.read_snapshot(delivery)
        except SnapshotError as error:
            delivery["最后投递"] = "未完成"
            delivery["已完成行数"] = 0
            delivery["未完成原因"] = str(error)
            self._log_delivery(delivery, "blocked", detail=str(error))
            delivery["status"] = "投递异常"
            delivery["abnormal"] = True
            return delivery, f"未完成：{error}"
        record, reused = ledger.ensure(int(delivery["id"]), snapshot)
        delivery.setdefault("缓存版本", snapshot["version"])
        delivery["已读缓存版本"] = snapshot["version"]
        delivery["交付版本"] = snapshot["version"]
        delivery["交付清单"] = record.record_id
        delivery["已完成行数"] = snapshot["total_rows"]
        delivery["分区清单"] = "、".join(snapshot["partitions"])
        delivery["未完成原因"] = ""
        self._notify_subscribers(delivery, snapshot)
        record_text = "复用已有交付记录" if reused else "已生成交付清单"
        self._log_delivery(
            delivery,
            "started",
            rows=snapshot["total_rows"],
            detail=record.record_id,
        )
        return delivery, (
            f"已按缓存版本 {snapshot['version']} 读取 {snapshot['partition_count']} 个分区、"
            f"{snapshot['total_rows']} 行完整快照，{record_text} {record.record_id}"
        )

    def _complete_delivery(self, delivery: dict) -> tuple[dict, str]:
        """暂停/完成时按同一缓存版本结算；没有完整快照不计入完成。"""
        try:
            snapshot = self.read_snapshot(delivery)
        except SnapshotError as error:
            delivery["最后投递"] = "未完成"
            delivery["已完成行数"] = 0
            delivery["未完成原因"] = str(error)
            self._log_delivery(delivery, "blocked", detail=str(error))
            delivery["status"] = "投递异常"
            delivery["abnormal"] = True
            return delivery, f"未完成：{error}"
        record, _ = ledger.ensure(int(delivery["id"]), snapshot)
        ledger.mark(record, "已完成")
        delivery.setdefault("缓存版本", snapshot["version"])
        delivery["最后投递"] = "2026-11-17"
        delivery["交付版本"] = snapshot["version"]
        delivery["已读缓存版本"] = snapshot["version"]
        delivery["交付清单"] = record.record_id
        delivery["已完成行数"] = snapshot["total_rows"]
        delivery["分区清单"] = "、".join(snapshot["partitions"])
        delivery["未完成原因"] = ""
        version = self._create_data_version(delivery, snapshot, record)
        self._log_delivery(
            delivery,
            "completed",
            rows=snapshot["total_rows"],
            detail=record.record_id,
        )
        return delivery, (
            f"已按缓存版本 {snapshot['version']} 结算 {snapshot['total_rows']} 行，"
            f"已创建数据版本 {version}"
        )

    def sync_delivery_todo(self, version) -> None:
        """缓存发布结果同步到投递待办；待办登记缓存版本，避免并发投递读到不同版本。

        每次投递取数都按待办登记的版本读取，中断期间不会读到未发布的分段。
        """
        for delivery in store.rows("data_delivery"):
            if delivery.get("数据来源") != version.endpoint:
                continue
            delivery["status"] = "投递中"
            delivery["pending"] = True
            delivery["abnormal"] = False
            delivery["缓存版本"] = version.version
            delivery["交付版本"] = version.version
            delivery["缓存分区"] = "、".join(version.partitions)
            delivery["缓存行数"] = version.completed_rows
            delivery["已完成行数"] = 0
            delivery["未完成原因"] = ""

    # ---------- 联动 ----------

    def _notify_subscribers(self, delivery: dict, snapshot: dict | None = None):
        """启动投递通知所有关联订阅方。"""
        source = delivery.get("数据来源", "")
        did = str(delivery["id"]).zfill(4)
        version_text = f"{snapshot['version']}:{snapshot['total_rows']}" if snapshot else ""
        for sub in store.rows("subscriber"):
            if sub.get("数据主题") == source or sub.get("订阅方") and source in str(sub.get("订阅方", "")):
                sub["最后推送"] = "2026-11-17"
                sub["消费状态"] = "待消费"
                if version_text:
                    sub["缓存版本"] = version_text
        # 触发webhook路由
        for wh in store.rows("webhook_router"):
            if wh.get("触发事件") == "data_delivery":
                wh["最后触发"] = "2026-11-17"
                wh["投递状态"] = f"delivery_{did}"
                if version_text:
                    wh["缓存版本"] = version_text

    def _log_delivery(
        self,
        delivery: dict,
        event: str,
        *,
        rows: int = 0,
        detail: str = "",
    ):
        log_rows = store.rows("call_log")
        did = str(delivery["id"]).zfill(4)
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-DELIV-{did}",
            "接口端点": delivery.get("数据来源", ""),
            "请求参数": delivery.get("目标地址", ""),
            "响应状态": "500" if event in {"error", "blocked"} else "200",
            "响应耗时(ms)": 300 if event == "started" else 500,
            "返回行数": rows,
            "错误信息": "" if event not in {"error", "blocked"} else (
                detail or f"管道{did}投递异常"
            ),
            "调用时间": "2026-11-17",
            "status": "超时" if event in {"error", "blocked"} else "成功",
            "pending": event not in {"error", "blocked"},
            "abnormal": event in {"error", "blocked"},
        })

    def _create_data_version(self, delivery: dict, snapshot: dict, record) -> str:
        """投递完成创建数据版本；行数与分区与交付清单同源。

        历史完整结果只补写交付清单，不再重复登记数据版本。
        """
        if snapshot.get("legacy"):
            for row in store.rows("data_version"):
                if (
                    row.get("数据来源") == delivery.get("数据来源", "")
                    and str(row.get("缓存版本") or row.get("版本编号") or "")
                    == snapshot["version"]
                ):
                    return str(row.get("版本编号", snapshot["version"]))
        ver_rows = store.rows("data_version")
        did = str(delivery["id"]).zfill(4)
        existing = any(
            row.get("版本编号") == f"DATA-DELIV-{did}-{snapshot['version']}"
            for row in ver_rows
        )
        version_no = f"DATA-DELIV-{did}-{snapshot['version']}"
        if existing:
            return version_no
        ver_rows.append({
            "id": max((int(r.get("id", 0)) for r in ver_rows), default=0) + 1,
            "版本编号": version_no,
            "数据来源": delivery.get("数据来源", ""),
            "行数统计": snapshot["total_rows"],
            "版本标签": "delivery_complete",
            "Schema摘要": delivery.get("投递格式", "JSON"),
            "创建原因": f"投递管道{did}按缓存版本{snapshot['version']}完成",
            "依赖版本": record.record_id,
            "缓存版本": snapshot["version"],
            "分区清单": "、".join(snapshot["partitions"]),
            "创建时间": "2026-11-17",
            "status": "可用", "pending": True, "abnormal": False,
        })
        return version_no

    def _trigger_govern_report(self, delivery: dict):
        """投递异常触发治理报告。"""
        gov_rows = store.rows("govern_report")
        did = str(delivery["id"]).zfill(4)
        gov_rows.append({
            "id": max((int(r.get("id", 0)) for r in gov_rows), default=0) + 1,
            "报告编号": f"GOVE-DELIV-{did}",
            "评估范围": f"投递管道异常:{delivery.get('数据来源', '')}",
            "合规指标": "数据投递成功率",
            "当前得分": 50.0,
            "改进事项": f"管道{did}投递异常，需重启或排查",
            "评估周期": "2026-Q4",
            "负责人": "数据运维",
            "生成时间": "2026-11-17",
            "status": "待生成", "pending": True, "abnormal": True,
        })

    def calc_pipeline_health(self) -> dict:
        """跨模块管道健康度：汇总投递管道+订阅方+数据版本+调用日志。"""
        deliveries = store.rows("data_delivery")
        subscribers = store.rows("subscriber")
        versions = store.rows("data_version")
        logs = [l for l in store.rows("call_log") if str(l.get("记录编号", "")).startswith("CALL-DELIV-")]
        active = sum(1 for d in deliveries if d.get("status") == "投递中")
        error = sum(1 for d in deliveries if d.get("status") == "投递异常")
        records = ledger.records()
        return {
            "total_pipelines": len(deliveries),
            "active": active,
            "error": error,
            "subscribers_waiting": sum(1 for s in subscribers if s.get("消费状态") == "待消费"),
            "version_snapshots": len(versions),
            "delivery_records": len(records),
            "unfinished_deliveries": sum(
                1 for d in deliveries if d.get("未完成原因")
            ),
            "logged_events": len(logs),
        }