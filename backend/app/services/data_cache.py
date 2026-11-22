"""数据缓存领域服务：管理结果缓存策略，与 data_delivery、subscriber、schema_diff 联动。

强制刷新按分段采集执行：所有分区提交成功才发布完整快照，发布后才通知订阅方并同步投递待办；
任一分区缺失时版本留在隐藏暂存区，已提交分区保留，恢复时原位续写，未完成的版本不会进入交付。
"""
from __future__ import annotations

import re

from app.services.base import BaseModuleService
from app.services.cache_snapshot import (
    INTERRUPTED,
    PUBLISHED,
    CacheVersion,
    snapshots,
)
from app.store import store

DEFAULT_PARTITIONS = ["p0", "p1"]
_PARTITION_HINT = re.compile(r"(?:partitions?|分区)\s*[=:]\s*([^\s;；]+)", re.IGNORECASE)
_SELECTION_HINT = re.compile(r"(?:segments?|分区|只写入)\s*[=:]\s*([^\s;；]+)", re.IGNORECASE)


def parse_partitions(policy: str) -> list[str]:
    """从投递管道的分区策略里解析采集分区，未声明时按两段采集。"""
    matched = _PARTITION_HINT.search(str(policy or ""))
    if not matched:
        return list(DEFAULT_PARTITIONS)
    partitions = [item.strip() for item in re.split(r"[,，|]", matched.group(1)) if item.strip()]
    return partitions or list(DEFAULT_PARTITIONS)


def parse_selection(remark: str | None) -> list[str] | None:
    """动作备注里可指定本次只写哪些分区，如「分区=p0」。"""
    if not remark:
        return None
    matched = _SELECTION_HINT.search(remark)
    if not matched:
        return None
    values: list[str] = []
    for item in re.split(r"[,，|]", matched.group(1)):
        text = item.strip()
        if text and text not in values:
            values.append(text)
    return values or None


class DataCacheService(BaseModuleService):
    MODULE = "data_cache"
    ENTITY = "缓存策略"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["策略编号", "目标端点", "缓存TTL", "缓存键", "最大条目", "淘汰算法", "命中次数", "最后刷新", "缓存版本", "已完成行数", "暂存版本", "暂存已完成行数"]
    REQUIRED_FIELDS = ["策略编号", "目标端点", "缓存TTL"]
    STATUS_ORDER = ["草稿", "生效中", "容量告警", "已停用"]
    ACTION_RULES = {"启效缓存": "生效中", "强制刷新": "容量告警", "停用缓存": "已停用"}
    NEGATIVE_ACTIONS = ["停用缓存"]

    def run_action(self, entry_id: int, action: str, remark: str | None = None):
        """启效缓存时同步投递管道；强制刷新写入完整快照后通知订阅方；停用缓存时更新Schema差异。"""
        entry, message = super().run_action(entry_id, action, remark)
        if entry is None:
            return entry, message
        if action == "启效缓存":
            self._sync_delivery_pipeline(entry)
            message += "，已同步数据投递管道"
        elif action == "强制刷新":
            entry, refresh_message = self.refresh_cache(entry, remark)
            message += f"，{refresh_message}"
        elif action == "停用缓存":
            self._record_schema_diff(entry)
            message += "，已记录Schema差异"
        return entry, message

    # ---------- 分段刷新 ----------

    def partitions_for(self, endpoint: str) -> list[str]:
        for delivery in store.rows("data_delivery"):
            if delivery.get("数据来源") == endpoint:
                return parse_partitions(delivery.get("分区策略", ""))
        return list(DEFAULT_PARTITIONS)

    def collect_partition_rows(self, endpoint: str, partition: str, partitions: list[str]) -> list[dict]:
        """采集一个分区的结果；同一端点同一分区重复采集得到相同内容。"""
        logs = [
            row for row in store.rows("call_log")
            if row.get("接口端点") == endpoint and "CALL-DELIV-" not in str(row.get("记录编号", ""))
        ]
        if logs:
            index = partitions.index(partition)
            selected = [
                {
                    "endpoint": endpoint,
                    "partition": partition,
                    "call": row.get("记录编号", ""),
                    "rows": row.get("返回行数", 0),
                    "at": row.get("调用时间", ""),
                }
                for offset, row in enumerate(logs)
                if offset % len(partitions) == index
            ]
            if selected:
                return selected
        return [
            {"endpoint": endpoint, "partition": partition, "seq": seq}
            for seq in range(1, 4)
        ]

    def refresh_cache(self, cache: dict, remark: str | None = None) -> tuple[dict, str]:
        """执行一次分段刷新；只有全部分区提交成功才发布并通知下游。"""
        endpoint = str(cache.get("目标端点", ""))
        if not endpoint:
            return cache, "目标端点为空，无法刷新缓存"
        # 没有投递管道的缓存策略先补齐管道，发布结果才有投递待办可以同步。
        self._sync_delivery_pipeline(cache)
        partitions = self.partitions_for(endpoint)
        version = snapshots.declare(endpoint, partitions, source="forced_refresh")
        selection = parse_selection(remark) or list(partitions)
        interrupt = any(token in str(remark or "") for token in ("中断", "interrupt"))
        if interrupt and selection == partitions:
            selection = partitions[:1]

        unknown = [name for name in selection if name not in partitions]
        if unknown:
            snapshots.interrupt(endpoint, version=version.version)
            self._mark_staged(cache, version)
            return cache, (
                f"缓存版本 {version.version} 未发布，分区 {'、'.join(unknown)} 未声明"
                f"（已声明：{'、'.join(partitions)}）"
            )

        for name in selection:
            rows = self.collect_partition_rows(endpoint, name, partitions)
            snapshots.submit_segment(endpoint, name, rows, version=version.version)
            if interrupt:
                snapshots.interrupt(endpoint, version=version.version)
                break

        cache["最后刷新"] = version.created_at
        if version.complete and version.state != PUBLISHED:
            published = snapshots.publish(endpoint, version=version.version)
            self._notify_subscribers(cache, published)
            self._sync_delivery_todo(published)
            cache["缓存版本"] = published.version
            cache["已完成行数"] = published.completed_rows
            cache.pop("暂存版本", None)
            cache.pop("暂存已完成行数", None)
            return cache, (
                f"缓存版本 {published.version} 已发布，"
                f"{published.manifest()['partition_count']} 个分区、"
                f"{published.completed_rows} 行已生成完整快照，已通知订阅方"
            )

        if version.state != INTERRUPTED:
            snapshots.interrupt(endpoint, version=version.version)
        self._mark_staged(cache, version)
        missing = version.missing_partitions()
        return cache, (
            f"缓存版本 {version.version} 采集中断，未发布：已完成分区 "
            f"{'、'.join(version.committed_partitions()) or '无'}，"
            f"缺失分区 {'、'.join(missing) or '无'}；订阅方和投递待办未更新"
        )

    def _mark_staged(self, cache: dict, version: CacheVersion) -> None:
        """未发布版本只登记在暂存字段，已发布版本的读取口径保持不变。"""
        cache["暂存版本"] = version.version
        cache["暂存分区"] = "、".join(version.committed_partitions())
        cache["暂存已完成行数"] = version.completed_rows

    def _sync_delivery_pipeline(self, cache: dict):
        """缓存生效后检查投递管道是否存在，不存在则创建。"""
        endpoint = cache.get("目标端点", "")
        exist = any(
            d.get("数据来源") == endpoint
            for d in store.rows("data_delivery")
        )
        if not exist:
            del_rows = store.rows("data_delivery")
            cid = str(cache["id"]).zfill(4)
            del_rows.append({
                "id": max((int(r.get("id", 0)) for r in del_rows), default=0) + 1,
                "管道编号": f"DATA-CACHE-{cid}",
                "数据来源": endpoint,
                "目标地址": f"cache://{endpoint}/v1",
                "投递格式": "JSON",
                "分区策略": f"partitions=p0,p1;ttl={cache.get('缓存TTL', '3600')}s",
                "投递频率": "on_cache_publish",
                "压缩方式": "snappy",
                "最后投递": "",
                "status": "待配置", "pending": True, "abnormal": False,
            })

    def _sync_delivery_todo(self, version: CacheVersion):
        """发布结果同步到投递待办，交付时只按该版本读取。"""
        from app.services.data_delivery import DataDeliveryService

        DataDeliveryService().sync_delivery_todo(version)

    def _notify_subscribers(self, cache: dict, version: CacheVersion | None = None):
        """完整快照发布后才通知所有订阅方和webhook路由。"""
        endpoint = cache.get("目标端点", "")
        detail = f"{version.version}:{version.completed_rows}" if version else ""
        for sub in store.rows("subscriber"):
            if sub.get("数据主题") == endpoint:
                sub["最后推送"] = "2026-11-17"
                sub["消费状态"] = "待消费"
                if detail:
                    sub["缓存版本"] = detail
        for wh in store.rows("webhook_router"):
            if wh.get("触发事件") and endpoint in str(wh.get("触发事件", "")):
                wh["最后触发"] = "2026-11-17"
                wh["投递状态"] = "待投递"
                if detail:
                    wh["缓存版本"] = detail

    def _record_schema_diff(self, cache: dict):
        """停用缓存时创建Schema差异记录。"""
        diff_rows = store.rows("schema_diff")
        cid = str(cache["id"]).zfill(4)
        diff_rows.append({
            "id": max((int(r.get("id", 0)) for r in diff_rows), default=0) + 1,
            "记录编号": f"SCHE-CACHE-{cid}",
            "关联端点": cache.get("目标端点", ""),
            "变更字段": "cache_policy",
            "旧定义": f"ttl={cache.get('缓存TTL', '')}",
            "新定义": "cached_disabled",
            "差异等级": "WARNING",
            "影响评估": f"缓存策略{cid}停用，下游需重新评估",
            "检测时间": "2026-11-17",
            "status": "已检测", "pending": True, "abnormal": False,
        })

    def calc_cache_hit_rate(self) -> dict:
        """跨模块缓存命中率统计：综合 data_cache + data_delivery + subscriber。"""
        caches = store.rows("data_cache")
        deliveries = store.rows("data_delivery")
        subscribers = store.rows("subscriber")
        total_hits = sum(int(c.get("命中次数", 0)) for c in caches)
        active = sum(1 for c in caches if c.get("status") == "生效中")
        return {
            "cache_count": len(caches),
            "active_caches": active,
            "total_hits": total_hits,
            "delivery_pipelines": len(deliveries),
            "active_subscribers": sum(1 for s in subscribers if s.get("status") == "推送中"),
            "hit_rate_ok": active > 0,
        }