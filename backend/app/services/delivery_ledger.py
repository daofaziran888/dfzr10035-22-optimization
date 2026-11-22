"""交付账本：记录每次投递基于哪个完整缓存版本生成交付清单。

账本只追加不覆盖，重复投递同一缓存版本会复用已有交付记录（同一版本只生成一条），
避免并发投递把同一份缓存快照重复落库。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class DeliveryRecord:
    record_id: str
    delivery_id: int
    endpoint: str
    version: str
    partitions: list[str]
    partition_rows: dict[str, int]
    total_rows: int
    manifest_checksum: str
    created_at: str = field(default_factory=_now)
    state: str = "已生成"
    version_entry: str = ""

    def manifest(self) -> dict[str, Any]:
        return {
            "record_id": self.record_id,
            "delivery_id": self.delivery_id,
            "endpoint": self.endpoint,
            "version": self.version,
            "partitions": list(self.partitions),
            "partition_count": len(self.partitions),
            "partition_rows": dict(self.partition_rows),
            "total_rows": self.total_rows,
            "checksum": self.manifest_checksum,
            "state": self.state,
            "generated_at": self.created_at,
            "version_entry": self.version_entry,
        }


class DeliveryLedger:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._records: list[DeliveryRecord] = []
        self._by_key: dict[tuple[int, str], DeliveryRecord] = {}

    def ensure(
        self,
        delivery_id: int,
        snapshot: dict[str, Any],
    ) -> tuple[DeliveryRecord, bool]:
        """按缓存版本生成交付记录；同一管道同一版本只生成一次。"""
        key = (delivery_id, snapshot["version"])
        with self._lock:
            existing = self._by_key.get(key)
            if existing is not None:
                return existing, True
            record_id = f"DELIV-{delivery_id:04d}-{snapshot['version']}"
            record = DeliveryRecord(
                record_id=record_id,
                delivery_id=delivery_id,
                endpoint=snapshot["endpoint"],
                version=snapshot["version"],
                partitions=list(snapshot["partitions"]),
                partition_rows=dict(snapshot["partition_rows"]),
                total_rows=int(snapshot["total_rows"]),
                manifest_checksum=snapshot["manifest"]["checksum"],
            )
            self._records.append(record)
            self._by_key[key] = record
            return record, False

    def mark(self, record: DeliveryRecord, state: str, version_entry: str = "") -> None:
        with self._lock:
            record.state = state
            if version_entry:
                record.version_entry = version_entry

    def records(self, delivery_id: int | None = None) -> list[dict[str, Any]]:
        with self._lock:
            return [
                record.manifest()
                for record in self._records
                if delivery_id is None or record.delivery_id == delivery_id
            ]


ledger = DeliveryLedger()
