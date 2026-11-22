"""分段缓存快照的端到端回归：写入、中断恢复、一致投递与版本可追溯。

运行：cd backend && .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import pathlib
import sys
import threading
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from app.services.cache_snapshot import (  # noqa: E402
    INTERRUPTED,
    PUBLISHED,
    SnapshotIncomplete,
    SnapshotStateError,
    snapshots,
)
from app.services.data_cache import DataCacheService  # noqa: E402
from app.services.data_delivery import DataDeliveryService  # noqa: E402
from app.services.data_version import DataVersionService  # noqa: E402
from app.services.delivery_ledger import ledger  # noqa: E402
from app.store import store  # noqa: E402


def _add_cache(endpoint: str) -> int:
    rows = store.rows("data_cache")
    rows.append({
        "id": max(int(r.get("id", 0)) for r in rows) + 1,
        "status": "生效中",
        "pending": True,
        "abnormal": False,
        "策略编号": f"DATA-TEST-{endpoint}",
        "目标端点": endpoint,
        "缓存TTL": "3600",
        "缓存键": endpoint,
        "最大条目": "1000",
        "淘汰算法": "LRU",
        "命中次数": "0",
        "最后刷新": "-",
    })
    return int(rows[-1]["id"])


def _add_delivery(endpoint: str) -> int:
    rows = store.rows("data_delivery")
    rows.append({
        "id": max(int(r.get("id", 0)) for r in rows) + 1,
        "status": "待配置",
        "pending": True,
        "abnormal": False,
        "管道编号": f"DATA-TEST-{endpoint}",
        "数据来源": endpoint,
        "目标地址": f"https://sink.example.com/{endpoint}",
        "投递格式": "JSON",
        "分区策略": "partitions=p0,p1;ttl=3600s",
        "投递频率": "on_cache_publish",
        "压缩方式": "snappy",
        "最后投递": "",
    })
    return int(rows[-1]["id"])


class SegmentSnapshotTest(unittest.TestCase):
    def setUp(self) -> None:
        self.endpoint = f"ENDP-TEST-{id(self)}"
        self.cache_id = _add_cache(self.endpoint)
        self.delivery_id = _add_delivery(self.endpoint)
        self.cache_service = DataCacheService()
        self.delivery_service = DataDeliveryService()

    def refresh(self, remark: str | None = None) -> str:
        _, message = self.cache_service.run_action(
            self.cache_id, "强制刷新", remark)
        return message

    def deliver(self, action: str) -> str:
        _, message = self.delivery_service.run_action(self.delivery_id, action)
        return message

    def delivery(self) -> dict:
        return store.rows("data_delivery")[self.delivery_id - 1]

    def test_full_refresh_publishes_shared_version_to_todo(self) -> None:
        self.refresh()
        snapshot = snapshots.latest_published(self.endpoint)
        self.assertEqual(snapshot["state"], PUBLISHED)
        self.assertEqual(snapshot["partition_count"], 2)
        todo = self.delivery()
        self.assertEqual(todo["缓存版本"], snapshot["version"])
        self.assertEqual(todo["status"], "投递中")

    def test_two_reads_of_one_delivery_use_same_version(self) -> None:
        self.refresh()
        v1 = snapshots.latest_published(self.endpoint)

        self.deliver("启动投递")
        first = self.delivery()["已完成行数"]
        self.deliver("暂停投递")
        todo = self.delivery()

        self.assertEqual(todo["缓存版本"], v1["version"])
        self.assertEqual(todo["已完成行数"], first)
        records = ledger.records(self.delivery_id)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["version"], v1["version"])
        self.assertEqual(
            sorted(records[0]["partitions"]), sorted(v1["partitions"]))

    def test_interrupted_version_is_hidden_and_resumable(self) -> None:
        self.refresh()
        v1 = snapshots.latest_published(self.endpoint)
        v1_rows = v1["total_rows"]

        message = self.refresh("分区=p0 中断")
        self.assertIn("未发布", message)
        state = snapshots.endpoint_state(self.endpoint)
        staging = state["staging"]
        self.assertEqual(staging["state"], INTERRUPTED)
        self.assertEqual(staging["completed_partitions"], ["p0"])
        self.assertEqual(staging["missing_partitions"], ["p1"])
        self.assertNotEqual(staging["version"], v1["version"])
        self.assertFalse(staging["visible"])
        self.assertTrue(state["published"]["visible"])

        with self.assertRaises(SnapshotIncomplete):
            snapshots.snapshot(self.endpoint, staging["version"])

        # 中断期间投递不混入隐藏分段，也不会丢掉已完成行数
        self.deliver("启动投递")
        todo = self.delivery()
        self.assertEqual(todo["缓存版本"], v1["version"])
        self.assertEqual(todo["已完成行数"], v1_rows)
        self.assertEqual(len(ledger.records(self.delivery_id)), 1)

        # 恢复缺失分区并发布
        self.refresh()
        v2 = snapshots.latest_published(self.endpoint)
        self.assertNotEqual(v2["version"], v1["version"])
        self.assertTrue(v2["complete"])
        self.assertEqual(
            snapshots.snapshot(self.endpoint, v1["version"])["total_rows"],
            v1_rows,
        )

        self.deliver("暂停投递")
        todo = self.delivery()
        self.assertEqual(todo["缓存版本"], v2["version"])
        record = ledger.records(self.delivery_id)[-1]
        self.assertEqual(sorted(record["partitions"]), sorted(v2["partitions"]))
        self.assertEqual(record["total_rows"], v2["total_rows"])
        versions = [
            row for row in store.rows("data_version")
            if row.get("缓存版本") == v2["version"]
            and row.get("数据来源") == self.endpoint
        ]
        self.assertEqual(len(versions), 1)
        self.assertEqual(
            sorted(str(versions[0]["分区清单"]).split("、")),
            sorted(record["partitions"]),
        )
        self.assertEqual(int(versions[0]["行数统计"]), record["total_rows"])

    def test_hidden_version_cannot_be_delivered(self) -> None:
        self.refresh("分区=p0 中断")
        staging = snapshots.endpoint_state(self.endpoint)["staging"]
        todo = self.delivery()
        todo["交付版本"] = staging["version"]

        message = self.deliver("暂停投递")
        todo = self.delivery()
        self.assertIn("未完成", message)
        self.assertEqual(todo["status"], "投递异常")
        self.assertEqual(todo["已完成行数"], 0)
        self.assertEqual(ledger.records(self.delivery_id), [])

    def test_duplicate_segment_write_is_idempotent(self) -> None:
        version = snapshots.declare(self.endpoint, ["p0", "p1"])
        rows = [{"endpoint": self.endpoint, "n": 1}]
        _, first, duplicate = snapshots.submit_segment(
            self.endpoint, "p0", rows, version=version.version)
        _, second, repeated = snapshots.submit_segment(
            self.endpoint, "p0", rows, version=version.version)
        self.assertFalse(duplicate)
        self.assertTrue(repeated)
        self.assertEqual(first.commits, second.commits)
        self.assertEqual(second.commits, 1)
        events = snapshots.version_view(self.endpoint, version.version)["events"]
        self.assertEqual(
            sum(1 for event in events if event["event"] == "segment_committed"), 1)

        with self.assertRaises(SnapshotIncomplete):
            snapshots.publish(self.endpoint, version=version.version)

    def test_published_segment_cannot_be_overwritten(self) -> None:
        version = snapshots.declare(self.endpoint, ["p0", "p1"])
        for name in ("p0", "p1"):
            snapshots.submit_segment(
                self.endpoint, name, [{"p": name}], version=version.version)
        snapshots.publish(self.endpoint, version=version.version)
        with self.assertRaises(SnapshotStateError):
            snapshots.submit_segment(
                self.endpoint, "p0", [{"p": "changed"}], version=version.version)

    def test_concurrent_reader_never_sees_mixed_partitions(self) -> None:
        parts = ["p0", "p1"]
        seed = snapshots.declare(self.endpoint, parts)
        for name in parts:
            snapshots.submit_segment(
                self.endpoint, name, [{"p": name, "n": 5}], version=seed.version)
        snapshots.publish(self.endpoint, version=seed.version)

        errors: list[str] = []
        stop = threading.Event()

        def reader() -> None:
            while not stop.is_set():
                snapshot = snapshots.snapshot(self.endpoint)
                versions = {
                    segment["version"] for segment in snapshot["segments"].values()
                }
                if versions != {snapshot["version"]}:
                    errors.append(f"mixed versions: {versions}")
                if not snapshot["complete"] or snapshot["partition_count"] != 2:
                    errors.append(f"incomplete: {snapshot['partitions']}")
                if snapshot["total_rows"] != sum(
                        snapshot["partition_rows"].values()):
                    errors.append("row total mismatch")

        thread = threading.Thread(target=reader)
        thread.start()
        try:
            for _ in range(10):
                version = snapshots.declare(self.endpoint, parts)
                snapshots.submit_segment(
                    self.endpoint, "p0", [{"p": "p0", "n": 5}],
                    version=version.version)
                snapshots.submit_segment(
                    self.endpoint, "p1", [{"p": "p1", "n": 5}],
                    version=version.version)
                snapshots.publish(self.endpoint, version=version.version)
        finally:
            stop.set()
            thread.join()
        self.assertEqual(errors[:3], [])

    def test_version_lineage_reports_complete_partitions(self) -> None:
        self.refresh()
        self.deliver("暂停投递")
        lineage = DataVersionService().check_version_lineage(self.endpoint)
        self.assertGreaterEqual(lineage["complete_versions"], 1)
        for stat in lineage["version_stats"]:
            self.assertEqual(stat["partition_count"], 2)
        self.assertEqual(
            sum(stat["rows"] for stat in lineage["version_stats"]),
            self.delivery()["已完成行数"],
        )


if __name__ == "__main__":
    unittest.main()
