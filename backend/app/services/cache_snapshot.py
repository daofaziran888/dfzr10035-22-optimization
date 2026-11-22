"""分段采集缓存快照：写入、暂存、恢复、发布与交付的完整性边界。

这个模块只维护缓存侧的版本事实，不直接改业务模块行：

- 一个版本只有在声明的全部分区都成功提交后才允许发布，发布是唯一的可见性翻转。
- 未发布的版本留在暂存区（采集中/已中断），刷新中断后可以原位恢复，已提交分区不会重复写入。
- 交付只读取某一个已发布版本的完整快照；快照、清单和校验摘要在同一把锁内生成，
  保证同一次投递读取的各段来自一致版本。
- 未成功提交的分区不计入已完成行数；中断版本只用于排查，不参与交付读取。
- 已发布的旧版本被新版本取代后仍然可以按版本号复查（上一份完整缓存快照）。
"""
from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

STAGING = "采集中"
INTERRUPTED = "已中断"
PUBLISHED = "已发布"
SUPERSEDED = "已归档"

VISIBLE_STATES = (PUBLISHED, SUPERSEDED)


class SnapshotError(RuntimeError):
    """缓存快照领域错误基类。"""


class SnapshotNotFound(SnapshotError):
    """端点或版本不存在。"""


class SnapshotStateError(SnapshotError):
    """当前状态不允许该操作，例如向已发布版本继续写入。"""


class SnapshotIncomplete(SnapshotError):
    """版本尚未发布或分区不完整，不能按完整快照读取。"""

    def __init__(
        self,
        endpoint: str,
        version: str,
        missing: Iterable[str],
        *,
        published: bool = False,
    ) -> None:
        self.endpoint = endpoint
        self.version = version
        self.missing = list(missing)
        self.published = published
        state = "分区不完整" if published else "尚未发布"
        missing_text = "、".join(self.missing) or "无"
        super().__init__(
            f"缓存版本 {endpoint}/{version} {state}，缺失分区：{missing_text}"
        )


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _checksum(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _rows_checksum(rows: Iterable[dict[str, Any]]) -> str:
    return _checksum(list(rows))


def _copy_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def _ordered_unique(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        if text in seen:
            raise SnapshotError(f"分区声明重复：{text}")
        seen.add(text)
        result.append(text)
    return result


@dataclass
class CacheSegment:
    """一个采集分区的已提交结果。"""

    partition: str
    rows: list[dict[str, Any]]
    checksum: str
    committed_at: str
    commits: int = 1

    @property
    def row_count(self) -> int:
        return len(self.rows)

    def as_dict(self, *, include_rows: bool = True, version: str = "") -> dict[str, Any]:
        data: dict[str, Any] = {
            "partition": self.partition,
            "version": version,
            "row_count": self.row_count,
            "checksum": self.checksum,
            "committed_at": self.committed_at,
            "commits": self.commits,
        }
        if include_rows:
            data["rows"] = _copy_rows(self.rows)
        return data


@dataclass
class CacheVersion:
    """一个端点的一次分段采集版本。"""

    endpoint: str
    version: str
    partitions: list[str]
    source: str
    created_at: str
    state: str = STAGING
    published_at: str = ""
    segments: dict[str, CacheSegment] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)

    def note(self, event: str, **detail: Any) -> None:
        self.events.append({"event": event, "at": _now(), **detail})

    def committed_partitions(self) -> list[str]:
        return [name for name in self.partitions if name in self.segments]

    def missing_partitions(self) -> list[str]:
        return [name for name in self.partitions if name not in self.segments]

    def unexpected_partitions(self) -> list[str]:
        return [name for name in self.segments if name not in self.partitions]

    @property
    def complete(self) -> bool:
        return not self.missing_partitions() and not self.unexpected_partitions()

    @property
    def visible(self) -> bool:
        return self.state in VISIBLE_STATES

    @property
    def completed_rows(self) -> int:
        return sum(segment.row_count for segment in self.segments.values())

    def manifest(self) -> dict[str, Any]:
        """交付清单：分区集合与实际写入分区的唯一口径。"""
        partition_rows = {
            name: self.segments[name].row_count
            for name in self.partitions
            if name in self.segments
        }
        partition_digests = [
            {"partition": name, "checksum": self.segments[name].checksum}
            for name in self.partitions
            if name in self.segments
        ]
        return {
            "endpoint": self.endpoint,
            "version": self.version,
            "partitions": list(self.partitions),
            "partition_count": len(self.partitions),
            "partition_rows": partition_rows,
            "total_rows": sum(partition_rows.values()),
            "checksum": _checksum(partition_digests),
            "complete": self.complete,
        }


class CacheSnapshotRegistry:
    """进程内分段缓存快照注册表；锁粒度覆盖一次完整快照读取。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._versions: dict[str, CacheVersion] = {}
        self._order: list[str] = []
        self._endpoint_versions: dict[str, list[str]] = {}
        self._staging: dict[str, str] = {}
        self._published: dict[str, str] = {}
        self._seq = 0

    # ---------- 内部工具 ----------

    def _new_version_id(self) -> str:
        self._seq += 1
        return f"v{self._seq:04d}"

    def _state_versions(self, endpoint: str) -> list[CacheVersion]:
        return [self._versions[vid] for vid in self._endpoint_versions.get(endpoint, [])]

    def _find(
        self,
        endpoint: str,
        version: str | None,
        *,
        prefer: str = "staging",
    ) -> CacheVersion:
        if version is None:
            order = (
                (self._published, self._staging)
                if prefer == "published"
                else (self._staging, self._published)
            )
            for source in order:
                current = source.get(endpoint)
                if current and current in self._versions:
                    return self._versions[current]
            raise SnapshotNotFound(f"端点 {endpoint} 还没有缓存版本")
        target = self._versions.get(version)
        if target is None or target.endpoint != endpoint:
            raise SnapshotNotFound(f"端点 {endpoint} 不存在缓存版本 {version}")
        return target

    # ---------- 声明与写入 ----------

    def declare(self, endpoint: str, partitions: Iterable[str], *, source: str = "refresh") -> CacheVersion:
        """声明一次分段采集；未完成的暂存版本优先原位恢复。"""
        endpoint = str(endpoint).strip()
        if not endpoint:
            raise SnapshotError("目标端点不能为空")
        declared = _ordered_unique(partitions)
        if not declared:
            raise SnapshotError("至少需要声明一个采集分区")
        with self._lock:
            current_id = self._staging.get(endpoint)
            if current_id:
                current = self._versions[current_id]
                if current.partitions == declared:
                    if current.state == INTERRUPTED:
                        current.state = STAGING
                        current.note("resume")
                    return current
                if current.segments:
                    current.state = INTERRUPTED
                    current.note("superseded_by_declaration", partitions=list(declared))
                else:
                    current.state = SUPERSEDED
                    current.note("discarded_empty_declaration")
                self._staging.pop(endpoint, None)
            version = CacheVersion(
                endpoint=endpoint,
                version=self._new_version_id(),
                partitions=declared,
                source=source,
                created_at=_now(),
            )
            version.note("declared", partitions=list(declared))
            self._versions[version.version] = version
            self._order.append(version.version)
            self._endpoint_versions.setdefault(endpoint, []).append(version.version)
            self._staging[endpoint] = version.version
            return version

    def submit_segment(
        self,
        endpoint: str,
        partition: str,
        rows: Iterable[dict[str, Any]],
        *,
        version: str | None = None,
    ) -> tuple[CacheVersion, CacheSegment, bool]:
        """写入一个分段；同一分段同内容重复提交只记一次写入。

        返回 (版本, 分段, 是否重复提交)。
        """
        partition = str(partition).strip()
        if not partition:
            raise SnapshotError("分区名不能为空")
        prepared = _copy_rows(rows)
        digest = _rows_checksum(prepared)
        with self._lock:
            target = self._find_target(endpoint, version)
            if partition not in target.partitions:
                raise SnapshotError(
                    f"分区 {partition} 未在版本 {target.version} 中声明，"
                    f"已声明分区：{'、'.join(target.partitions)}"
                )
            existing = target.segments.get(partition)
            if existing is not None and existing.checksum == digest:
                # 幂等：同一版本同一分区同内容重复提交，只保留第一次写入。
                target.note("duplicate_segment", partition=partition, checksum=digest)
                return target, existing, True
            if target.state in VISIBLE_STATES:
                raise SnapshotStateError(
                    f"版本 {target.version} 已发布，分区 {partition} 不允许覆盖写入"
                )
            if existing is not None:
                target.note(
                    "rewrite_segment",
                    partition=partition,
                    previous_checksum=existing.checksum,
                    checksum=digest,
                )
            segment = CacheSegment(
                partition=partition,
                rows=prepared,
                checksum=digest,
                committed_at=_now(),
                commits=(existing.commits + 1) if existing is not None else 1,
            )
            target.segments[partition] = segment
            if target.state == INTERRUPTED:
                target.state = STAGING
                target.note("resume")
            target.note(
                "segment_committed",
                partition=partition,
                checksum=digest,
                row_count=segment.row_count,
            )
            return target, segment, False

    def _find_target(self, endpoint: str, version: str | None) -> CacheVersion:
        if version is not None:
            return self._find(endpoint, version)
        if endpoint in self._staging:
            return self._versions[self._staging[endpoint]]
        raise SnapshotStateError(
            f"端点 {endpoint} 没有可写入的采集版本，请先声明分区"
        )

    def interrupt(self, endpoint: str, *, version: str | None = None) -> CacheVersion:
        """模拟刷新中断：分区保留在暂存区，但对交付读取不可见。"""
        with self._lock:
            target = self._find(endpoint, version)
            if target.state == PUBLISHED:
                raise SnapshotStateError(f"版本 {target.version} 已发布，不能标记中断")
            if target.state == INTERRUPTED:
                return target
            target.state = INTERRUPTED
            target.note("interrupted", committed=target.committed_partitions())
            return target

    # ---------- 发布 ----------

    def publish(self, endpoint: str, *, version: str | None = None) -> CacheVersion:
        """全部分区提交后发布；重复发布直接返回，旧版本转为可复查归档。"""
        with self._lock:
            target = self._find(endpoint, version)
            if target.state == PUBLISHED:
                return target
            if target.state == SUPERSEDED:
                raise SnapshotStateError(f"版本 {target.version} 已被归档，不能重新发布")
            missing = target.missing_partitions()
            if missing or target.unexpected_partitions():
                raise SnapshotIncomplete(
                    endpoint, target.version, missing, published=False
                )
            previous_id = self._published.get(endpoint)
            if previous_id and previous_id != target.version:
                previous = self._versions[previous_id]
                previous.state = SUPERSEDED
                previous.note("superseded", by=target.version)
            # 发布顺序：先翻转可见状态，再清暂存指针，避免并发读取
            # 在两步之间看到「暂存指向已发布版本」而拒绝读取。
            target.state = PUBLISHED
            target.published_at = _now()
            target.note("published", total_rows=target.completed_rows)
            self._published[endpoint] = target.version
            if self._staging.get(endpoint) == target.version:
                self._staging.pop(endpoint, None)
            return target

    # ---------- 读取 ----------

    def snapshot(
        self,
        endpoint: str,
        version: str | None = None,
        *,
        include_hidden: bool = False,
    ) -> dict[str, Any]:
        """读取完整缓存快照；默认只返回已发布版本，且各段保证同版本。"""
        with self._lock:
            target = self._find(endpoint, version, prefer="published")
            if not target.visible and not include_hidden:
                raise SnapshotIncomplete(
                    endpoint,
                    target.version,
                    target.missing_partitions(),
                    published=False,
                )
            if target.visible and not target.complete:
                raise SnapshotIncomplete(
                    endpoint,
                    target.version,
                    target.missing_partitions(),
                    published=True,
                )
            return self._render(target)

    def latest_published(self, endpoint: str) -> dict[str, Any]:
        with self._lock:
            version_id = self._published.get(endpoint)
            if not version_id:
                raise SnapshotNotFound(f"端点 {endpoint} 还没有已发布的缓存快照")
            return self.snapshot(endpoint, version_id)

    def stage_view(self, endpoint: str, version: str | None = None) -> dict[str, Any]:
        """查看隐藏的暂存/中断版本：已完成行数只累计成功提交的分区。"""
        with self._lock:
            target = self._find(endpoint, version)
            view = self._render(target)
            view["hidden"] = not target.visible
            return view

    def _render(self, target: CacheVersion) -> dict[str, Any]:
        manifest = target.manifest()
        segments = {
            name: target.segments[name].as_dict(version=target.version)
            for name in target.partitions
            if name in target.segments
        }
        completed = target.committed_partitions()
        missing = target.missing_partitions()
        return {
            "endpoint": target.endpoint,
            "version": target.version,
            "state": target.state,
            "visible": target.visible,
            "published": target.state == PUBLISHED,
            "complete": target.complete,
            "created_at": target.created_at,
            "published_at": target.published_at,
            "partitions": list(target.partitions),
            "partition_count": len(target.partitions),
            "completed_partitions": completed,
            "missing_partitions": missing,
            "partition_rows": manifest["partition_rows"],
            "completed_rows": target.completed_rows,
            "total_rows": target.completed_rows,
            "manifest": manifest,
            "segments": segments,
            "checksum": _checksum(
                {name: target.segments[name].checksum for name in completed}
            ),
        }

    def version_view(self, endpoint: str, version: str) -> dict[str, Any]:
        with self._lock:
            target = self._find(endpoint, version)
            return {
                "endpoint": target.endpoint,
                "version": target.version,
                "state": target.state,
                "visible": target.visible,
                "complete": target.complete,
                "created_at": target.created_at,
                "published_at": target.published_at,
                "partitions": list(target.partitions),
                "completed_partitions": target.committed_partitions(),
                "missing_partitions": target.missing_partitions(),
                "completed_rows": target.completed_rows,
                "events": list(target.events),
                "segments": [
                    target.segments[name].as_dict(
                        include_rows=False, version=target.version)
                    for name in target.partitions
                    if name in target.segments
                ],
            }

    def versions(self, endpoint: str) -> list[dict[str, Any]]:
        with self._lock:
            return [
                self.version_view(endpoint, target.version)
                for target in self._state_versions(endpoint)
            ]

    def endpoint_state(self, endpoint: str) -> dict[str, Any]:
        with self._lock:
            if not self._endpoint_versions.get(endpoint):
                raise SnapshotNotFound(f"端点 {endpoint} 还没有缓存版本")
            staging_id = self._staging.get(endpoint)
            published_id = self._published.get(endpoint)
            return {
                "endpoint": endpoint,
                "staging": self.version_view(endpoint, staging_id) if staging_id else None,
                "published": self.version_view(endpoint, published_id) if published_id else None,
                "versions": self.versions(endpoint),
            }


snapshots = CacheSnapshotRegistry()

