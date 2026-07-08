from __future__ import annotations

"""Data store for Magical Index.

Three tables:
- **resources**: 7-column resource registry (resource_id, resource_name,
  location, resource_type, resource_status, last_by, last_update_time).
- **relationships**: pure graph edges (upstream_resource_id,
  downstream_resource_id, relationship_type, notes).
- **activity_logs**: immutable audit trail for every mutation.

Design intent:
- Keep endpoint functions thin; concentrate state changes here.
- Make it easy to swap the backend by keeping a small set of methods.
"""

import logging
import threading
import queue
import json
import atexit
from collections.abc import Iterable
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional
from pathlib import Path
import os
import tempfile

logger = logging.getLogger(__name__)

from app.schemas.activity_log import (
    ActionType,
    ActivityLogCreate,
    ActivityLogRead,
    generate_log_id,
)
from app.schemas.pagination import decode_cursor, encode_cursor
from app.schemas.relationship import (
    RelationshipCreate,
    RelationshipDelete,
    RelationshipRead,
    RelationshipType,
)
from app.schemas.resource import (
    ActorType,
    ParentInfo,
    ResourceCreate,
    ResourceRead,
    ResourceStatus,
    ResourceTypeFilter,
    ResourceUpdate,
    UpstreamResourceEnriched,
    generate_resource_id,
)
from app.schemas.prep_extraction import (
    ExtractionTask,
    PrepareExtractionRequest,
    PrepareExtractionResponse,
)

from app.core.config import settings


def derive_target_location(upstream_resource_name: str) -> str:
    """Derive the target resource location from the upstream use case folder's resource name."""
    return upstream_resource_name.strip() if upstream_resource_name else ""


def _apply_resource_type_filter(
    resources: list[ResourceRead], resource_type_filter: Optional[ResourceTypeFilter]
) -> list[ResourceRead]:
    """Filter a list of resources by resource_type include/exclude lists."""
    if resource_type_filter is None:
        return resources
    if resource_type_filter.include:
        include_upper = {t.upper() for t in resource_type_filter.include}
        resources = [r for r in resources if r.resource_type and r.resource_type.value.upper() in include_upper]
    elif resource_type_filter.exclude:
        exclude_upper = {t.upper() for t in resource_type_filter.exclude}
        resources = [r for r in resources if not r.resource_type or r.resource_type.value.upper() not in exclude_upper]
    return resources


# ======================================================================
# Write Buffer — decouples API response time from Databricks write latency
# ======================================================================

class _WriteOp:
    """A single deferred write operation."""
    __slots__ = ("table", "sql", "params", "log_sql", "log_params", "key", "retries", "seq")

    def __init__(self, table: str, sql: str, params: tuple,
                 log_sql: str, log_params: tuple, key: str) -> None:
        self.table = table
        self.sql = sql
        self.params = params
        self.log_sql = log_sql
        self.log_params = log_params
        self.key = key          # resource_id or "up|down|type" for dedup
        self.retries = 0
        self.seq: int = 0       # WAL sequence number (assigned by WAL)

    def to_dict(self) -> dict:
        """Serialize to a JSON-safe dict for WAL persistence."""
        return {
            "table": self.table,
            "sql": self.sql,
            "params": [str(p) if isinstance(p, datetime) else p for p in self.params],
            "log_sql": self.log_sql,
            "log_params": [str(p) if isinstance(p, datetime) else p for p in self.log_params],
            "key": self.key,
            "seq": self.seq,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "_WriteOp":
        """Reconstruct a _WriteOp from a WAL dict."""
        op = cls(
            table=d["table"],
            sql=d["sql"],
            params=tuple(d["params"]),
            log_sql=d["log_sql"],
            log_params=tuple(d["log_params"]),
            key=d["key"],
        )
        op.seq = d.get("seq", 0)
        return op


class _WAL:
    """Append-only write-ahead log backed by a JSONL file.

    Every accepted write is persisted to disk before entering the in-memory
    queue.  A separate checkpoint file tracks the highest sequence number
    that has been successfully flushed to Databricks.  On startup,
    ``replay()`` returns unflushed entries so they can be re-enqueued.
    """

    def __init__(self, wal_dir: str) -> None:
        self._dir = Path(wal_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._wal_path = self._dir / "wal.jsonl"
        self._ckpt_path = self._dir / "checkpoint"
        self._dead_letter_path = self._dir / "dead_letter.jsonl"
        self._lock = threading.Lock()
        self._seq: int = self._read_max_wal_seq()

    def _read_max_wal_seq(self) -> int:
        """Scan the WAL to find the highest seq (for startup)."""
        max_seq = 0
        if self._wal_path.exists():
            try:
                with open(self._wal_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            d = json.loads(line)
                            max_seq = max(max_seq, d.get("seq", 0))
                        except json.JSONDecodeError:
                            continue
            except OSError:
                pass
        return max_seq

    def append(self, op: _WriteOp) -> int:
        """Persist op to WAL; returns the assigned sequence number."""
        with self._lock:
            self._seq += 1
            op.seq = self._seq
            with open(self._wal_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(op.to_dict()) + "\n")
                f.flush()
                os.fsync(f.fileno())
            return self._seq

    def checkpoint(self, seq: int) -> None:
        """Record that all ops up to *seq* have been flushed."""
        with self._lock:
            with open(self._ckpt_path, "w", encoding="utf-8") as f:
                f.write(str(seq))
                f.flush()
                os.fsync(f.fileno())
            self._truncate_wal(seq)

    def _read_checkpoint(self) -> int:
        """Return the last checkpointed sequence number (0 if none)."""
        if self._ckpt_path.exists():
            try:
                return int(self._ckpt_path.read_text().strip())
            except (ValueError, OSError):
                return 0
        return 0

    def _truncate_wal(self, up_to_seq: int) -> None:
        """Remove WAL entries that have been checkpointed."""
        if not self._wal_path.exists():
            return
        remaining: list[str] = []
        try:
            with open(self._wal_path, "r", encoding="utf-8") as f:
                for line in f:
                    line_s = line.strip()
                    if not line_s:
                        continue
                    try:
                        d = json.loads(line_s)
                        if d.get("seq", 0) > up_to_seq:
                            remaining.append(line_s)
                    except json.JSONDecodeError:
                        continue
        except OSError:
            return
        with open(self._wal_path, "w", encoding="utf-8") as f:
            for r in remaining:
                f.write(r + "\n")
            f.flush()
            os.fsync(f.fileno())

    def replay(self) -> list[_WriteOp]:
        """Return unflushed ops (seq > checkpoint) for re-enqueue on startup."""
        ckpt = self._read_checkpoint()
        ops: list[_WriteOp] = []
        if not self._wal_path.exists():
            return ops
        try:
            with open(self._wal_path, "r", encoding="utf-8") as f:
                for line in f:
                    line_s = line.strip()
                    if not line_s:
                        continue
                    try:
                        d = json.loads(line_s)
                        if d.get("seq", 0) > ckpt:
                            ops.append(_WriteOp.from_dict(d))
                    except (json.JSONDecodeError, KeyError):
                        continue
        except OSError:
            pass
        return ops

    def append_dead_letter(self, op: _WriteOp, error: str) -> None:
        """Persist a permanently failed op to the dead-letter file."""
        entry = op.to_dict()
        entry["error"] = error
        entry["dead_letter_time"] = datetime.now(timezone.utc).isoformat()
        with self._lock:
            with open(self._dead_letter_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
                f.flush()
                os.fsync(f.fileno())

    def read_dead_letters(self) -> list[dict]:
        """Return all dead-letter entries."""
        entries: list[dict] = []
        if not self._dead_letter_path.exists():
            return entries
        try:
            with open(self._dead_letter_path, "r", encoding="utf-8") as f:
                for line in f:
                    line_s = line.strip()
                    if not line_s:
                        continue
                    try:
                        entries.append(json.loads(line_s))
                    except json.JSONDecodeError:
                        continue
        except OSError:
            pass
        return entries

    def clear_dead_letters(self) -> int:
        """Remove all dead-letter entries. Returns count removed."""
        count = len(self.read_dead_letters())
        if self._dead_letter_path.exists():
            self._dead_letter_path.write_text("")
        return count


class BufferFullError(Exception):
    """Raised when the write buffer exceeds the memory guard limit."""
    pass


class WriteBuffer:
    """Thread-safe write-behind buffer that batches Databricks INSERTs.

    - ``enqueue()`` adds a validated write op and returns immediately.
    - A daemon thread flushes to Databricks every *flush_interval* ms or
      when the batch threshold is reached.
    - An in-memory cache (``_resource_cache`` / ``_relationship_cache``)
      allows immediate read-after-write.
    - Failed ops are retried up to *max_retries* times; permanently failed
      ops land in ``dead_letter`` for diagnostics.
    - **Phase 1**: Optional WAL persists every op to disk before enqueue.
    - **Phase 2**: Bounded flush batches + memory guard (503 backpressure).
    - **Phase 4**: Dead-letter entries are persisted to disk via WAL.
    """

    def __init__(
        self,
        connect_fn,
        flush_interval_ms: int = 500,
        batch_threshold: int = 100,
        max_retries: int = 3,
        wal_dir: str = "",
        max_flush_batch: int = 500,
        max_memory_items: int = 50_000,
    ) -> None:
        self._connect = connect_fn
        self._base_flush_interval = flush_interval_ms / 1000.0
        self._batch_threshold = batch_threshold
        self._max_retries = max_retries
        self._max_flush_batch = max_flush_batch
        self._max_memory_items = max_memory_items

        self._queue: queue.Queue[_WriteOp] = queue.Queue()

        # Read-through caches keyed by resource_id / relationship key
        self._resource_cache: dict[str, ResourceRead] = {}
        self._relationship_cache: dict[str, RelationshipRead] = {}
        self._cache_lock = threading.Lock()

        # Dead-letter list for permanently failed writes (in-memory view)
        self.dead_letter: list[_WriteOp] = []
        self._dead_letter_lock = threading.Lock()

        # Phase 1: WAL for crash recovery
        self._wal: Optional[_WAL] = None
        if wal_dir:
            self._wal = _WAL(wal_dir)

        # Flush trigger event (wakes flusher when batch threshold hit)
        self._flush_event = threading.Event()

        # Shutdown flag
        self._shutdown = False

        # Start daemon flusher thread
        self._thread = threading.Thread(
            target=self._flusher_loop, name="write-buffer-flusher", daemon=True
        )
        self._thread.start()

        # Register atexit handler for graceful shutdown
        atexit.register(self.shutdown)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def _check_memory_guard(self) -> None:
        """Raise BufferFullError if the queue exceeds the memory limit."""
        if self._queue.qsize() >= self._max_memory_items:
            raise BufferFullError(
                f"Write buffer at capacity ({self._max_memory_items} items). "
                f"Retry after a short delay."
            )

    def enqueue_resource(self, op: _WriteOp, resource: ResourceRead) -> None:
        """Enqueue a resource write and cache the ResourceRead for immediate reads."""
        self._check_memory_guard()
        if self._wal:
            self._wal.append(op)
        with self._cache_lock:
            self._resource_cache[resource.resource_id] = resource
        self._queue.put_nowait(op)
        if self._queue.qsize() >= self._batch_threshold:
            self._flush_event.set()

    def enqueue_relationship(self, op: _WriteOp, relationship: RelationshipRead) -> None:
        """Enqueue a relationship write and cache the RelationshipRead."""
        self._check_memory_guard()
        if self._wal:
            self._wal.append(op)
        with self._cache_lock:
            self._relationship_cache[op.key] = relationship
        self._queue.put_nowait(op)
        if self._queue.qsize() >= self._batch_threshold:
            self._flush_event.set()

    def replay_wal(self) -> int:
        """Replay unflushed WAL entries into the queue. Returns count replayed."""
        if not self._wal:
            return 0
        ops = self._wal.replay()
        for op in ops:
            self._queue.put_nowait(op)
        if ops:
            logger.info("WAL replay: re-enqueued %d unflushed ops", len(ops))
            self._flush_event.set()
        return len(ops)

    def lookup_resource(self, resource_id: str) -> Optional[ResourceRead]:
        """Return a buffered ResourceRead if it hasn't flushed yet, else None."""
        with self._cache_lock:
            return self._resource_cache.get(resource_id)

    def lookup_relationship(self, key: str) -> Optional[RelationshipRead]:
        """Return a buffered RelationshipRead if it hasn't flushed yet, else None."""
        with self._cache_lock:
            return self._relationship_cache.get(key)

    def resource_in_buffer(self, resource_id: str) -> bool:
        """Check if a resource_id is pending in the buffer."""
        with self._cache_lock:
            return resource_id in self._resource_cache

    @property
    def pending_count(self) -> int:
        return self._queue.qsize()

    @property
    def dead_letter_count(self) -> int:
        with self._dead_letter_lock:
            return len(self.dead_letter)

    def shutdown(self) -> None:
        """Flush remaining items and stop the flusher thread."""
        if self._shutdown:
            return
        self._shutdown = True
        self._flush_event.set()
        self._thread.join(timeout=10)
        # Final drain attempt
        self._flush_batch()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _adaptive_flush_interval(self) -> float:
        """Return a shorter sleep interval when the queue is large.

        Normal:        sleep for the configured base interval (default 500ms).
        Under pressure (queue > threshold): sleep only 50ms so the flusher
        catches up quickly.  Once the queue drains below the threshold the
        interval returns to normal automatically.
        """
        if self._queue.qsize() > self._batch_threshold:
            return 0.05  # 50ms — aggressive drain mode
        return self._base_flush_interval

    def _flusher_loop(self) -> None:
        """Daemon loop: adaptive wait, then flush pending items in bounded batches."""
        while not self._shutdown:
            self._flush_event.wait(timeout=self._adaptive_flush_interval())
            self._flush_event.clear()
            # Flush in bounded batches; keep looping if items remain
            while True:
                flushed = self._flush_batch()
                if flushed == 0 or self._shutdown:
                    break

    def _flush_batch(self) -> int:
        """Drain up to ``_max_flush_batch`` items and execute them.

        Returns the number of ops attempted (0 means queue was empty).
        """
        ops: list[_WriteOp] = []
        for _ in range(self._max_flush_batch):
            try:
                ops.append(self._queue.get_nowait())
            except queue.Empty:
                break

        if not ops:
            return 0

        succeeded_ops: list[_WriteOp] = []
        failed_ops: list[_WriteOp] = []
        last_error = ""

        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    for op in ops:
                        try:
                            cur.execute(op.sql, op.params)
                            cur.execute(op.log_sql, op.log_params)
                            succeeded_ops.append(op)
                        except Exception as exc:
                            last_error = str(exc)
                            logger.warning(
                                "Write buffer: op %s failed: %s", op.key, exc
                            )
                            failed_ops.append(op)
                    conn.commit()
        except Exception as exc:
            # Connection-level failure — all ops in this batch failed
            last_error = str(exc)
            logger.error("Write buffer: batch flush failed: %s", exc)
            failed_ops = ops
            succeeded_ops = []

        # Checkpoint WAL for successfully flushed items
        if succeeded_ops and self._wal:
            max_seq = max(op.seq for op in succeeded_ops)
            self._wal.checkpoint(max_seq)

        # Evict successfully flushed items from cache
        with self._cache_lock:
            for op in succeeded_ops:
                if op.table == "resource":
                    self._resource_cache.pop(op.key, None)
                else:
                    self._relationship_cache.pop(op.key, None)

        # Retry or dead-letter failed ops
        for op in failed_ops:
            op.retries += 1
            if op.retries < self._max_retries:
                self._queue.put_nowait(op)
            else:
                self._move_to_dead_letter(op, last_error)

        return len(ops)

    def _move_to_dead_letter(self, op: _WriteOp, error: str = "") -> None:
        logger.error(
            "Write buffer: permanently failed op %s after %d retries",
            op.key, op.retries,
        )
        with self._dead_letter_lock:
            self.dead_letter.append(op)
        # Persist to disk if WAL is enabled
        if self._wal:
            self._wal.append_dead_letter(op, error)
        # Also remove from cache so reads fall through to Databricks
        with self._cache_lock:
            if op.table == "resource":
                self._resource_cache.pop(op.key, None)
            else:
                self._relationship_cache.pop(op.key, None)


class InMemoryStore:
    """Minimal storage façade for local dev.

    Methods here mirror the needs of the API layer. A future ``DeltaStore``
    should expose the same public methods so wiring can be swapped based on
    configuration (e.g., an environment variable).
    """

    def __init__(self) -> None:
        self._resources: Dict[str, ResourceRead] = {}
        self._relationships: List[RelationshipRead] = []
        self._activity_logs: List[ActivityLogRead] = []

    # ------------------------------------------------------------------
    # Activity log helpers
    # ------------------------------------------------------------------
    def _append_log(
        self,
        resource_id: str,
        action: ActionType,
        actor_type: ActorType,
        actor_name: str,
        details: Optional[str] = None,
        **_: object,
    ) -> ActivityLogRead:
        entry = ActivityLogRead(
            log_id=generate_log_id(),
            resource_id=resource_id,
            action=action,
            actor_type=actor_type,
            actor_name=actor_name,
            details=details,
            timestamp=datetime.now(timezone.utc),
        )
        self._activity_logs.append(entry)
        return entry

    def log_api_request(
        self,
        method: str,
        path: str,
        status_code: int,
        duration_ms: float,
        error: Optional[str] = None,
    ) -> None:
        """Record middleware request timing information in the activity log."""
        action = (
            ActionType.API_REQUEST_FAILED
            if (error or status_code >= 400)
            else ActionType.API_REQUEST
        )
        details_parts = [
            f"method={method}",
            f"path={path}",
            f"status={status_code}",
            f"duration_ms={duration_ms:.2f}",
        ]
        if error:
            details_parts.append(f"error={error}")

        self._append_log(
            resource_id=path,
            action=action,
            actor_type=ActorType.SYSTEM,
            actor_name="MI-SYS",
            details=" ".join(details_parts),
        )

    # ------------------------------------------------------------------
    # Resource operations
    # ------------------------------------------------------------------
    def create_resource(self, data: ResourceCreate) -> ResourceRead:
        """Create a new resource. Auto-generates resource_id if not provided."""
        rid = data.resource_id or generate_resource_id(
            resource_type=data.resource_type or ResourceType.DATASET,
            platform=data.platform or "auto"
        )

        if rid in self._resources:
            raise ValueError(f"Resource already exists: {rid}")

        now = datetime.now(timezone.utc)
        last_by = f"{data.actor_type.value}:{data.actor_name}"

        resource = ResourceRead(
            resource_id=rid,
            resource_name=data.resource_name,
            location=data.location,
            resource_type=data.resource_type,
            resource_status=data.resource_status,
            last_by=last_by,
            last_update_time=now,
        )
        self._resources[rid] = resource

        self._append_log(
            resource_id=rid,
            action=ActionType.RESOURCE_CREATED,
            actor_type=data.actor_type,
            actor_name=data.actor_name,
            details=f"Created resource '{data.resource_name or rid}' with status {data.resource_status.value}",
        )
        return resource

    def get_resource(self, resource_id: str) -> Optional[ResourceRead]:
        """Fetch a single resource by id or return ``None`` if not found."""
        return self._resources.get(resource_id)

    def list_resources(
        self,
        resource_status: Optional[ResourceStatus] = None,
        resource_type: Optional[str] = None,
        platform: Optional[str] = None,
        cursor: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> tuple[list[ResourceRead], int, Optional[str]]:
        """List resources with pagination, optionally filtering by status, type, or platform.
        
        Returns:
            Tuple of (paginated items, total count, next_cursor)
        """
        resources: Iterable[ResourceRead] = self._resources.values()
        if resource_status is not None:
            resources = [r for r in resources if r.resource_status == resource_status]
        if resource_type is not None:
            resources = [r for r in resources if r.resource_type and r.resource_type.value == resource_type]
        if platform is not None:
            resources = [r for r in resources if r.resource_id.startswith(f"{platform}:")]
        sorted_resources = sorted(resources, key=lambda r: r.resource_id)
        total = len(sorted_resources)

        if cursor:
            cursor_data = decode_cursor(cursor)
            after = [r for r in sorted_resources if r.resource_id > cursor_data["rid"]]
            paginated = after[:limit]
            has_more = len(after) > limit
        else:
            paginated = sorted_resources[offset:offset + limit]
            has_more = (offset + len(paginated)) < total

        next_cursor = (
            encode_cursor({"rid": paginated[-1].resource_id})
            if paginated and has_more else None
        )
        return paginated, total, next_cursor

    def update_resource(
        self, resource_id: str, data: ResourceUpdate
    ) -> Optional[ResourceRead]:
        """Update mutable fields on a resource. Returns None if not found."""
        existing = self._resources.get(resource_id)
        if existing is None:
            return None

        now = datetime.now(timezone.utc)
        last_by = f"{data.actor_type.value}:{data.actor_name}"

        changes: list[str] = []
        updates: dict[str, object] = {
            "last_by": last_by,
            "last_update_time": now,
        }

        if data.resource_name is not None:
            updates["resource_name"] = data.resource_name
            changes.append(f"resource_name -> '{data.resource_name}'")
        if data.location is not None:
            updates["location"] = data.location
            changes.append(f"location -> '{data.location}'")
        if data.resource_type is not None:
            updates["resource_type"] = data.resource_type
            changes.append(f"resource_type -> {data.resource_type.value}")
        if data.resource_status is not None:
            changes.append(f"resource_status -> {data.resource_status.value} (was {existing.resource_status.value})")
            updates["resource_status"] = data.resource_status

        updated = existing.model_copy(update=updates)
        self._resources[resource_id] = updated

        self._append_log(
            resource_id=resource_id,
            action=ActionType.RESOURCE_UPDATED,
            actor_type=data.actor_type,
            actor_name=data.actor_name,
            details="; ".join(changes) if changes else "no-op update",
        )
        return updated

    def delete_resource(self, resource_id: str, actor_type: ActorType, actor_name: str) -> bool:
        """Delete a resource and any relationships where it appears upstream or downstream."""
        if resource_id not in self._resources:
            return False

        del self._resources[resource_id]
        self._relationships = [
            rel
            for rel in self._relationships
            if rel.upstream_resource_id != resource_id
            and rel.downstream_resource_id != resource_id
        ]

        self._append_log(
            resource_id=resource_id,
            action=ActionType.RESOURCE_DELETED,
            actor_type=actor_type,
            actor_name=actor_name,
            details=f"Deleted resource {resource_id} and cascade-removed relationships",
        )
        return True

    # ------------------------------------------------------------------
    # Relationship operations
    # ------------------------------------------------------------------
    def create_relationship(self, data: RelationshipCreate) -> RelationshipRead:
        """Create a relationship edge; operation is idempotent for identical edges.

        Raises ``ValueError`` if either resource does not exist.
        """
        missing: list[str] = []
        if self._resources.get(data.upstream_resource_id) is None:
            missing.append(data.upstream_resource_id)
        if self._resources.get(data.downstream_resource_id) is None:
            missing.append(data.downstream_resource_id)
        if missing:
            raise ValueError(f"Resource(s) not found: {', '.join(missing)}")

        # Idempotency: return existing if same edge + type already present
        for rel in self._relationships:
            if (
                rel.upstream_resource_id == data.upstream_resource_id
                and rel.downstream_resource_id == data.downstream_resource_id
                and rel.relationship_type == data.relationship_type
            ):
                return rel

        relationship = RelationshipRead(
            upstream_resource_id=data.upstream_resource_id,
            downstream_resource_id=data.downstream_resource_id,
            relationship_type=data.relationship_type,
            notes=data.notes,
        )
        self._relationships.append(relationship)

        self._append_log(
            resource_id=data.upstream_resource_id,
            action=ActionType.RELATIONSHIP_CREATED,
            actor_type=data.actor_type,
            actor_name=data.actor_name,
            details=f"{data.upstream_resource_id} {data.relationship_type.value} {data.downstream_resource_id}",
        )
        return relationship

    def delete_relationship(self, data: RelationshipDelete) -> int:
        """Delete relationship(s) matching the criteria; returns number of rows removed."""
        before = len(self._relationships)
        self._relationships = [
            rel
            for rel in self._relationships
            if not (
                rel.upstream_resource_id == data.upstream_resource_id
                and rel.downstream_resource_id == data.downstream_resource_id
                and (
                    data.relationship_type is None
                    or rel.relationship_type == data.relationship_type
                )
            )
        ]
        deleted_count = before - len(self._relationships)
        if deleted_count > 0:
            relationship_type = data.relationship_type.value if data.relationship_type is not None else "ANY"
            self._append_log(
                resource_id=data.upstream_resource_id,
                action=ActionType.RELATIONSHIP_DELETED,
                actor_type=data.actor_type,
                actor_name=data.actor_name,
                details=(
                    f"Deleted {deleted_count} relationship(s): "
                    f"upstream={data.upstream_resource_id}, "
                    f"downstream={data.downstream_resource_id}, "
                    f"relationship_type={relationship_type}"
                ),
            )
        return deleted_count

    def list_upstream_resources(
        self, resource_id: str, resource_type_filter: Optional[ResourceTypeFilter] = None,
        cursor: Optional[str] = None, offset: int = 0, limit: int = 100
    ) -> tuple[list[ResourceRead], int, Optional[str]]:
        """Return resources that feed into ``resource_id`` (upstream set) with pagination.
        
        Returns:
            Tuple of (paginated items, total count, next_cursor)
        """
        upstream_ids = {
            rel.upstream_resource_id
            for rel in self._relationships
            if rel.downstream_resource_id == resource_id
        }
        resources = [
            res
            for rid, res in self._resources.items()
            if rid in upstream_ids
        ]
        resources = _apply_resource_type_filter(resources, resource_type_filter)
        sorted_resources = sorted(resources, key=lambda r: r.resource_id)
        total = len(sorted_resources)

        if cursor:
            cursor_data = decode_cursor(cursor)
            after = [r for r in sorted_resources if r.resource_id > cursor_data["rid"]]
            paginated = after[:limit]
            has_more = len(after) > limit
        else:
            paginated = sorted_resources[offset:offset + limit]
            has_more = (offset + len(paginated)) < total

        next_cursor = (
            encode_cursor({"rid": paginated[-1].resource_id})
            if paginated and has_more else None
        )
        return paginated, total, next_cursor

    def list_downstream_resources(
        self, resource_id: str, resource_type_filter: Optional[ResourceTypeFilter] = None,
        cursor: Optional[str] = None, offset: int = 0, limit: int = 100
    ) -> tuple[list[ResourceRead], int, Optional[str]]:
        """Return resources that depend on ``resource_id`` (downstream set) with pagination.
        
        Returns:
            Tuple of (paginated items, total count, next_cursor)
        """
        downstream_ids = {
            rel.downstream_resource_id
            for rel in self._relationships
            if rel.upstream_resource_id == resource_id
        }
        resources = [
            res
            for rid, res in self._resources.items()
            if rid in downstream_ids
        ]
        resources = _apply_resource_type_filter(resources, resource_type_filter)
        sorted_resources = sorted(resources, key=lambda r: r.resource_id)
        total = len(sorted_resources)

        if cursor:
            cursor_data = decode_cursor(cursor)
            after = [r for r in sorted_resources if r.resource_id > cursor_data["rid"]]
            paginated = after[:limit]
            has_more = len(after) > limit
        else:
            paginated = sorted_resources[offset:offset + limit]
            has_more = (offset + len(paginated)) < total

        next_cursor = (
            encode_cursor({"rid": paginated[-1].resource_id})
            if paginated and has_more else None
        )
        return paginated, total, next_cursor

    def list_upstream_resource_ids(self, resource_ids: list[str], resource_type_filter: Optional[ResourceTypeFilter] = None) -> list[str]:
        
        """Return upstream resource IDs shared by ALL provided resource_ids (intersection) using DFS.
            Args:
                resource_ids: list of resource_id strings (must be non-empty)
                resource_type_filter: optional filter to include/exclude by resource_type

            Returns:
                Sorted list of upstream resource IDs reachable from every input resource.

            Raises:
                ValueError: if resource_ids is not a list[str], is empty, contains blanks,
                            or contains ids that do not exist.
        """
        
        # Enforce list-only (no Union/back-compat)
        if not isinstance(resource_ids, list):
            raise ValueError("resource_ids must be a list of strings")

        if not resource_ids:
            raise ValueError("resource_ids must be a non-empty list of strings")

        normalized: list[str] = []
        for rid in resource_ids:
            if rid is None or not str(rid).strip():
                raise ValueError("resource_ids must contain non-empty strings only")
            normalized.append(str(rid).strip())

        # Validate existence
        missing = [rid for rid in normalized if rid not in self._resources]
        if missing:
            raise ValueError(f"Resource(s) not found: {', '.join(missing)}")

        # Build downstream->upstream adjacency map once
        upstream_map: dict[str, list[str]] = {}
        for rel in self._relationships:
            upstream_map.setdefault(rel.downstream_resource_id, []).append(rel.upstream_resource_id)
        
        # helper function to collect all upstream ids for a given resource using DFS
        def collect_upstream(start_rid: str) -> set[str]:
            visited: set[str] = set()
            stack: list[str] = [start_rid]
            while stack:
                current = stack.pop()
                for up in upstream_map.get(current, []):
                    if up not in visited:
                        visited.add(up)
                        stack.append(up)
            return visited

        shared: set[str] | None = None
        for rid in normalized:
            ups = collect_upstream(rid)
            shared = ups if shared is None else (shared & ups)
            if not shared:
                break

        result_ids = sorted(shared or set())

        # Apply resource_type filter to the result IDs
        if resource_type_filter and (resource_type_filter.include or resource_type_filter.exclude):
            resources = [self._resources[rid] for rid in result_ids if rid in self._resources]
            resources = _apply_resource_type_filter(resources, resource_type_filter)
            result_ids = [r.resource_id for r in resources]

        return result_ids

    def list_upstream_resources_enriched(
        self, resource_id: str, resource_type_filter: Optional[ResourceTypeFilter] = None,
        offset: int = 0, limit: int = 50
    ) -> tuple[list[UpstreamResourceEnriched], int]:
        """Return all transitive upstream resources enriched with github_url and parents.

        Args:
            resource_id: The starting resource ID.
            resource_type_filter: optional filter to include/exclude by resource_type
            offset: Pagination offset.
            limit: Page size.

        Returns:
            Tuple of (paginated enriched items, total count).
        """
        # Fetch the input resource to extract the project use case name
        source_resource = self._resources.get(resource_id)
        if source_resource is None:
            raise ValueError(f"Resource not found: {resource_id}")

        # Extract project use case name (2nd path segment of resource_name)
        segments = source_resource.resource_name.split("/")
        project_use_case = segments[2] if len(segments) > 2 else ""

        # Build adjacency map and collect all transitive upstream IDs via DFS
        upstream_map: dict[str, list[str]] = {}
        for rel in self._relationships:
            upstream_map.setdefault(rel.downstream_resource_id, []).append(rel.upstream_resource_id)

        visited: set[str] = set()
        stack: list[str] = [resource_id]
        while stack:
            current = stack.pop()
            for up in upstream_map.get(current, []):
                if up not in visited:
                    visited.add(up)
                    stack.append(up)

        # Fetch all upstream resources
        upstream_resources = [
            self._resources[rid] for rid in visited if rid in self._resources
        ]
        upstream_resources = _apply_resource_type_filter(upstream_resources, resource_type_filter)
        upstream_resources.sort(key=lambda r: r.resource_id)
        total = len(upstream_resources)

        # Build github_url lookup: resource_name -> location for resources with 'github' in resource_id
        github_urls: dict[str, str] = {}
        for rid, res in self._resources.items():
            if "github" in rid.lower() and res.location:
                github_urls.setdefault(res.resource_name, res.location)

        # Build parents lookup: downstream_resource_id -> list of (location, resource_name, resource_status)
        parents_map: dict[str, list[tuple[str | None, str, ResourceStatus]]] = {}
        for rel in self._relationships:
            if rel.downstream_resource_id in visited:
                parent_res = self._resources.get(rel.upstream_resource_id)
                if parent_res:
                    parents_map.setdefault(rel.downstream_resource_id, []).append(
                        (parent_res.location, parent_res.resource_name, parent_res.resource_status)
                    )

        # Paginate
        paginated = upstream_resources[offset:offset + limit]

        # Assemble enriched response
        enriched: list[UpstreamResourceEnriched] = []
        for res in paginated:
            parents = [
                ParentInfo(
                    location=loc,
                    resource_status=p_status,
                    is_outsider=project_use_case.lower() not in (p_name or "").lower() if project_use_case else False,
                )
                for loc, p_name, p_status in parents_map.get(res.resource_id, [])
            ]
            enriched.append(UpstreamResourceEnriched(
                resource_id=res.resource_id,
                resource_name=res.resource_name,
                resource_type=res.resource_type,
                resource_status=res.resource_status,
                last_update_time=res.last_update_time,
                resource_url=res.location,
                github_url=github_urls.get(res.resource_name),
                parents=parents,
            ))

        return enriched, total

    
    def _find_existing_github_downstream(self, resource_id: str) -> ResourceRead | None:
        """Check if the resource already has a downstream with a GitHub location.

        Returns the first downstream resource whose location contains "github"
        (case-insensitive), or ``None`` if no such downstream exists.
        """
        downstream_ids = {
            rel.downstream_resource_id
            for rel in self._relationships
            if rel.upstream_resource_id == resource_id
        }
        for did in downstream_ids:
            res = self._resources.get(did)
            if res is not None and (res.location or "").lower().find("github") != -1:
                return res
        return None

    def prepare_upstream_resource_extraction(
        self,
        data: PrepareExtractionRequest,
    ) -> PrepareExtractionResponse:
        """
        For each input resource_id, create a paired ``-extracted`` target
        resource, establish a FEEDS relationship, and return structured
        tasks / info.

        Workflow per resource_id:
        1. Validate the resource exists (ValueError → 404 if not).
        2. Skip with info if the resource is not in DISCOVERED status.
        3. **Reuse check**: if the resource already has a downstream with a
           GitHub location, return it as a task (no new resource created).
        4. Create ``{rid}-extracted`` target resource (DISCOVERED initially).
        5. Update target status to PLANNED.
        6. Create FEEDS relationship: source → target.
        7. Classify into tasks (target has location) or info (no location).

        Args:
            data: request containing resource_ids, actor_type, actor_name

        Returns:
            PrepareExtractionResponse with tasks and info lists.

        Raises:
            ValueError: if any resource_id does not exist.
        """
        tasks: list[ExtractionTask] = []
        pending_location_count = 0

        for rid in data.resource_ids:
            # 1. Validate existence
            resource = self._resources.get(rid)
            if resource is None:
                raise ValueError(f"Resource(s) not found: {rid}")

            target_rid = f"{rid}-extracted"

            # 2. Skip if not PLANNED
            if resource.resource_status != ResourceStatus.PLANNED:
                continue

            # 3. Reuse check — existing downstream with GitHub location
            existing = self._find_existing_github_downstream(rid)
            if existing is not None:
                tasks.append(ExtractionTask(
                    source_resource_id=rid,
                    target_location=existing.location,
                ))
                continue

            # 4. Look up pre-existing target resource
            target = self._resources.get(target_rid)
            if target is None:
                pending_location_count += 1
                continue

            # 7. Classify
            target_location = target.location or ""
            if target_location:
                tasks.append(ExtractionTask(
                    source_resource_id=rid,
                    target_location=target_location,
                ))
            else:
                pending_location_count += 1

        return PrepareExtractionResponse(tasks=tasks, pending_location_count=pending_location_count)



    # ------------------------------------------------------------------
    # Activity log operations
    # ------------------------------------------------------------------
    def list_logs(
        self, resource_id: Optional[str] = None, cursor: Optional[str] = None, offset: int = 0, limit: int = 100
    ) -> tuple[list[ActivityLogRead], int, Optional[str]]:
        """List activity logs with pagination, optionally filtered by resource_id.
        
        Returns:
            Tuple of (paginated items, total count, next_cursor)
        """
        logs: Iterable[ActivityLogRead] = self._activity_logs
        if resource_id is not None:
            logs = [lg for lg in logs if lg.resource_id == resource_id]
        sorted_logs = sorted(logs, key=lambda lg: (lg.timestamp, lg.log_id), reverse=True)
        total = len(sorted_logs)

        if cursor:
            from datetime import datetime as _dt
            cursor_data = decode_cursor(cursor)
            cursor_ts = _dt.fromisoformat(cursor_data["ts"])
            cursor_lid = cursor_data["lid"]
            after = [
                lg for lg in sorted_logs
                if (lg.timestamp, lg.log_id) < (cursor_ts, cursor_lid)
            ]
            paginated = after[:limit]
            has_more = len(after) > limit
        else:
            paginated = sorted_logs[offset:offset + limit]
            has_more = (offset + len(paginated)) < total

        next_cursor = (
            encode_cursor({"ts": paginated[-1].timestamp.isoformat(), "lid": paginated[-1].log_id})
            if paginated and has_more else None
        )
        return paginated, total, next_cursor

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------
    def create_resources_batch(self, items: list[ResourceCreate]) -> list[ResourceRead]:
        """Create multiple resources in one call. Returns list of created resources."""
        results: list[ResourceRead] = []
        for data in items:
            results.append(self.create_resource(data))
        return results

    def create_relationships_batch(self, items: list[RelationshipCreate]) -> list[RelationshipRead]:
        """Create multiple relationships in one call. Returns list of created/existing relationships."""
        results: list[RelationshipRead] = []
        for data in items:
            results.append(self.create_relationship(data))
        return results

    def ping(self) -> bool:
        """Always ready for in-memory backend."""
        return True


class DeltaStore:
    def __init__(self) -> None:
        self.host = os.getenv("DATABRICKS_HOST")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")
        self.catalog = os.getenv("UC_CATALOG")
        self.schema = os.getenv("UC_SCHEMA")
        self.rid_table = os.getenv("MAGICAL_INDEX_RID_TABLE", "gandalf-magical-index-rid")
        self.rel_table = os.getenv("MAGICAL_INDEX_REL_TABLE", "gandalf-magical-index-rel")
        self.log_table = os.getenv("MAGICAL_INDEX_LOG_TABLE", "gandalf-magical-index-log")

        if not all([self.host, self.http_path, self.catalog, self.schema]):
            raise RuntimeError("Missing Databricks/UC environment configuration for DeltaStore")

        def q(x: str) -> str:
            return f"`{x.replace('`', '``')}`"

        self._rid_fqn = f"{q(self.catalog)}.{q(self.schema)}.{q(self.rid_table)}"
        self._rel_fqn = f"{q(self.catalog)}.{q(self.schema)}.{q(self.rel_table)}"
        self._log_fqn = f"{q(self.catalog)}.{q(self.schema)}.{q(self.log_table)}"

        # --- Connection pool ---
        self._pool_size = settings.DB_POOL_SIZE
        self._pool: list = []
        self._pool_lock = threading.Lock()
        self._pool_semaphore = threading.Semaphore(self._pool_size)

        # --- Write buffer ---
        self._buffer_enabled = settings.WRITE_BUFFER_ENABLED
        self._skip_prevalidation = settings.WRITE_BUFFER_SKIP_PREVALIDATION
        self._write_buffer: Optional[WriteBuffer] = None
        if self._buffer_enabled:
            self._write_buffer = WriteBuffer(
                connect_fn=self._connect,
                flush_interval_ms=settings.WRITE_BUFFER_FLUSH_INTERVAL_MS,
                batch_threshold=settings.WRITE_BUFFER_BATCH_THRESHOLD,
                max_retries=settings.WRITE_BUFFER_MAX_RETRIES,
                wal_dir=settings.WRITE_BUFFER_WAL_DIR,
                max_flush_batch=settings.WRITE_BUFFER_MAX_FLUSH_BATCH,
                max_memory_items=settings.WRITE_BUFFER_MAX_MEMORY_ITEMS,
            )

    def _create_connection(self):
        from app.services.db_auth import get_connection
        return get_connection(host=self.host, http_path=self.http_path)

    def _validate_connection(self, conn) -> bool:
        """Return True if the connection is alive."""
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            return True
        except Exception:
            return False

    @contextmanager
    def _connect(self):
        """Check out a connection from the pool, yield it, then return it.

        If the pool is empty, create a new connection on-the-fly (up to
        pool_size concurrent connections enforced by the semaphore).
        Stale connections are detected and replaced transparently.
        """
        acquired = self._pool_semaphore.acquire(timeout=30)
        if not acquired:
            raise RuntimeError("Connection pool exhausted (timed out after 30s)")

        conn = None
        try:
            with self._pool_lock:
                if self._pool:
                    conn = self._pool.pop()

            # Validate or create
            if conn is not None:
                if not self._validate_connection(conn):
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = self._create_connection()
            else:
                conn = self._create_connection()

            yield conn
        except Exception:
            # On error, discard the connection (may be corrupt) and re-raise
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None
            raise
        finally:
            if conn is not None:
                with self._pool_lock:
                    self._pool.append(conn)
            self._pool_semaphore.release()

    # ------------------------------------------------------------------
    # Row mappers
    # ------------------------------------------------------------------
    @staticmethod
    def _coerce_enum(v: object) -> object:
        if not isinstance(v, str):
            return v
        # Handle fully-qualified enum names like "ResourceType.OBJECT_TYPE"
        if "." in v:
            v = v.split(".")[-1]
        # Normalize human-readable labels like "Object Type" → "OBJECT_TYPE"
        if " " in v:
            v = v.replace(" ", "_").upper()
        # Normalize lowercase/mixed-case to uppercase (e.g., "object_type" → "OBJECT_TYPE")
        if v != v.upper():
            v = v.upper()
        return v

    # Base SQL filter to exclude non-MI rows in the shared UC table
    _VALID_STATUSES = "('PLANNED','DISCOVERED','EXTRACTED','TRANSFORMED','DEPLOYED','STAGED_FOR_DECOMMISSION')"
    _VALID_STATUS_LIST = ['PLANNED', 'DISCOVERED', 'EXTRACTED', 'TRANSFORMED', 'DEPLOYED', 'STAGED_FOR_DECOMMISSION']

    def _row_to_resource(self, row) -> Optional[ResourceRead]:
        try:
            return ResourceRead(
                resource_id=row[0],
                resource_name=row[1],
                location=row[2],
                resource_type=self._coerce_enum(row[3]),
                resource_status=self._coerce_enum(row[4]),
                last_by=row[5],
                last_update_time=row[6],
            )
        except Exception as exc:
            logger.warning(
                "_row_to_resource failed for row %s: %s",
                row, exc,
            )
            return None

    def _row_to_log(self, row) -> ActivityLogRead:
        return ActivityLogRead(
            log_id=row[0],
            resource_id=row[1],
            action=self._coerce_enum(row[2]),
            actor_type=self._coerce_enum(row[3]),
            actor_name=row[4],
            details=row[5],
            timestamp=row[6],
        )

    def _select_resource_sql(self) -> str:
        return (
            f"SELECT resource_id, resource_name, location, resource_type, "
            f"resource_status, last_by, last_update_time FROM {self._rid_fqn}"
        )

    def _select_log_sql(self) -> str:
        return (
            f"SELECT log_id, resource_id, action, actor_type, actor_name, "
            f"details, timestamp FROM {self._log_fqn}"
        )

    # ------------------------------------------------------------------
    # Activity log helper
    # ------------------------------------------------------------------
    def _insert_log(self, cur, resource_id: str, action: ActionType,
                    actor_type: ActorType, actor_name: str,
                    details: Optional[str] = None) -> None:
        log_id = generate_log_id()
        cur.execute(
            f"INSERT INTO {self._log_fqn} (log_id, resource_id, action, actor_type, actor_name, details, timestamp) "
            f"VALUES (?, ?, ?, ?, ?, ?, current_timestamp())",
            (log_id, resource_id, action.value, actor_type.value, actor_name, details),
        )

    def log_api_request(
        self,
        method: str,
        path: str,
        status_code: int,
        duration_ms: float,
        error: Optional[str] = None,
    ) -> None:
        """Persist middleware request timing and failure details to the log table."""
        action = "API_REQUEST_FAILED" if (error or status_code >= 400) else "API_REQUEST"
        details_parts = [
            f"method={method}",
            f"path={path}",
            f"status={status_code}",
            f"duration_ms={duration_ms:.2f}",
        ]
        if error:
            details_parts.append(f"error={error}")

        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"INSERT INTO {self._log_fqn} (log_id, resource_id, action, actor_type, actor_name, details, timestamp) "
                        f"VALUES (?, ?, ?, ?, ?, ?, current_timestamp())",
                        (
                            generate_log_id(),
                            path,
                            action,
                            ActorType.SYSTEM.value,
                            "MI-SYS",
                            " ".join(details_parts),
                        ),
                    )
                    conn.commit()
        except Exception as exc:
            logger.warning("log_api_request failed: %s", exc)

    # ------------------------------------------------------------------
    # Resource operations
    # ------------------------------------------------------------------
    def create_resource(self, data: ResourceCreate) -> ResourceRead:
        rid = data.resource_id or generate_resource_id(
            resource_type=data.resource_type or None,
            platform=data.platform or "auto"
        )
        last_by = f"{data.actor_type.value}:{data.actor_name}"
        now = datetime.now(timezone.utc)

        # Check buffer cache first (avoid duplicate while item is pending flush)
        if self._write_buffer and self._write_buffer.resource_in_buffer(rid):
            raise ValueError(f"Resource already exists: {rid}")

        # Pre-validate against Databricks (skipped when skip_prevalidation is enabled)
        if not (self._write_buffer and self._skip_prevalidation):
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT 1 FROM {self._rid_fqn} WHERE resource_id = ?", (rid,))
                    if cur.fetchone():
                        raise ValueError(f"Resource already exists: {rid}")

        resource = ResourceRead(
            resource_id=rid,
            resource_name=data.resource_name,
            location=data.location,
            resource_type=data.resource_type,
            resource_status=data.resource_status,
            last_by=last_by,
            last_update_time=now,
        )

        resource_type_value = data.resource_type.value if data.resource_type else None

        if self._write_buffer:
            # Build deferred SQL ops
            insert_sql = (
                f"INSERT INTO {self._rid_fqn} (resource_id, resource_name, location, resource_type, "
                f"resource_status, last_by, last_update_time) VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            insert_params = (
                rid, data.resource_name, data.location, resource_type_value,
                data.resource_status.value, last_by, now,
            )
            log_id = generate_log_id()
            log_sql = (
                f"INSERT INTO {self._log_fqn} (log_id, resource_id, action, actor_type, actor_name, details, timestamp) "
                f"VALUES (?, ?, ?, ?, ?, ?, current_timestamp())"
            )
            log_params = (
                log_id, rid, ActionType.RESOURCE_CREATED.value, data.actor_type.value,
                data.actor_name,
                f"Created resource '{data.resource_name or rid}' with status {data.resource_status.value}",
            )
            op = _WriteOp(
                table="resource", sql=insert_sql, params=insert_params,
                log_sql=log_sql, log_params=log_params, key=rid,
            )
            self._write_buffer.enqueue_resource(op, resource)
            return resource

        # Synchronous fallback (buffer disabled)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {self._rid_fqn} (resource_id, resource_name, location, resource_type, "
                    f"resource_status, last_by, last_update_time) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (rid, data.resource_name, data.location, resource_type_value,
                     data.resource_status.value, last_by, now),
                )
                self._insert_log(
                    cur, rid, ActionType.RESOURCE_CREATED, data.actor_type, data.actor_name,
                    f"Created resource '{data.resource_name or rid}' with status {data.resource_status.value}",
                )
                conn.commit()
                return resource

    def get_resource(self, resource_id: str) -> Optional[ResourceRead]:
        # Check write buffer cache first (read-through)
        if self._write_buffer:
            cached = self._write_buffer.lookup_resource(resource_id)
            if cached is not None:
                return cached
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(self._select_resource_sql() + " WHERE resource_id = ?", (resource_id,))
                row = cur.fetchone()
                if row is None:
                    logger.info("get_resource: no row found for resource_id=%s", resource_id)
                    return None
                result = self._row_to_resource(row)
                if result is None:
                    logger.warning(
                        "get_resource: row found but _row_to_resource returned None for resource_id=%s, raw row=%s",
                        resource_id, row,
                    )
                return result

    def _valid_status_clause(self) -> tuple[str, list[str]]:
        """Return a parameterized IN clause and corresponding param values.

        Using parameterized placeholders instead of a literal string avoids
        confusing the ``databricks-sql-connector`` parameter parser when the
        SQL also contains other ``?`` markers.
        """
        ph = ",".join(["?"] * len(self._VALID_STATUS_LIST))
        return f"resource_status IN ({ph})", list(self._VALID_STATUS_LIST)

    def _resource_type_filter_clause(
        self, resource_type_filter: Optional[ResourceTypeFilter]
    ) -> tuple[str, list[str]]:
        """Return a SQL clause and params for resource_type include/exclude filtering.

        Returns ("", []) if no filter is active.
        """
        if resource_type_filter is None:
            return "", []
        if resource_type_filter.include:
            values = [t.upper() for t in resource_type_filter.include]
            ph = ",".join(["?"] * len(values))
            return f"UPPER(resource_type) IN ({ph})", values
        if resource_type_filter.exclude:
            values = [t.upper() for t in resource_type_filter.exclude]
            ph = ",".join(["?"] * len(values))
            return f"UPPER(resource_type) NOT IN ({ph})", values
        return "", []

    def list_resources(
        self,
        resource_status: Optional[ResourceStatus] = None,
        resource_type: Optional[str] = None,
        platform: Optional[str] = None,
        cursor: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> tuple[list[ResourceRead], int, Optional[str]]:
        """List resources with pagination, optionally filtering by status, type, or platform.
        
        Returns:
            Tuple of (paginated items, total count, next_cursor)
        """
        # Always restrict to valid MI statuses (UC table may contain non-MI rows)
        status_clause, status_params = self._valid_status_clause()
        conditions: list[str] = [status_clause]
        params: list = list(status_params)
        if resource_status is not None:
            conditions.append("resource_status = ?")
            params.append(resource_status.value)
        if resource_type is not None:
            conditions.append("resource_type = ?")
            params.append(resource_type)
        if platform is not None:
            conditions.append("resource_id LIKE ? || ':%'")
            params.append(platform)
        base_where = " WHERE " + " AND ".join(conditions)
        base_params = list(params)

        with self._connect() as conn:
            with conn.cursor() as cur:
                count_sql = f"SELECT COUNT(*) FROM {self._rid_fqn}{base_where}"
                cur.execute(count_sql, tuple(base_params))
                total = cur.fetchone()[0]
                if total == 0:
                    return [], 0, None

                data_conditions = list(conditions)
                data_params = list(params)

                if cursor:
                    cursor_data = decode_cursor(cursor)
                    data_conditions.append("resource_id > ?")
                    data_params.append(cursor_data["rid"])

                data_where = " WHERE " + " AND ".join(data_conditions)
                fetch_limit = limit + 1
                data_sql = (
                    self._select_resource_sql()
                    + f"{data_where} ORDER BY resource_id"
                    + f" LIMIT {int(fetch_limit)}"
                )
                if not cursor:
                    data_sql += f" OFFSET {int(offset)}"

                cur.execute(data_sql, tuple(data_params))
                rows = cur.fetchall()
                resources = [r for r in (self._row_to_resource(row) for row in rows) if r is not None]

                has_more = len(resources) > limit
                if has_more:
                    resources = resources[:limit]

                next_cursor = (
                    encode_cursor({"rid": resources[-1].resource_id})
                    if resources and has_more else None
                )
                return resources, total, next_cursor

    def update_resource(self, resource_id: str, data: ResourceUpdate) -> Optional[ResourceRead]:
        last_by = f"{data.actor_type.value}:{data.actor_name}"
        set_parts = ["last_by = ?", "last_update_time = current_timestamp()"]
        params: list = [last_by]
        changes: list[str] = []

        if data.resource_name is not None:
            set_parts.append("resource_name = ?")
            params.append(data.resource_name)
            changes.append(f"resource_name -> '{data.resource_name}'")
        if data.location is not None:
            set_parts.append("location = ?")
            params.append(data.location)
            changes.append(f"location -> '{data.location}'")
        if data.resource_type is not None:
            set_parts.append("resource_type = ?")
            params.append(data.resource_type.value)
            changes.append(f"resource_type -> {data.resource_type.value}")
        if data.resource_status is not None:
            set_parts.append("resource_status = ?")
            params.append(data.resource_status.value)
            changes.append(f"resource_status -> {data.resource_status.value}")

        params.append(resource_id)

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT 1 FROM {self._rid_fqn} WHERE resource_id = ?", (resource_id,))
                if not cur.fetchone():
                    return None

                cur.execute(
                    f"UPDATE {self._rid_fqn} SET {', '.join(set_parts)} WHERE resource_id = ?",
                    tuple(params),
                )
                self._insert_log(
                    cur, resource_id, ActionType.RESOURCE_UPDATED,
                    data.actor_type, data.actor_name,
                    "; ".join(changes) if changes else "no-op update",
                )
                conn.commit()

                cur.execute(self._select_resource_sql() + " WHERE resource_id = ?", (resource_id,))
                row = cur.fetchone()
                return self._row_to_resource(row) if row else None

    def delete_resource(self, resource_id: str, actor_type: ActorType, actor_name: str) -> bool:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT 1 FROM {self._rid_fqn} WHERE resource_id = ?", (resource_id,))
                if not cur.fetchone():
                    return False

                cur.execute(
                    f"DELETE FROM {self._rel_fqn} WHERE upstream_resource_id = ? OR downstream_resource_id = ?",
                    (resource_id, resource_id),
                )
                cur.execute(f"DELETE FROM {self._rid_fqn} WHERE resource_id = ?", (resource_id,))
                self._insert_log(
                    cur, resource_id, ActionType.RESOURCE_DELETED,
                    actor_type, actor_name,
                    f"Deleted resource {resource_id} and cascade-removed relationships",
                )
                conn.commit()
                return True

    # ------------------------------------------------------------------
    # Relationship operations
    # ------------------------------------------------------------------
    @staticmethod
    def _rel_cache_key(upstream: str, downstream: str, rel_type: str) -> str:
        return f"{upstream}|{downstream}|{rel_type}"

    def create_relationship(self, data: RelationshipCreate) -> RelationshipRead:
        """Raises ``ValueError`` if either resource does not exist."""
        rel_key = self._rel_cache_key(
            data.upstream_resource_id, data.downstream_resource_id,
            data.relationship_type.value,
        )

        # Check buffer cache for idempotency
        if self._write_buffer:
            cached = self._write_buffer.lookup_relationship(rel_key)
            if cached is not None:
                return cached

        if self._write_buffer and self._skip_prevalidation:
            # Skip synchronous Databricks reads; rely on buffer cache + DB constraints at flush
            ids_to_check = {data.upstream_resource_id, data.downstream_resource_id}
            if self._write_buffer:
                buffered = {rid for rid in ids_to_check if self._write_buffer.resource_in_buffer(rid)}
                ids_to_check -= buffered
            # If any IDs are not in buffer, we can't verify without DB — trust the caller
        else:
            # Pre-validate: both resources must exist (check buffer + Databricks)
            ids_to_check = {data.upstream_resource_id, data.downstream_resource_id}
            found: set[str] = set()

            # Check buffer cache for pending resources
            if self._write_buffer:
                for rid in ids_to_check:
                    if self._write_buffer.resource_in_buffer(rid):
                        found.add(rid)

            # Check Databricks for the rest
            remaining = ids_to_check - found
            if remaining:
                with self._connect() as conn:
                    with conn.cursor() as cur:
                        placeholders = ",".join(["?"] * len(remaining))
                        cur.execute(
                            f"SELECT resource_id FROM {self._rid_fqn} WHERE resource_id IN ({placeholders})",
                            tuple(remaining),
                        )
                        found.update(row[0] for row in cur.fetchall())

            missing = sorted(ids_to_check - found)
            if missing:
                raise ValueError(f"Resource(s) not found: {', '.join(missing)}")

            # Check Databricks for existing identical relationship (idempotency)
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT upstream_resource_id, downstream_resource_id, relationship_type, notes "
                        f"FROM {self._rel_fqn} WHERE upstream_resource_id = ? AND downstream_resource_id = ? AND relationship_type = ?",
                        (data.upstream_resource_id, data.downstream_resource_id, data.relationship_type.value),
                    )
                    row = cur.fetchone()
                    if row:
                        return RelationshipRead(
                            upstream_resource_id=row[0],
                            downstream_resource_id=row[1],
                            relationship_type=self._coerce_enum(row[2]),
                            notes=row[3],
                        )

        relationship = RelationshipRead(
            upstream_resource_id=data.upstream_resource_id,
            downstream_resource_id=data.downstream_resource_id,
            relationship_type=data.relationship_type,
            notes=data.notes,
        )

        if self._write_buffer:
            insert_sql = (
                f"INSERT INTO {self._rel_fqn} (upstream_resource_id, downstream_resource_id, relationship_type, notes) "
                f"VALUES (?, ?, ?, ?)"
            )
            insert_params = (
                data.upstream_resource_id, data.downstream_resource_id,
                data.relationship_type.value, data.notes,
            )
            log_id = generate_log_id()
            log_sql = (
                f"INSERT INTO {self._log_fqn} (log_id, resource_id, action, actor_type, actor_name, details, timestamp) "
                f"VALUES (?, ?, ?, ?, ?, ?, current_timestamp())"
            )
            log_params = (
                log_id, data.upstream_resource_id,
                ActionType.RELATIONSHIP_CREATED.value, data.actor_type.value,
                data.actor_name,
                f"{data.upstream_resource_id} {data.relationship_type.value} {data.downstream_resource_id}",
            )
            op = _WriteOp(
                table="relationship", sql=insert_sql, params=insert_params,
                log_sql=log_sql, log_params=log_params, key=rel_key,
            )
            self._write_buffer.enqueue_relationship(op, relationship)
            return relationship

        # Synchronous fallback (buffer disabled)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {self._rel_fqn} (upstream_resource_id, downstream_resource_id, relationship_type, notes) "
                    f"VALUES (?, ?, ?, ?)",
                    (data.upstream_resource_id, data.downstream_resource_id,
                     data.relationship_type.value, data.notes),
                )
                self._insert_log(
                    cur, data.upstream_resource_id, ActionType.RELATIONSHIP_CREATED,
                    data.actor_type, data.actor_name,
                    f"{data.upstream_resource_id} {data.relationship_type.value} {data.downstream_resource_id}",
                )
                conn.commit()
                return relationship

    def delete_relationship(self, data: RelationshipDelete) -> int:
        where = ["upstream_resource_id = ?", "downstream_resource_id = ?"]
        params: list = [data.upstream_resource_id, data.downstream_resource_id]
        if data.relationship_type is not None:
            where.append("relationship_type = ?")
            params.append(data.relationship_type.value)
        where_clause = " AND ".join(where)
        with self._connect() as conn:
            with conn.cursor() as cur:
                # Pre-count because Databricks SQL connector returns -1 for cur.rowcount on DML
                cur.execute(f"SELECT COUNT(*) FROM {self._rel_fqn} WHERE {where_clause}", tuple(params))
                count = cur.fetchone()[0]
                if count > 0:
                    cur.execute(f"DELETE FROM {self._rel_fqn} WHERE {where_clause}", tuple(params))
                    relationship_type = data.relationship_type.value if data.relationship_type is not None else "ANY"
                    self._insert_log(
                        cur,
                        data.upstream_resource_id,
                        ActionType.RELATIONSHIP_DELETED,
                        data.actor_type,
                        data.actor_name,
                        (
                            f"Deleted {count} relationship(s): "
                            f"upstream={data.upstream_resource_id}, "
                            f"downstream={data.downstream_resource_id}, "
                            f"relationship_type={relationship_type}"
                        ),
                    )
                    conn.commit()
                return count

    def list_upstream_resources(
        self, resource_id: str, resource_type_filter: Optional[ResourceTypeFilter] = None,
        cursor: Optional[str] = None, offset: int = 0, limit: int = 100
    ) -> tuple[list[ResourceRead], int, Optional[str]]:
        """Return resources that feed into ``resource_id`` (upstream set) with pagination.
        
        Returns:
            Tuple of (paginated items, total count, next_cursor)
        """
        status_clause, status_params = self._valid_status_clause()
        sub = f"SELECT upstream_resource_id FROM {self._rel_fqn} WHERE downstream_resource_id = ?"
        base_conditions = [f"resource_id IN ({sub})", status_clause]
        base_params: list = [resource_id] + status_params

        rt_clause, rt_params = self._resource_type_filter_clause(resource_type_filter)
        if rt_clause:
            base_conditions.append(rt_clause)
            base_params.extend(rt_params)

        base_where = " WHERE " + " AND ".join(base_conditions)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self._rid_fqn}{base_where}", tuple(base_params))
                total = cur.fetchone()[0]
                if total == 0:
                    return [], 0, None

                data_conditions = list(base_conditions)
                data_params = list(base_params)
                if cursor:
                    cursor_data = decode_cursor(cursor)
                    data_conditions.append("resource_id > ?")
                    data_params.append(cursor_data["rid"])

                data_where = " WHERE " + " AND ".join(data_conditions)
                fetch_limit = limit + 1
                data_sql = (
                    self._select_resource_sql()
                    + f"{data_where} ORDER BY resource_id"
                    + f" LIMIT {int(fetch_limit)}"
                )
                if not cursor:
                    data_sql += f" OFFSET {int(offset)}"

                cur.execute(data_sql, tuple(data_params))
                rows = cur.fetchall()
                resources = [r for r in (self._row_to_resource(row) for row in rows) if r is not None]

                has_more = len(resources) > limit
                if has_more:
                    resources = resources[:limit]

                next_cursor = (
                    encode_cursor({"rid": resources[-1].resource_id})
                    if resources and has_more else None
                )
                return resources, total, next_cursor

    def list_downstream_resources(
        self, resource_id: str, resource_type_filter: Optional[ResourceTypeFilter] = None,
        cursor: Optional[str] = None, offset: int = 0, limit: int = 100
    ) -> tuple[list[ResourceRead], int, Optional[str]]:
        """Return resources that depend on ``resource_id`` (downstream set) with pagination.
        
        Returns:
            Tuple of (paginated items, total count, next_cursor)
        """
        status_clause, status_params = self._valid_status_clause()
        sub = f"SELECT downstream_resource_id FROM {self._rel_fqn} WHERE upstream_resource_id = ?"
        base_conditions = [f"resource_id IN ({sub})", status_clause]
        base_params: list = [resource_id] + status_params

        rt_clause, rt_params = self._resource_type_filter_clause(resource_type_filter)
        if rt_clause:
            base_conditions.append(rt_clause)
            base_params.extend(rt_params)

        base_where = " WHERE " + " AND ".join(base_conditions)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self._rid_fqn}{base_where}", tuple(base_params))
                total = cur.fetchone()[0]
                if total == 0:
                    return [], 0, None

                data_conditions = list(base_conditions)
                data_params = list(base_params)
                if cursor:
                    cursor_data = decode_cursor(cursor)
                    data_conditions.append("resource_id > ?")
                    data_params.append(cursor_data["rid"])

                data_where = " WHERE " + " AND ".join(data_conditions)
                fetch_limit = limit + 1
                data_sql = (
                    self._select_resource_sql()
                    + f"{data_where} ORDER BY resource_id"
                    + f" LIMIT {int(fetch_limit)}"
                )
                if not cursor:
                    data_sql += f" OFFSET {int(offset)}"

                cur.execute(data_sql, tuple(data_params))
                rows = cur.fetchall()
                resources = [r for r in (self._row_to_resource(row) for row in rows) if r is not None]

                has_more = len(resources) > limit
                if has_more:
                    resources = resources[:limit]

                next_cursor = (
                    encode_cursor({"rid": resources[-1].resource_id})
                    if resources and has_more else None
                )
                return resources, total, next_cursor

    # Safety limits for graph traversal to prevent runaway queries / 504 timeouts
    _BFS_MAX_DEPTH = 50
    _BFS_MAX_NODES = 5000

    def list_upstream_resource_ids(self, resource_ids: list[str], resource_type_filter: Optional[ResourceTypeFilter] = None) -> list[str]:
        """
        Return upstream resource IDs shared by ALL provided resource_ids (intersection),
        using batched BFS backed by SQL queries.

        Each BFS level is resolved in a single ``WHERE downstream_resource_id IN (...)``
        query, reducing the total number of SQL round-trips from O(nodes) to O(depth).
        Safety limits (_BFS_MAX_DEPTH, _BFS_MAX_NODES) prevent runaway traversals on
        large graphs from causing 504 upstream timeouts.

        Args:
            resource_ids: list of resource_id strings (must be non-empty)
            resource_type_filter: optional filter to include/exclude by resource_type

        Returns:
            Sorted list of upstream resource IDs reachable from every input resource.

        Raises:
            ValueError: if resource_ids is empty or any resource does not exist.
        """

        # ---- Validate input (store-level, API already does this) ----
        if not isinstance(resource_ids, list) or not resource_ids:
            raise ValueError("resource_ids must be a non-empty list")

        normalized = [rid.strip() for rid in resource_ids if rid and rid.strip()]
        if len(normalized) != len(resource_ids):
            raise ValueError("resource_ids must contain non-empty strings only")

        # ---- Validate existence ----
        missing = [rid for rid in normalized if self.get_resource(rid) is None]
        if missing:
            raise ValueError(f"Resource(s) not found: {', '.join(missing)}")

        # ---- Helper: collect all upstream IDs for ONE resource via batched BFS ----
        def collect_upstream(start_rid: str) -> set[str]:
            visited: set[str] = set()
            frontier: list[str] = [start_rid]

            with self._connect() as conn:
                with conn.cursor() as cur:
                    depth = 0
                    while frontier and depth < self._BFS_MAX_DEPTH:
                        depth += 1
                        placeholders = ",".join(["?"] * len(frontier))
                        sql = (
                            f"SELECT upstream_resource_id FROM {self._rel_fqn} "
                            f"WHERE downstream_resource_id IN ({placeholders})"
                        )
                        cur.execute(sql, tuple(frontier))
                        rows = cur.fetchall()

                        next_frontier: list[str] = []
                        for (upstream_id,) in rows:
                            if upstream_id not in visited:
                                visited.add(upstream_id)
                                next_frontier.append(upstream_id)
                                if len(visited) >= self._BFS_MAX_NODES:
                                    logger.warning(
                                        "BFS node limit (%d) reached for resource %s at depth %d",
                                        self._BFS_MAX_NODES, start_rid, depth,
                                    )
                                    return visited
                        frontier = next_frontier

                    if depth >= self._BFS_MAX_DEPTH:
                        logger.warning(
                            "BFS depth limit (%d) reached for resource %s",
                            self._BFS_MAX_DEPTH, start_rid,
                        )

            return visited

        # ---- Compute intersection across all resource_ids ----
        shared_upstream: set[str] | None = None

        for rid in normalized:
            upstream_set = collect_upstream(rid)
            shared_upstream = (
                upstream_set if shared_upstream is None
                else shared_upstream & upstream_set
            )

            # Early exit if intersection is empty
            if not shared_upstream:
                break

        result_ids = sorted(shared_upstream or set())

        # Apply resource_type filter
        if resource_type_filter and (resource_type_filter.include or resource_type_filter.exclude):
            if result_ids:
                with self._connect() as conn:
                    with conn.cursor() as cur:
                        placeholders = ",".join(["?"] * len(result_ids))
                        rt_clause, rt_params = self._resource_type_filter_clause(resource_type_filter)
                        sql = (
                            f"SELECT resource_id FROM {self._rid_fqn}"
                            f" WHERE resource_id IN ({placeholders})"
                        )
                        params: list = list(result_ids)
                        if rt_clause:
                            sql += f" AND {rt_clause}"
                            params.extend(rt_params)
                        cur.execute(sql, tuple(params))
                        filtered_ids = {row[0] for row in cur.fetchall()}
                result_ids = sorted(filtered_ids)

        return result_ids

    def list_upstream_resources_enriched(
        self, resource_id: str, resource_type_filter: Optional[ResourceTypeFilter] = None,
        offset: int = 0, limit: int = 50
    ) -> tuple[list[UpstreamResourceEnriched], int]:
        """Return all transitive upstream resources enriched with github_url and parents.

        Uses batched BFS for upstream ID discovery, then enriches with:
        - github_url: matched by resource_name where resource_id contains 'github'
        - parents: direct upstream resources with location, status, and is_outsider flag

        Args:
            resource_id: The starting resource ID.
            resource_type_filter: optional filter to include/exclude by resource_type
            offset: Pagination offset.
            limit: Page size.

        Returns:
            Tuple of (paginated enriched items, total count).
        """
        # Fetch the input resource to extract the project use case name
        source_resource = self.get_resource(resource_id)
        if source_resource is None:
            raise ValueError(f"Resource not found: {resource_id}")

        # Extract project use case name (2nd path segment of resource_name)
        segments = source_resource.resource_name.split("/")
        project_use_case = segments[2] if len(segments) > 2 else ""

        # Collect all transitive upstream IDs via batched BFS
        visited: set[str] = set()
        frontier: list[str] = [resource_id]

        with self._connect() as conn:
            with conn.cursor() as cur:
                depth = 0
                while frontier and depth < self._BFS_MAX_DEPTH:
                    depth += 1
                    placeholders = ",".join(["?"] * len(frontier))
                    sql = (
                        f"SELECT upstream_resource_id FROM {self._rel_fqn} "
                        f"WHERE downstream_resource_id IN ({placeholders})"
                    )
                    cur.execute(sql, tuple(frontier))
                    rows = cur.fetchall()

                    next_frontier: list[str] = []
                    for (upstream_id,) in rows:
                        if upstream_id not in visited:
                            visited.add(upstream_id)
                            next_frontier.append(upstream_id)
                            if len(visited) >= self._BFS_MAX_NODES:
                                break
                    frontier = next_frontier

                if not visited:
                    return [], 0

                # Apply resource_type filter at the SQL level
                all_ids = sorted(visited)
                rt_clause, rt_params = self._resource_type_filter_clause(resource_type_filter)
                if rt_clause:
                    # Filter the IDs by resource_type in the DB
                    ph = ",".join(["?"] * len(all_ids))
                    filter_sql = (
                        f"SELECT resource_id FROM {self._rid_fqn}"
                        f" WHERE resource_id IN ({ph}) AND {rt_clause}"
                    )
                    cur.execute(filter_sql, tuple(all_ids) + tuple(rt_params))
                    all_ids = sorted(row[0] for row in cur.fetchall())

                total = len(all_ids)
                page_ids = all_ids[offset:offset + limit]

                if not page_ids:
                    return [], total

                placeholders = ",".join(["?"] * len(page_ids))
                cur.execute(
                    self._select_resource_sql()
                    + f" WHERE resource_id IN ({placeholders})"
                    + " ORDER BY resource_id",
                    tuple(page_ids),
                )
                resource_rows = cur.fetchall()
                resources = [r for r in (self._row_to_resource(row) for row in resource_rows) if r is not None]

                # Collect resource_names for github URL lookup
                resource_names = list({r.resource_name for r in resources})

                # Query 2: Fetch github URLs keyed by resource_name
                github_urls: dict[str, str] = {}
                if resource_names:
                    batch_size = 500
                    for i in range(0, len(resource_names), batch_size):
                        batch = resource_names[i:i + batch_size]
                        ph = ",".join(["?"] * len(batch))
                        cur.execute(
                            f"SELECT resource_name, location FROM {self._rid_fqn} "
                            f"WHERE resource_name IN ({ph}) AND resource_id LIKE '%github%'",
                            tuple(batch),
                        )
                        for row in cur.fetchall():
                            if row[1]:  # location not null
                                github_urls.setdefault(row[0], row[1])

                # Query 3: Fetch parents (direct upstream) for each resource in the page
                parents_map: dict[str, list[tuple[str | None, str, str]]] = {}
                ph = ",".join(["?"] * len(page_ids))
                cur.execute(
                    f"SELECT rel.downstream_resource_id, rid.location, rid.resource_name, rid.resource_status "
                    f"FROM {self._rel_fqn} AS rel "
                    f"INNER JOIN {self._rid_fqn} AS rid "
                    f"ON rel.upstream_resource_id = rid.resource_id "
                    f"WHERE rel.downstream_resource_id IN ({ph})",
                    tuple(page_ids),
                )
                for row in cur.fetchall():
                    parents_map.setdefault(row[0], []).append((row[1], row[2], row[3]))

        # Assemble enriched response
        enriched: list[UpstreamResourceEnriched] = []
        for res in resources:
            parents = [
                ParentInfo(
                    location=loc,
                    resource_status=self._coerce_enum(p_status),
                    is_outsider=project_use_case.lower() not in (p_name or "").lower() if project_use_case else False,
                )
                for loc, p_name, p_status in parents_map.get(res.resource_id, [])
            ]
            enriched.append(UpstreamResourceEnriched(
                resource_id=res.resource_id,
                resource_name=res.resource_name,
                resource_type=res.resource_type,
                resource_status=res.resource_status,
                last_update_time=res.last_update_time,
                resource_url=res.location,
                github_url=github_urls.get(res.resource_name),
                parents=parents,
            ))

        return enriched, total


    def _find_existing_github_downstream(self, resource_id: str) -> ResourceRead | None:
        """Check if the resource already has a downstream with a GitHub location.

        Queries the relationship + resource tables for any downstream resource
        whose location contains "github" (case-insensitive).
        Returns the first match, or ``None``.
        """
        downstream_resources, _, _ = self.list_downstream_resources(resource_id, offset=0, limit=1000)
        for res in downstream_resources:
            if (res.location or "").lower().find("github") != -1:
                return res
        return None

    def prepare_upstream_resource_extraction(
        self,
        data: PrepareExtractionRequest,
    ) -> PrepareExtractionResponse:
        """
        For each input resource_id, create a paired ``-extracted`` target
        resource, establish a FEEDS relationship, and return structured
        tasks / info.

        Workflow per resource_id:
        1. Validate the resource exists (ValueError → 404 if not).
        2. Skip with info if the resource is not in DISCOVERED status.
        3. **Reuse check**: if the resource already has a downstream with a
           GitHub location, return it as a task (no new resource created).
        4. Create ``{rid}-extracted`` target resource (DISCOVERED initially).
        5. Update target status to PLANNED.
        6. Create FEEDS relationship: source → target.
        7. Classify into tasks (target has location) or info (no location).

        Args:
            data: request containing resource_ids, actor_type, actor_name

        Returns:
            PrepareExtractionResponse with tasks and info lists.

        Raises:
            ValueError: if any resource_id does not exist.
        """
        tasks: list[ExtractionTask] = []
        pending_location_count = 0

        last_by = f"{data.actor_type.value}:{data.actor_name}"

        for rid in data.resource_ids:
            target_rid = f"{rid}-extracted"

            # 1. Validate existence
            resource = self.get_resource(rid)
            if resource is None:
                raise ValueError(f"Resource(s) not found: {rid}")

            # 2. Skip if not PLANNED
            if resource.resource_status != ResourceStatus.PLANNED:
                continue

            # 3. Reuse check — existing downstream with GitHub location
            existing = self._find_existing_github_downstream(rid)
            if existing is not None:
                tasks.append(ExtractionTask(
                    source_resource_id=rid,
                    target_location=existing.location,
                ))
                continue

            # 4. Look up pre-existing target resource
            target = self.get_resource(target_rid)
            if target is None:
                pending_location_count += 1
                continue

            # 7. Classify
            target_location = (target.location or "").strip()
            if target_location:
                tasks.append(ExtractionTask(
                    source_resource_id=rid,
                    target_location=target_location,
                ))
            else:
                pending_location_count += 1

        return PrepareExtractionResponse(tasks=tasks, pending_location_count=pending_location_count)

    # ------------------------------------------------------------------
    # Activity log operations
    # ------------------------------------------------------------------
    def list_logs(
        self, resource_id: Optional[str] = None, cursor: Optional[str] = None, offset: int = 0, limit: int = 100
    ) -> tuple[list[ActivityLogRead], int, Optional[str]]:
        """List activity logs with pagination, optionally filtered by resource_id.
        
        Returns:
            Tuple of (paginated items, total count, next_cursor)
        """
        conditions: list[str] = []
        params: list = []
        if resource_id is not None:
            conditions.append("resource_id = ?")
            params.append(resource_id)

        base_where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        base_params = list(params)

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self._log_fqn}{base_where}", tuple(base_params))
                total = cur.fetchone()[0]
                if total == 0:
                    return [], 0, None

                data_conditions = list(conditions)
                data_params = list(params)

                if cursor:
                    cursor_data = decode_cursor(cursor)
                    data_conditions.append(
                        "(timestamp < ? OR (timestamp = ? AND log_id < ?))"
                    )
                    data_params.extend([cursor_data["ts"], cursor_data["ts"], cursor_data["lid"]])

                data_where = (" WHERE " + " AND ".join(data_conditions)) if data_conditions else ""
                fetch_limit = limit + 1
                data_sql = (
                    self._select_log_sql()
                    + f"{data_where} ORDER BY timestamp DESC, log_id DESC"
                    + f" LIMIT {int(fetch_limit)}"
                )
                if not cursor:
                    data_sql += f" OFFSET {int(offset)}"

                cur.execute(data_sql, tuple(data_params))
                rows = cur.fetchall()
                logs = [self._row_to_log(r) for r in rows]

                has_more = len(logs) > limit
                if has_more:
                    logs = logs[:limit]

                next_cursor = (
                    encode_cursor({"ts": logs[-1].timestamp.isoformat(), "lid": logs[-1].log_id})
                    if logs and has_more else None
                )
                return logs, total, next_cursor

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------
    def create_resources_batch(self, items: list[ResourceCreate]) -> list[ResourceRead]:
        """Create multiple resources in a single transaction (or buffered).

        When write buffer is enabled, each item is enqueued individually
        after a batched existence check against Databricks + buffer cache.

        Raises ``ValueError`` if any resource_id already exists.
        """
        if not items:
            return []

        # Pre-resolve IDs
        resolved: list[tuple[str, str, ResourceCreate]] = []
        for data in items:
            rid = data.resource_id or generate_resource_id(
                resource_type=data.resource_type or None,
                platform=data.platform or "auto",
            )
            last_by = f"{data.actor_type.value}:{data.actor_name}"
            resolved.append((rid, last_by, data))

        all_rids = [r[0] for r in resolved]
        now = datetime.now(timezone.utc)

        # Check buffer cache for duplicates
        if self._write_buffer:
            buffer_dupes = [rid for rid in all_rids if self._write_buffer.resource_in_buffer(rid)]
            if buffer_dupes:
                raise ValueError(f"Resource(s) already exist: {', '.join(sorted(buffer_dupes))}")

        # Check Databricks for duplicates (skipped when skip_prevalidation is enabled)
        if not (self._write_buffer and self._skip_prevalidation):
            with self._connect() as conn:
                with conn.cursor() as cur:
                    ph = ",".join(["?"] * len(all_rids))
                    cur.execute(
                        f"SELECT resource_id FROM {self._rid_fqn} WHERE resource_id IN ({ph})",
                        tuple(all_rids),
                    )
                    existing = {row[0] for row in cur.fetchall()}
                    if existing:
                        raise ValueError(f"Resource(s) already exist: {', '.join(sorted(existing))}")

        if self._write_buffer:
            results: list[ResourceRead] = []
            for rid, last_by, data in resolved:
                resource = ResourceRead(
                    resource_id=rid,
                    resource_name=data.resource_name,
                    location=data.location,
                    resource_type=data.resource_type,
                    resource_status=data.resource_status,
                    last_by=last_by,
                    last_update_time=now,
                )
                resource_type_value = data.resource_type.value if data.resource_type else None
                insert_sql = (
                    f"INSERT INTO {self._rid_fqn} (resource_id, resource_name, location, resource_type, "
                    f"resource_status, last_by, last_update_time) VALUES (?, ?, ?, ?, ?, ?, ?)"
                )
                insert_params = (
                    rid, data.resource_name, data.location, resource_type_value,
                    data.resource_status.value, last_by, now,
                )
                log_id = generate_log_id()
                log_sql = (
                    f"INSERT INTO {self._log_fqn} (log_id, resource_id, action, actor_type, actor_name, details, timestamp) "
                    f"VALUES (?, ?, ?, ?, ?, ?, current_timestamp())"
                )
                log_params = (
                    log_id, rid, ActionType.RESOURCE_CREATED.value, data.actor_type.value,
                    data.actor_name,
                    f"Created resource '{data.resource_name or rid}' with status {data.resource_status.value}",
                )
                op = _WriteOp(
                    table="resource", sql=insert_sql, params=insert_params,
                    log_sql=log_sql, log_params=log_params, key=rid,
                )
                self._write_buffer.enqueue_resource(op, resource)
                results.append(resource)
            return results

        # Synchronous fallback (buffer disabled)
        with self._connect() as conn:
            with conn.cursor() as cur:
                results = []
                for rid, last_by, data in resolved:
                    resource_type_value = data.resource_type.value if data.resource_type else None
                    cur.execute(
                        f"INSERT INTO {self._rid_fqn} (resource_id, resource_name, location, resource_type, "
                        f"resource_status, last_by, last_update_time) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (rid, data.resource_name, data.location, resource_type_value,
                         data.resource_status.value, last_by, now),
                    )
                    self._insert_log(
                        cur, rid, ActionType.RESOURCE_CREATED, data.actor_type, data.actor_name,
                        f"Created resource '{data.resource_name or rid}' with status {data.resource_status.value}",
                    )
                    results.append(ResourceRead(
                        resource_id=rid,
                        resource_name=data.resource_name,
                        location=data.location,
                        resource_type=data.resource_type,
                        resource_status=data.resource_status,
                        last_by=last_by,
                        last_update_time=now,
                    ))
                conn.commit()
                return results

    def create_relationships_batch(self, items: list[RelationshipCreate]) -> list[RelationshipRead]:
        """Create multiple relationships in a single transaction (or buffered).

        When write buffer is enabled, delegates to single-item create_relationship
        which handles buffer enqueuing with proper validation.

        Raises ``ValueError`` if any referenced resource does not exist.
        """
        if not items:
            return []

        if self._write_buffer:
            # Delegate to single-item path which handles buffer + validation
            results: list[RelationshipRead] = []
            for data in items:
                results.append(self.create_relationship(data))
            return results

        # Synchronous fallback (buffer disabled)
        # Collect all resource IDs that need to exist
        all_resource_ids: set[str] = set()
        for data in items:
            all_resource_ids.add(data.upstream_resource_id)
            all_resource_ids.add(data.downstream_resource_id)

        with self._connect() as conn:
            with conn.cursor() as cur:
                # Batch existence check
                ph = ",".join(["?"] * len(all_resource_ids))
                cur.execute(
                    f"SELECT resource_id FROM {self._rid_fqn} WHERE resource_id IN ({ph})",
                    tuple(all_resource_ids),
                )
                found = {row[0] for row in cur.fetchall()}
                missing = sorted(all_resource_ids - found)
                if missing:
                    raise ValueError(f"Resource(s) not found: {', '.join(missing)}")

                results = []
                for data in items:
                    # Idempotency check
                    cur.execute(
                        f"SELECT upstream_resource_id, downstream_resource_id, relationship_type, notes "
                        f"FROM {self._rel_fqn} WHERE upstream_resource_id = ? AND downstream_resource_id = ? AND relationship_type = ?",
                        (data.upstream_resource_id, data.downstream_resource_id, data.relationship_type.value),
                    )
                    row = cur.fetchone()
                    if row:
                        results.append(RelationshipRead(
                            upstream_resource_id=row[0],
                            downstream_resource_id=row[1],
                            relationship_type=self._coerce_enum(row[2]),
                            notes=row[3],
                        ))
                        continue

                    cur.execute(
                        f"INSERT INTO {self._rel_fqn} (upstream_resource_id, downstream_resource_id, relationship_type, notes) "
                        f"VALUES (?, ?, ?, ?)",
                        (data.upstream_resource_id, data.downstream_resource_id,
                         data.relationship_type.value, data.notes),
                    )
                    self._insert_log(
                        cur, data.upstream_resource_id, ActionType.RELATIONSHIP_CREATED,
                        data.actor_type, data.actor_name,
                        f"{data.upstream_resource_id} {data.relationship_type.value} {data.downstream_resource_id}",
                    )
                    results.append(RelationshipRead(
                        upstream_resource_id=data.upstream_resource_id,
                        downstream_resource_id=data.downstream_resource_id,
                        relationship_type=data.relationship_type,
                        notes=data.notes,
                    ))

                conn.commit()
                return results

    def ping(self) -> bool:
        """Simple connectivity check against Databricks SQL endpoint."""
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            return True
        except Exception:
            return False


store = DeltaStore() if settings.STORAGE_BACKEND.lower() == "delta" else InMemoryStore()
