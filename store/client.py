"""
StoreClient — bi-temporal event-sourced object store.

All operations are append-only INSERTs into object_events.
Reads return the latest non-deleted version per entity.
RLS enforces zero-trust access control automatically.
"""

import json
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, Generic, TypeVar

import psycopg2
import psycopg2.extras

from store.base import Embedded, Storable, _json_decoder_hook, _JSONEncoder
from store.subscriptions import ChangeEvent


class VersionConflict(Exception):
    """Raised when optimistic concurrency check fails."""

    def __init__(self, entity_id: str, expected_version: int, actual_version: int) -> None:
        self.entity_id = entity_id
        self.expected_version = expected_version
        self.actual_version = actual_version
        super().__init__(
            f"Version conflict on entity {entity_id}: "
            f"expected {expected_version}, actual {actual_version}"
        )


_S = TypeVar("_S", bound=Storable)


class QueryResult(Generic[_S]):
    """Result of a paginated query. Contains items and an optional next_cursor."""

    def __init__(self, items: list[_S], next_cursor: Any = None) -> None:
        self.items = items
        self.next_cursor = next_cursor

    def __iter__(self) -> Iterator[_S]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def __getitem__(self, index: int) -> _S:
        return self.items[index]


class StoreClient:
    """
    Connects to the object store as a specific user.
    All reads/writes are filtered by PostgreSQL RLS — no middleware needed.

    Every mutation creates a new immutable event (never overwrites).
    Bi-temporal: tx_time (system) + valid_from/valid_to (business).

    Usage:
        client = StoreClient(user="alice", password="secret", host="/tmp/pg", port=5432)
        client.write(Trade(symbol="AAPL", quantity=100, price=228.0, side="BUY"))
        trades = client.query(Trade)
        client.close()
    """

    def __init__(self, user: str, password: str, host: str = "localhost", port: int = 5432, dbname: str = "postgres",
                 event_bus: Any = None) -> None:
        self.user = user
        self.event_bus = event_bus
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
        )
        self.conn.autocommit = True
        psycopg2.extras.register_uuid()

    # ── Write operations (all append-only) ────────────────────────────

    def write(self, obj: Storable, valid_from: datetime | None = None) -> str:
        """
        Create a new entity (version 1). Returns the entity_id.
        If the Storable class has a state machine, initial state is set automatically.
        """
        if isinstance(obj, Embedded):
            raise TypeError(
                f"{type(obj).__name__} is Embedded — write the parent Storable instead"
            )
        entity_id = str(uuid.uuid4())
        json_data = obj.to_json()
        type_name = obj.type_name()
        state = None
        if obj._state_machine is not None:
            state = obj._state_machine.initial

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO object_events
                    (entity_id, version, type_name, data, state, event_type, valid_from)
                VALUES (%s, 1, %s, %s::jsonb, %s, 'CREATED', COALESCE(%s, now()))
                RETURNING event_id, entity_id, owner, updated_by, tx_time, valid_from, state
                """,
                (entity_id, type_name, json_data, state, valid_from),
            )
            row = cur.fetchone()
            obj._store_entity_id = str(row[1])
            obj._store_version = 1
            obj._store_owner = row[2]
            obj._store_updated_by = row[3]
            obj._store_tx_time = row[4]
            obj._store_valid_from = row[5]
            obj._store_valid_to = None
            obj._store_state = row[6]
            obj._store_event_type = "CREATED"
            self._emit_event(obj)
            return obj._store_entity_id

    def update(self, obj: Storable, valid_from: datetime | None = None) -> None:
        """
        Create a new version of an existing entity (never overwrites).
        Automatically determines event_type: UPDATED or CORRECTED (if backdated).

        Optimistic concurrency is automatic: if someone else wrote a new
        version since you read this object, raises VersionConflict.
        """
        if not obj._store_entity_id:
            raise ValueError("Object has no entity_id — write() it first")

        next_ver = self._next_version(obj._store_entity_id)

        # Automatic optimistic concurrency: obj._store_version must match
        if obj._store_version is not None:
            actual = next_ver - 1
            if actual != obj._store_version:
                raise VersionConflict(obj._store_entity_id, obj._store_version, actual)
        json_data = obj.to_json()
        type_name = obj.type_name()

        # Determine event type
        event_type = "UPDATED"
        if valid_from is not None:
            now = datetime.now(timezone.utc)
            if valid_from < now:
                event_type = "CORRECTED"

        # Carry forward state and permissions from previous version
        state = obj._store_state

        with self.conn.cursor() as cur:
            # Copy owner, readers/writers from latest version
            cur.execute(
                """
                SELECT owner, readers, writers FROM object_events
                WHERE entity_id = %s ORDER BY version DESC LIMIT 1
                """,
                (obj._store_entity_id,),
            )
            prev = cur.fetchone()
            original_owner = prev[0] if prev else self.user
            readers = prev[1] if prev else []
            writers = prev[2] if prev else []

            # Only the owner or a writer can create new versions
            if self.user != original_owner and self.user not in writers:
                raise PermissionError(
                    f"Cannot update entity {obj._store_entity_id} — "
                    f"not owner or writer"
                )

            cur.execute(
                """
                INSERT INTO object_events
                    (entity_id, version, type_name, owner, data, state, event_type,
                     readers, writers, valid_from)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, COALESCE(%s, now()))
                RETURNING event_id, tx_time, valid_from
                """,
                (obj._store_entity_id, next_ver, type_name, original_owner,
                 json_data, state, event_type, readers, writers, valid_from),
            )
            row = cur.fetchone()
            obj._store_version = next_ver
            obj._store_tx_time = row[1]
            obj._store_valid_from = row[2]
            obj._store_event_type = event_type
            self._emit_event(obj)

    def delete(self, obj: Storable) -> bool:
        """
        Soft-delete: creates a DELETED tombstone event.
        The entity disappears from read()/query() but remains in history().

        Optimistic concurrency is automatic.
        """
        if not obj._store_entity_id:
            raise ValueError("Object has no entity_id — write() it first")

        next_ver = self._next_version(obj._store_entity_id)

        # Automatic optimistic concurrency
        if obj._store_version is not None:
            actual = next_ver - 1
            if actual != obj._store_version:
                raise VersionConflict(obj._store_entity_id, obj._store_version, actual)
        type_name = obj.type_name()
        json_data = obj.to_json()

        with self.conn.cursor() as cur:
            # Carry forward original owner
            cur.execute(
                """
                SELECT owner FROM object_events
                WHERE entity_id = %s ORDER BY version DESC LIMIT 1
                """,
                (obj._store_entity_id,),
            )
            prev = cur.fetchone()
            original_owner = prev[0] if prev else self.user

            cur.execute(
                """
                INSERT INTO object_events
                    (entity_id, version, type_name, owner, data, state, event_type)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s, 'DELETED')
                RETURNING event_id, tx_time
                """,
                (obj._store_entity_id, next_ver, type_name, original_owner,
                 json_data, obj._store_state),
            )
            row = cur.fetchone()
            obj._store_version = next_ver
            obj._store_tx_time = row[1]
            obj._store_event_type = "DELETED"
            self._emit_event(obj)
            return row[0] is not None

    def transition(self, obj: Storable, new_state: str, valid_from: datetime | None = None) -> None:
        """
        Transition an entity to a new lifecycle state.

        Three tiers of side-effects:
          Tier 1 (action):         Runs inside the DB transaction — atomic.
          Tier 2 (on_exit/on_enter): Fire-and-forget after commit.
          Tier 3 (start_workflow): Durable workflow dispatch after commit.
        """
        if obj._state_machine is None:
            raise ValueError(
                f"{type(obj).__name__} has no state machine registered"
            )

        current_state = obj._store_state
        assert current_state is not None, f"{type(obj).__name__} has no current state"
        sm = obj._state_machine

        # Build context from object data for guard evaluation
        context = json.loads(obj.to_json())

        # Validate: checks edge exists, guard passes, user is permitted
        t = sm.validate_transition(
            current_state, new_state, context=context, user=self.user, obj=obj
        )

        assert obj._store_entity_id is not None
        next_ver = self._next_version(obj._store_entity_id)
        json_data = obj.to_json()
        type_name = obj.type_name()

        # Richer event_meta for audit
        meta: dict[str, object] = {
            "from_state": current_state,
            "to_state": new_state,
            "triggered_by": self.user,
        }
        if t.guard is not None:
            meta["guard"] = str(t.guard)
        if t.allowed_by is not None:
            meta["allowed_by"] = list(t.allowed_by)
        event_meta = json.dumps(meta)

        # === TIER 1: state change + action inside transaction ===
        old_autocommit = self.conn.autocommit
        self.conn.autocommit = False
        try:
            with self.conn.cursor() as cur:
                # Copy owner, readers/writers from latest version
                cur.execute(
                    """
                    SELECT owner, readers, writers FROM object_events
                    WHERE entity_id = %s ORDER BY version DESC LIMIT 1
                    """,
                    (obj._store_entity_id,),
                )
                prev = cur.fetchone()
                original_owner = prev[0] if prev else self.user
                readers = prev[1] if prev else []
                writers = prev[2] if prev else []

                cur.execute(
                    """
                    INSERT INTO object_events
                        (entity_id, version, type_name, owner, data, state, event_type,
                         event_meta, readers, writers, valid_from)
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s, 'STATE_CHANGE',
                            %s::jsonb, %s, %s, COALESCE(%s, now()))
                    RETURNING event_id, tx_time, valid_from
                    """,
                    (obj._store_entity_id, next_ver, type_name, original_owner,
                     json_data, new_state, event_meta, readers, writers, valid_from),
                )
                row = cur.fetchone()
                obj._store_version = next_ver
                obj._store_state = new_state
                obj._store_tx_time = row[1]
                obj._store_valid_from = row[2]
                obj._store_event_type = "STATE_CHANGE"

            # Tier 1: action runs inside transaction — atomic with state change
            if t.action is not None:
                t.action(obj, current_state, new_state)

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.conn.autocommit = old_autocommit

        # === TIER 2: fire-and-forget hooks (after commit) ===
        if t.on_exit is not None:
            try:
                t.on_exit(obj, current_state, new_state)
            except Exception:
                pass
        if t.on_enter is not None:
            try:
                t.on_enter(obj, current_state, new_state)
            except Exception:
                pass
        self._emit_event(obj)

        # === TIER 3: durable workflow (after commit) ===
        if t.start_workflow is not None:
            wf_engine = getattr(type(obj), '_workflow_engine', None)
            if wf_engine is None:
                raise RuntimeError(
                    f"{type(obj).__name__}._workflow_engine is not set but "
                    f"transition {current_state}→{new_state} has start_workflow="
                )
            wf_engine.workflow(t.start_workflow, obj._store_entity_id)

    def write_many(self, objects: list[Storable], valid_from: datetime | None = None) -> list[str]:
        """
        Write multiple new entities in a single transaction.
        Returns list of entity_ids.
        """
        old_autocommit = self.conn.autocommit
        self.conn.autocommit = False
        try:
            entity_ids = []
            for obj in objects:
                eid = self.write(obj, valid_from=valid_from)
                entity_ids.append(eid)
            self.conn.commit()
            return entity_ids
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.conn.autocommit = old_autocommit

    def update_many(self, objects: list[Storable], valid_from: datetime | None = None) -> None:
        """
        Update multiple entities in a single transaction.
        Optimistic concurrency is automatic on each entity.
        """
        old_autocommit = self.conn.autocommit
        self.conn.autocommit = False
        try:
            for obj in objects:
                self.update(obj, valid_from=valid_from)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.conn.autocommit = old_autocommit

    # ── Read operations ───────────────────────────────────────────────

    def read(self, cls: type[_S], entity_id: str) -> _S | None:
        """
        Read the latest non-deleted version of an entity.
        Returns None if not found, not visible, or deleted.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_id, entity_id, version, type_name, owner,
                       updated_by, readers, writers, data, state, event_type,
                       tx_time, valid_from, valid_to
                FROM object_events
                WHERE entity_id = %s
                ORDER BY version DESC
                LIMIT 1
                """,
                (entity_id,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            # If latest version is DELETED, entity is gone
            if row[10] == "DELETED":
                return None
            return self._row_to_object(cls, row)

    def query(self, cls: type[_S], filters: dict | None = None, limit: int = 100, cursor: Any = None) -> QueryResult[_S]:
        """
        Query current (latest non-deleted) entities of a given type.

        filters: dict of key-value pairs matched against the data column.
            e.g. {"symbol": "AAPL"} → data @> '{"symbol": "AAPL"}'

        Pagination: pass cursor=next_cursor from a previous QueryResult to
        get the next page. Returns a QueryResult with .items and .next_cursor.
        """
        type_name = cls.type_name()
        params: list[Any] = [type_name]

        sql = """
            SELECT DISTINCT ON (entity_id)
                   event_id, entity_id, version, type_name, owner,
                   updated_by, readers, writers, data, state, event_type,
                   tx_time, valid_from, valid_to
            FROM object_events
            WHERE type_name = %s
        """

        if filters:
            filter_json = json.dumps(filters, cls=_JSONEncoder)
            sql += " AND data @> %s::jsonb"
            params.append(filter_json)

        sql += " ORDER BY entity_id, version DESC"

        # Wrap to filter out DELETED, apply cursor + limit
        cursor_clause = ""
        if cursor is not None:
            cursor_clause = "AND tx_time < %s"
            params.append(cursor)

        wrapped = f"""
            SELECT * FROM ({sql}) sub
            WHERE event_type != 'DELETED'
            {cursor_clause}
            ORDER BY tx_time DESC
            LIMIT %s
        """
        params.append(limit)

        with self.conn.cursor() as cur:
            cur.execute(wrapped, params)
            rows = cur.fetchall()
            items = [self._row_to_object(cls, row) for row in rows]

        next_cursor = None
        if len(items) == limit:
            next_cursor = items[-1]._store_tx_time

        return QueryResult(items=items, next_cursor=next_cursor)

    def history(self, cls: type[_S], entity_id: str) -> list[_S]:
        """
        Return all versions of an entity, ordered by version ascending.
        Includes DELETED tombstones.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_id, entity_id, version, type_name, owner,
                       updated_by, readers, writers, data, state, event_type,
                       tx_time, valid_from, valid_to
                FROM object_events
                WHERE entity_id = %s
                ORDER BY version ASC
                """,
                (entity_id,),
            )
            rows = cur.fetchall()
            return [self._row_to_object(cls, row) for row in rows]

    def as_of(self, cls: type[_S], entity_id: str, tx_time: datetime | None = None, valid_time: datetime | None = None) -> _S | None:
        """
        Bi-temporal point-in-time query.

        - tx_time only: "what did we know at time T?"
        - valid_time only: "what was effective at business time T?"
        - both: "what did we know at T about business time T'?"
        """
        conditions = ["entity_id = %s"]
        params: list[Any] = [entity_id]

        if tx_time is not None:
            conditions.append("tx_time <= %s")
            params.append(tx_time)

        if valid_time is not None:
            conditions.append("valid_from <= %s")
            params.append(valid_time)

        where = " AND ".join(conditions)

        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT event_id, entity_id, version, type_name, owner,
                       updated_by, readers, writers, data, state, event_type,
                       tx_time, valid_from, valid_to
                FROM object_events
                WHERE {where}
                ORDER BY version DESC
                LIMIT 1
                """,
                params,
            )
            row = cur.fetchone()
            if row is None:
                return None
            return self._row_to_object(cls, row)

    def count(self, cls: type[Storable] | None = None) -> int:
        """Count current (latest non-deleted) entities visible to this user."""
        with self.conn.cursor() as cur:
            if cls:
                type_name = cls.type_name()
                cur.execute(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT DISTINCT ON (entity_id) entity_id, event_type
                        FROM object_events
                        WHERE type_name = %s
                        ORDER BY entity_id, version DESC
                    ) sub
                    WHERE event_type != 'DELETED'
                    """,
                    (type_name,),
                )
            else:
                cur.execute(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT DISTINCT ON (entity_id) entity_id, event_type
                        FROM object_events
                        ORDER BY entity_id, version DESC
                    ) sub
                    WHERE event_type != 'DELETED'
                    """
                )
            return int(cur.fetchone()[0])

    def audit(self, entity_id: str) -> list[dict[str, Any]]:
        """
        Return the full audit trail for an entity: who changed what, when.
        Returns list of AuditEntry dicts ordered by version.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT version, event_type, owner, updated_by, state,
                       event_meta, tx_time, valid_from
                FROM object_events
                WHERE entity_id = %s
                ORDER BY version ASC
                """,
                (entity_id,),
            )
            rows = cur.fetchall()
            return [
                {
                    "version": row[0],
                    "event_type": row[1],
                    "owner": row[2],
                    "updated_by": row[3],
                    "state": row[4],
                    "event_meta": row[5],
                    "tx_time": row[6],
                    "valid_from": row[7],
                }
                for row in rows
            ]

    def list_types(self) -> list[str]:
        """List distinct type_names visible to the current user."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT type_name FROM object_events ORDER BY type_name"
            )
            return [row[0] for row in cur.fetchall()]

    # ── Internal helpers ──────────────────────────────────────────────

    def _emit_event(self, obj: Storable) -> None:
        """Emit a ChangeEvent to the event bus (if wired)."""
        if self.event_bus is None:
            return
        if obj._store_entity_id is None or obj._store_version is None:
            return
        event = ChangeEvent(
            entity_id=obj._store_entity_id,
            version=obj._store_version,
            event_type=obj._store_event_type or "UNKNOWN",
            type_name=obj.type_name(),
            updated_by=self.user,
            state=obj._store_state,
            tx_time=obj._store_tx_time or datetime.now(timezone.utc),
        )
        self.event_bus.emit(event)

    def _next_version(self, entity_id: str) -> int:
        """Get the next version number for an entity."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(version), 0) + 1 FROM object_events WHERE entity_id = %s",
                (entity_id,),
            )
            return int(cur.fetchone()[0])

    def _row_to_object(self, cls: type[_S], row: tuple) -> _S:
        """Convert a database row to a typed Python object with bi-temporal metadata."""
        (_event_id, entity_id, version, _type_name, owner,
         updated_by, _readers, _writers, data, state, event_type,
         tx_time, valid_from, valid_to) = row

        # data is already a dict (psycopg2 auto-parses JSONB)
        if isinstance(data, str):
            data = json.loads(data, object_hook=_json_decoder_hook)
        else:
            data = json.loads(
                json.dumps(data, cls=_JSONEncoder),
                object_hook=_json_decoder_hook,
            )

        obj: _S = cls.from_json(json.dumps(data, cls=_JSONEncoder))  # type: ignore[assignment]  # from_json returns Self at runtime
        obj._store_entity_id = str(entity_id)
        obj._store_version = version
        obj._store_owner = owner
        obj._store_updated_by = updated_by
        obj._store_tx_time = tx_time
        obj._store_valid_from = valid_from
        obj._store_valid_to = valid_to
        obj._store_state = state
        obj._store_event_type = event_type
        return obj

    def close(self) -> None:
        """Close the database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()

    def __enter__(self) -> "StoreClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
