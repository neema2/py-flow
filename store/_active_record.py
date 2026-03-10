"""
Active Record mixin — persistence SQL + Active Record API for Storable.

All SQL logic for the bi-temporal event-sourced object store lives here.
Each method obtains the raw psycopg2 connection and event bus from the
active UserConnection via thread-local storage.

Storable inherits this mixin, keeping base.py free of persistence deps.
"""

from __future__ import annotations

import dataclasses
import json
import uuid
from datetime import datetime, timezone
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self
from typing import Any, ClassVar, TypeVar


from store.query_result import QueryResult


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


_S = TypeVar("_S", bound="ActiveRecordMixin")


class ActiveRecordMixin:
    """Mixin providing Active Record persistence and all underlying SQL.

    Uses the active UserConnection for the raw psycopg2 connection
    and event bus.  All persistence goes through Active Record methods.
    """

    # Declared by Storable — listed here for type-checker visibility
    _store_entity_id: str | None
    _store_version: int | None
    _store_owner: str | None
    _store_updated_by: str | None
    _store_tx_time: Any
    _store_valid_from: Any
    _store_valid_to: Any
    _store_state: str | None
    _store_event_type: str | None
    _state_machine: ClassVar[Any]  # type[StateMachine] | None

    # These must be implemented by Storable (or its subclasses)
    @classmethod
    def type_name(cls) -> str: raise NotImplementedError
    def to_json(self) -> str: raise NotImplementedError
    @classmethod
    def from_json(cls, json_str: str) -> Any: raise NotImplementedError

    # ── Connection helpers ────────────────────────────────────────────

    @staticmethod
    def _get_conn() -> Any:
        """Return the raw psycopg2 connection from the active UserConnection."""
        from store.connection import active_connection
        return active_connection().conn

    @staticmethod
    def _get_user() -> str:
        """Return the current user from the active UserConnection."""
        from store.connection import active_connection
        return active_connection().user

    @staticmethod
    def _get_event_bus() -> Any:
        """Return the event bus from the active UserConnection (may be None)."""
        from store.connection import active_connection
        return active_connection().event_bus

    # ── Internal SQL helpers ──────────────────────────────────────────

    @staticmethod
    def _emit_event(obj: ActiveRecordMixin) -> None:
        """Emit a ChangeEvent to the event bus (if wired)."""
        from store.connection import active_connection
        conn = active_connection()
        if conn.event_bus is None:
            return
        if obj._store_entity_id is None or obj._store_version is None:
            return
        from store.subscriptions import ChangeEvent
        event = ChangeEvent(
            entity_id=obj._store_entity_id,
            version=obj._store_version,
            event_type=obj._store_event_type or "UNKNOWN",
            type_name=obj.type_name(),
            updated_by=conn.user,
            state=obj._store_state,
            tx_time=obj._store_tx_time or datetime.now(timezone.utc),
        )
        conn.event_bus.emit(event)

    @staticmethod
    def _next_version(pg_conn: Any, entity_id: str) -> int:
        """Get the next version number for an entity."""
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(version), 0) + 1 FROM object_events WHERE entity_id = %s",
                (entity_id,),
            )
            return int(cur.fetchone()[0])

    @staticmethod
    def _row_to_object(cls: type[_S], row: tuple) -> _S:
        """Convert a database row to a typed Python object with bi-temporal metadata."""
        from store.base import _json_decoder_hook, _JSONEncoder

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

        obj: _S = cls.from_json(json.dumps(data, cls=_JSONEncoder))  # type: ignore[assignment]
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

    # ── SQL Write operations (all append-only) ────────────────────────

    @staticmethod
    def _sql_write(pg_conn: Any, user: str, obj: ActiveRecordMixin,
                   valid_from: datetime | None = None) -> str:
        """Create a new entity (version 1). Returns the entity_id."""
        from store.base import Embedded
        if isinstance(obj, Embedded):
            raise TypeError(
                f"{type(obj).__name__} is Embedded — write the parent Storable instead"
            )
        entity_id = obj._store_entity_id or str(uuid.uuid4())
        json_data = obj.to_json()
        type_name = obj.type_name()
        state = None
        if obj._state_machine is not None:
            state = obj._state_machine.initial

        with pg_conn.cursor() as cur:
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
            ActiveRecordMixin._emit_event(obj)
            return obj._store_entity_id

    @staticmethod
    def _sql_update(pg_conn: Any, user: str, obj: ActiveRecordMixin,
                    valid_from: datetime | None = None) -> None:
        """Create a new version of an existing entity (never overwrites)."""
        if not obj._store_entity_id:
            raise ValueError("Object has no entity_id — save() it first")

        next_ver = ActiveRecordMixin._next_version(pg_conn, obj._store_entity_id)

        # Automatic optimistic concurrency
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

        state = obj._store_state

        with pg_conn.cursor() as cur:
            # Copy owner, readers/writers from latest version
            cur.execute(
                """
                SELECT owner, readers, writers FROM object_events
                WHERE entity_id = %s ORDER BY version DESC LIMIT 1
                """,
                (obj._store_entity_id,),
            )
            prev = cur.fetchone()
            original_owner = prev[0] if prev else user
            readers = prev[1] if prev else []
            writers = prev[2] if prev else []

            # Only the owner or a writer can create new versions
            if user != original_owner and user not in writers:
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
            ActiveRecordMixin._emit_event(obj)

    @staticmethod
    def _sql_delete(pg_conn: Any, user: str, obj: ActiveRecordMixin) -> bool:
        """Soft-delete: creates a DELETED tombstone event."""
        if not obj._store_entity_id:
            raise ValueError("Object has no entity_id — save() it first")

        next_ver = ActiveRecordMixin._next_version(pg_conn, obj._store_entity_id)

        # Automatic optimistic concurrency
        if obj._store_version is not None:
            actual = next_ver - 1
            if actual != obj._store_version:
                raise VersionConflict(obj._store_entity_id, obj._store_version, actual)
        type_name = obj.type_name()
        json_data = obj.to_json()

        with pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT owner FROM object_events
                WHERE entity_id = %s ORDER BY version DESC LIMIT 1
                """,
                (obj._store_entity_id,),
            )
            prev = cur.fetchone()
            original_owner = prev[0] if prev else user

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
            ActiveRecordMixin._emit_event(obj)
            return row[0] is not None

    @staticmethod
    def _sql_transition(pg_conn: Any, user: str, obj: ActiveRecordMixin,
                        new_state: str, valid_from: datetime | None = None) -> None:
        """Transition an entity to a new lifecycle state."""
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
            current_state, new_state, context=context, user=user, obj=obj
        )

        assert obj._store_entity_id is not None
        next_ver = ActiveRecordMixin._next_version(pg_conn, obj._store_entity_id)
        json_data = obj.to_json()
        type_name = obj.type_name()

        # Richer event_meta for audit
        meta: dict[str, object] = {
            "from_state": current_state,
            "to_state": new_state,
            "triggered_by": user,
        }
        if t.guard is not None:
            meta["guard"] = str(t.guard)
        if t.allowed_by is not None:
            meta["allowed_by"] = list(t.allowed_by)
        event_meta = json.dumps(meta)

        # === TIER 1: state change + action inside transaction ===
        old_autocommit = pg_conn.autocommit
        pg_conn.autocommit = False
        try:
            with pg_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT owner, readers, writers FROM object_events
                    WHERE entity_id = %s ORDER BY version DESC LIMIT 1
                    """,
                    (obj._store_entity_id,),
                )
                prev = cur.fetchone()
                original_owner = prev[0] if prev else user
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

            pg_conn.commit()
        except Exception:
            pg_conn.rollback()
            raise
        finally:
            pg_conn.autocommit = old_autocommit

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
        ActiveRecordMixin._emit_event(obj)

        # === TIER 3: durable workflow (after commit) ===
        if t.start_workflow is not None:
            wf_engine = getattr(type(obj), '_workflow_engine', None)
            if wf_engine is None:
                raise RuntimeError(
                    f"{type(obj).__name__}._workflow_engine is not set but "
                    f"transition {current_state}→{new_state} has start_workflow="
                )
            wf_engine.workflow(t.start_workflow, obj._store_entity_id)

    @staticmethod
    def _sql_write_many(pg_conn: Any, user: str, objects: list[ActiveRecordMixin],
                        valid_from: datetime | None = None) -> list[str]:
        """Write multiple new entities in a single transaction."""
        old_autocommit = pg_conn.autocommit
        pg_conn.autocommit = False
        try:
            entity_ids = []
            for obj in objects:
                eid = ActiveRecordMixin._sql_write(pg_conn, user, obj, valid_from=valid_from)
                entity_ids.append(eid)
            pg_conn.commit()
            return entity_ids
        except Exception:
            pg_conn.rollback()
            raise
        finally:
            pg_conn.autocommit = old_autocommit

    @staticmethod
    def _sql_update_many(pg_conn: Any, user: str, objects: list[ActiveRecordMixin],
                         valid_from: datetime | None = None) -> None:
        """Update multiple entities in a single transaction."""
        old_autocommit = pg_conn.autocommit
        pg_conn.autocommit = False
        try:
            for obj in objects:
                ActiveRecordMixin._sql_update(pg_conn, user, obj, valid_from=valid_from)
            pg_conn.commit()
        except Exception:
            pg_conn.rollback()
            raise
        finally:
            pg_conn.autocommit = old_autocommit

    # ── SQL Read operations ───────────────────────────────────────────

    @staticmethod
    def _sql_read(pg_conn: Any, cls: type[_S], entity_id: str) -> _S | None:
        """Read the latest non-deleted version of an entity."""
        with pg_conn.cursor() as cur:
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
            if row[10] == "DELETED":
                return None
            return ActiveRecordMixin._row_to_object(cls, row)

    @staticmethod
    def _sql_query(pg_conn: Any, cls: type[_S], filters: dict | None = None,
                   limit: int = 100, cursor: Any = None) -> QueryResult[_S]:
        """Query current (latest non-deleted) entities of a given type."""
        from store.base import _JSONEncoder

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

        with pg_conn.cursor() as cur:
            cur.execute(wrapped, params)
            rows = cur.fetchall()
            items = [ActiveRecordMixin._row_to_object(cls, row) for row in rows]

        next_cursor = None
        if len(items) == limit:
            next_cursor = items[-1]._store_tx_time

        return QueryResult(items=items, next_cursor=next_cursor)

    @staticmethod
    def _sql_history(pg_conn: Any, cls: type[_S], entity_id: str) -> list[_S]:
        """Return all versions of an entity, ordered by version ascending."""
        with pg_conn.cursor() as cur:
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
            return [ActiveRecordMixin._row_to_object(cls, row) for row in rows]

    @staticmethod
    def _sql_as_of(pg_conn: Any, cls: type[_S], entity_id: str,
                   tx_time: datetime | None = None,
                   valid_time: datetime | None = None) -> _S | None:
        """Bi-temporal point-in-time query."""
        conditions = ["entity_id = %s"]
        params: list[Any] = [entity_id]

        if tx_time is not None:
            conditions.append("tx_time <= %s")
            params.append(tx_time)

        if valid_time is not None:
            conditions.append("valid_from <= %s")
            params.append(valid_time)

        where = " AND ".join(conditions)

        with pg_conn.cursor() as cur:
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
            return ActiveRecordMixin._row_to_object(cls, row)

    @staticmethod
    def _sql_count(pg_conn: Any, cls: type[ActiveRecordMixin] | None = None) -> int:
        """Count current (latest non-deleted) entities visible to this user."""
        with pg_conn.cursor() as cur:
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

    @staticmethod
    def _sql_audit(pg_conn: Any, entity_id: str) -> list[dict[str, Any]]:
        """Return the full audit trail for an entity."""
        with pg_conn.cursor() as cur:
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

    @staticmethod
    def _sql_list_types(pg_conn: Any) -> list[str]:
        """List distinct type_names visible to the current user."""
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT type_name FROM object_events ORDER BY type_name"
            )
            return [row[0] for row in cur.fetchall()]

    # ── Active Record API ─────────────────────────────────────────────
    # These are the public methods that users call on Storable instances.

    def save(self, valid_from: datetime | None = None) -> str:
        """Persist this object: create if new, update if existing.

        Returns entity_id on first save.
        """
        pg = self._get_conn()
        user = self._get_user()
        if self._store_entity_id is None:
            return self._sql_write(pg, user, self, valid_from=valid_from)
        else:
            self._sql_update(pg, user, self, valid_from=valid_from)
            return self._store_entity_id

    def delete(self) -> bool:
        """Soft-delete this object (DELETED tombstone)."""
        return self._sql_delete(self._get_conn(), self._get_user(), self)

    def transition(self, new_state: str, valid_from: datetime | None = None) -> None:
        """Transition to a new lifecycle state."""
        self._sql_transition(self._get_conn(), self._get_user(), self, new_state, valid_from=valid_from)

    def refresh(self) -> None:
        """Reload this object's data from the store (latest version)."""
        if not self._store_entity_id:
            raise ValueError("Object has no entity_id — save() it first")
        fresh = self._sql_read(self._get_conn(), type(self), self._store_entity_id)
        if fresh is None:
            raise ValueError(
                f"Entity {self._store_entity_id} not found or deleted"
            )
        # Copy all data fields — use setattr so reactive Signals update
        if dataclasses.is_dataclass(self):
            for f in dataclasses.fields(self):  # type: ignore[unreachable]
                setattr(self, f.name, getattr(fresh, f.name))
        # Copy store metadata
        for attr in ('_store_entity_id', '_store_version', '_store_owner',
                     '_store_updated_by', '_store_tx_time', '_store_valid_from',
                     '_store_valid_to', '_store_state', '_store_event_type'):
            object.__setattr__(self, attr, getattr(fresh, attr))

    def history(self) -> list:
        """Return all versions of this entity."""
        if not self._store_entity_id:
            raise ValueError("Object has no entity_id — save() it first")
        return self._sql_history(self._get_conn(), type(self), self._store_entity_id)

    def audit(self) -> list[dict]:
        """Return the full audit trail for this entity."""
        if not self._store_entity_id:
            raise ValueError("Object has no entity_id — save() it first")
        return self._sql_audit(self._get_conn(), self._store_entity_id)

    def as_of(self, *, tx_time: datetime | None = None,
              valid_time: datetime | None = None) -> Self | None:
        """Return this entity at a different point in time.

        - tx_time only: "what did we know at time T?"
        - valid_time only: "what was effective at business time T?"
        - both: "what did we know at T about business time T'?"
        """
        if not self._store_entity_id:
            raise ValueError("Object has no entity_id — save() it first")
        return self._sql_as_of(self._get_conn(), type(self), self._store_entity_id, tx_time=tx_time, valid_time=valid_time)  # type: ignore[return-value]

    def share(self, user: str, mode: str = "read") -> bool:
        """Grant access to another user. mode='read' or 'write'."""
        if not self._store_entity_id:
            raise ValueError("Object has no entity_id — save() it first")
        from store.permissions import share_read, share_write
        conn = self._get_conn()
        if mode == "write":
            return share_write(conn, self._store_entity_id, user)
        return share_read(conn, self._store_entity_id, user)

    def unshare(self, user: str, mode: str = "read") -> bool:
        """Revoke access from another user. mode='read' or 'write'."""
        if not self._store_entity_id:
            raise ValueError("Object has no entity_id — save() it first")
        from store.permissions import unshare_read, unshare_write
        conn = self._get_conn()
        if mode == "write":
            return unshare_write(conn, self._store_entity_id, user)
        return unshare_read(conn, self._store_entity_id, user)

    @classmethod
    def find(cls, entity_id: str | None) -> Self | None:
        """Read the latest non-deleted version of an entity by ID."""
        if entity_id is None:
            return None
        return cls._sql_read(cls._get_conn(), cls, entity_id)  # type: ignore[return-value]

    @classmethod
    def get(cls, entity_id: str | None) -> Self:
        """Read an entity by ID, raising KeyError if not found."""
        if entity_id is None:
            raise KeyError(f"{cls.__name__}: entity_id is None")
        obj = cls.find(entity_id)
        if obj is None:
            raise KeyError(f"{cls.__name__} with entity_id={entity_id!r} not found")
        return obj

    @classmethod
    def query(cls, filters: dict | None = None, limit: int = 100,
              cursor: Any = None) -> QueryResult[Self]:
        """Query current entities of this type with optional filters."""
        return cls._sql_query(cls._get_conn(), cls, filters=filters, limit=limit, cursor=cursor)  # type: ignore[return-value]

    @classmethod
    def count(cls) -> int:
        """Count current (latest non-deleted) entities of this type."""
        return cls._sql_count(cls._get_conn(), cls)

    @classmethod
    def write_many(cls, objects: list[Self],
                   valid_from: datetime | None = None) -> list[str]:
        """Write multiple new entities in a single transaction."""
        return cls._sql_write_many(cls._get_conn(), cls._get_user(), objects, valid_from=valid_from)  # type: ignore[arg-type]

    @classmethod
    def update_many(cls, objects: list[Self],
                    valid_from: datetime | None = None) -> None:
        """Update multiple entities in a single transaction."""
        cls._sql_update_many(cls._get_conn(), cls._get_user(), objects, valid_from=valid_from)  # type: ignore[arg-type]

    @classmethod
    def history_of(cls, entity_id: str) -> list[Self]:
        """Return all versions of an entity by ID."""
        return cls._sql_history(cls._get_conn(), cls, entity_id)  # type: ignore[return-value]

    @classmethod
    def as_of_entity(cls, entity_id: str, *, tx_time: datetime | None = None,
                     valid_time: datetime | None = None) -> Self | None:
        """Bi-temporal point-in-time query by entity ID."""
        return cls._sql_as_of(cls._get_conn(), cls, entity_id, tx_time=tx_time, valid_time=valid_time)  # type: ignore[return-value]

    @classmethod
    def audit_trail(cls, entity_id: str) -> list[dict[str, Any]]:
        """Return the full audit trail for an entity by ID."""
        return cls._sql_audit(cls._get_conn(), entity_id)

    @classmethod
    def list_types(cls) -> list[str]:
        """List distinct type_names visible to the current user."""
        return cls._sql_list_types(cls._get_conn())

