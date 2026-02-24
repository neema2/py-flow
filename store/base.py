"""
Storable base class — defines how Python objects serialize to/from JSONB.
Subclass with @dataclass to create persistable types.

Bi-temporal metadata:
- entity_id: stable identity across versions
- version: monotonic per entity
- tx_time: when this version was recorded (system, immutable)
- valid_from: when this version is effective (user, defaults to now)
- valid_to: when this version stops being effective
- state: lifecycle state (if a state machine is registered)
- event_type: CREATED, UPDATED, DELETED, STATE_CHANGE, CORRECTED
"""

import json
import uuid
import asyncio
import logging
import dataclasses
from datetime import datetime, date
from decimal import Decimal
from typing import Optional

from reaktiv import Signal, Computed, Effect, batch

logger = logging.getLogger(__name__)


class _JSONEncoder(json.JSONEncoder):
    """Handles datetime, date, Decimal, UUID, and dataclass serialization."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return {"__type__": "datetime", "value": obj.isoformat()}
        if isinstance(obj, date):
            return {"__type__": "date", "value": obj.isoformat()}
        if isinstance(obj, Decimal):
            return {"__type__": "Decimal", "value": str(obj)}
        if isinstance(obj, uuid.UUID):
            return {"__type__": "UUID", "value": str(obj)}
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            return dataclasses.asdict(obj)
        return super().default(obj)


def _json_decoder_hook(d):
    """Reconstruct special types from JSONB."""
    if "__type__" in d:
        t = d["__type__"]
        v = d["value"]
        if t == "datetime":
            return datetime.fromisoformat(v)
        if t == "date":
            return date.fromisoformat(v)
        if t == "Decimal":
            return Decimal(v)
        if t == "UUID":
            return uuid.UUID(v)
    return d


class Storable:
    """
    Base class for objects stored in the bi-temporal event-sourced object store.

    Subclass as a dataclass:

        @dataclass
        class Trade(Storable):
            symbol: str
            quantity: int
            price: float
            side: str

    Then use StoreClient to persist:

        client.write(Trade(symbol="AAPL", quantity=100, price=228.0, side="BUY"))

    Every write/update creates an immutable event with bi-temporal timestamps.
    """

    # Bi-temporal metadata — set by the store after writing / reading
    _store_entity_id: Optional[str] = None
    _store_version: Optional[int] = None
    _store_owner: Optional[str] = None
    _store_updated_by: Optional[str] = None
    _store_tx_time: Optional[datetime] = None
    _store_valid_from: Optional[datetime] = None
    _store_valid_to: Optional[datetime] = None
    _store_state: Optional[str] = None
    _store_event_type: Optional[str] = None

    # Optional state machine — set on the class by the user
    _state_machine = None

    # Optional workflow engine — enables start_workflow= on Transitions
    _workflow_engine = None

    # Column registry — mandatory enforcement for all subclasses
    _registry = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls._registry is not None:
            # __init_subclass__ fires BEFORE @dataclass, so we use
            # __annotations__ (not dataclasses.fields) to check columns.
            own_annotations = {
                k: v for k, v in getattr(cls, '__annotations__', {}).items()
                if not k.startswith('_')
            }
            if own_annotations:
                cls._registry.validate_class(cls)

    # ── Reactive wiring ────────────────────────────────────────────

    def __post_init__(self):
        """Auto-create Signals for fields, Computed for @computed, Effects for @effect."""
        from reactive.computed import ComputedProperty, EffectMethod, _ReactiveProxy

        # _signals: field_name → Signal
        signals = {}
        if dataclasses.is_dataclass(self):
            for f in dataclasses.fields(self):
                if f.name.startswith('_'):
                    continue
                signals[f.name] = Signal(getattr(self, f.name))

        object.__setattr__(self, '_signals', signals)
        object.__setattr__(self, '_computeds', {})
        object.__setattr__(self, '_effects', {})
        object.__setattr__(self, '_reactive_ready', False)

        # Discover @computed descriptors on the class
        computeds = {}
        for name in dir(type(self)):
            attr = getattr(type(self), name, None)
            if isinstance(attr, ComputedProperty):
                cp = attr
                if cp.expr is not None:
                    # Single-entity: evaluate Expr against signal values
                    def _make_single(expression, sigs):
                        def compute():
                            ctx = {k: sig() for k, sig in sigs.items()}
                            return expression.eval(ctx)
                        return compute
                    comp = Computed(_make_single(cp.expr, signals))
                else:
                    # Cross-entity: call original function with reactive proxy
                    def _make_cross(func, obj):
                        proxy = _ReactiveProxy(obj)
                        def compute():
                            return func(proxy)
                        return compute
                    comp = Computed(_make_cross(cp.fn, self))
                computeds[name] = comp

        object.__setattr__(self, '_computeds', computeds)

        # Discover @effect descriptors on the class
        effects = {}
        for name in dir(type(self)):
            attr = getattr(type(self), name, None)
            if isinstance(attr, EffectMethod):
                em = attr
                target = em.target_computed
                if target not in computeds:
                    raise ValueError(
                        f"@effect '{em.name}' watches '{target}' but no "
                        f"@computed '{target}' exists on {type(self).__name__}"
                    )
                comp_signal = computeds[target]
                bound_fn = em.fn.__get__(self, type(self))

                def _make_effect(callback, comp_sig):
                    def effect_fn():
                        value = comp_sig()
                        try:
                            callback(value)
                        except Exception:
                            logger.exception(
                                f"@effect {callback.__name__} raised"
                            )
                    return effect_fn

                eff = Effect(_make_effect(bound_fn, comp_signal))
                effects[name] = eff

        object.__setattr__(self, '_effects', effects)
        object.__setattr__(self, '_reactive_ready', True)

        # Tick effects once to register dependencies
        self._tick()

    def __setattr__(self, name, value):
        """Intercept field sets to update the corresponding Signal."""
        object.__setattr__(self, name, value)
        # Only cascade after reactive wiring is complete
        try:
            ready = object.__getattribute__(self, '_reactive_ready')
        except AttributeError:
            ready = False
        if ready:
            signals = object.__getattribute__(self, '_signals')
            if name in signals:
                signals[name].set(value)
                self._tick()

    def batch_update(self, **kwargs):
        """Update multiple fields with a single recomputation.

        Usage:
            pos.batch_update(current_price=235.0, quantity=150)
        """
        signals = object.__getattribute__(self, '_signals')
        with batch():
            for name, value in kwargs.items():
                object.__setattr__(self, name, value)
                if name in signals:
                    signals[name].set(value)
        self._tick()

    def _tick(self):
        """Process pending effects by running the event loop briefly."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(asyncio.sleep(0))

    def to_json(self) -> str:
        """Serialize this object to a JSON string for JSONB storage."""
        if dataclasses.is_dataclass(self):
            data = dataclasses.asdict(self)
        else:
            data = {
                k: v for k, v in self.__dict__.items()
                if not k.startswith("_")
            }
        return json.dumps(data, cls=_JSONEncoder)

    @classmethod
    def from_json(cls, json_str: str) -> "Storable":
        """Deserialize from a JSON string back to a typed object."""
        data = json.loads(json_str, object_hook=_json_decoder_hook)
        if dataclasses.is_dataclass(cls):
            # Filter to only fields the dataclass expects
            field_names = {f.name for f in dataclasses.fields(cls)}
            filtered = {k: v for k, v in data.items() if k in field_names}
            return cls(**filtered)
        else:
            obj = cls.__new__(cls)
            obj.__dict__.update(data)
            return obj

    @classmethod
    def type_name(cls) -> str:
        """The type identifier stored in the database."""
        return f"{cls.__module__}.{cls.__qualname__}"

    # ── Active Record API ─────────────────────────────────────────────

    @staticmethod
    def _get_client():
        """Return the StoreClient from the active UserConnection."""
        from store.connection import get_connection
        return get_connection()._client

    @staticmethod
    def _get_conn():
        """Return the raw psycopg2 connection from the active UserConnection."""
        from store.connection import get_connection
        return get_connection().conn

    def save(self, valid_from=None):
        """Persist this object: create if new, update if existing.

        Returns entity_id on first save.
        """
        client = self._get_client()
        if self._store_entity_id is None:
            return client.write(self, valid_from=valid_from)
        else:
            client.update(self, valid_from=valid_from)
            return self._store_entity_id

    def delete(self):
        """Soft-delete this object (DELETED tombstone)."""
        client = self._get_client()
        return client.delete(self)

    def transition(self, new_state, valid_from=None):
        """Transition to a new lifecycle state."""
        client = self._get_client()
        client.transition(self, new_state, valid_from=valid_from)

    def refresh(self):
        """Reload this object's data from the store (latest version)."""
        if not self._store_entity_id:
            raise ValueError("Object has no entity_id — save() it first")
        client = self._get_client()
        fresh = client.read(type(self), self._store_entity_id)
        if fresh is None:
            raise ValueError(
                f"Entity {self._store_entity_id} not found or deleted"
            )
        # Copy all data fields
        if dataclasses.is_dataclass(self):
            for f in dataclasses.fields(self):
                object.__setattr__(self, f.name, getattr(fresh, f.name))
        # Copy store metadata
        for attr in ('_store_entity_id', '_store_version', '_store_owner',
                     '_store_updated_by', '_store_tx_time', '_store_valid_from',
                     '_store_valid_to', '_store_state', '_store_event_type'):
            object.__setattr__(self, attr, getattr(fresh, attr))

    def history(self):
        """Return all versions of this entity."""
        if not self._store_entity_id:
            raise ValueError("Object has no entity_id — save() it first")
        client = self._get_client()
        return client.history(type(self), self._store_entity_id)

    def audit(self):
        """Return the full audit trail for this entity."""
        if not self._store_entity_id:
            raise ValueError("Object has no entity_id — save() it first")
        client = self._get_client()
        return client.audit(self._store_entity_id)

    def share(self, user, mode="read"):
        """Grant access to another user. mode='read' or 'write'."""
        if not self._store_entity_id:
            raise ValueError("Object has no entity_id — save() it first")
        from store.permissions import share_read, share_write
        conn = self._get_conn()
        if mode == "write":
            return share_write(conn, self._store_entity_id, user)
        return share_read(conn, self._store_entity_id, user)

    def unshare(self, user, mode="read"):
        """Revoke access from another user. mode='read' or 'write'."""
        if not self._store_entity_id:
            raise ValueError("Object has no entity_id — save() it first")
        from store.permissions import unshare_read, unshare_write
        conn = self._get_conn()
        if mode == "write":
            return unshare_write(conn, self._store_entity_id, user)
        return unshare_read(conn, self._store_entity_id, user)

    @classmethod
    def find(cls, entity_id):
        """Read the latest non-deleted version of an entity by ID."""
        client = cls._get_client()
        return client.read(cls, entity_id)

    @classmethod
    def query(cls, filters=None, limit=100, cursor=None):
        """Query current entities of this type with optional filters."""
        client = cls._get_client()
        return client.query(cls, filters=filters, limit=limit, cursor=cursor)

    @classmethod
    def count(cls):
        """Count current (latest non-deleted) entities of this type."""
        client = cls._get_client()
        return client.count(cls)

    @classmethod
    def as_of(cls, entity_id, *, tx_time=None, valid_time=None):
        """Bi-temporal point-in-time query."""
        client = cls._get_client()
        return client.as_of(cls, entity_id, tx_time=tx_time, valid_time=valid_time)


# ── Wire mandatory column registry (no circular import — columns/ does not import base) ──
from store.columns import REGISTRY  # noqa: E402
Storable._registry = REGISTRY
