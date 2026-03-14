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

import asyncio
import dataclasses
import json
import logging
import uuid
from collections import namedtuple
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal
from typing import Any, ClassVar

from reaktiv import Computed, Effect, Signal, batch
from reaktiv.signal import ComputeSignal as _ComputeSignal
from workflow.engine import WorkflowEngine

from store._active_record import ActiveRecordMixin
from store.registry import ColumnRegistry
from store.state_machine import StateMachine

logger = logging.getLogger(__name__)

# Reactive node: one per field and one per @computed
_RNode = namedtuple('_RNode', ['read', 'write'])
_UNSET = object()


class _JSONEncoder(json.JSONEncoder):
    """Handles datetime, date, Decimal, UUID, and dataclass serialization."""

    def default(self, obj: object) -> Any:
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


def _json_decoder_hook(d: dict) -> Any:
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


class Storable(ActiveRecordMixin):
    """
    Base class for objects stored in the bi-temporal event-sourced object store.

    Subclass as a dataclass:

        @dataclass
        class Trade(Storable):
            symbol: str
            quantity: int
            price: float
            side: str

    Then persist with Active Record methods:

        trade = Trade(symbol="AAPL", quantity=100, price=228.0, side="BUY")
        trade.save()

    Every save/update creates an immutable event with bi-temporal timestamps.
    """

    # Bi-temporal metadata — set by the store after writing / reading
    _store_entity_id: str | None = None
    _store_version: int | None = None
    _store_owner: str | None = None
    _store_updated_by: str | None = None
    _store_tx_time: datetime | None = None
    _store_valid_from: datetime | None = None
    _store_valid_to: datetime | None = None
    _store_state: str | None = None
    _store_event_type: str | None = None

    # ── Public read-only accessors for store metadata ──────────────────
    @property
    def entity_id(self) -> str | None:
        """Stable identity across versions."""
        return self._store_entity_id

    @property
    def version(self) -> int | None:
        """Monotonic version number per entity."""
        return self._store_version

    @property
    def owner(self) -> str | None:
        """User who created this entity."""
        return self._store_owner

    @property
    def updated_by(self) -> str | None:
        """User who last updated this entity."""
        return self._store_updated_by

    @property
    def tx_time(self) -> datetime | None:
        """Transaction timestamp — when the write was committed."""
        return self._store_tx_time

    @property
    def valid_from(self) -> datetime | None:
        """Business-time start of this version's validity."""
        return self._store_valid_from

    @property
    def valid_to(self) -> datetime | None:
        """Business-time end of this version's validity."""
        return self._store_valid_to

    @property
    def state(self) -> str | None:
        """Lifecycle state (if a state machine is registered)."""
        return self._store_state

    @property
    def event_type(self) -> str | None:
        """Event type of the last write (INSERT, UPDATE, DELETE, etc.)."""
        return self._store_event_type

    # Optional state machine — set on the class by the user
    _state_machine: ClassVar[type[StateMachine] | None] = None

    # Optional workflow engine — enables start_workflow= on Transitions
    _workflow_engine: ClassVar[WorkflowEngine | None] = None

    # Column registry — mandatory enforcement for all subclasses
    _registry: ClassVar[ColumnRegistry | None] = None

    # Reactive internals — class-level defaults, overwritten per-instance
    _reactive: ClassVar[dict] = {}      # name → _RNode(read, write)
    _effects: ClassVar[list] = []       # Effect objects (prevent GC)

    def __init_subclass__(cls, **kwargs: Any) -> None:
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

    def __post_init__(self) -> None:
        """Auto-create Signals for fields, Computed for @computed, Effects for @effect."""
        from reactive.computed import ComputedProperty, EffectMethod, _ReactiveProxy

        reactive = {}

        # 1. Fields → Signal + _RNode
        # Storable itself isn't @dataclass but all subclasses are — safe at runtime
        for f in dataclasses.fields(self):  # type: ignore[arg-type]
            if f.name.startswith('_'):
                continue
            sig = Signal(getattr(self, f.name))
            reactive[f.name] = _RNode(read=sig, write=sig.set)

        # 2. Computeds → Computed (with override) + _RNode
        signals = {name: node.read for name, node in reactive.items()}
        for name in dir(type(self)):
            attr = getattr(type(self), name, None)
            if isinstance(attr, ComputedProperty):
                cp = attr
                override_sig = Signal(_UNSET)

                if cp.expr is not None:
                    # Single-entity: evaluate Expr against signal values
                    def _make_single(expression: Any, sigs: dict, ov_sig: Signal) -> Callable[[], Any]:
                        def compute() -> Any:
                            ov = ov_sig()
                            if ov is not _UNSET:
                                return ov
                            ctx = {k: sig() for k, sig in sigs.items()}
                            return expression.eval(ctx)
                        return compute
                    comp = Computed(_make_single(cp.expr, signals, override_sig))
                else:
                    # Cross-entity: call original function with reactive proxy
                    def _make_cross(func: Callable[..., Any], obj: Any, ov_sig: Signal) -> Callable[[], Any]:
                        def compute() -> Any:
                            ov = ov_sig()
                            if ov is not _UNSET:
                                return ov
                            return func(obj)
                        return compute
                    comp = Computed(_make_cross(cp.fn, self, override_sig))

                reactive[name] = _RNode(read=comp, write=override_sig.set)

        object.__setattr__(self, '_reactive', reactive)

        # 3. Effects
        effects = []
        for name in dir(type(self)):
            attr = getattr(type(self), name, None)
            if isinstance(attr, EffectMethod):
                em = attr
                target_node = reactive.get(em.target_computed)
                if target_node is None:
                    raise ValueError(
                        f"@effect '{em.name}' watches '{em.target_computed}' "
                        f"but no @computed exists on {type(self).__name__}"
                    )
                bound_fn = em.fn.__get__(self, type(self))

                def _make_effect(callback: Callable[..., Any], comp: Computed) -> Callable[[], None]:
                    def effect_fn() -> None:
                        value = comp()
                        try:
                            callback(value)
                        except Exception:
                            logger.exception(
                                f"@effect {callback.__name__} raised"
                            )
                    return effect_fn

                eff = Effect(_make_effect(bound_fn, target_node.read))
                effects.append(eff)

        object.__setattr__(self, '_effects', effects)

        # Tick effects once to register dependencies
        self._tick()

    def __getattribute__(self, name: str) -> Any:
        """Route reactive field/computed reads through Signals/Computeds."""
        # Avoid recursion on _reactive
        if name == "_reactive" or name.startswith("__"):
            return object.__getattribute__(self, name)
            
        reactive = object.__getattribute__(self, '_reactive')
        node = reactive.get(name)
        if node is not None:
            val = node.read()
            return val
        return object.__getattribute__(self, name)

    def __setattr__(self, name: str, value: object) -> None:
        """Intercept field sets to update Signals; computed sets to override."""
        object.__setattr__(self, name, value)
        node = object.__getattribute__(self, '_reactive').get(name)
        if node is not None:
            node.write(value)
            self._tick()

    def batch_update(self, **kwargs: Any) -> None:
        """Update multiple fields with a single recomputation.

        Usage:
            pos.batch_update(current_price=235.0, quantity=150)
        """
        reactive = object.__getattribute__(self, '_reactive')
        with batch():
            for name, value in kwargs.items():
                object.__setattr__(self, name, value)
                node = reactive.get(name)
                if node is not None:
                    node.write(value)
        self._tick()

    def clear_override(self, name: str) -> None:
        """Remove computed override, revert to formula. Ripples downstream."""
        node = object.__getattribute__(self, '_reactive').get(name)
        if node is None or not isinstance(node.read, _ComputeSignal):
            raise ValueError(f"'{name}' is not a @computed")
        node.write(_UNSET)
        self._tick()

    def _tick(self) -> None:
        """Process pending effects by running the event loop briefly."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        if not loop.is_running():
            loop.run_until_complete(asyncio.sleep(0))

    def to_json(self) -> str:
        """Serialize this object to a JSON string for JSONB storage."""
        if dataclasses.is_dataclass(self):
            data = dataclasses.asdict(self)  # type: ignore[unreachable]
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
            field_names = {f.name for f in dataclasses.fields(cls)}  # type: ignore[unreachable]
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




class Embedded(Storable):
    """A Storable that lives inside another Storable, not persisted independently.

    Gets full reactive wiring (Signals, @computed, @effect) via __post_init__
    but is never written to PG as its own entity.
    Serialized as part of the parent's JSONB via dataclasses.asdict().

    Usage::

        @dataclass
        class TaskDef(Embedded):
            name: str = ""
            target_fn: str = ""
            enabled: bool = True

        @dataclass
        class DAG(Storable):
            tasks: list = field(default_factory=list)  # list of TaskDef
    """
    # Opt out of column registry validation — Embedded types are internal
    _registry = None


# ── Wire mandatory column registry (no circular import — columns/ does not import base) ──
from store.columns import REGISTRY  # noqa: E402

Storable._registry = REGISTRY
