"""
Column Registry — enforced schema catalog.

Every column that can appear on any Storable must first be defined here.
Enforcement happens at class-definition time via Storable.__init_subclass__.

ColumnDef captures:
  A. Core type (name, python_type, nullable, default)
  B. Constraints (enum, min/max, max_length, pattern)
  C. Semantic / AI (description, synonyms, sample_values, semantic_type)
  D. Analytics / OLAP (role, aggregation, unit)
  E. Display / UI (display_name, format, category)
  F. Governance (sensitivity, deprecated, tags)
  G. Cross-layer hints (legend_type, dh_type_override)
"""

import dataclasses
import re
from typing import Any

# Sentinel for "no default provided"
_MISSING = object()


class RegistryError(Exception):
    """Raised when a column definition or class validation fails."""


@dataclasses.dataclass
class ColumnDef:
    """Canonical definition of a single column in the schema catalog."""

    # ── A. Core Type ──────────────────────────────────────────────
    name: str
    python_type: type
    nullable: bool = False
    default: Any = _MISSING

    # ── B. Constraints ────────────────────────────────────────────
    enum: list | None = None
    min_value: float | None = None
    max_value: float | None = None
    max_length: int | None = None
    pattern: str | None = None

    # ── C. Semantic / AI ──────────────────────────────────────────
    description: str = ""
    synonyms: list | None = None
    sample_values: list | None = None
    semantic_type: str | None = None

    # ── D. Analytics / OLAP ───────────────────────────────────────
    role: str = ""              # "dimension", "measure", or "attribute"
    aggregation: str | None = None  # sum, avg, last, min, max, count, weighted_avg
    unit: str | None = None  # required for measures

    # ── E. Display / UI ───────────────────────────────────────────
    display_name: str | None = None
    format: str | None = None
    category: str | None = None

    # ── F. Governance ─────────────────────────────────────────────
    sensitivity: str = "public"  # public, internal, confidential, pii
    deprecated: bool = False
    tags: list | None = None

    # ── G. Cross-Layer Hints ──────────────────────────────────────
    legend_type: str | None = None
    dh_type_override: str | None = None

    # ── Prefix support ────────────────────────────────────────
    allowed_prefixes: list | None = None


class ColumnRegistry:
    """
    Enforced schema catalog for all Storable columns.

    Every column must be defined() before it can be used on any entity.
    Prefixed columns (e.g. trader_name) are resolved against the base
    column's allowed_prefixes list.
    """

    def __init__(self) -> None:
        self._columns: dict[str, ColumnDef] = {}
        self._entities: dict[type, list[str]] = {}  # cls → [column_names]

    # ── Define columns ────────────────────────────────────────────

    def define(self, name: str, python_type: type, **kwargs: Any) -> ColumnDef:
        """Register a new column in the catalog.

        Raises RegistryError if:
        - Column name already defined
        - role is missing or invalid
        - description is missing
        - role="measure" but unit is missing
        """
        if name in self._columns:
            raise RegistryError(f"Column '{name}' is already defined")

        role = kwargs.get("role", "")
        description = kwargs.get("description", "")

        if not role:
            raise RegistryError(
                f"Column '{name}': 'role' is required "
                f"(dimension, measure, or attribute)"
            )
        if role not in ("dimension", "measure", "attribute"):
            raise RegistryError(
                f"Column '{name}': role must be 'dimension', 'measure', "
                f"or 'attribute', got '{role}'"
            )
        if not description:
            raise RegistryError(
                f"Column '{name}': 'description' is required"
            )
        if role == "measure" and not kwargs.get("unit"):
            raise RegistryError(
                f"Column '{name}': measures require 'unit' "
                f"(e.g. 'USD', 'shares', 'bps')"
            )

        col = ColumnDef(name=name, python_type=python_type, **kwargs)
        self._columns[name] = col
        return col

    # ── Lookup ────────────────────────────────────────────────────

    def get(self, name: str) -> ColumnDef:
        """Get a column definition by exact name.

        Raises RegistryError if not found.
        """
        if name not in self._columns:
            raise RegistryError(f"Column '{name}' is not defined in registry")
        return self._columns[name]

    def resolve(self, field_name: str) -> tuple:
        """Resolve a field name to (ColumnDef, prefix_or_None).

        Resolution order:
        1. Exact match against defined columns
        2. Prefix split: try {prefix}_{base} where base is a defined column
           and prefix is in that column's allowed_prefixes

        Returns (ColumnDef, None) for direct columns,
                (ColumnDef, prefix) for prefixed columns.
        Raises RegistryError if no match.
        """
        # 1. Exact match
        if field_name in self._columns:
            return (self._columns[field_name], None)

        # 2. Prefix split — try every underscore position
        parts = field_name.split("_")
        for i in range(1, len(parts)):
            prefix = "_".join(parts[:i])
            base = "_".join(parts[i:])
            if base in self._columns:
                col = self._columns[base]
                if col.allowed_prefixes and prefix in col.allowed_prefixes:
                    return (col, prefix)

        raise RegistryError(
            f"Column '{field_name}' is not defined in registry and does not "
            f"match any base_column with an allowed prefix"
        )

    def has(self, name: str) -> bool:
        """Check if a column is defined (exact match only)."""
        return name in self._columns

    def is_prefixed(self, field_name: str) -> bool:
        """Check if a field name resolves to a prefixed column."""
        try:
            _, prefix = self.resolve(field_name)
            return prefix is not None
        except RegistryError:
            return False

    def prefixed_columns(self, base_name: str) -> list:
        """List all possible prefixed variants of a base column."""
        col = self.get(base_name)
        if not col.allowed_prefixes:
            return []
        return [f"{p}_{base_name}" for p in col.allowed_prefixes]

    # ── All columns ───────────────────────────────────────────────

    def all_columns(self) -> dict:
        """Return a copy of all defined columns."""
        return dict(self._columns)

    # ── Class validation ──────────────────────────────────────────

    def validate_class(self, cls: type) -> None:
        """Validate that all fields on cls are registered columns.

        Called by Storable.__init_subclass__ (which fires BEFORE @dataclass).
        Uses cls.__annotations__ instead of dataclasses.fields().

        Also validates @computed column names (ComputedProperty descriptors).

        Checks:
        - Every field resolves (direct or prefixed)
        - Python type matches the column's python_type
        - Every @computed name resolves to a registered column

        Records the class in the entity map on success.
        """
        from typing import get_args, get_origin, get_type_hints

        from reactive.computed import ComputedProperty

        # get_type_hints resolves forward refs and includes parent annotations
        try:
            hints = get_type_hints(cls)
        except Exception:
            hints = getattr(cls, '__annotations__', {})

        # Only validate non-private fields
        field_names = []

        for field_name, field_type in hints.items():
            if field_name.startswith('_'):
                continue

            # Unwrap Optional[X] → X
            origin = get_origin(field_type)
            if origin is not None:
                args = get_args(field_type)
                non_none = [a for a in args if a is not type(None)]
                if non_none:
                    field_type = non_none[0]

            # Resolve column
            try:
                col_def, _prefix = self.resolve(field_name)
            except RegistryError:
                raise RegistryError(
                    f"{cls.__name__}.{field_name}: column '{field_name}' "
                    f"is not defined in the column registry"
                ) from None

            # Type check: allow subclasses if col_def.python_type is object or a base class
            is_match = (field_type == col_def.python_type)
            
            # If not exact match, check sub-classing (for cross-entity refs)
            if not is_match and isinstance(field_type, type) and isinstance(col_def.python_type, type):
                if col_def.python_type is object:
                    is_match = True
                elif issubclass(field_type, col_def.python_type):
                    is_match = True

            if not is_match:
                ft_name = getattr(field_type, '__name__', str(field_type))
                ct_name = getattr(col_def.python_type, '__name__', str(col_def.python_type))
                raise RegistryError(
                    f"{cls.__name__}.{field_name}: type {ft_name} "
                    f"does not match registry type "
                    f"{ct_name} for column "
                    f"'{col_def.name}'"
                )

            field_names.append(field_name)

        # Validate @computed descriptors
        for attr_name in list(cls.__dict__.keys()):
            attr = cls.__dict__[attr_name]
            if isinstance(attr, ComputedProperty):
                try:
                    col_def, _ = self.resolve(attr_name)
                except RegistryError:
                    raise RegistryError(
                        f"{cls.__name__}.{attr_name}: @computed column "
                        f"'{attr_name}' is not defined in the column registry"
                    ) from None
                field_names.append(attr_name)

        self._entities[cls] = field_names

    # ── Instance validation ───────────────────────────────────────

    def validate_instance(self, obj: Any) -> list:
        """Validate runtime values against column constraints.

        Checks enum, min_value, max_value, max_length, pattern, nullable.
        Returns list of error strings (empty = valid).
        """
        errors: list[str] = []
        cls = type(obj)

        if not dataclasses.is_dataclass(cls):
            return errors

        for field in dataclasses.fields(cls):
            value = getattr(obj, field.name)
            col_def, _ = self.resolve(field.name)

            # Nullable check
            if value is None:
                if not col_def.nullable:
                    errors.append(
                        f"{field.name}: None not allowed "
                        f"(column is not nullable)"
                    )
                continue

            # Enum check
            if col_def.enum is not None and value not in col_def.enum:
                errors.append(
                    f"{field.name}: value {value!r} not in "
                    f"allowed values {col_def.enum}"
                )

            # Min/max checks
            if col_def.min_value is not None and value < col_def.min_value:
                errors.append(
                    f"{field.name}: value {value} < "
                    f"min_value {col_def.min_value}"
                )
            if col_def.max_value is not None and value > col_def.max_value:
                errors.append(
                    f"{field.name}: value {value} > "
                    f"max_value {col_def.max_value}"
                )

            # Max length check
            if (col_def.max_length is not None
                    and isinstance(value, str)
                    and len(value) > col_def.max_length):
                errors.append(
                    f"{field.name}: length {len(value)} > "
                    f"max_length {col_def.max_length}"
                )

            # Pattern check
            if (col_def.pattern is not None
                    and isinstance(value, str)
                    and value  # skip empty strings
                    and not re.match(col_def.pattern, value)):
                errors.append(
                    f"{field.name}: value {value!r} does not match "
                    f"pattern {col_def.pattern!r}"
                )

        return errors

    # ── Introspection ─────────────────────────────────────────────

    def entities(self) -> list:
        """Return all validated entity classes."""
        return list(self._entities.keys())

    def columns_for(self, cls: type) -> list:
        """Return ColumnDefs for all fields on a validated entity class."""
        if cls not in self._entities:
            return []
        result = []
        for field_name in self._entities[cls]:
            col_def, _ = self.resolve(field_name)
            result.append(col_def)
        return result

    def entities_with(self, column_name: str) -> list:
        """Return all entity classes that use a given column name.

        Matches both direct columns and prefixed variants of the column.
        """
        result = []
        for cls, fields in self._entities.items():
            for f in fields:
                col_def, _ = self.resolve(f)
                if col_def.name == column_name:
                    result.append(cls)
                    break
        return result
