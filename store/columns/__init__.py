"""
Column catalog — the single source of truth for all Storable field definitions.

Exports REGISTRY, the global ColumnRegistry instance populated with all
approved columns from trading, finance, and general domains.
"""

from store.registry import ColumnRegistry

REGISTRY = ColumnRegistry()

# Import domain modules to populate the registry
from store.columns import general   # noqa: F401, E402
from store.columns import trading   # noqa: F401, E402
from store.columns import finance   # noqa: F401, E402
from store.columns import media     # noqa: F401, E402
