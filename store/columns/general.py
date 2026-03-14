"""
General-purpose columns — name, label, title, color, weight, etc.

Used across test entities and general-purpose Storables.
"""

from datetime import datetime

from store.columns import REGISTRY

# ── Identifiers / Labels ─────────────────────────────────────────

REGISTRY.define("name", str,
    description="Entity or person name",
    semantic_type="person_name",
    role="dimension",
    synonyms=["person", "individual"],
    sensitivity="pii",
    allowed_prefixes=["trader", "salesperson", "client", "approver"],
)

REGISTRY.define("label", str,
    description="Short descriptive label",
    semantic_type="label",
    role="dimension",
    synonyms=["tag", "descriptor"],
)

REGISTRY.define("title", str,
    description="Title or heading",
    semantic_type="label",
    role="dimension",
    synonyms=["heading", "subject"],
)

REGISTRY.define("color", str,
    description="Color identifier",
    semantic_type="label",
    role="dimension",
    synonyms=["colour"],
)

REGISTRY.define("status", str,
    description="Current status label",
    semantic_type="label",
    role="dimension",
    display_name="Status",
)

REGISTRY.define("notes", str,
    description="Free text notes or comments",
    semantic_type="free_text",
    role="attribute",
    nullable=True,
)

REGISTRY.define("tags", list,
    description="List of classification tags",
    semantic_type="label",
    role="attribute",
    nullable=True,
)

# ── Measures (general) ───────────────────────────────────────────

REGISTRY.define("weight", float,
    description="Weight measurement",
    semantic_type="count",
    role="measure",
    unit="units",
    display_name="Weight",
)

REGISTRY.define("amount", float,
    description="Monetary amount",
    semantic_type="currency_amount",
    role="measure",
    unit="USD",
    display_name="Amount",
)

REGISTRY.define("value", float,
    description="Numeric value or measurement",
    semantic_type="count",
    role="measure",
    unit="units",
    display_name="Value",
)

REGISTRY.define("width", float,
    description="Width dimension",
    semantic_type="count",
    role="measure",
    unit="units",
)

REGISTRY.define("height", float,
    description="Height dimension",
    semantic_type="count",
    role="measure",
    unit="units",
)

REGISTRY.define("threshold", float,
    description="Threshold value for triggering",
    semantic_type="count",
    role="measure",
    unit="units",
)

# ── Attributes ────────────────────────────────────────────────────

REGISTRY.define("unit", str,
    description="Unit of measurement label",
    semantic_type="label",
    role="attribute",
)

REGISTRY.define("ts", str,
    description="Timestamp string (ISO format)",
    semantic_type="timestamp",
    role="attribute",
    nullable=True,
)

REGISTRY.define("active", bool,
    description="Active/inactive flag",
    semantic_type="boolean_flag",
    role="attribute",
)

REGISTRY.define("is_target", bool,
    description="Internal solver/target flag",
    semantic_type="toggle",
    role="attribute",
)

REGISTRY.define("created", datetime,
    description="Creation timestamp",
    semantic_type="timestamp",
    role="attribute",
    nullable=True,
)

REGISTRY.define("quote_symbol", str,
    description="Associated market quote symbol",
    semantic_type="identifier",
    role="dimension",
)


REGISTRY.define("model_name", str,
    description="Model identifier",
    semantic_type="identifier",
    role="dimension",
    synonyms=["model", "algorithm"],
)

# ── Computed columns (general) ──────────────────────────────────

REGISTRY.define("area", float,
    description="Computed area (width × height)",
    role="measure", unit="units",
)

REGISTRY.define("doubled", float,
    description="Computed doubled value",
    role="measure", unit="units",
)

REGISTRY.define("above_threshold", bool,
    description="Whether value exceeds threshold",
    role="attribute",
)

REGISTRY.define("above", bool,
    description="Whether value exceeds threshold (alias)",
    role="attribute",
)

REGISTRY.define("celsius", float,
    description="Fahrenheit to Celsius conversion",
    role="measure", unit="°C",
)

REGISTRY.define("effective", float,
    description="Effective value (coalesce of override/default)",
    role="measure", unit="units",
)

REGISTRY.define("upper_label", str,
    description="Upper-cased label",
    role="attribute",
)

REGISTRY.define("override", float,
    description="Override value",
    role="measure", unit="units", nullable=True,
)

REGISTRY.define("default", float,
    description="Default value",
    role="measure", unit="units",
)

REGISTRY.define("positions", list,
    description="List of position objects",
    role="attribute",
)

REGISTRY.define("weights", dict,
    description="Weight breakdown (e.g. sector weights)",
    role="attribute",
)

REGISTRY.define("risk", float,
    description="Risk metric value",
    role="measure", unit="USD",
)

REGISTRY.define("risk_ranking", list,
    description="Ranked list of risk contributors",
    role="attribute",
)

REGISTRY.define("total_mv", float,
    description="Total market value across positions",
    role="measure", unit="USD",
)

REGISTRY.define("total_pnl", float,
    description="Total P&L across positions",
    role="measure", unit="USD",
)

REGISTRY.define("max_mv", float,
    description="Maximum market value in group",
    role="measure", unit="USD",
)

REGISTRY.define("avg_mv", float,
    description="Average market value in group",
    role="measure", unit="USD",
)

REGISTRY.define("total_qty", int,
    description="Total quantity across positions",
    role="measure", unit="shares",
)

REGISTRY.define("spread_mv", float,
    description="Spread of market values",
    role="measure", unit="USD",
)

REGISTRY.define("radius", float,
    description="Radius measurement",
    role="measure", unit="units",
)

REGISTRY.define("diameter", float,
    description="Computed diameter (2 × radius)",
    role="measure", unit="units",
)

REGISTRY.define("circumference", float,
    description="Computed circumference",
    role="measure", unit="units",
)

REGISTRY.define("items", list,
    description="List of items",
    role="attribute",
)
