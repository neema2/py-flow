"""
symbols — Naming convention for financial objects.

All storable objects use a structured symbol:

    <Type>_<Asset>_<Measure>.<Index1>.<Index2>

Examples:
    IR_USD_OIS_QUOTE.5Y       — 5Y USD OIS swap quote
    IR_USD_YC_FIT.5Y          — fitted 5Y yield curve point
    IR_USD_YC_JACOBIAN.5Y.10Y — ∂fit_5Y / ∂quote_10Y
    IR_USD_IRS.5Y             — 5Y interest rate swap

The convention enables:
    - Programmatic construction of related symbols
    - Parsing to extract type, asset, tenor
    - Grouping by type/asset for risk aggregation
"""

from __future__ import annotations


def quote_symbol(currency: str, curve: str, tenor: str) -> str:
    """IR_USD_OIS_QUOTE.5Y"""
    return f"IR_{currency}_{curve}_QUOTE.{tenor}"


def fit_symbol(currency: str, tenor: str) -> str:
    """IR_USD_DISC_USD.5Y"""
    return f"IR_{currency}_DISC_{currency}.{tenor}"


def jacobian_symbol(currency: str, output_name: str, input_symbol: str) -> str:
    """∂pillar / ∂quote.
    If output_name is already a full fit symbol, it appends _SENS.
    """
    if "_DISC_" in output_name:
        return f"{output_name}_SENS.{input_symbol}"
    return f"IR_{currency}_YC_JACOBIAN.{output_name}.{input_symbol}"





def tenor_name(years: float) -> str:
    """Convert numeric tenor to label: 0.5 → '6M', 1.0 → '1Y', etc."""
    if years < 1.0:
        months = int(years * 12)
        return f"{months}M"
    if years == int(years):
        return f"{int(years)}Y"
    return f"{years}Y"


def parse_symbol(symbol: str) -> dict:
    """Parse a structured symbol into components.

    Returns dict with keys: type, asset, measure, indices.
    """
    parts = symbol.split(".")
    head = parts[0]  # e.g. "IR_USD_OIS_QUOTE"
    indices = parts[1:]  # e.g. ["5Y"] or ["5Y", "10Y"]

    tokens = head.split("_")
    return {
        "type": tokens[0] if tokens else "",       # IR
        "currency": tokens[1] if len(tokens) > 1 else "",  # USD
        "measure": "_".join(tokens[2:]),  # OIS_QUOTE, YC_FIT, YC_JACOBIAN
        "indices": indices,
    }
