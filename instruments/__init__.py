"""
instruments — Financial instrument domain models.

Each instrument is a separate module with its own Storable/ticking classes.
Instruments depend on market models (marketmodel/) via abstract interfaces
(fwd_at, df_at, df_array), not on specific interpolation implementations.

    instruments/
        ir_swap_fixed_floatapprox.py — IRSwapFixedFloatApprox, SwapPortfolio
        portfolio.py                 — Portfolio (named collections)
"""

from instruments.ir_swap_fixed_floatapprox import IRSwapFixedFloatApprox, SwapPortfolio
from instruments.portfolio import Portfolio

__all__ = [
    "IRSwapFixedFloatApprox",
    "SwapPortfolio",
    "Portfolio",
]
