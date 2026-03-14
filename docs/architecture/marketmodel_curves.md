# Yield Curve Interpolation (marketmodel/)

This document outlines the core yield curve structural patterns inside `py-flow`, allowing a dynamic `Expr` mapping across complex instruments.

## 1. Curve Representations
The primary entry for risk and pricing is the `YieldCurve` (`store/base.yaml`). A curve is mathematically just an array of `YieldCurvePoint`s (nodes) defining an interest rate (y-axis) against a continuous timeline (x-axis in `tenor_years`).
- `YieldCurve._sorted_points()` natively organises these discrete parameters.
- Instruments (like a `IRSwapFixedFloat`) consume discrete points to price continuous schedules using continuous approximation.
- To produce `.df(t)`, the curve linearly interpolates the rate between `pts[i]` and `pts[i+1]` on a weight `w = (t - t1)/(t2 - t1)`.

## 2. DAG Interleaving over AST
When building continuous structures:
1. Every numerical evaluation node evaluates as a unique object.
2. In large portfolio structures (1,000s of distinct swaps mapped statically to a single `USD_OIS_DISC` yield curve object), memory bloats if every instrument instantiates an identical interpolation mapping for `df(5.0)`.
3. To mitigate memory limits, `df(t)` natively injects an `_df_cache` on the `YieldCurve` memory object space. When an identical tenor is queried out of 100 swaps simultaneously, the explicit DAG `id(expr)` is statically reused directly across all memory profiles globally. 
- *Consequence*: Differentiating mathematically `diff(portfolio_npv, pillar_name)` natively diffs thousands of instruments identically via a single mathematical pass mathematically equivalent to PyTorch autograd.

## 3. Forward Rates
The explicit mathematical requirement of multi-model floating swap instruments mandates robust multi-period rate lookups defined solely mathematically against the continuous discount curve geometry algorithm:
`fwd(start, end) = (df(start) / df(end) - 1.0) / (end - start)`
- `YieldCurve.fwd(start, end)` structurally builds an algebraic expression node that guarantees `fwd(1.0, 5.0)` collapses exactly equivalently natively to Python compilation logic OR duckdb SQL compilation targets exactly equivalent organically. `fwd(start, end)` builds the explicit polynomial algebraic string directly out of `Const(dt)` and sub-expressions returned by standard `df(start) / df(end)` routines organically.

## To-Do

1. Consider speed difference of `df(t) = Exp(-rT)` compared to `1/(1+r)^T` that we are doing now.
2. Consider smoother interpolation, for example left derivative preserving, and if that is better, making deals depend on `YieldCurvePoint`s of previous knots or an extra parameter for derivative. Alternatively, a common approach uses a short rate `r(t)` and integrates it for interpolation, which might perhaps cache more similarly to a current integrated rate.
3. Learn best practice on modern RFR OIS projection, that might not be exactly rolled up, so there might be convexity etc. or just a mismatch period to payment dates, like traditional Libor curve 6 date scheduling.
4. Consider Turn-of-Year (ToY) spikes pass through fitter.

These choices can have substantial impact on the complexity of the graph used in fitting, breaking design assumptions of what is appropriate.
