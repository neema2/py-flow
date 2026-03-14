# Reactive Expression DAG Architecture (reactive/)

This document outlines the core architectural shift in py-flow from Python Abstract Syntax Tree (AST) parsing (`@computed`) to Execution Tracing via Directed Acyclic Graphs (`@computed_expr`). 

## 1. The AST Limitation vs. Execution Tracing
Historically, the system attempted to generate SQL/pure expression equivalents by statically parsing Python source code via `ast.parse` in `reactive/computed.py`. 
- **The Problem**: AST parsing fails rapidly on complex logic (loops over arrays, cross-entity method resolutions across multiple curves, dictionaries). Python syntax is too vast to maintain a parallel compiler. The system would silently bail out (`cp.expr = None`), forcing the engine back to native python scalar evaluations and destroying SQL generation and symbolic risk differentiation capabilities.
- **The Solution - `@computed_expr`**: We shifted to execution tracing (identical to PyTorch 2.0 / JAX / Ibis workflows). Instead of parsing the code text, the python function executes *once* locally. However, instead of passing numbers (e.g., `notional = 100.0`), the system injects `Expr` node wrapper objects (e.g., `Const(100.0)` or `Variable("yield")`). As Python executes basic arithmetic (`+`, `*`, `If()`), it builds an explicit structural DAG memory map.

## 2. DAG Execution Targets
Once a property returns an `Expr` DAG, we can execute that explicit map against three completely different compiler backends for free:
1. **Live Dynamic Ticking**: We traverse the DAG using `.eval_cached(ctx)` locally, replacing `Variables` with live data feeds in micro-seconds.
2. **Database Pushdown**: We compile the tree using `.to_sql()`. The swap logic runs natively inside DuckDB/Deephaven instead of Python.
3. **Symbolic Risk Differentiation (AD)**: The `diff(expr, "PILLAR")` engine algebraically differentiates the polynomial tree to produce an exact Jacobian derivative map `∂npv/∂pillar_rate` mathematically.

## 3. Sub-Expression Sharing (`eval_cached`)
When you take the symbolic derivative of a massive function composed of 60 floating periods, you create mathematical expressions that reuse the exact same sub-trees repeatedly (e.g. `df_T`). 
- **The Risk**: Calling a naive recursive `.eval(ctx)` on highly derivative trees results in exponential time-complexity (O(2^N)) re-evaluations of the *same* static branch.
- **The Fix**: `eval_cached(expr, ctx, _cache)` tracks the memory address `id(expr)` of every traversed node. An identical node branch is only evaluated once globally during a pricing tick, reducing the traversal complexity back down to O(V+E) linear time.

## 4. The Magic `CallableFloat`
To ensure cross-compatibility between standard scalars (`@computed`) and complex trees (`@computed_expr`), the `@computed_expr` descriptor produces an overloaded `CallableFloat` return value.
- `swap.dv01` → Acts like a standard lazily evaluated numerical float object.
- `swap.dv01()` → Invokes the `__call__` dunder method to expose the raw underlying `Expr` tree DAG for SQL and derivative transformations.

However, explicitly expanding this pattern to `CallableStr` and `CallableDict` objects induces severe upstream instability when paired natively with PyArrow streaming. During `@ticking` serialization, PyArrow's schema inference frequently fails on these structures, attempting to forcibly coerce them into `double` native arrays (`ArrowInvalid: Could not convert 'LOSS' with type CallableStr: tried to convert to double`).

**Best Practices for Streaming:**
1. **Complex Dictionaries:** Multi-dimensional maps evaluated via `Expr` derivatives (like `risk` arrays) must be explicitly excluded from continuous stream ingestion: `@ticking(exclude={"curve", "risk"})`.
2. **Text State Signals:** Standard string emissions (like `pnl_status` returning "PROFIT"/"LOSS") must avoid `@computed_expr` entirely and fall back to native `@computed` scalar execution returning raw native strings to align safely with PyArrow types.
