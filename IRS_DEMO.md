# Interest Rate Swap (IRS) Pricing & Risk Demo

This demo showcases the high-performance pricing and risk engine for Interest Rate Swaps in `py-flow`. The architecture leverages a unified expression-tree system that supports both native Python evaluation and highly optimized, DAG-aware SQL generation.

## Key Methodology

The system has moved from simple expression trees to a **Directed Acyclic Graph (DAG)** representation. This enables maximum sub-expression sharing across complex instrument portfolios.

### 1. DAG-Aware SQL Generation
Instead of generating repetitive SQL code, the engine identifies common sub-expressions (e.g., interpolated discount factors used by multiple swaps) and compiles them into **Common Table Expressions (CTEs)**. This results in:
- **Dramatic SQL Size Reduction**: Queries that previously spanned hundreds of kilobytes are now compact and readable.
- **Improved Performance**: Database engines (like DuckDB) can optimize the evaluation of shared nodes once per row.

### 2. Jacobian Pruning
The engine uses **Symbolic Differentiation** to compute the full Jacobian matrix ($\partial NPV_i / \partial Pillar_j$).
- **Analytic Risk**: Sensitivities are computed exactly without the overhead of finite difference (bump-and-grind).
- **Pruning**: Columns with guaranteed zero sensitivity (e.g., a 1Y swap is not sensitive to the 30Y pillar) are automatically pruned from the SQL generation. This minimizes data transfer and simplifies the final query result.

### 3. Unified Expression Architecture
A single symbolic representation handles:
- **Python Evaluation**: Fast in-memory calculations for interactive analysis.
- **SQL Generation**: Heavy-lifting execution on billionaire-row datasets via DuckDB or Postgres.
- **Symbolic Sensitivity**: Automatic generation of risk ladders.

## Architecture

The system is split into two zones by the **fitter boundary**:

```mermaid
graph TD
    MDS[Market Data Server] -->|USD OIS Ticks| SQ[SwapQuote]
    
    subgraph "Fitter Boundary — iterative solver"
        SQ -->|quote_ref| YCP["YieldCurvePoint (pillar)"]
        Fitter["CurveFitter"] -.->|solve until NPV ≈ 0| YCP
        Fitter -.->|publishes| JAC[Fitter Jacobian<br/>∂pillar/∂quote]
    end
    
    subgraph "Expr DAG — symbolic, optimized"
        YCP -->|df / interp| EXPR[NPV DAG]
        EXPR -->|"diff(expr, pillar)"| DERIV[∂NPV/∂Pillar DAGs]
    end

    subgraph "Evaluation Targets"
        EXPR -->|Portfolio.to_sql_optimized| SQL[DuckDB / CTE-based SQL]
        EXPR -->|.eval(ctx)| PY[Python float]
        DERIV -->|eval_cached| RISK["∂NPV/∂Pillar"]
        RISK -->|"× Jacobian matrix"| QUOTE_RISK["∂NPV/∂Quote"]
    end
```

The pipeline flows from market quotes through an iterative solver to the final risk-aware SQL generation:

1.  **Market Quotes**: Raw par rates from the market (e.g., 5Y OIS at 4.0%).
2.  **Curve Fitter**: An iterative boundary (using Levenberg-Marquardt) that finds the set of zero-rate pillars that price the benchmark swaps to par.
3.  **Variable Registry**: Fitted pillars and market quotes are represented as symbolic `Variable` nodes.
4.  **Instrument Portfolio**: Aggregates `Expr` trees for all swaps. Identifies shared dependencies.
5.  **SQL Compiler**: Generates a single CTE-based query that computes NPVs and sensitivities in one pass.

## Domain Models

| Domain | Entity | Responsibility |
|---|---|---|
| **Market** | `SwapQuote` | Direct consumer of live ticks (bid/ask/rate) |
| **Market** | `YieldCurvePoint` | Fitted pillar (rate/DF), output of the fitter |
| **Market** | `YieldCurve` | Interpolation, `df_at()` for reactive, `df()` for Expr tree |
| **Instrument** | `InterestRateSwap` | Reactive valuation via `@computed` & `@computed_expr` |
| **Instrument** | `Portfolio` | Named collection of properties for fitter + risk |

## Naming Convention

**`<Type>_<Asset>_<Measure>.<Index>`**

| Object Type | Example Symbol | Description |
|---|---|---|
| Quote | `IR_USD_OIS_QUOTE.10Y` | 10Y USD OIS swap quote (input) |
| Fit | `IR_USD_DISC_USD.10Y` | 10Y fitted yield curve pillar (output) |
| Jacobian | `IR_USD_DISC_USD.10Y_SENS.IR_USD_OIS_QUOTE.5Y` | ∂fit_10Y / ∂quote_5Y |
| Instrument | `IR_USD_IRS.7Y` | 7Y interest rate swap instance |

## Key Files

```
├── reactive/
│   ├── expr.py             # Expr nodes, diff(), eval_cached(), simplification
│   ├── computed.py         # @computed (AST parsing for scalar reactive)
│   └── computed_expr.py    # @computed_expr (Tracing logic for complex DAGs)
├── marketmodel/
│   ├── yield_curve.py      # YieldCurve, YieldCurvePoint, df()
│   ├── curve_fitter.py     # CurveFitter (scipy solver loop)
│   └── swap_curve.py       # SwapQuote definition
├── instruments/
│   ├── ir_swap_fixed_floatapprox.py  # IRSwapFixedFloatApprox (domain model)
│   └── portfolio.py        # Portfolio class, to_sql_optimized()
└── tests/
    ├── test_expr_sql_duckdb.py  # Python vs optimized SQL cross-validation
    └── test_fixed_floatapprox_swap.py # Core Expr tree tests
```

## Example Output

When pricing a portfolio of swaps, the generated SQL looks like this:

```sql
WITH pillars AS (
  SELECT 0.02 AS "USD_1Y", 0.04 AS "USD_5Y", ...
),
shared_nodes AS (
  SELECT 
    EXP(-tenor * rate) AS df_5y,
    ...
  FROM pillars
)
SELECT 
  notional * (df_0 - df_T) AS npv_payer,
  ...
FROM shared_nodes
```

The resulting sensitivities are returned as additional columns, pruned to only show active pillars.

## Tests to Run

```bash
# Core Expr tree tests (swap, portfolio, fitter, risk, SQL generation)
pytest tests/test_fixed_floatapprox_swap.py -v

# Python vs DuckDB cross-validation (16 tests executing DuckDB in-memory)
pytest tests/test_expr_sql_duckdb.py -v

# Reactive cascade + live grid engine pricing
pytest tests/test_demo_ir_swap.py -v
```

---

## Future Enhancements & TODO

### Deephaven Server-Side Execution (`.to_deephaven()`)
Deephaven supports **server-side formula evaluation** on ticking tables via `.update(["formula"])`. We could push NPV and risk calculations down into the Deephaven JVM instead of computing them in Python.

**Goal**: Add a `.to_deephaven()` target to Expr nodes, producing Deephaven formula strings.

```python
# Target: push the mathematical AST formula down to Deephaven
client.run_script(f"""
npv_ticking = curve_table.update([
    "NPV = {swap.npv().to_deephaven()}"
])
""")
```
This eliminates the Python round-trip: curve data ticks → DH evaluates NPV formula server-side → result ticks out.

### FINOS Legend Engine Integration
The `to_pure()` target currently generates Legend Pure expression strings. The FINOS Legend Engine can translate Pure to optimised SQL for H2, DuckDB, Snowflake, Postgres, and Spark, offering tighter integration with major bank tech stacks.

### Explicit Float Swap
- [ ] `InterestRateFloatSwap` unrolls with `fwd_expr()` from projection curve
- [ ] Same diff/eval/to_sql machinery works identically
- [ ] Supports separate projection and discount curves

### Multi-Currency Support
- [ ] Add JPY Curve: Support JPY OIS benchmark swaps
- [ ] Cross-Currency: Implement XCCY Basis Swaps
- [ ] Scalability: Verify fitter stability when solving multiple curves (USD/JPY/XCCY) simultaneously

---

## Appendix: The Architectural Shift from AST Parsing to Execution Tracing

The `py-flow` framework supports two distinct approaches to reactive expression generation:

1. **`@computed` (AST Parsing)**: Reads the Python source code at compile time and translates the AST syntax (`ast.py`) directly into a mathematical expression tree.
2. **`@computed_expr` (Execution Tracing)**: Ignores the source code entirely. Instead, it executes the function top-down dynamically at runtime using `Expr` math overloads (intercepting `__add__`, `__mul__`, etc.) to explicitly track and trace the mathematical graph.

This journey from AST parsing to Execution Tracing mirrors one of the most famous architectural paradigm shifts in modern compiler design, specifically within Machine Learning and Data Engineering.

### 1. PyTorch 2.0: The Failure of AST Parsing (`TorchScript` vs `TorchDynamo`)
PyTorch went through this exact evolution, culminating in the release of PyTorch 2.0:
* **The AST Approach (`TorchScript`)**: Historically, PyTorch engineers built `@torch.jit.script` (similar to our `_ASTTranslator`). It parsed Python source code's AST to build a static C++ graph. The problem? Python's dynamic nature is vast. `TorchScript` continually struggled to handle Python dictionaries, list comprehensions, or dynamic `if` statements, forcing PyTorch engineers to maintain a fragile parallel Python compiler.
* **The Tracing Approach (`TorchDynamo`)**: In PyTorch 2.0, they entirely abandoned the AST approach. They introduced **TorchDynamo**, which uses "JIT Tracing". Instead of reading source code, it hooks into Python's native execution. When you run a function, Torch tracks operations dynamically to build the graph (just like our `@computed_expr` nodes do). If it hits something it doesn't understand, it does a "graph break" and falls back to standard Python.

### 2. JAX: Pure Tracing by Design
Google's **JAX** recognized the limitations of AST parsing and decided never to build one. As detailed in the famous *"Autodiff Cookbook"*, JAX's tracing architecture works elegantly:
When you decorate a function with `@jax.jit` and call it, JAX doesn't pass in real data. It passes in "Tracer" objects. These Tracers look and act exactly like Python floats (much like our `CallableFloat`), but when you add them together, they silently record the operation to an internal graph called a `jaxpr` (JAX Expression). JAX proved that Operator Overloading + Execution Tracing is vastly superior and easier to maintain than AST parsing.

### 3. Frameworks for Deriving SQL
In the data engineering world, several frameworks mirror these exact two approaches for SQL generation:

**The Tracing / Operator Overloading Frameworks (Like our `@computed_expr`)**
* **Ibis (by Voltron Data):** The industry standard for explicit expression building. Ibis allows you to write Python code that dynamically chains together mathematical operations. Under the hood, it builds an explicit expression tree that compiles flawlessly into DuckDB, Snowflake, or Postgres SQL.
* **SQLAlchemy 2.0:** SQLAlchemy's Expression Language (`table.c.price * table.c.qty`) works exactly like our `Expr` tracking. It overloads `__mul__` to return a `BinaryExpression` node, which later calls `.compile()` to spit out SQL string fragments.

**The AST Parsing Frameworks (Like our `_ASTTranslator`)**
* **Pony ORM:** One of the few famous frameworks that went the AST compilation route. In Pony ORM, you write literal Python generator expressions (`select(p for p in Person if p.age > 20)`). Pony hooks into Python's AST, decompiles your generator into an abstract tree, and attempts to translate it to SQL. Just like our `@computed`, it feels like absolute magic when it works, but it famously crashes if you use an unsupported Python feature.

By implementing *both* in our `py-flow` framework, we essentially have the magic of **Pony ORM** for simple column-level logic, and the unconstrained scale of **Ibis / JAX** for deep, telescoping financial pricing graphs!
