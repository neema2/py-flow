"""
instruments/portfolio — Named collection of instrument Expr trees.

Provides the Portfolio class, which aggregates multiple IRSwapFixedFloatApprox
Expr trees on a shared curve.  This enables maximum sub-expression sharing
and provides symbolic Jacobian matrices for the fitter.
"""

from __future__ import annotations
from collections import Counter
from typing import Any

from reactive.expr import (
    Const, Expr, diff, eval_cached, 
    Variable, VariableMixin, Field,
    BinOp, UnaryOp, Func, If, Coalesce, IsNull, StrOp,
    _cast_numeric_sql
)
from instruments.ir_swap_fixed_floatapprox import IRSwapFixedFloatApprox


class Portfolio:
    """A collection of named swap Expr trees on a shared curve.

    Because all swaps call the same curve's df(t), and df
    caches Expr objects, the portfolio's expression graph has maximum
    sub-expression sharing.  For example, df(5.0) is the same
    Python object in the 5Y swap's tree and the 10Y swap's tree.

    Provides:
      .npv_exprs         → {name: Expr}           named NPV expressions
      .residual_exprs    → {name: Expr}           NPV/notional (for fitter)
      .risk_exprs        → {name: {pillar: Expr}} per-swap Jacobian rows
      .jacobian_exprs    → {name: {pillar: Expr}} ∂residual/∂pillar (for fitter)
      .total_npv_expr    → Expr                   Σ NPV across portfolio

    All return Expr trees — eval(ctx) for Python, to_sql() for SQL.
    """

    def __init__(self, curve):
        self.curve = curve
        self._swaps: dict[str, Any] = {}
        self._pillar_names = [p.name for p in curve._sorted_points()]

    def add(
        self,
        name: str,
        notional: float,
        fixed_rate: float,
        tenor_years: float,
        side: str = "RECEIVER",
    ) -> IRSwapFixedFloatApprox:
        """Add a swap to the portfolio.  Returns the swap for inspection."""
        swap = IRSwapFixedFloatApprox(
            symbol=f"PORT_{name}",
            notional=notional,
            fixed_rate=fixed_rate,
            tenor_years=tenor_years,
            side=side,
            curve=self.curve,
        )
        self._swaps[name] = swap
        return swap

    def add_swap(self, name: str, swap: Any):
        """Add a pre-constructed swap to the portfolio."""
        self._swaps[name] = swap
        return swap

    @property
    def names(self) -> list[str]:
        return list(self._swaps.keys())

    @property
    def pillar_names(self) -> list[str]:
        return self._pillar_names

    # ── Named dictionaries of Expr trees ───────────────────────────────

    @property
    def npv_exprs(self) -> dict[str, Expr]:
        """Named NPV expressions: {swap_name: npv_expr}."""
        return {name: swap.npv() for name, swap in self._swaps.items()}

    @property
    def residual_exprs(self) -> dict[str, Expr]:
        """NPV / notional for each swap (what the fitter minimizes).

        Scaling by notional makes the residuals dimensionless and
        comparable across swaps of different size.
        """
        return {
            name: swap.npv() * Const(1.0 / getattr(swap, "notional", getattr(swap, "leg1_notional", 1.0)))
            for name, swap in self._swaps.items()
        }

    @property
    def total_npv_expr(self) -> Expr:
        """Sum of all NPVs — a single Expr tree."""
        total = Const(0.0)
        for swap in self._swaps.values():
            total = total + swap.npv()
        return total

    @property
    def risk_exprs(self) -> dict[str, dict[str, Expr]]:
        """Per-swap risk: {swap_name: {pillar_label: ∂npv/∂pillar Expr}}."""
        return {
            name: {
                pillar_name: diff(swap.npv(), pillar_name)
                for pillar_name in self._pillar_names
            }
            for name, swap in self._swaps.items()
        }

    @property
    def jacobian_exprs(self) -> dict[str, dict[str, Expr]]:
        """Fitter Jacobian: ∂residual_i / ∂pillar_j as Expr trees.

        Each row is a swap, each column is a pillar.
        residual = npv / notional, so:
            ∂residual/∂pillar = (1/notional) × ∂npv/∂pillar
        """
        result = {}
        for name, swap in self._swaps.items():
            notional = getattr(swap, "notional", getattr(swap, "leg1_notional", 1.0))
            scale = Const(1.0 / notional)
            result[name] = {
                pillar_name: diff(swap.npv(), pillar_name) * scale
                for pillar_name in self._pillar_names
            }
        return result

    # ── Convenience evaluators ─────────────────────────────────────────

    def pillar_context(self) -> dict[str, float]:
        """Current pillar rates from the curve."""
        rates = self.curve.pillar_rates
        return dict(zip(self._pillar_names, rates))

    def eval_npvs(self, ctx: dict) -> dict[str, float]:
        """Evaluate all NPVs."""
        return {name: eval_cached(expr, ctx) for name, expr in self.npv_exprs.items()}

    def eval_residuals(self, ctx: dict) -> dict[str, float]:
        """Evaluate residual vector (what the fitter minimizes)."""
        return {name: eval_cached(expr, ctx) for name, expr in self.residual_exprs.items()}

    def eval_jacobian(self, ctx: dict) -> dict[str, dict[str, float]]:
        """Evaluate the full Jacobian matrix."""
        return {
            name: {label: eval_cached(expr, ctx) for label, expr in row.items()}
            for name, row in self.jacobian_exprs.items()
        }

    def eval_total_risk(self, ctx: dict) -> dict[str, float]:
        """Aggregate ∂(Σnpv)/∂pillar across all swaps."""
        total_expr = self.total_npv_expr
        return {
            pillar_name: eval_cached(diff(total_expr, pillar_name), ctx)
            for pillar_name in self._pillar_names
        }

    # ── Sub-expression sharing stats ───────────────────────────────────

    def shared_nodes(self) -> dict[str, int]:
        """Count how many swaps reference each cached df node."""
        node_ids: Counter[int] = Counter()
        for swap in self._swaps.values():
            seen = set()
            _walk_ids(swap.npv(), seen)
            for nid in seen:
                node_ids[nid] += 1
        return {f"node_{nid}": count for nid, count in node_ids.items() if count > 1}

    def to_sql_optimized(self, ctx: dict[str, float]) -> str:
        """Generate a single optimized SQL query for the entire Portfolio Jacobian.

        Uses Common Table Expressions (CTEs) to preserve the Expr DAG structure,
        ensuring shared sub-expressions (like discount factors) are computed
        exactly once.

        Implements pruning: zero derivatives (Const 0.0) are omitted from the
        output columns.

        Returns a single SQL string with:
          - A 'pillars' CTE for the input rates.
          - Multiple 'node_NNN' CTEs for shared sub-expressions.
          - A final SELECT with all NPV and Jacobian columns.
        """
        # 1. Identify all target expressions and their sub-expression counts
        npv_targets = self.npv_exprs
        jac_targets = self.jacobian_exprs

        all_exprs: list[Expr] = list(npv_targets.values())
        relevant_jac: dict[str, dict[str, Expr]] = {}

        for swap_name, row in jac_targets.items():
            relevant_jac[swap_name] = {}
            for pillar, expr in row.items():
                if isinstance(expr, Const) and expr.value == 0.0:
                    continue # Prune zero sensitivities
                relevant_jac[swap_name][pillar] = expr
                all_exprs.append(expr)

        # 2. Find shared nodes (in-degree > 1 across the whole portfolio)
        node_counts: Counter[int] = Counter()
        def _collect(e: Expr, visited: set[int]):
            node_counts[id(e)] += 1
            if id(e) in visited: return
            visited.add(id(e))
            for child in _get_children(e):
                _collect(child, visited)

        global_visited: set[int] = set()
        for expr in all_exprs:
            _collect(expr, global_visited)

        shared_node_ids = {nid for nid, count in node_counts.items() if count > 1}

        # 3. Topological sort of shared nodes for CTE generation
        # (We need to define a node's CTE after its children's CTEs)
        ordered_shared: list[Expr] = []
        visited_shared = set()
        def _topo(e: Expr):
            if id(e) in visited_shared: return
            for child in _get_children(e):
                _topo(child)
            if id(e) in shared_node_ids:
                visited_shared.add(id(e))
                ordered_shared.append(e)

        for expr in all_exprs:
            _topo(expr)

        # 4. Generate SQL fragments
        cte_defs = []
        # Input pillars
        pillar_cols = ", ".join(f'{rate} AS "{name}"' for name, rate in ctx.items())
        cte_defs.append(f'pillars AS (SELECT {pillar_cols})')

        # Use a substitution map: if id(e) is in here, use the CTE name
        subst: dict[int, str] = {}

        def _get_sql(e: Expr) -> str:
            return _to_sql_dag(e, subst, "pillars")

        # Shared node CTEs
        for i, node in enumerate(ordered_shared):
            # Leaves don't need their own CTEs if they are just pillars or constants
            if isinstance(node, (Variable, VariableMixin, Const)):
                subst[id(node)] = _get_sql(node)
                continue

            name = f"node_{i}"
            sql = _get_sql(node)
            
            # Identify which previous nodes this CTE depends on
            dependencies = ["pillars"]
            for prev_idx in range(i):
                if f"node_{prev_idx}.val" in sql:
                    dependencies.append(f"node_{prev_idx}")
            
            from_clause = ", ".join(dependencies)
            cte_defs.append(f'{name} AS (SELECT ({sql}) AS val FROM {from_clause})')
            subst[id(node)] = f"{name}.val"

        # 5. Final SELECT columns
        select_cols = []
        for name, expr in npv_targets.items():
            select_cols.append(f'{_get_sql(expr)} AS "{name}_NPV"')
        
        for swap_name, row in relevant_jac.items():
            for pillar, expr in row.items():
                col_name = f"{swap_name}_dNPV_d{pillar}"
                select_cols.append(f'{_get_sql(expr)} AS "{col_name}"')

        with_clause = "WITH " + ",\n  ".join(cte_defs)
        
        # Identify all final dependencies for the final SELECT
        final_deps = ["pillars"]
        final_sql_all = " ".join(select_cols)
        for i in range(len(ordered_shared)):
             if f"node_{i}.val" in final_sql_all:
                 final_deps.append(f"node_{i}")

        from_final = ", ".join(final_deps)
        final_select = "SELECT\n  " + ",\n  ".join(select_cols) + f"\nFROM {from_final}"

        return f"{with_clause}\n{final_select}"


def _to_sql_dag(expr: Expr, subst: dict[int, str], col: str) -> str:
    """Efficiently build SQL fragment for a DAG node using substitution map."""
    if id(expr) in subst:
        return subst[id(expr)]

    # Handle leaves
    if isinstance(expr, (Variable, VariableMixin)):
        return expr.expr_to_sql(col)
    if isinstance(expr, Const):
        return expr.to_sql(col)
    if isinstance(expr, Field):
        return expr.to_sql(col)

    # Handle composite nodes
    if isinstance(expr, BinOp):
        l_sql = _to_sql_dag(expr.left, subst, col)
        r_sql = _to_sql_dag(expr.right, subst, col)
        
        # Numeric casting logic from expr.py
        if expr.op in ("+", "-", "*", "/", "%", "**", ">", "<", ">=", "<="):
            if id(expr.left) not in subst and isinstance(expr.left, Field):
                l_sql = f"({col}->>'{expr.left.name}')::float"
            if id(expr.right) not in subst and isinstance(expr.right, Field):
                r_sql = f"({col}->>'{expr.right.name}')::float"
        
        from reactive.expr import _SQL_OPS
        op = _SQL_OPS[expr.op]
        return f"({l_sql} {op} {r_sql})"

    if isinstance(expr, UnaryOp):
        s = _to_sql_dag(expr.operand, subst, col)
        if id(expr.operand) not in subst and isinstance(expr.operand, Field):
            s = f"({col}->>'{expr.operand.name}')::float"
            
        if expr.op == "neg": return f"(-{s})"
        if expr.op == "abs": return f"ABS({s})"
        if expr.op == "not": return f"NOT ({s})"
        return s

    if isinstance(expr, Func):
        from reactive.expr import Func as PREFunc
        sql_name = PREFunc._SQL_FUNCS.get(expr.name, expr.name.upper())
        args = []
        for a in expr.args:
            asql = _to_sql_dag(a, subst, col)
            if id(a) not in subst and isinstance(a, Field):
                asql = f"({col}->>'{a.name}')::float"
            args.append(asql)
        return f"{sql_name}({', '.join(args)})"

    if isinstance(expr, If):
        cond = _to_sql_dag(expr.condition, subst, col)
        t = _to_sql_dag(expr.then_, subst, col)
        e = _to_sql_dag(expr.else_, subst, col)
        return f"CASE WHEN {cond} THEN {t} ELSE {e} END"

    # Fallback to naive if unknown (should not happen for basic IRS)
    return expr.to_sql(col)


def _get_children(expr: Expr) -> list[Expr]:
    """Helper to extract child expressions from any node type."""
    if isinstance(expr, BinOp):
        return [expr.left, expr.right]
    if isinstance(expr, UnaryOp):
        return [expr.operand]
    if isinstance(expr, (Func, Coalesce)):
        return list(getattr(expr, 'args', [])) if isinstance(expr, Func) else list(getattr(expr, 'exprs', []))
    if isinstance(expr, If):
        return [expr.condition, expr.then_, expr.else_]
    if isinstance(expr, (IsNull, StrOp)):
        res = [expr.operand]
        if hasattr(expr, 'arg') and expr.arg:
            res.append(expr.arg)
        return res
    return []


def _walk_ids(expr: Expr, seen: set[int]) -> None:
    """Walk an Expr tree collecting node ids."""
    nid = id(expr)
    if nid in seen:
        return
    seen.add(nid)
    for child in _get_children(expr):
        _walk_ids(child, seen)


def expr_to_executable_sql(expr: Expr, ctx: dict[str, float]) -> str:
    """Wrap an Expr's to_sql() fragment in a complete executable query."""
    cols = ", ".join(f'{rate} AS "{name}"' for name, rate in ctx.items())
    fragment = expr.to_sql()
    return f'WITH pillars AS (SELECT {cols})\nSELECT ({fragment}) AS result FROM pillars'
