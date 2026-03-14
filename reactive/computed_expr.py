from reactive.computed import ComputedProperty

class CallableFloat(float):
    """A float returned by computed_expr properties that can be called to get its underlying Expr tree."""
    def __new__(cls, value, expr):
        obj = super().__new__(cls, float(value))
        obj._expr_tree = expr
        return obj

    def __call__(self, ctx=None):
        if ctx is not None:
            from reactive.expr import eval_cached
            return eval_cached(self._expr_tree, ctx)
        return self._expr_tree

class CallableStr(str):
    """A str returned by computed_expr properties that can be called."""
    def __new__(cls, value, expr):
        obj = super().__new__(cls, str(value))
        obj._expr_tree = expr
        return obj

    def __call__(self, ctx=None):
        if ctx is not None:
            from reactive.expr import eval_cached
            return eval_cached(self._expr_tree, ctx)
        return self._expr_tree

class CallableDict(dict):
    """A dict returned by computed_expr properties that can be called."""
    def __init__(self, val_dict, expr_dict):
        super().__init__(val_dict)
        self._expr_tree = expr_dict

    def __call__(self, ctx=None):
        if ctx is not None:
            from reactive.expr import eval_cached
            return {k: eval_cached(v, ctx) for k, v in self._expr_tree.items()}
        return self._expr_tree


class computed_expr(ComputedProperty):
    """
    Decorator for purely symbolic properties on a pricing model.
    The wrapped method should build and return an `Expr` tree (e.g. `dv01()`).
    
    This replaces boilerplate by:
      1. Automatically caching the built `Expr` on the instance.
      2. Returning a `CallableFloat` via the reactive framework:
         `swap.dv01` → returns the float value.
         `swap.dv01()` → returns the `Expr` tree.
      3. Injecting `eval_{name}` method to support evaluation with a custom context.
    """
    def __init__(self, fn):
        super().__init__(self._compute_val, None, fn.__name__)
        self.expr_fn = fn
        self.expr_attr = f"_{fn.__name__}_cache"

    def _compute_val(self, instance):
        expr = self.get_expr(instance)
        if expr is None:
            return CallableFloat(0.0, None)
        from reactive.expr import eval_cached
        ctx = getattr(instance, "pillar_context", lambda: {})()
        
        if isinstance(expr, dict):
            val = {k: eval_cached(v, ctx) for k, v in expr.items()}
            return CallableDict(val, expr)
            
        val = eval_cached(expr, ctx)
        if isinstance(val, str):
            return CallableStr(val, expr)
        if isinstance(val, dict):
            return CallableDict(val, expr)
        return CallableFloat(val, expr)

    def get_expr(self, instance):
        curve = getattr(instance, "curve", getattr(instance, "discount_curve", getattr(instance, "leg1_discount_curve", getattr(instance, "leg2_discount_curve", None))))
        if curve is None:
            return None
        # Short-circuit if curve has no points (causes Expr generation issues)
        if not hasattr(curve, "df"):
            return None

        return self.expr_fn(instance)

    def __set_name__(self, owner, name):
        super().__set_name__(owner, name)
        
        # Inject `eval_<name>` method
        def _eval(instance, ctx, self=self):
            expr = self.get_expr(instance)
            if expr is None: return 0.0
            from reactive.expr import eval_cached
            if isinstance(expr, dict):
                return {k: eval_cached(v, ctx) for k, v in expr.items()}
            return eval_cached(expr, ctx)
        setattr(owner, f"eval_{name}", _eval)
