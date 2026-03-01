"""
Aggregate helpers for @computed — group_by and rank_by.

These accept list-comprehension-style [(key, value)] tuples, keeping
the map step in user code (Pythonic, explicit, IDE-friendly) and
providing the fold/sort step as a declarative helper.

Usage inside @computed::

    from reactive.agg import group_by, rank_by

    @computed
    def sector_weights(self):
        return group_by([(p.sector, p.market_value) for p in self.positions],
                        normalize=True)

    @computed
    def top_risk_contributors(self):
        return rank_by([(p.symbol, p.var_1d_95) for p in self.positions],
                       as_pct=True)
"""


def group_by(pairs, *, normalize=False):
    """Group-and-sum over (key, value) pairs.

    Args:
        pairs: Iterable of (key, value) tuples — typically a list
            comprehension like ``[(p.sector, p.market_value) for p in items]``.
        normalize: If True, return percentages (summing to ~100) rounded
            to 1 decimal place instead of raw sums.

    Returns:
        dict mapping each unique key to the summed (or normalized) value.

    Example::

        group_by([("Tech", 100), ("Finance", 50), ("Tech", 200)])
        # → {"Tech": 300, "Finance": 50}

        group_by([("Tech", 100), ("Finance", 50), ("Tech", 200)], normalize=True)
        # → {"Tech": 85.7, "Finance": 14.3}
    """
    groups = {}
    for key, value in pairs:
        groups[key] = groups.get(key, 0) + value

    if not normalize:
        return groups

    total = sum(groups.values())
    if total == 0:
        return {k: 0.0 for k in groups}
    return {k: round(v / total * 100, 1) for k, v in groups.items()}


def rank_by(pairs, *, desc=True, as_pct=False):
    """Sort (label, value) pairs and optionally compute contribution %.

    Args:
        pairs: Iterable of (label, value) tuples — typically a list
            comprehension like ``[(p.symbol, p.var_1d_95) for p in items]``.
        desc: Sort descending (largest first). Default True.
        as_pct: If True, return contribution percentages instead of raw
            values. Contribution is computed as ``value² / sum(value²) * 100``
            (variance contribution for VaR-like quantities) when values can
            be negative/positive, or ``value / sum(values) * 100`` for simple
            share.

    Returns:
        List of dicts with ``"label"`` and either ``"value"`` or ``"pct"`` keys,
        sorted by value.

    Example::

        rank_by([("AAPL", 5000), ("NVDA", 8000), ("MSFT", 3000)])
        # → [{"label": "NVDA", "value": 8000},
        #    {"label": "AAPL", "value": 5000},
        #    {"label": "MSFT", "value": 3000}]

        rank_by([("AAPL", 5000), ("NVDA", 8000), ("MSFT", 3000)], as_pct=True)
        # → [{"label": "NVDA", "pct": 65.3},
        #    {"label": "AAPL", "pct": 25.5},
        #    {"label": "MSFT", "pct": 9.2}]
    """
    items = list(pairs)
    items.sort(key=_pair_sort_key, reverse=desc)

    if not as_pct:
        return [{"label": label, "value": value} for label, value in items]

    total_sq = sum(v ** 2 for _, v in items)
    if total_sq == 0:
        return [{"label": label, "pct": 0.0} for label, _ in items]
    return [
        {"label": label, "pct": round(value ** 2 / total_sq * 100, 1)}
        for label, value in items
    ]


def _pair_sort_key(pair):
    """Sort key for (label, value) pairs — extracts the value."""
    return pair[1]
