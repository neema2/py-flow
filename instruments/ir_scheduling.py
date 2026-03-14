"""
ir_scheduling — Helpers for generating swap schedules.
"""

def rack_dates(tenor_years: float) -> list[float]:
    """Complete schedule rack: includes 0.0 and walks back from tenor.

    Short front stub convention: last date is the tenor, then walk back
    by 1.0, stopping at 0.0.

    Examples:
        rack_dates(5.0) → [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
        rack_dates(2.5) → [0.0, 0.5, 1.5, 2.5]
    """
    if tenor_years < 0:
        raise ValueError(f"tenor_years must be >= 0, got {tenor_years}")

    dates = []
    t = float(tenor_years)
    while t > 1e-9:
        dates.append(t)
        t -= 1.0
    dates.append(0.0)
    return sorted(dates)


def payment_dates(tenor_years: float) -> list[float]:
    """Payment dates = rack minus the first (0.0).

    These are the dates where coupons are paid.
    """
    return rack_dates(tenor_years)[1:]  # remove 0.0


def reset_dates(tenor_years: float) -> list[float]:
    """Float reset dates = rack minus the last (tenor_years).

    These are the dates where the floating rate is fixed (observed).
    """
    return rack_dates(tenor_years)[:-1]  # remove final
