def fmt(value: float) -> str:
    """9.5 -> '9.5', 7.0 -> '7', 950.0 -> '950', 0.93 -> '0.93': what a person would write."""
    return format(float(value), ".4g")
