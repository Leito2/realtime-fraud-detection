"""Single source of truth for feature names, order, defaults and derived features (PLAN.md §4.3)."""
import math

VELOCITY = ["f_cnt_10m", "f_sum_10m", "f_max_10m", "f_small_tx_cnt_10m"]
PROFILE = ["avg_amount_30d", "std_amount_30d", "n_known_devices", "account_age_days"]
DERIVED = ["amount_to_avg_ratio", "amount_zscore", "is_foreign", "ip_country_mismatch"]
FEATURE_ORDER = ["amount", *VELOCITY, *PROFILE, *DERIVED]

# New-user defaults: training MUST use exactly the same values (PLAN.md §12, skew).
PROFILE_DEFAULTS = {"avg_amount_30d": 0.0, "std_amount_30d": 0.0, "n_known_devices": 0.0,
                    "account_age_days": 0.0, "home_country": None}


def derived(amount: float, country: str, ip_country: str, profile: dict) -> dict:
    avg = float(profile.get("avg_amount_30d") or 0.0)
    std = float(profile.get("std_amount_30d") or 0.0)
    home = profile.get("home_country")
    return {
        "amount_to_avg_ratio": amount / avg if avg > 0 else 0.0,
        "amount_zscore": (amount - avg) / std if std > 0 else 0.0,
        "is_foreign": float(home is not None and country != home),
        "ip_country_mismatch": float(country != ip_country),
    }


def vectorize(row: dict) -> list[float]:
    """Order features exactly as the model expects; missing values are a bug, not a NaN."""
    missing = [f for f in FEATURE_ORDER if f not in row]
    if missing:
        raise KeyError(f"missing features: {missing}")
    values = [float(row[f]) for f in FEATURE_ORDER]
    if any(math.isnan(v) for v in values):
        raise ValueError("NaN feature value")
    return values
