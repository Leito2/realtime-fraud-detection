"""Single source of truth for feature names, order, defaults and derived features (PLAN.md §4.3)."""
import math

VELOCITY = ["f_cnt_10m", "f_sum_10m", "f_max_10m", "f_small_tx_cnt_10m"]
PROFILE = ["avg_amount_30d", "std_amount_30d", "n_known_devices", "account_age_days"]
DERIVED = ["amount_to_avg_ratio", "amount_zscore", "is_foreign", "ip_country_mismatch"]
FEATURE_ORDER = ["amount", *VELOCITY, *PROFILE, *DERIVED]

# New-user defaults: training MUST use exactly the same values (PLAN.md §12, skew).
# Velocity window of the Flink job (flink/sql/*.sql); a test pins these to the SQL text.
WINDOW_S = 600
SMALL_TX_AMOUNT = 10.0

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


def build_row(event, profile: dict) -> dict:
    """Merge an enriched event (dict or pydantic model) with a profile into one feature row.

    Unknown users get ``PROFILE_DEFAULTS``; training must build rows through this same function.
    """
    e = event if isinstance(event, dict) else event.model_dump()
    prof = {**PROFILE_DEFAULTS, **{k: v for k, v in profile.items() if v is not None}}
    row = {"amount": e["amount"]}
    row.update({f: e[f] for f in VELOCITY})
    row.update({f: prof[f] for f in PROFILE})
    row.update(derived(e["amount"], e["country"], e["ip_country"], prof))
    return row


def velocity_reference(events) -> list[dict]:
    """Offline reference of the Flink OVER window (PLAN.md §4.3 parity).

    ``events``: dicts with ``user_id``, ``amount`` and ``ts`` (seconds), in arrival order per user.
    Each result includes the current payment; payments exactly ``WINDOW_S`` old are still inside.
    Training computes velocity with this function and a parity test compares it with Flink's output.
    """
    history: dict[str, list[tuple[float, float]]] = {}
    out = []
    for e in events:
        window = [(t, a) for t, a in history.get(e["user_id"], []) if t >= e["ts"] - WINDOW_S]
        window.append((e["ts"], e["amount"]))
        history[e["user_id"]] = window
        amounts = [a for _, a in window]
        out.append({
            "f_cnt_10m": len(amounts),
            "f_sum_10m": float(sum(amounts)),
            "f_max_10m": float(max(amounts)),
            "f_small_tx_cnt_10m": sum(1 for a in amounts if a < SMALL_TX_AMOUNT),
        })
    return out


def sync_velocity(snapshot: dict | None, amount: float, now_ts: float) -> dict:
    """Velocity for a payment scored synchronously (PLAN.md 14.2), from the Feature Writer's last state.

    The OVER window includes the current row (ADR-3), so the payment is added to the stored state. A state
    older than ``WINDOW_S`` is an empty window. The stored state still holds events that may have just left
    the window, so this approximates Flink at the window edge; the gap is measured, not assumed.
    """
    live = snapshot is not None and now_ts - float(snapshot["ts"]) <= WINDOW_S
    cnt = int(snapshot["f_cnt_10m"]) if live else 0
    return {
        "f_cnt_10m": cnt + 1,
        "f_sum_10m": (float(snapshot["f_sum_10m"]) if live else 0.0) + amount,
        "f_max_10m": max(float(snapshot["f_max_10m"]), amount) if live else amount,
        "f_small_tx_cnt_10m": (int(snapshot["f_small_tx_cnt_10m"]) if live else 0)
        + (1 if amount < SMALL_TX_AMOUNT else 0),
    }
