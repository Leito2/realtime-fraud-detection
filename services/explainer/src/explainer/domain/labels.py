"""Plain-language names of the features, for templates and prompts."""
LABELS = {
    "amount": "payment amount", "f_cnt_10m": "payments by this user in the last 10 minutes",
    "f_sum_10m": "amount spent in the last 10 minutes", "f_max_10m": "largest payment in the last 10 minutes",
    "f_small_tx_cnt_10m": "small test-size payments in the last 10 minutes",
    "avg_amount_30d": "user's average payment (30 days)",
    "std_amount_30d": "spread of the user's payments",
    "n_known_devices": "devices known for this user", "account_age_days": "account age in days",
    "amount_to_avg_ratio": "amount relative to the user's average",
    "amount_zscore": "how unusual the amount is",
    "is_foreign": "payment from outside the home country",
    "ip_country_mismatch": "IP country differs from card country",
}


def label(feature: str) -> str:
    return LABELS.get(feature, feature)
