"""Pure payment synthesis: no Kafka, no clock. Fraud scenarios arrive with the training data (M3)."""
import math
import random
from datetime import UTC, datetime
from itertools import accumulate

COUNTRIES = ["CO", "US", "MX", "BR", "AR", "ES", "RO", "DE"]
MCCS = ["5411", "5812", "5999", "4111", "5732", "7011", "5311", "5912"]
CHANNELS = ["web", "app", "pos"]


class Population:
    """Users with fixed habits (home country, devices, typical amount) and Zipf-like activity."""

    def __init__(self, n_users: int, seed: int, worker: int = 0, workers: int = 1) -> None:
        rng = random.Random(seed)
        self.user_ids = [f"u_{i:06d}" for i in range(n_users) if i % workers == worker]
        self._habits = {}
        for uid in [f"u_{i:06d}" for i in range(n_users)]:      # same draws for every worker
            self._habits[uid] = {
                "country": rng.choice(COUNTRIES),
                "devices": [f"d_{uid[2:]}_{k}" for k in range(rng.randint(1, 3))],
                "mu": rng.uniform(2.5, 5.0),                       # log-amount location
                "card": f"c_{uid[2:]}",
            }
        # Zipf weights over this worker's users: rank r gets 1/r
        self._cum_weights = list(accumulate(1.0 / (r + 1) for r in range(len(self.user_ids))))

    def habits(self, user_id: str) -> dict:
        return self._habits[user_id]

    def pick_user(self, rng: random.Random) -> str:
        return rng.choices(self.user_ids, cum_weights=self._cum_weights, k=1)[0]     # O(log n)


def next_payment(pop: Population, rng: random.Random, t_scheduled_ns: int, t_sent_ns: int) -> dict:
    uid = pop.pick_user(rng)
    h = pop.habits(uid)
    amount = round(math.exp(rng.gauss(h["mu"], 0.8)), 2)
    return {
        "payment_id": f"{rng.getrandbits(64):016x}{rng.getrandbits(64):016x}",
        "user_id": uid,
        "card_id": h["card"],
        "amount": max(amount, 0.01),
        "currency": "USD",
        "merchant_id": f"m_{rng.randint(0, 4999):04d}",
        "mcc": rng.choice(MCCS),
        "country": h["country"],
        "device_id": rng.choice(h["devices"]),
        "ip_country": h["country"] if rng.random() > 0.03 else rng.choice(COUNTRIES),
        "channel": rng.choice(CHANNELS),
        "event_time": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "t_scheduled_ns": t_scheduled_ns,
        "t_sent_ns": t_sent_ns,
    }
