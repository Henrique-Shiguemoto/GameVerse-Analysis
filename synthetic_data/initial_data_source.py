from faker import Faker
import random
import json
import csv
from datetime import datetime, timedelta
import os
import numpy as np

# Parameters
NUM_PLAYERS = 98938
NUM_SESSIONS = 1265893
NUM_PURCHASES = 245837
OUTPUT_DIR = "synthetic_data"

fake = Faker()
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Helper: Vectorized Weighted Day-of-Week Random Time ---
# Mon=0 ... Sun=6
dow_weights = np.array([3, 2, 1, 2, 4, 7, 6], dtype=float)  # weekdays lighter, weekends heavier
dow_weights = dow_weights / dow_weights.sum()

def weighted_random_time_fast(start_ts, end_ts, size, dow_weights):
    """Generate biased timestamps quickly without rejection sampling."""
    start_dt = datetime.fromtimestamp(start_ts)
    end_dt = datetime.fromtimestamp(end_ts)

    num_days = (end_dt.date() - start_dt.date()).days + 1
    all_days = [start_dt.date() + timedelta(days=i) for i in range(num_days)]
    day_of_week = np.array([d.weekday() for d in all_days])

    # Normalize weights by weekday
    day_weights = np.array([dow_weights[d] for d in day_of_week], dtype=float)
    day_weights /= day_weights.sum()

    # Step 1: pick days (biased)
    chosen_days = np.random.choice(all_days, size=size, p=day_weights)

    # Step 2: random seconds within the chosen day
    random_seconds = np.random.randint(0, 24*60*60, size=size)

    # Step 3: convert back to timestamps
    times = np.array([
        int((datetime.combine(day, datetime.min.time()) + timedelta(seconds=int(sec))).timestamp())
        for day, sec in zip(chosen_days, random_seconds)
    ], dtype=int)

    return times

# =========================
# Player Profiles
# =========================
mean_age = 24
sigma = 0.4
ages = np.random.lognormal(mean=np.log(mean_age), sigma=sigma, size=NUM_PLAYERS)

with open(os.path.join(OUTPUT_DIR, "player_profiles.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["plid", "country", "age", "acc_creat_date", "gender"])

    country_weights = {
        "US": 50, "GB": 20, "CA": 15, "AU": 10, "DE": 10,
        "IN": 25, "CN": 30, "BR": 20, "ZA": 5, "MX": 15,
        "JP": 15, "FR": 10, "IT": 10, "Others": 10
    }

    codes = list(country_weights.keys())
    weights = np.array(list(country_weights.values()), dtype=float)
    weights /= weights.sum()

    # Vectorized sampling
    countries = np.random.choice(codes, size=NUM_PLAYERS, p=weights)
    ages_int = np.round(ages).astype(int)
    genders = np.random.choice([1, 2, 3], size=NUM_PLAYERS, p=[3/11, 7/11, 1/11])

    acc_start = datetime.now() - timedelta(days=365)
    acc_start_ts = int(acc_start.timestamp())
    acc_end_ts = int(datetime.now().timestamp())
    acc_timestamps = np.random.randint(acc_start_ts, acc_end_ts, size=NUM_PLAYERS)
    acc_dates = [datetime.fromtimestamp(int(ts)).date().isoformat() for ts in acc_timestamps]

    for i in range(NUM_PLAYERS):
        writer.writerow([
            f"P{i+1:05d}",
            countries[i],
            int(ages_int[i]),
            acc_dates[i],
            int(genders[i])
        ])

print("player_profiles.csv written!")

# =========================
# Player Sessions
# =========================
player_ids = [f"P{i+1:05d}" for i in range(NUM_PLAYERS)]
player_indexes = list(range(NUM_PLAYERS))

# Player activity weights (log-normal to simulate hardcore/casual mix)
activity_weights = np.random.lognormal(mean=0, sigma=1, size=NUM_PLAYERS)
activity_weights = activity_weights / activity_weights.sum()

end_date = datetime.now() - timedelta(days=1)
start_date = end_date - timedelta(days=90)
start_ts = int(start_date.timestamp())
end_ts = int(end_date.timestamp())

# Pick players for sessions
player_indices = np.random.choice(player_indexes, size=NUM_SESSIONS, p=activity_weights)
player_ids_sessions = [player_ids[int(idx)] for idx in player_indices]

# Generate login times with weekend bias (fast)
login_times = weighted_random_time_fast(start_ts, end_ts, NUM_SESSIONS, dow_weights)

# Assign session lengths
weights_for_players = activity_weights[player_indices]
session_minutes = np.zeros(NUM_SESSIONS, dtype=int)
session_minutes[weights_for_players < 0.00005] = np.random.randint(5, 60, size=(weights_for_players < 0.00005).sum())
session_minutes[(weights_for_players >= 0.00005) & (weights_for_players < 0.0005)] = np.random.randint(20, 150, size=((weights_for_players >= 0.00005) & (weights_for_players < 0.0005)).sum())
session_minutes[weights_for_players >= 0.0005] = np.random.randint(20, 300, size=(weights_for_players >= 0.0005).sum())

logout_times = login_times + (session_minutes * 60)

# Platform distribution
platforms = np.random.choice([1, 2, 3, 4], size=NUM_SESSIONS, p=[7/14, 3/14, 3/14, 1/14])

# =========================
# Actions distribution (global)
# =========================

# 0 - 'PvP Battle'
# 1 - 'Dungeon Raid'
# 2 - 'Crafting'
# 3 - 'Exploration'
# 4 - 'Trading'
# 5 - 'Gathering'
# 6 - 'Questing / Story Progression'
# 7 - 'Social Interaction'

actions = list(range(8))
action_weights = np.array([20, 30, 5, 15, 10, 8, 7, 25], dtype=float)
action_prob = action_weights / action_weights.sum()

with open(os.path.join(OUTPUT_DIR, "game_logs.json"), "w", encoding="utf-8") as f:
    for i in range(NUM_SESSIONS):
        # Number of actions depends on session length (~1 per 10 min)
        n_actions = np.random.poisson(lam=session_minutes[i] / 10)
        n_actions = max(1, min(n_actions, 50))  # keep reasonable bounds

        actions_chosen = np.random.choice(actions, size=n_actions, p=action_prob).tolist()

        session = {
            "_player_id": str(player_ids_sessions[i]),
            "_session_id": f"S{i+1:06d}",
            "_login": datetime.fromtimestamp(int(login_times[i])).isoformat(),
            "_logout": datetime.fromtimestamp(int(logout_times[i])).isoformat(),
            "_action": actions_chosen,
            "_platform": int(platforms[i])
        }
        f.write(json.dumps(session) + "\n")

print("game_logs.json written!")

# =========================
# Player Purchases
# =========================
items = list(range(15))
item_weights = [random.randint(1, 100) for _ in items]
item_prob = np.array(item_weights) / sum(item_weights)
item_prices = [round(random.uniform(0.99, 49.99), 2) for _ in items]

chosen_items = np.random.choice(items, size=NUM_PURCHASES, p=item_prob)
pmethods = np.random.choice([1, 2, 3], size=NUM_PURCHASES, p=[5/9, 1/9, 3/9])
purchase_players = np.random.choice(player_ids, size=NUM_PURCHASES, p=activity_weights)

# Purchase times with weekend bias (fast)
purchase_times = weighted_random_time_fast(start_ts, end_ts, NUM_PURCHASES, dow_weights)

with open(os.path.join(OUTPUT_DIR, "purchases.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["transaction_id", "player_id", "item", "item_price", "pmethod", "time"])

    for i in range(NUM_PURCHASES):
        writer.writerow([
            f"T{i+1:07d}",
            str(purchase_players[i]),
            int(chosen_items[i]),
            float(item_prices[int(chosen_items[i])]),
            int(pmethods[i]),
            datetime.fromtimestamp(int(purchase_times[i])).isoformat()
        ])

print("purchases.csv written!")
