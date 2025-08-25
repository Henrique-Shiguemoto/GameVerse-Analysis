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

# Player Profiles
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

# Player Sessions
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

# Generate login times
login_times = np.random.randint(start_ts, end_ts, size=NUM_SESSIONS)

# Assign session lengths
weights_for_players = activity_weights[player_indices]
session_minutes = np.zeros(NUM_SESSIONS, dtype=int)
session_minutes[weights_for_players < 0.00005] = np.random.randint(5, 60, size=(weights_for_players < 0.00005).sum())
session_minutes[(weights_for_players >= 0.00005) & (weights_for_players < 0.0005)] = np.random.randint(20, 150, size=((weights_for_players >= 0.00005) & (weights_for_players < 0.0005)).sum())
session_minutes[weights_for_players >= 0.0005] = np.random.randint(20, 300, size=(weights_for_players >= 0.0005).sum())

logout_times = login_times + (session_minutes * 60)

# Platform distribution
platforms = np.random.choice([1, 2, 3, 4], size=NUM_SESSIONS, p=[7/14, 3/14, 3/14, 1/14])

# Actions distribution
actions = list(range(8))
action_prob = np.array([random.randint(1, 100) for _ in actions])
action_prob = action_prob / action_prob.sum()
actions_matrix = np.random.choice(actions, size=(NUM_SESSIONS, 8), p=action_prob)

with open(os.path.join(OUTPUT_DIR, "game_logs.json"), "w", encoding="utf-8") as f:
    for i in range(NUM_SESSIONS):
        session = {
            "_player_id": str(player_ids_sessions[i]),
            "_session_id": f"S{i+1:06d}",
            "_login": datetime.fromtimestamp(int(login_times[i])).isoformat(),
            "_logout": datetime.fromtimestamp(int(logout_times[i])).isoformat(),
            "_action": [int(a) for a in set(actions_matrix[i])],
            "_platform": int(platforms[i])
        }
        f.write(json.dumps(session) + "\n")

print("game_logs.json written!")

# Player Purchases
items = list(range(15))
item_weights = [random.randint(1, 100) for _ in items]
item_prob = np.array(item_weights) / sum(item_weights)
item_prices = [round(random.uniform(0.99, 49.99), 2) for _ in items]

chosen_items = np.random.choice(items, size=NUM_PURCHASES, p=item_prob)
pmethods = np.random.choice([1, 2, 3], size=NUM_PURCHASES, p=[5/9, 1/9, 3/9])
purchase_players = np.random.choice(player_ids, size=NUM_PURCHASES, p=activity_weights)
purchase_times = np.random.randint(start_ts, end_ts, size=NUM_PURCHASES)

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
