from faker import Faker
import random
import json
import csv
from datetime import datetime, timedelta
import os
import numpy as np

# Parameters
NUM_PLAYERS = 51635
NUM_SESSIONS = 254726
NUM_PURCHASES = 21287
OUTPUT_DIR = "synthetic_data"

fake = Faker()
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Generating ages for players according to a distribution which is a bit skewed
mean_age = 24
sigma = 0.4
ages = np.random.lognormal(mean=np.log(mean_age), sigma=sigma, size=NUM_PLAYERS)

# Player profiles, in this case I'm saving in a csv file, but this could've come from some server snapshot
players = []
with open(os.path.join(OUTPUT_DIR, "player_profiles.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["plid", "country", "age", "acc_creat_date", "gender"])
    for i in range(NUM_PLAYERS):
        pid = f"P{i+1:05d}"
        player = {
            "plid": pid,
            "country": fake.country_code(),
            "age": np.round(ages[i]).astype(int),
            "acc_creat_date": fake.date_between(start_date="-1y", end_date="today").isoformat(),
            "gender": random.choices(population=[1, 2, 3], weights=[3, 7, 1], k=1)[0] # The players will be more frequent in the male gender class
        }
        players.append(player)
        writer.writerow(player.values())

# Player sessions, in this case I'm saving in a json file
end_date = datetime.now() - timedelta(days=1)
start_date = end_date - timedelta(days=90)
with open(os.path.join(OUTPUT_DIR, "game_logs.json"), "w", encoding="utf-8") as f:
    for i in range(NUM_SESSIONS):
        login = fake.date_time_between(start_date=start_date, end_date=end_date)
        logout = login + timedelta(minutes=random.randint(10, 180))
        session = {
            "_player_id": random.choice(players)["plid"],
            "_session_id": f"S{i+1:06d}",
            "_login": login.isoformat(),
            "_logout": logout.isoformat(),
            "_action": list(set(random.choices(population=[1, 2, 3, 4], weights=[7, 3, 6, 2], k=4))), # 1 - PvP Battle, 2 - Item_Crafted, 3 - Exploration, 4 - "Story"
            "_platform": random.choices(population=[1, 2, 3, 4], weights=[7, 3, 3, 1], k=1)[0] # 1 - "PC", 2 - "PS5", 3 - "Xbox Series X", 4 - "Mobile"
        }
        f.write(json.dumps(session) + "\n")

# Player purchases, also saving it in a csv file, in the real world this could've come from a service like SalesForce or something
with open(os.path.join(OUTPUT_DIR, "purchases.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["transaction_id", "player_id", "item", "item_price", "pmethod", "time"])
    for i in range(NUM_PURCHASES):
        pid = random.choice(players)["plid"]
        purchase = {
            "transaction_id": f"T{i+1:07d}",
            "player_id": pid,
            "item": random.choices(population=[1, 2, 3, 4, 5, 6, 7], weights=[2, 1, 1, 4, 7, 4, 5], k=1)[0], # 1 - Sword of Dawn, 2 - Shield of Ages, 3 - Health Potion, 4 - Mana Potion, 5 - 100 Gems, 6 - Skin Pack, 7 - Expansion Pass
            "item_price": round(random.uniform(0.99, 49.99), 2), # TODO(Rick): I should define the value in dollars for each item and maybe choose the volume randomly
            "pmethod": random.choices(population=[1, 2, 3], weights=[5, 1, 3], k=1)[0], # 1 - CreditCard, 2 - PayPal, 3 - GiftCard
            "time": fake.date_time_between(start_date=start_date, end_date=end_date).isoformat()
        }
        writer.writerow(purchase.values())
