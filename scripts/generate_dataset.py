#!/usr/bin/env python3
"""
Synthetic Kaggle Dataset Generator for Steam Games & Reviews
Generates raw CSV files in data/raw matching standard Kaggle Steam dataset formats.
"""

import os
import random
import csv
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")
os.makedirs(DATA_DIR, exist_ok=True)

GAMES_CSV = os.path.join(DATA_DIR, "steam_games.csv")
REVIEWS_CSV = os.path.join(DATA_DIR, "steam_reviews.csv")

GENRES_LIST = [
    "Action", "RPG", "Strategy", "Indie", "Adventure", "Simulation", 
    "Sports & Racing", "Casual", "Massively Multiplayer", "Early Access"
]

CATEGORIES_LIST = [
    "Single-player", "Multi-player", "Co-op", "Steam Achievements",
    "Full controller support", "Steam Cloud", "Steam Trading Cards"
]

DEVELOPERS_POOLS = [
    ("Valve", "Publisher: Valve", "USA", "AAA"),
    ("CD Projekt Red", "CD Projekt", "Poland", "AAA"),
    ("FromSoftware", "Bandai Namco", "Japan", "AAA"),
    ("Bethesda Softworks", "Bethesda Softworks", "USA", "AAA"),
    ("Capcom", "Capcom", "Japan", "AAA"),
    ("Square Enix", "Square Enix", "Japan", "AAA"),
    ("Supergiant Games", "Supergiant Games", "USA", "Indie"),
    ("ConcernedApe", "ConcernedApe", "USA", "Indie"),
    ("ReLogic", "ReLogic", "USA", "Indie"),
    ("Team Cherry", "Team Cherry", "Australia", "Indie"),
    ("Larian Studios", "Larian Studios", "Belgium", "AAA"),
    ("Motion Twin", "Motion Twin", "France", "Indie"),
    ("Klei Entertainment", "Klei Entertainment", "Canada", "Indie"),
    ("Paradox Development Studio", "Paradox Interactive", "Sweden", "AA"),
    ("Techland", "Techland", "Poland", "AA")
]

GAME_TITLE_PREFIXES = [
    "Shadows of", "Cyber", "Kingdom of", "Eternal", "Chronicles of", "The Lost",
    "Overcooked", "Pixel", "Starlight", "Dragon", "Dungeon", "Galactic", "Age of",
    "Chrono", "Realm of", "Super", "Master", "Legend of", "Ghost", "Warhammer"
]

GAME_TITLE_SUFFIXES = [
    "Reckoning", "Odyssey", "Warfare", "Legends", "Valley", "Frontier",
    "Protocol", "Ascension", "Tactics", "Survivors", "Online", "Vanguard",
    "Simulator", "Rift", "Empire", "Outpost", "Reborn", "Zero", "Unbound"
]

REVIEW_COMMENTS_POSITIVE = [
    "Absolute masterpiece! Spent over 200 hours in this game.",
    "Great gameplay, amazing soundtrack, highly recommended!",
    "One of the best indie games I have played this year.",
    "Solid mechanics, great performance, active developers.",
    "10/10 worth every cent. Must buy during Steam sales!"
]

REVIEW_COMMENTS_NEGATIVE = [
    "Full of bugs, crashes on startup after recent update.",
    "Too repetitive and expensive for the amount of content.",
    "Poor optimization and awful monetization system.",
    "Overrated game. Story is weak and servers lag constantly.",
    "Not recommended in its current state, wait for patches."
]

def generate_games_dataset(num_games=350):
    print(f"[Generator] Generating {num_games} Steam Store games...")
    games = []
    
    start_date = datetime(2012, 1, 1)
    end_date = datetime(2025, 6, 1)
    date_range = (end_date - start_date).days
    
    app_ids = set()
    
    for i in range(num_games):
        app_id = random.randint(100000, 999999)
        while app_id in app_ids:
            app_id = random.randint(100000, 999999)
        app_ids.add(app_id)
        
        dev_info = random.choice(DEVELOPERS_POOLS)
        dev_name = dev_info[0]
        pub_name = dev_info[1]
        
        prefix = random.choice(GAME_TITLE_PREFIXES)
        suffix = random.choice(GAME_TITLE_SUFFIXES)
        title = f"{prefix} {suffix} {random.randint(1, 4) if random.random() > 0.7 else ''}".strip()
        
        rel_date = start_date + timedelta(days=random.randint(0, date_range))
        release_date_str = rel_date.strftime("%Y-%m-%d")
        
        # Genres (1 to 3)
        sample_genres = random.sample(GENRES_LIST, k=random.randint(1, 3))
        genres_str = ",".join(sample_genres)
        
        # Categories (2 to 4)
        sample_cats = random.sample(CATEGORIES_LIST, k=random.randint(2, 4))
        categories_str = ",".join(sample_cats)
        
        # Pricing
        if "Indie" in dev_info[3]:
            prices = [0.00, 4.99, 9.99, 14.99, 19.99, 24.99]
        else:
            prices = [0.00, 19.99, 29.99, 39.99, 49.99, 59.99, 69.99]
            
        original_price = random.choice(prices)
        discount_rate = random.choice([0.0, 0.10, 0.20, 0.33, 0.50, 0.75]) if original_price > 0 else 0.0
        discount_price = round(original_price * (1.0 - discount_rate), 2)
        
        # Review counts
        total_rev = random.randint(50, 50000)
        pos_ratio = random.uniform(0.40, 0.98)
        pos_reviews = int(total_rev * pos_ratio)
        neg_reviews = total_rev - pos_reviews
        
        owners = f"{random.choice([10, 50, 100, 500, 1000, 5000])}k-{random.choice([50, 100, 500, 1000, 5000, 10000])}k"
        
        games.append({
            "app_id": app_id,
            "name": title,
            "release_date": release_date_str,
            "developer": dev_name,
            "publisher": pub_name,
            "genres": genres_str,
            "categories": categories_str,
            "original_price": original_price,
            "discount_price": discount_price,
            "positive_reviews": pos_reviews,
            "negative_reviews": neg_reviews,
            "owners": owners
        })

    with open(GAMES_CSV, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=games[0].keys())
        writer.writeheader()
        writer.writerows(games)
        
    print(f"[Generator] Wrote {len(games)} records to {GAMES_CSV}")
    return games

def generate_reviews_dataset(games, num_reviews=4000):
    print(f"[Generator] Generating {num_reviews} user reviews...")
    reviews = []
    
    author_ids = [f"76561198{random.randint(10000000, 99999999)}" for _ in range(1200)]
    
    for i in range(num_reviews):
        review_id = 10000000 + i
        game = random.choice(games)
        app_id = game["app_id"]
        author_id = random.choice(author_ids)
        
        # Higher positive probability if game has high positive ratio
        game_pos_ratio = game["positive_reviews"] / max(1, (game["positive_reviews"] + game["negative_reviews"]))
        voted_up = random.random() < game_pos_ratio
        
        if voted_up:
            review_text = random.choice(REVIEW_COMMENTS_POSITIVE)
        else:
            review_text = random.choice(REVIEW_COMMENTS_NEGATIVE)
            
        votes_up = random.randint(0, 250) if random.random() > 0.7 else random.randint(0, 10)
        votes_funny = random.randint(0, 50) if random.random() > 0.85 else 0
        playtime_forever = random.randint(10, 15000) # minutes
        
        # Timestamp created (last 3 years)
        game_rel_date = datetime.strptime(game["release_date"], "%Y-%m-%d")
        now = datetime.now()
        if game_rel_date < now:
            days_since = (now - game_rel_date).days
            review_date = game_rel_date + timedelta(days=random.randint(0, max(1, days_since)))
        else:
            review_date = now
            
        timestamp_created = int(review_date.timestamp())
        
        reviews.append({
            "review_id": review_id,
            "app_id": app_id,
            "author_id": author_id,
            "review_text": review_text,
            "voted_up": voted_up,
            "votes_up": votes_up,
            "votes_funny": votes_funny,
            "playtime_forever": playtime_forever,
            "timestamp_created": timestamp_created
        })

    with open(REVIEWS_CSV, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=reviews[0].keys())
        writer.writeheader()
        writer.writerows(reviews)
        
    print(f"[Generator] Wrote {len(reviews)} records to {REVIEWS_CSV}")

def main():
    games = generate_games_dataset(num_games=350)
    generate_reviews_dataset(games, num_reviews=4000)
    print("[Generator] Dataset generation complete!")

if __name__ == "__main__":
    main()
