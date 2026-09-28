#!/usr/bin/env python3
"""
ETL Pipeline for Steam Games Analytics Data Warehouse (DM_Project)
Extracts raw Kaggle CSVs, transforms data into Star Schema dimensions and facts,
and loads into PostgreSQL (primary Data Warehouse) with optional SQLite fallback.
"""

import os
import sys
import argparse
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, date

try:
    import psycopg2
    from psycopg2.extras import execute_values
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, "data", "processed")
SQL_SCHEMA_FILE = os.path.join(BASE_DIR, "scripts", "schema.sql")
SQLITE_DB_FILE = os.path.join(BASE_DIR, "data", "processed", "steam_dw.db")

os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# -----------------------------------------------------------------------------
# Database Connection Helpers
# -----------------------------------------------------------------------------

def get_postgres_connection(dbname="steam_dw", user="root", host="localhost", port=5432, password=None):
    """Establishes connection to PostgreSQL Data Warehouse."""
    if not HAS_PSYCOPG2:
        raise ImportError("psycopg2 is required for PostgreSQL connection. Install it with: pip install psycopg2-binary")
    
    # Try unix domain socket first (common in linux root/postgres environments)
    try:
        conn = psycopg2.connect(dbname=dbname)
        return conn
    except Exception:
        pass
    
    # Try host/user parameters
    conn_kwargs = {"dbname": dbname, "host": host, "port": port}
    if user:
        conn_kwargs["user"] = user
    if password:
        conn_kwargs["password"] = password
    return psycopg2.connect(**conn_kwargs)

def get_sqlite_connection():
    """Establishes connection to SQLite fallback."""
    conn = sqlite3.connect(SQLITE_DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

# -----------------------------------------------------------------------------
# Extraction
# -----------------------------------------------------------------------------

def extract_raw_data():
    """Extract raw CSV files into Pandas DataFrames."""
    games_csv = os.path.join(RAW_DATA_DIR, "steam_games.csv")
    reviews_csv = os.path.join(RAW_DATA_DIR, "steam_reviews.csv")
    
    print("\n[ETL - Extract] Reading raw CSV source datasets...")
    if not os.path.exists(games_csv) or not os.path.exists(reviews_csv):
        raise FileNotFoundError("Raw CSV files not found in data/raw/. Run generate_dataset.py first.")
        
    df_games = pd.read_csv(games_csv)
    df_reviews = pd.read_csv(reviews_csv)
    
    print(f"  ✓ Extracted {len(df_games):,} records from steam_games.csv")
    print(f"  ✓ Extracted {len(df_reviews):,} records from steam_reviews.csv")
    return df_games, df_reviews

# -----------------------------------------------------------------------------
# PostgreSQL Pipeline Implementation
# -----------------------------------------------------------------------------

def run_postgres_etl(df_games, df_reviews, pg_config):
    """Executes full ETL cycle targeting PostgreSQL Data Warehouse."""
    print("\n" + "="*70)
    print("      EXECUTING ETL PIPELINE ON POSTGRESQL (STAR SCHEMA)")
    print("="*70)
    
    conn = get_postgres_connection(**pg_config)
    conn.autocommit = False
    cur = conn.cursor()
    
    try:
        # Step 1: Initialize Schema DDL
        print("[PostgreSQL ETL] Step 1: Initializing Star Schema tables and indexes...")
        with open(SQL_SCHEMA_FILE, "r", encoding="utf-8") as f:
            ddl_sql = f.read()
        cur.execute(ddl_sql)
        conn.commit()
        print("  ✓ Staging, Dimension, Bridge, Fact tables & Indexes created successfully.")
        
        # Step 2: Ingest into Staging Tables
        print("[PostgreSQL ETL] Step 2: Ingesting raw datasets into Staging Area...")
        games_stg = [
            (
                int(row["app_id"]),
                str(row["name"])[:255],
                str(row["release_date"]),
                str(row["developer"])[:255] if pd.notna(row["developer"]) else None,
                str(row["publisher"])[:255] if pd.notna(row["publisher"]) else None,
                str(row["genres"]) if pd.notna(row["genres"]) else None,
                str(row["categories"]) if pd.notna(row["categories"]) else None,
                float(row["original_price"]) if pd.notna(row["original_price"]) else 0.0,
                float(row["discount_price"]) if pd.notna(row["discount_price"]) else 0.0,
                int(row["positive_reviews"]) if pd.notna(row["positive_reviews"]) else 0,
                int(row["negative_reviews"]) if pd.notna(row["negative_reviews"]) else 0,
                str(row["owners"])[:100] if pd.notna(row["owners"]) else None
            )
            for _, row in df_games.iterrows()
        ]
        execute_values(
            cur,
            """
            INSERT INTO stg_steam_games (
                app_id, name, release_date, developer, publisher,
                genres, categories, original_price, discount_price,
                positive_reviews, negative_reviews, owners
            ) VALUES %s
            """,
            games_stg
        )
        
        reviews_stg = [
            (
                int(row["review_id"]),
                int(row["app_id"]),
                str(row["author_id"])[:100],
                str(row["review_text"]) if pd.notna(row["review_text"]) else "",
                bool(row["voted_up"]),
                int(row["votes_up"]),
                int(row["votes_funny"]),
                int(row["playtime_forever"]),
                int(row["timestamp_created"])
            )
            for _, row in df_reviews.iterrows()
        ]
        execute_values(
            cur,
            """
            INSERT INTO stg_steam_reviews (
                review_id, app_id, author_id, review_text, voted_up,
                votes_up, votes_funny, playtime_forever, timestamp_created
            ) VALUES %s
            """,
            reviews_stg
        )
        conn.commit()
        print(f"  ✓ Staging loaded: {len(games_stg)} games, {len(reviews_stg)} reviews.")
        
        # Step 3: Populate Dimensions
        print("[PostgreSQL ETL] Step 3: Transforming and loading Dimension Tables...")
        
        # 3.1 dim_developer
        aaa_devs = ["Valve", "CD Projekt Red", "FromSoftware", "Bethesda Softworks", "Capcom", "Square Enix", "Larian Studios", "Bandai Namco", "Ubisoft"]
        aa_devs = ["Paradox Development Studio", "Techland"]
        dev_records = []
        for dev in df_games["developer"].dropna().unique():
            dev_str = str(dev).strip()
            if dev_str in aaa_devs:
                tier = "AAA"
            elif dev_str in aa_devs:
                tier = "AA"
            else:
                tier = "Indie"
            dev_records.append((dev_str, "International", tier))
            
        execute_values(
            cur,
            """
            INSERT INTO dim_developer (developer_name, country, dev_tier)
            VALUES %s
            ON CONFLICT (developer_name) DO NOTHING
            """,
            dev_records
        )
        
        # 3.2 dim_genre
        all_genres = set()
        for g_list in df_games["genres"].dropna():
            for g in str(g_list).split(","):
                g_clean = g.strip()
                if g_clean:
                    all_genres.add(g_clean)
        genre_records = [
            (g, "Core" if g in ["Action", "RPG", "Strategy"] else "Specialty")
            for g in sorted(all_genres)
        ]
        execute_values(
            cur,
            """
            INSERT INTO dim_genre (genre_name, genre_category)
            VALUES %s
            ON CONFLICT (genre_name) DO NOTHING
            """,
            genre_records
        )
        
        # 3.3 dim_game
        game_records = []
        for _, row in df_games.iterrows():
            app_id = int(row["app_id"])
            title = str(row["name"])[:255]
            dev = str(row["developer"])[:255] if pd.notna(row["developer"]) else "Unknown"
            pub = str(row["publisher"])[:255] if pd.notna(row["publisher"]) else "Unknown"
            try:
                rel_dt = datetime.strptime(str(row["release_date"]), "%Y-%m-%d").date()
                rel_year = rel_dt.year
            except Exception:
                rel_dt = date(2020, 1, 1)
                rel_year = 2020
            is_indie = dev not in aaa_devs
            game_records.append((app_id, title, dev, pub, "E10+", rel_dt, rel_year, is_indie))
            
        execute_values(
            cur,
            """
            INSERT INTO dim_game (app_id, game_title, developer_name, publisher_name, age_rating, release_date, release_year, is_indie)
            VALUES %s
            ON CONFLICT (app_id) DO NOTHING
            """,
            game_records
        )
        
        # 3.4 bridge_game_genre
        cur.execute("SELECT app_id, game_key FROM dim_game;")
        game_map = {row[0]: row[1] for row in cur.fetchall()}
        cur.execute("SELECT genre_name, genre_key FROM dim_genre;")
        genre_map = {row[0]: row[1] for row in cur.fetchall()}
        
        bridge_records = []
        for _, row in df_games.iterrows():
            app_id = int(row["app_id"])
            g_key = game_map.get(app_id)
            if g_key and pd.notna(row["genres"]):
                for g in str(row["genres"]).split(","):
                    g_clean = g.strip()
                    genre_k = genre_map.get(g_clean)
                    if genre_k:
                        bridge_records.append((g_key, genre_k))
                        
        execute_values(
            cur,
            """
            INSERT INTO bridge_game_genre (game_key, genre_key)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            bridge_records
        )
        
        # 3.5 dim_user
        user_counts = df_reviews["author_id"].value_counts()
        user_records = []
        for author_id, count in user_counts.items():
            if count >= 5:
                level = "Expert"
            elif count >= 3:
                level = "Enthusiast"
            else:
                level = "Casual"
            user_records.append((str(author_id)[:100], int(count), level))
            
        execute_values(
            cur,
            """
            INSERT INTO dim_user (author_id, total_reviews_written, reviewer_level)
            VALUES %s
            ON CONFLICT (author_id) DO NOTHING
            """,
            user_records
        )
        
        # 3.6 dim_time
        rev_timestamps = pd.to_datetime(df_reviews["timestamp_created"], unit="s", utc=True).dt.date
        game_dates = pd.to_datetime(df_games["release_date"]).dt.date
        unique_dates = sorted(list(set(rev_timestamps).union(set(game_dates))))
        
        time_records = []
        for d in unique_dates:
            time_key = int(d.strftime("%Y%m%d"))
            year = d.year
            quarter = (d.month - 1) // 3 + 1
            month = d.month
            month_name = d.strftime("%B")
            day = d.day
            day_of_week = d.isoweekday()
            day_name = d.strftime("%A")
            is_weekend = day_of_week in [6, 7]
            time_records.append((time_key, d, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend))
            
        execute_values(
            cur,
            """
            INSERT INTO dim_time (time_key, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend)
            VALUES %s
            ON CONFLICT (time_key) DO NOTHING
            """,
            time_records
        )
        conn.commit()
        print("  ✓ Dimension and Bridge tables populated.")
        
        # Step 4: Populate Fact Table
        print("[PostgreSQL ETL] Step 4: Transforming and loading Fact Table (fact_game_reviews_analytics)...")
        cur.execute("SELECT developer_name, developer_key FROM dim_developer;")
        dev_map = {row[0]: row[1] for row in cur.fetchall()}
        
        cur.execute("SELECT author_id, user_key FROM dim_user;")
        user_map = {row[0]: row[1] for row in cur.fetchall()}
        
        cur.execute("SELECT app_id, game_key, developer_name FROM dim_game;")
        game_rows = cur.fetchall()
        game_map = {row[0]: row[1] for row in game_rows}
        game_dev_map = {row[0]: row[2] for row in game_rows}
        
        cur.execute("SELECT time_key FROM dim_time;")
        valid_time_keys = set(row[0] for row in cur.fetchall())
        
        games_dict = df_games.set_index("app_id").to_dict(orient="index")
        df_reviews["rev_date"] = pd.to_datetime(df_reviews["timestamp_created"], unit="s", utc=True).dt.date
        
        fact_records = []
        for _, rev in df_reviews.iterrows():
            app_id = int(rev["app_id"])
            game_k = game_map.get(app_id)
            if not game_k:
                continue
                
            rev_dt = rev["rev_date"]
            time_k = int(rev_dt.strftime("%Y%m%d"))
            if time_k not in valid_time_keys:
                continue
                
            user_k = user_map.get(str(rev["author_id"]))
            if not user_k:
                continue
                
            dev_name = game_dev_map.get(app_id)
            dev_k = dev_map.get(dev_name)
            if not dev_k:
                continue
                
            g_info = games_dict.get(app_id, {})
            orig_price = float(g_info.get("original_price", 0.0))
            disc_price = float(g_info.get("discount_price", orig_price))
            pos_rev = int(g_info.get("positive_reviews", 0))
            neg_rev = int(g_info.get("negative_reviews", 0))
            total_rev = pos_rev + neg_rev
            pos_ratio = round(pos_rev / total_rev, 4) if total_rev > 0 else 0.0
            discount_pct = round(((orig_price - disc_price) / orig_price) * 100, 2) if orig_price > 0 else 0.0
            
            review_score = 1 if bool(rev["voted_up"]) else 0
            votes_up = int(rev["votes_up"])
            playtime_hours = round(float(rev["playtime_forever"]) / 60.0, 2)
            est_revenue = round(disc_price * total_rev * 1.5, 2)
            
            fact_records.append((
                game_k, time_k, user_k, dev_k,
                disc_price, discount_pct, review_score, votes_up,
                playtime_hours, pos_rev, neg_rev, pos_ratio, est_revenue
            ))
            
        execute_values(
            cur,
            """
            INSERT INTO fact_game_reviews_analytics (
                game_key, time_key, user_key, developer_key,
                price_usd, discount_pct, review_score, votes_up,
                playtime_hours, total_positive_reviews, total_negative_reviews,
                positive_ratio, estimated_revenue_usd
            ) VALUES %s
            """,
            fact_records
        )
        conn.commit()
        print(f"  ✓ Fact table loaded: {len(fact_records):,} analytical fact records.")
        
        # Summary
        print_summary(cur)
        
    finally:
        cur.close()
        conn.close()

# -----------------------------------------------------------------------------
# SQLite Pipeline Implementation (Portability Fallback)
# -----------------------------------------------------------------------------

def run_sqlite_etl(df_games, df_reviews):
    """Executes ETL cycle targeting SQLite local file for quick demo / portable testing."""
    print("\n" + "="*70)
    print("      EXECUTING ETL PIPELINE ON SQLITE (PORTABLE FALLBACK)")
    print("="*70)
    
    conn = get_sqlite_connection()
    cur = conn.cursor()
    
    try:
        with open(SQL_SCHEMA_FILE, "r", encoding="utf-8") as f:
            ddl_sql = f.read()
            
        sqlite_ddl = ddl_sql.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
        sqlite_ddl = sqlite_ddl.replace("BIGSERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
        sqlite_ddl = sqlite_ddl.replace("NUMERIC(10, 2)", "REAL")
        sqlite_ddl = sqlite_ddl.replace("NUMERIC(5, 2)", "REAL")
        sqlite_ddl = sqlite_ddl.replace("NUMERIC(12, 2)", "REAL")
        sqlite_ddl = sqlite_ddl.replace(" CASCADE;", ";")
        sqlite_ddl = sqlite_ddl.replace(" CASCADE\n", "\n")
        
        cur.executescript(sqlite_ddl)
        conn.commit()
        
        df_games.to_sql("stg_steam_games", conn, if_exists="replace", index=False)
        df_reviews.to_sql("stg_steam_reviews", conn, if_exists="replace", index=False)
        conn.commit()
        
        # Dimensions
        aaa_devs = ["Valve", "CD Projekt Red", "FromSoftware", "Bethesda Softworks", "Capcom", "Square Enix", "Larian Studios", "Bandai Namco", "Ubisoft"]
        dev_records = [(dev, "International", "AAA" if dev in aaa_devs else "Indie") for dev in df_games["developer"].dropna().unique()]
        cur.executemany("INSERT OR IGNORE INTO dim_developer (developer_name, country, dev_tier) VALUES (?, ?, ?)", dev_records)
        
        all_genres = set()
        for g_list in df_games["genres"].dropna():
            for g in str(g_list).split(","):
                g_clean = g.strip()
                if g_clean:
                    all_genres.add(g_clean)
        genre_records = [(g, "Core" if g in ["Action", "RPG", "Strategy"] else "Specialty") for g in sorted(all_genres)]
        cur.executemany("INSERT OR IGNORE INTO dim_genre (genre_name, genre_category) VALUES (?, ?)", genre_records)
        
        game_records = []
        for _, row in df_games.iterrows():
            app_id = int(row["app_id"])
            title = str(row["name"])[:255]
            dev = str(row["developer"]) if pd.notna(row["developer"]) else "Unknown"
            pub = str(row["publisher"]) if pd.notna(row["publisher"]) else "Unknown"
            try:
                rel_dt = datetime.strptime(str(row["release_date"]), "%Y-%m-%d").date()
                rel_year = rel_dt.year
            except Exception:
                rel_dt = date(2020, 1, 1)
                rel_year = 2020
            is_indie = dev not in aaa_devs
            game_records.append((app_id, title, dev, pub, "E10+", rel_dt.strftime("%Y-%m-%d"), rel_year, is_indie))
        cur.executemany("INSERT OR IGNORE INTO dim_game (app_id, game_title, developer_name, publisher_name, age_rating, release_date, release_year, is_indie) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", game_records)
        
        cur.execute("SELECT app_id, game_key FROM dim_game;")
        game_map = {row[0]: row[1] for row in cur.fetchall()}
        cur.execute("SELECT genre_name, genre_key FROM dim_genre;")
        genre_map = {row[0]: row[1] for row in cur.fetchall()}
        
        bridge_records = []
        for _, row in df_games.iterrows():
            app_id = int(row["app_id"])
            g_key = game_map.get(app_id)
            if g_key and pd.notna(row["genres"]):
                for g in str(row["genres"]).split(","):
                    genre_k = genre_map.get(g.strip())
                    if genre_k:
                        bridge_records.append((g_key, genre_k))
        cur.executemany("INSERT OR IGNORE INTO bridge_game_genre (game_key, genre_key) VALUES (?, ?)", bridge_records)
        
        user_counts = df_reviews["author_id"].value_counts()
        user_records = [(str(author_id), int(count), "Expert" if count >= 5 else "Enthusiast" if count >= 3 else "Casual") for author_id, count in user_counts.items()]
        cur.executemany("INSERT OR IGNORE INTO dim_user (author_id, total_reviews_written, reviewer_level) VALUES (?, ?, ?)", user_records)
        
        rev_timestamps = pd.to_datetime(df_reviews["timestamp_created"], unit="s", utc=True).dt.date
        game_dates = pd.to_datetime(df_games["release_date"]).dt.date
        unique_dates = sorted(list(set(rev_timestamps).union(set(game_dates))))
        time_records = []
        for d in unique_dates:
            time_key = int(d.strftime("%Y%m%d"))
            day_of_week = d.isoweekday()
            time_records.append((time_key, d.strftime("%Y-%m-%d"), d.year, (d.month - 1) // 3 + 1, d.month, d.strftime("%B"), d.day, day_of_week, d.strftime("%A"), day_of_week in [6, 7]))
        cur.executemany("INSERT OR IGNORE INTO dim_time (time_key, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", time_records)
        conn.commit()
        
        # Facts
        cur.execute("SELECT developer_name, developer_key FROM dim_developer;")
        dev_map = {row[0]: row[1] for row in cur.fetchall()}
        cur.execute("SELECT author_id, user_key FROM dim_user;")
        user_map = {row[0]: row[1] for row in cur.fetchall()}
        cur.execute("SELECT app_id, game_key, developer_name FROM dim_game;")
        game_rows = cur.fetchall()
        game_map = {row[0]: row[1] for row in game_rows}
        game_dev_map = {row[0]: row[2] for row in game_rows}
        cur.execute("SELECT time_key FROM dim_time;")
        valid_time_keys = set(row[0] for row in cur.fetchall())
        
        games_dict = df_games.set_index("app_id").to_dict(orient="index")
        df_reviews["rev_date"] = pd.to_datetime(df_reviews["timestamp_created"], unit="s", utc=True).dt.date
        
        fact_records = []
        for _, rev in df_reviews.iterrows():
            app_id = int(rev["app_id"])
            game_k = game_map.get(app_id)
            if not game_k:
                continue
            rev_dt = rev["rev_date"]
            time_k = int(rev_dt.strftime("%Y%m%d"))
            if time_k not in valid_time_keys:
                continue
            user_k = user_map.get(str(rev["author_id"]))
            if not user_k:
                continue
            dev_name = game_dev_map.get(app_id)
            dev_k = dev_map.get(dev_name)
            if not dev_k:
                continue
            g_info = games_dict.get(app_id, {})
            orig_price = float(g_info.get("original_price", 0.0))
            disc_price = float(g_info.get("discount_price", orig_price))
            pos_rev = int(g_info.get("positive_reviews", 0))
            neg_rev = int(g_info.get("negative_reviews", 0))
            total_rev = pos_rev + neg_rev
            pos_ratio = round(pos_rev / total_rev, 4) if total_rev > 0 else 0.0
            discount_pct = round(((orig_price - disc_price) / orig_price) * 100, 2) if orig_price > 0 else 0.0
            review_score = 1 if bool(rev["voted_up"]) else 0
            votes_up = int(rev["votes_up"])
            playtime_hours = round(float(rev["playtime_forever"]) / 60.0, 2)
            est_revenue = round(disc_price * total_rev * 1.5, 2)
            
            fact_records.append((
                game_k, time_k, user_k, dev_k,
                disc_price, discount_pct, review_score, votes_up,
                playtime_hours, pos_rev, neg_rev, pos_ratio, est_revenue
            ))
            
        cur.executemany("""
            INSERT INTO fact_game_reviews_analytics (
                game_key, time_key, user_key, developer_key,
                price_usd, discount_pct, review_score, votes_up,
                playtime_hours, total_positive_reviews, total_negative_reviews,
                positive_ratio, estimated_revenue_usd
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, fact_records)
        conn.commit()
        print(f"  ✓ SQLite Fact table loaded: {len(fact_records):,} records.")
        print_summary(cur)
    finally:
        conn.close()

def print_summary(cur):
    """Outputs table row counts from the database."""
    tables = [
        "stg_steam_games", "stg_steam_reviews",
        "dim_game", "dim_time", "dim_genre", "dim_developer", "dim_user",
        "bridge_game_genre", "fact_game_reviews_analytics"
    ]
    print("\n" + "-"*65)
    print(f"  {'DATA WAREHOUSE TABLE':<35} | {'ROW COUNT':>15}")
    print("-"*65)
    for t in tables:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        cnt = cur.fetchone()[0]
        print(f"  {t:<35} | {cnt:>15,}")
    print("-"*65 + "\n")

# -----------------------------------------------------------------------------
# Main Runner
# -----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Steam Games Analytics ETL Pipeline")
    parser.add_argument("--target", choices=["postgres", "sqlite", "both"], default="both",
                        help="Target database engine (default: both for maximum flexibility)")
    parser.add_argument("--pg-db", default=os.getenv("PGDATABASE", "steam_dw"), help="PostgreSQL database name")
    parser.add_argument("--pg-user", default=os.getenv("PGUSER", "root"), help="PostgreSQL username")
    parser.add_argument("--pg-host", default=os.getenv("PGHOST", "localhost"), help="PostgreSQL host")
    parser.add_argument("--pg-port", type=int, default=int(os.getenv("PGPORT", "5432")), help="PostgreSQL port")
    parser.add_argument("--pg-password", default=os.getenv("PGPASSWORD", None), help="PostgreSQL password")
    
    args = parser.parse_args()
    
    df_games, df_reviews = extract_raw_data()
    
    if args.target in ["postgres", "both"]:
        if HAS_PSYCOPG2:
            pg_config = {
                "dbname": args.pg_db,
                "user": args.pg_user,
                "host": args.pg_host,
                "port": args.pg_port,
                "password": args.pg_password
            }
            try:
                run_postgres_etl(df_games, df_reviews, pg_config)
            except Exception as e:
                print(f"[PostgreSQL Error] {e}")
                if args.target == "postgres":
                    raise
        else:
            print("[Warning] psycopg2 not installed. Skipping PostgreSQL.")
            
    if args.target in ["sqlite", "both"]:
        run_sqlite_etl(df_games, df_reviews)
        
    print("\n[ETL Pipeline] All ETL operations completed successfully!\n")

if __name__ == "__main__":
    main()
