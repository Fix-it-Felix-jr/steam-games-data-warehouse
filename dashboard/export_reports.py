#!/usr/bin/env python3
"""
Visual Report Exporter for Steam DW (DM_Project)
Executes OLAP queries and exports high-resolution visual chart figures into dashboard/plots/
Supports PostgreSQL (primary) and SQLite (fallback).
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQLITE_DB_FILE = os.path.join(BASE_DIR, "data", "processed", "steam_dw.db")
PLOTS_DIR = os.path.join(BASE_DIR, "dashboard", "plots")

os.makedirs(PLOTS_DIR, exist_ok=True)

# Set global Matplotlib aesthetics
plt.style.use('ggplot')
plt.rcParams['font.size'] = 10
plt.rcParams['axes.labelsize'] = 11
plt.rcParams['axes.titlesize'] = 13
plt.rcParams['xtick.labelsize'] = 9
plt.rcParams['ytick.labelsize'] = 9
plt.rcParams['figure.titlesize'] = 14

def get_connection():
    if HAS_PSYCOPG2:
        try:
            return psycopg2.connect(dbname="steam_dw")
        except Exception:
            try:
                return psycopg2.connect(dbname="steam_dw", user="root", host="localhost")
            except Exception:
                pass
    return sqlite3.connect(SQLITE_DB_FILE)

def generate_visual_reports():
    print("[Visual Reports Engine] Connecting to Steam Data Warehouse...")
    conn = get_connection()
    is_sqlite = isinstance(conn, sqlite3.Connection)
    engine_name = "SQLite" if is_sqlite else "PostgreSQL"
    print(f"[Visual Reports Engine] Connected using {engine_name}.\n")
    
    # -------------------------------------------------------------------------
    # Chart 1: Revenue by Genre & Dev Tier (Stacked Bar Chart)
    # -------------------------------------------------------------------------
    print("[Visual Reports] Generating Chart 1: Revenue by Genre & Developer Tier...")
    q1 = """
        SELECT 
            g.genre_name,
            dev.dev_tier,
            ROUND((SUM(f.estimated_revenue_usd) / 1e6)::numeric, 2) AS revenue_millions
        FROM fact_game_reviews_analytics f
        JOIN dim_game dm ON f.game_key = dm.game_key
        JOIN bridge_game_genre bgg ON dm.game_key = bgg.game_key
        JOIN dim_genre g ON bgg.genre_key = g.genre_key
        JOIN dim_developer dev ON f.developer_key = dev.developer_key
        GROUP BY g.genre_name, dev.dev_tier
        ORDER BY revenue_millions DESC;
    """
    if is_sqlite:
        q1 = q1.replace("::numeric", "")
    df1 = pd.read_sql_query(q1, conn)
    pivot1 = df1.pivot(index='genre_name', columns='dev_tier', values='revenue_millions').fillna(0)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    pivot1.plot(kind='bar', stacked=True, color=['#3b82f6', '#10b981', '#f59e0b'], ax=ax, width=0.7)
    ax.set_title("Steam DW: Estimated Revenue by Genre & Developer Tier ($ Millions)", fontweight='bold')
    ax.set_xlabel("Game Genre")
    ax.set_ylabel("Revenue ($ Millions)")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    chart1_path = os.path.join(PLOTS_DIR, "revenue_by_genre.png")
    plt.savefig(chart1_path, dpi=300)
    plt.close()
    print(f"  ✓ Saved: {chart1_path}")

    # -------------------------------------------------------------------------
    # Chart 2: QoQ Revenue Growth Trend (Line Chart)
    # -------------------------------------------------------------------------
    print("[Visual Reports] Generating Chart 2: Quarter-over-Quarter Revenue Trend...")
    q2 = """
        SELECT 
            (t.year || '-Q' || t.quarter) AS year_quarter,
            ROUND((SUM(f.estimated_revenue_usd) / 1e6)::numeric, 2) AS quarterly_revenue_m
        FROM fact_game_reviews_analytics f
        JOIN dim_time t ON f.time_key = t.time_key
        GROUP BY t.year, t.quarter
        ORDER BY t.year, t.quarter;
    """
    if is_sqlite:
        q2 = q2.replace("::numeric", "")
    df2 = pd.read_sql_query(q2, conn)
    
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(df2['year_quarter'], df2['quarterly_revenue_m'], marker='o', color='#10b981', linewidth=2, markersize=4)
    ax.set_title("Steam DW: Quarter-over-Quarter Revenue Evolution (2012-2025)", fontweight='bold')
    ax.set_xlabel("Time (Year-Quarter)")
    ax.set_ylabel("Revenue ($ Millions)")
    plt.xticks(rotation=90, fontsize=8)
    plt.tight_layout()
    chart2_path = os.path.join(PLOTS_DIR, "qoq_revenue_trend.png")
    plt.savefig(chart2_path, dpi=300)
    plt.close()
    print(f"  ✓ Saved: {chart2_path}")

    # -------------------------------------------------------------------------
    # Chart 3: Price Elasticity vs User Approval Rating (Dual Axis Bar & Line)
    # -------------------------------------------------------------------------
    print("[Visual Reports] Generating Chart 3: Price Bracket Analysis...")
    q3 = """
        SELECT 
            CASE 
                WHEN f.price_usd = 0 THEN '1. Free'
                WHEN f.price_usd < 15.00 THEN '2. Budget (<$15)'
                WHEN f.price_usd < 35.00 THEN '3. Mid ($15-$35)'
                WHEN f.price_usd < 60.00 THEN '4. Premium ($35-$60)'
                ELSE '5. AAA (>$60)'
            END AS price_bracket,
            ROUND((AVG(f.positive_ratio) * 100)::numeric, 2) AS positive_approval_pct,
            ROUND(AVG(f.playtime_hours)::numeric, 2) AS avg_playtime_hours
        FROM fact_game_reviews_analytics f
        GROUP BY price_bracket
        ORDER BY price_bracket;
    """
    if is_sqlite:
        q3 = q3.replace("::numeric", "")
    df3 = pd.read_sql_query(q3, conn)
    
    fig, ax1 = plt.subplots(figsize=(9, 5))
    color = '#6366f1'
    ax1.set_xlabel('Price Bracket')
    ax1.set_ylabel('Positive Approval (%)', color=color)
    ax1.bar(df3['price_bracket'], df3['positive_approval_pct'], color=color, alpha=0.75, width=0.5)
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.set_ylim(0, 100)

    ax2 = ax1.twinx()
    color = '#ef4444'
    ax2.set_ylabel('Avg Playtime (Hours)', color=color)
    ax2.plot(df3['price_bracket'], df3['avg_playtime_hours'], color=color, marker='s', linewidth=2.5)
    ax2.tick_params(axis='y', labelcolor=color)

    plt.title("Steam DW: User Sentiment and Playtime by Price Tier", fontweight='bold')
    plt.tight_layout()
    chart3_path = os.path.join(PLOTS_DIR, "price_sentiment_analysis.png")
    plt.savefig(chart3_path, dpi=300)
    plt.close()
    print(f"  ✓ Saved: {chart3_path}")

    # -------------------------------------------------------------------------
    # Chart 4: Developer Market Share (Horizontal Bar Chart)
    # -------------------------------------------------------------------------
    print("[Visual Reports] Generating Chart 4: Developer Market Share...")
    q4 = """
        SELECT 
            dev.developer_name,
            ROUND((SUM(f.estimated_revenue_usd) / 1e6)::numeric, 2) AS total_revenue_m
        FROM fact_game_reviews_analytics f
        JOIN dim_developer dev ON f.developer_key = dev.developer_key
        GROUP BY dev.developer_name
        ORDER BY total_revenue_m ASC;
    """
    if is_sqlite:
        q4 = q4.replace("::numeric", "")
    df4 = pd.read_sql_query(q4, conn)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(df4['developer_name'], df4['total_revenue_m'], color='#06b6d4', height=0.6)
    ax.set_title("Steam DW: Developer Revenue Market Share ($ Millions)", fontweight='bold')
    ax.set_xlabel("Revenue ($ Millions)")
    ax.set_ylabel("Developer")
    plt.tight_layout()
    chart4_path = os.path.join(PLOTS_DIR, "developer_market_share.png")
    plt.savefig(chart4_path, dpi=300)
    plt.close()
    print(f"  ✓ Saved: {chart4_path}")

    conn.close()
    print("\n[Visual Reports Engine] All 4 visual charts generated successfully!\n")

if __name__ == "__main__":
    generate_visual_reports()
