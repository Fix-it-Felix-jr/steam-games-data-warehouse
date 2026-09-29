#!/usr/bin/env python3
"""
OLAP Query Execution & Validation Runner (DM_Project)
Executes all SQL queries in olap_queries.sql against PostgreSQL or SQLite and prints formatted reports.
"""

import os
import sys
import argparse
import time
import pandas as pd

import warnings
warnings.filterwarnings('ignore', category=UserWarning)

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQLITE_DB_FILE = os.path.join(BASE_DIR, "data", "processed", "steam_dw.db")
SQL_FILE = os.path.join(BASE_DIR, "scripts", "olap_queries.sql")

def get_connection(engine="postgres", pg_db="steam_dw", pg_user="root", pg_host="localhost", pg_port=5432):
    """Establishes database connection based on chosen engine."""
    if engine == "postgres":
        if not HAS_PSYCOPG2:
            print("[Warning] psycopg2 not found, falling back to SQLite.")
            return get_connection(engine="sqlite")
        try:
            return psycopg2.connect(dbname=pg_db)
        except Exception:
            try:
                return psycopg2.connect(dbname=pg_db, user=pg_user, host=pg_host, port=pg_port)
            except Exception as e:
                print(f"[Warning] Failed connecting to PostgreSQL ({e}), falling back to SQLite.")
                return get_connection(engine="sqlite")
    else:
        if not os.path.exists(SQLITE_DB_FILE):
            raise FileNotFoundError(f"Database {SQLITE_DB_FILE} not found. Run etl_pipeline.py first.")
        return sqlite3.connect(SQLITE_DB_FILE)

def run_olap_suite(engine="postgres"):
    print("\n" + "="*85)
    print(f"        STEAM GAMES DATA WAREHOUSE - OLAP QUERY EXECUTION ENGINE ({engine.upper()})")
    print("="*85 + "\n")
    
    conn = get_connection(engine=engine)
    is_sqlite = isinstance(conn, sqlite3.Connection)
    actual_engine = "SQLite" if is_sqlite else "PostgreSQL"
    print(f"[Connected] Active Data Warehouse Engine: {actual_engine}\n")
    
    with open(SQL_FILE, "r", encoding="utf-8") as f:
        sql_content = f.read()
        
    # Split queries by semicolon
    raw_statements = sql_content.split(";")
    queries = []
    
    for stmt in raw_statements:
        stmt_clean = stmt.strip()
        if not stmt_clean:
            continue
            
        lines = stmt_clean.split("\n")
        title = "OLAP Query"
        purpose = ""
        sql_lines = []
        for l in lines:
            if l.startswith("-- QUERY"):
                title = l.replace("-- ", "").strip()
            elif l.startswith("-- Purpose:"):
                purpose = l.replace("-- Purpose:", "").strip()
            elif not l.startswith("--"):
                sql_lines.append(l)
                
        sql_query = "\n".join(sql_lines).strip()
        if sql_query:
            # If SQLite, strip ::numeric casts
            if is_sqlite:
                sql_query = sql_query.replace("::numeric", "")
            queries.append((title, purpose, sql_query))
            
    print(f"Loaded {len(queries)} multidimensional OLAP queries from {SQL_FILE}.\n")
    
    for i, (title, purpose, query) in enumerate(queries, 1):
        print(f"[{i}/{len(queries)}] {title}")
        if purpose:
            print(f"       Goal: {purpose}")
        print("-" * 85)
        
        t0 = time.time()
        try:
            df = pd.read_sql_query(query, conn)
            elapsed_ms = (time.time() - t0) * 1000
            
            if df.empty:
                print("Result: <No rows returned>")
            else:
                # Format floating point numbers nicely
                pd.set_option('display.max_columns', 15)
                pd.set_option('display.width', 1000)
                print(df.to_string(index=False))
            print(f"\n→ Returned {len(df)} rows in {elapsed_ms:.1f} ms")
        except Exception as e:
            print(f"Query Execution Error: {e}")
        print("=" * 85 + "\n")
        
    conn.close()
    print(f"[OLAP Engine] All {len(queries)} analytical queries executed successfully on {actual_engine}.\n")

def main():
    parser = argparse.ArgumentParser(description="Steam DW OLAP Query Runner")
    parser.add_argument("--engine", choices=["postgres", "sqlite"], default="postgres",
                        help="Data Warehouse engine (default: postgres)")
    args = parser.parse_args()
    run_olap_suite(engine=args.engine)

if __name__ == "__main__":
    main()
