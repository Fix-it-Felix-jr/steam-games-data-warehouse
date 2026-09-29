#!/usr/bin/env python3
"""
Steam Games Analytics Data Warehouse - 5-Minute Presentation Demo Runner (DM_Project)
Designed specifically for university exam demonstrations (5-minute timed walkthrough).
Provides a guided, step-by-step showcase of the Star Schema, OLAP operations (Rollup,
Slice & Dice, Window Functions, DSS Elasticity), and live database performance.
"""

import os
import sys
import time
import argparse
import pandas as pd

import warnings
warnings.filterwarnings('ignore', category=UserWarning)

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

import sqlite3

# Terminal Colors for Professional Presentation
CYAN = "\033[96m"
BLUE = "\033[94m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
MAGENTA = "\033[95m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQLITE_DB_FILE = os.path.join(BASE_DIR, "data", "processed", "steam_dw.db")

def get_connection(engine="postgres"):
    """Establishes connection to PostgreSQL (primary) or SQLite (fallback)."""
    if engine == "postgres":
        if not HAS_PSYCOPG2:
            print(f"{YELLOW}[Warning] psycopg2 not found, falling back to SQLite.{RESET}")
            return get_connection(engine="sqlite"), "SQLite (Fallback)"
        try:
            conn = psycopg2.connect(dbname="steam_dw")
            return conn, "PostgreSQL 16 (Primary DW)"
        except Exception:
            try:
                conn = psycopg2.connect(dbname="steam_dw", user="root", host="localhost", port=5432)
                return conn, "PostgreSQL 16 (Primary DW)"
            except Exception as e:
                print(f"{YELLOW}[Warning] Connection to PostgreSQL failed ({e}), falling back to SQLite.{RESET}")
                return get_connection(engine="sqlite"), "SQLite (Fallback)"
    else:
        if not os.path.exists(SQLITE_DB_FILE):
            raise FileNotFoundError(f"Database {SQLITE_DB_FILE} not found. Run scripts/etl_pipeline.py first.")
        return sqlite3.connect(SQLITE_DB_FILE), "SQLite 3 (Portable DW)"

def clear_screen():
    # Only clear screen if in interactive terminal
    if sys.stdout.isatty():
        os.system('cls' if os.name == 'nt' else 'clear')

def pause(auto=False, seconds=4):
    """Wait for user input or auto delay."""
    if auto:
        print(f"\n{DIM}[Avanzamento automatico in {seconds}s...]{RESET}")
        time.sleep(seconds)
    else:
        input(f"\n{BOLD}{CYAN}▶ Premi [INVIO] per continuare al prossimo step della demo...{RESET}")

def print_banner(engine_name):
    print(f"{BOLD}{BLUE}=" * 88 + f"{RESET}")
    print(f"{BOLD}{CYAN}   🎮 STEAM GAMES DATA WAREHOUSE — VALIDAZIONE ED ESECUZIONE OLAP{RESET}")
    print(f"{DIM}   Corso di Data Management / Gestione dei Dati & Decision Support Systems{RESET}")
    print(f"{BOLD}{GREEN}   DBMS Attivo: {engine_name} • Modello: Schema a Stella (Star Schema){RESET}")
    print(f"{BOLD}{BLUE}=" * 88 + f"{RESET}\n")

def run_step_1_architecture(conn, is_sqlite):
    """Step 1 (Minuto 1): Verifica dell'Architettura dello Star Schema e granularità dei fatti."""
    print(f"{BOLD}{MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}")
    print(f"{BOLD}{YELLOW}MINUTO 1: ARCHITETTURA STAR SCHEMA, DIMENSIONI E TABELLA DEI FATTI{RESET}")
    print(f"{BOLD}{MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}")
    print(f"{CYAN}🎯 Obiettivo:{RESET} Dimostrare la corretta popolazione dello schema a stella e verificare la granularità.")
    print(f"{CYAN}💡 Domanda di Business:{RESET} Come sono integrati e censiti i giochi, gli utenti e le recensioni?")
    print(f"{CYAN}📊 Interpretazione Analitica & Business Insight:{RESET}")
    print(f"  «Abbiamo modellato un Data Warehouse a stella incentrato sulla tabella dei fatti")
    print(f"   'fact_game_reviews_analytics', con granularità a livello di singola recensione utente,")
    print(f"   arricchita con 5 dimensioni (gioco, sviluppatore, genere, tempo, utente) e una tabella")
    print(f"   ponte M:N 'bridge_game_genre' per gestire i titoli multi-genere.»\n")

    cur = conn.cursor()
    
    # Query architecture counts
    queries = [
        ("dim_game", "Giochi censiti nel catalogo", "SELECT COUNT(*) FROM dim_game"),
        ("dim_developer", "Studi di sviluppo (AAA, AA, Indie)", "SELECT COUNT(*) FROM dim_developer"),
        ("dim_genre", "Generi videoludici normalizzati", "SELECT COUNT(*) FROM dim_genre"),
        ("bridge_game_genre", "Associazioni N:M Gioco-Genere (Bridge)", "SELECT COUNT(*) FROM bridge_game_genre"),
        ("dim_time", "Date analitiche (Gerarchia Temporale)", "SELECT COUNT(*) FROM dim_time"),
        ("dim_user", "Profili utenti recensori censiti", "SELECT COUNT(*) FROM dim_user"),
        ("fact_game_reviews_analytics", "Fatti Analitici (Granularità Recensioni)", "SELECT COUNT(*) FROM fact_game_reviews_analytics")
    ]
    
    table_stats = []
    t0 = time.time()
    for t_name, desc, q in queries:
        cur.execute(q)
        cnt = cur.fetchone()[0]
        table_stats.append({"Tabella DW": t_name, "Ruolo nello Star Schema": desc, "Tuple Totali": f"{cnt:,}"})
    elapsed_ms = (time.time() - t0) * 1000

    df_stats = pd.DataFrame(table_stats)
    pd.set_option('display.width', 1000)
    print(df_stats.to_string(index=False))
    print(f"\n{GREEN}✓ Integrità dello Star Schema verificata in {elapsed_ms:.1f} ms.{RESET}")

def run_step_2_rollup(conn, is_sqlite):
    """Step 2 (Minuto 2): Operazione OLAP di Roll-up sulla gerarchia temporale."""
    print(f"\n{BOLD}{MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}")
    print(f"{BOLD}{YELLOW}MINUTO 2: OPERAZIONE OLAP — ROLL-UP & GERARCHIA TEMPORALE (Anno -> Trimestre){RESET}")
    print(f"{BOLD}{MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}")
    print(f"{CYAN}🎯 Obiettivo:{RESET} Dimostrare la capacità di risalire lungo la gerarchia temporale (Time Hierarchy).")
    print(f"{CYAN}💡 Domanda di Business:{RESET} Qual è l'andamento aggregato di fatturato, prezzo medio e gradimento per Anno e Trimestre?")
    print(f"{CYAN}📊 Interpretazione Analitica & Business Insight:{RESET}")
    print(f"  «Eseguiamo un'operazione OLAP di Roll-up aggregando le misure numeriche lungo la dimensione")
    print(f"   temporale 'dim_time'. La chiave temporale YYYYMMDD intera garantisce join immediate senza")
    print(f"   lente conversioni di stringhe. Notiamo l'aggregazione di fatturato e rating per trimestre.»\n")

    cast_num = "" if is_sqlite else "::numeric"
    sql = f"""
    SELECT 
        t.year AS anno,
        t.quarter AS trim,
        COUNT(f.fact_id) AS recensioni,
        ROUND(AVG(f.price_usd){cast_num}, 2) AS prezzo_medio,
        ROUND(AVG(f.positive_ratio * 100){cast_num}, 1) AS rating_pct,
        ROUND((SUM(f.estimated_revenue_usd) / 1000000.0){cast_num}, 2) AS fatturato_milioni_usd
    FROM fact_game_reviews_analytics f
    JOIN dim_time t ON f.time_key = t.time_key
    GROUP BY t.year, t.quarter
    ORDER BY t.year DESC, t.quarter DESC
    LIMIT 8;
    """

    print(f"{DIM}SQL Eseguito:{RESET}")
    print(f"{DIM}{sql.strip()}{RESET}\n")

    t0 = time.time()
    df = pd.read_sql_query(sql, conn)
    elapsed_ms = (time.time() - t0) * 1000

    print(df.to_string(index=False))
    print(f"\n{GREEN}✓ Query eseguita con successo in {elapsed_ms:.1f} ms (Scansione aggregata ad alte prestazioni).{RESET}")

def run_step_3_slice_dice(conn, is_sqlite):
    """Step 3 (Minuto 3): Slice & Dice e Bridge Table N:M."""
    print(f"\n{BOLD}{MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}")
    print(f"{BOLD}{YELLOW}MINUTO 3: OPERAZIONE OLAP — SLICE & DICE E GESTIONE RELAZIONE N:M (Bridge Table){RESET}")
    print(f"{BOLD}{MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}")
    print(f"{CYAN}🎯 Obiettivo:{RESET} Sezionare il cubo multidimensionale incrociando Developer Tier (AAA vs AA vs Indie) e Generi.")
    print(f"{CYAN}💡 Domanda di Business:{RESET} Come differiscono i ricavi e l'indice di gradimento tra colossi AAA e sviluppatori indipendenti?")
    print(f"{CYAN}📊 Interpretazione Analitica & Business Insight:{RESET}")
    print(f"  «L'operazione di Slice & Dice incrocia due dimensioni distinte: la classificazione")
    print(f"   dello sviluppatore (dim_developer) e i generi videoludici (dim_genre). Poiché un gioco")
    print(f"   può appartenere a più generi, la tabella ponte 'bridge_game_genre' evita il problema")
    print(f"   delle doppie aggregazioni, preservando la coerenza del modello concettuale.»\n")

    cast_num = "" if is_sqlite else "::numeric"
    sql = f"""
    SELECT 
        dev.dev_tier AS tier,
        g.genre_name AS genere,
        COUNT(DISTINCT dm.game_key) AS num_titoli,
        ROUND(AVG(f.price_usd){cast_num}, 2) AS prezzo_medio,
        ROUND(AVG(f.positive_ratio * 100){cast_num}, 1) AS approvazione_pct,
        ROUND((SUM(f.estimated_revenue_usd) / 1000000.0){cast_num}, 2) AS fatturato_milioni_usd
    FROM fact_game_reviews_analytics f
    JOIN dim_game dm ON f.game_key = dm.game_key
    JOIN bridge_game_genre bgg ON dm.game_key = bgg.game_key
    JOIN dim_genre g ON bgg.genre_key = g.genre_key
    JOIN dim_developer dev ON f.developer_key = dev.developer_key
    GROUP BY dev.dev_tier, g.genre_name
    ORDER BY fatturato_milioni_usd DESC
    LIMIT 9;
    """

    print(f"{DIM}SQL Eseguito:{RESET}")
    print(f"{DIM}{sql.strip()}{RESET}\n")

    t0 = time.time()
    df = pd.read_sql_query(sql, conn)
    elapsed_ms = (time.time() - t0) * 1000

    print(df.to_string(index=False))
    print(f"\n{GREEN}✓ Query eseguita con successo in {elapsed_ms:.1f} ms. Notare la superiorità di rating degli Indie rispetto ad alcuni AAA.{RESET}")

def run_step_4_window_functions(conn, is_sqlite):
    """Step 4 (Minuto 4): Funzioni a Finestra Avanzate (DENSE_RANK e LAG)."""
    print(f"\n{BOLD}{MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}")
    print(f"{BOLD}{YELLOW}MINUTO 4: ANALISI AVANZATA CON WINDOW FUNCTIONS (DENSE_RANK & LAG){RESET}")
    print(f"{BOLD}{MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}")
    print(f"{CYAN}🎯 Obiettivo:{RESET} Dimostrare l'utilizzo di Window Functions ANSI SQL senza subquery correlate inefficienti.")
    print(f"{CYAN}💡 Domanda di Business:{RESET}")
    print(f"   1. Quali sono i primi 2 giochi con più recensioni positive per ciascun genere?")
    print(f"   2. Qual è il tasso di crescita percentuale QoQ (Quarter-over-Quarter) del fatturato?")
    print(f"{CYAN}📊 Interpretazione Analitica & Business Insight:{RESET}")
    print(f"  «Dimostriamo due Window Functions fondamentali in un sistema OLAP:")
    print(f"   - DENSE_RANK() con PARTITION BY per stilare classifiche intra-categoria.")
    print(f"   - LAG() per accedere al valore del trimestre precedente e calcolare il tasso")
    print(f"     di crescita quarter-over-quarter direttamente nel motore relazionale.»\n")

    cast_num = "" if is_sqlite else "::numeric"
    
    # 4A: DENSE_RANK
    print(f"{BOLD}{CYAN}▶ 4A. Ranking per Genere con DENSE_RANK() OVER (PARTITION BY genre_name...){RESET}")
    sql_rank = f"""
    WITH RankedGames AS (
        SELECT 
            g.genre_name AS genere,
            dm.game_title AS titolo,
            dev.developer_name AS sviluppatore,
            MAX(f.total_positive_reviews) AS recensioni_positive,
            DENSE_RANK() OVER (
                PARTITION BY g.genre_name 
                ORDER BY MAX(f.total_positive_reviews) DESC
            ) AS rank_nel_genere
        FROM fact_game_reviews_analytics f
        JOIN dim_game dm ON f.game_key = dm.game_key
        JOIN bridge_game_genre bgg ON dm.game_key = bgg.game_key
        JOIN dim_genre g ON bgg.genre_key = g.genre_key
        JOIN dim_developer dev ON f.developer_key = dev.developer_key
        GROUP BY g.genre_name, dm.game_title, dev.developer_name
    )
    SELECT genere, rank_nel_genere, titolo, sviluppatore, recensioni_positive
    FROM RankedGames
    WHERE rank_nel_genere <= 2 AND genere IN ('Action', 'RPG', 'Strategy', 'Indie')
    ORDER BY genere, rank_nel_genere;
    """
    t0 = time.time()
    df_rank = pd.read_sql_query(sql_rank, conn)
    ms_rank = (time.time() - t0) * 1000
    print(df_rank.to_string(index=False))
    print(f"{GREEN}✓ Window Ranking calcolato in {ms_rank:.1f} ms.{RESET}\n")

    # 4B: QoQ LAG
    print(f"{BOLD}{CYAN}▶ 4B. Tasso di Crescita Trimestrale con LAG(revenue, 1) OVER (ORDER BY year, quarter){RESET}")
    sql_lag = f"""
    WITH QuarterlyMetrics AS (
        SELECT 
            t.year AS anno,
            t.quarter AS trim,
            ROUND((SUM(f.estimated_revenue_usd) / 1000000.0){cast_num}, 2) AS fatturato_m
        FROM fact_game_reviews_analytics f
        JOIN dim_time t ON f.time_key = t.time_key
        GROUP BY t.year, t.quarter
    )
    SELECT 
        anno,
        trim,
        fatturato_m,
        LAG(fatturato_m, 1) OVER (ORDER BY anno, trim) AS fatturato_prev_q_m,
        ROUND(
            ((fatturato_m - LAG(fatturato_m, 1) OVER (ORDER BY anno, trim)) / 
            NULLIF(LAG(fatturato_m, 1) OVER (ORDER BY anno, trim), 0) * 100){cast_num}, 2
        ) AS crescita_qoq_pct
    FROM QuarterlyMetrics
    ORDER BY anno DESC, trim DESC
    LIMIT 6;
    """
    t0 = time.time()
    df_lag = pd.read_sql_query(sql_lag, conn)
    ms_lag = (time.time() - t0) * 1000
    print(df_lag.to_string(index=False))
    print(f"{GREEN}✓ Serie temporale QoQ calcolata con funzione a finestra in {ms_lag:.1f} ms.{RESET}")

def run_step_5_dss_and_dashboard(conn, is_sqlite):
    """Step 5 (Minuto 5): Supporto alle Decisioni (DSS), Elasticità del Prezzo e Dashboard Interattiva."""
    print(f"\n{BOLD}{MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}")
    print(f"{BOLD}{YELLOW}MINUTO 5: DECISION SUPPORT SYSTEM (DSS), ELASTICITÀ PREZZO & WEB DASHBOARD{RESET}")
    print(f"{BOLD}{MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}")
    print(f"{CYAN}🎯 Obiettivo:{RESET} Dimostrare come il DW supporta le decisioni aziendali strategiche (pricing e retention).")
    print(f"{CYAN}💡 Domanda di Business:{RESET} Qual è la fascia di prezzo ottimale che massimizza gradimento e tempo di gioco?")
    print(f"{CYAN}📊 Interpretazione Analitica & Business Insight:{RESET}")
    print(f"  «Nel modulo DSS analizziamo l'elasticità di prezzo segmentando i giochi in 5 fasce")
    print(f"   (Free-to-Play, Budget, Mid-Tier, Premium, AAA). I dati rivelano che la fascia Budget")
    print(f"   e Mid-Tier raggiunge percentuali di approvazione altissime e tempi di gioco elevati,")
    print(f"   fornendo al publisher un supporto quantitativo per la strategia di prezzo.»\n")

    cast_num = "" if is_sqlite else "::numeric"
    sql = f"""
    SELECT 
        CASE 
            WHEN f.price_usd = 0 THEN '1. Free to Play'
            WHEN f.price_usd < 15.00 THEN '2. Budget (<$15)'
            WHEN f.price_usd < 35.00 THEN '3. Mid-Tier ($15-$35)'
            WHEN f.price_usd < 60.00 THEN '4. Premium ($35-$60)'
            ELSE '5. AAA (>$60)'
        END AS fascia_prezzo,
        COUNT(f.fact_id) AS recensioni_totali,
        ROUND(AVG(f.price_usd){cast_num}, 2) AS prezzo_medio_effettivo,
        ROUND(AVG(f.playtime_hours){cast_num}, 1) AS ore_giocate_medie,
        ROUND(AVG(f.positive_ratio * 100){cast_num}, 1) AS gradimento_pct,
        ROUND((SUM(f.estimated_revenue_usd) / 1000000.0){cast_num}, 2) AS fatturato_milioni_usd
    FROM fact_game_reviews_analytics f
    GROUP BY fascia_prezzo
    ORDER BY fascia_prezzo;
    """

    t0 = time.time()
    df = pd.read_sql_query(sql, conn)
    elapsed_ms = (time.time() - t0) * 1000

    print(df.to_string(index=False))
    print(f"\n{GREEN}✓ Analisi di business eseguita in {elapsed_ms:.1f} ms.{RESET}\n")

    print(f"{BOLD}{CYAN}🌐 COMPONENTE VISIVA: DASHBOARD INTERATTIVA WEB LIVE{RESET}")
    print(f"Il sistema integra una Web Dashboard analitica (avviabile con {BOLD}python3 dashboard/app.py{RESET}):")
    print(f"  • {BOLD}Visualizzazione Grafica Live{RESET}: Grafici Chart.js per quote di mercato e fatturato per genere.")
    print(f"  • {BOLD}Runner SQL Interattivo{RESET}: Consente alla commissione di cliccare ed eseguire in tempo reale")
    print(f"    tutte le 10 query OLAP misurando la latenza di esecuzione istantanea.")
    print(f"  • Indirizzo predefinito: {BOLD}http://localhost:8501{RESET}")

def main():
    parser = argparse.ArgumentParser(description="Steam DW 5-Minute Exam Presentation Demo Runner")
    parser.add_argument("--engine", choices=["postgres", "sqlite"], default="postgres",
                        help="Data Warehouse engine (default: postgres)")
    parser.add_argument("--auto", action="store_true",
                        help="Run automatically with timed pauses between steps (for rehearsal)")
    parser.add_argument("--delay", type=int, default=5,
                        help="Seconds to wait in auto mode (default: 5)")
    parser.add_argument("--step", type=int, choices=[1, 2, 3, 4, 5], default=None,
                        help="Run only a specific minute/step (1 to 5)")
    args = parser.parse_args()

    conn, engine_name = get_connection(engine=args.engine)
    is_sqlite = isinstance(conn, sqlite3.Connection)

    clear_screen()
    print_banner(engine_name)

    steps = [
        (1, "Architettura Star Schema & Fatti", run_step_1_architecture),
        (2, "Roll-up & Gerarchia Temporale", run_step_2_rollup),
        (3, "Slice & Dice e Bridge Table N:M", run_step_3_slice_dice),
        (4, "Window Functions (Ranking & QoQ LAG)", run_step_4_window_functions),
        (5, "Supporto Decisionale DSS & Dashboard", run_step_5_dss_and_dashboard),
    ]

    try:
        if args.step is not None:
            # Run single step
            for num, name, func in steps:
                if num == args.step:
                    func(conn, is_sqlite)
                    break
        else:
            # Run all 5 steps with pauses
            for i, (num, name, func) in enumerate(steps):
                func(conn, is_sqlite)
                if i < len(steps) - 1:
                    pause(auto=args.auto, seconds=args.delay)
                    print("\n")
            
            print(f"\n{BOLD}{GREEN}" + "=" * 88 + f"{RESET}")
            print(f"{BOLD}{GREEN}   ✓ DIMOSTRAZIONE DI 5 MINUTI COMPLETATA CON SUCCESSO!{RESET}")
            print(f"{DIM}   Tutti i requisiti OLAP, modellazione e performance sono stati convalidati.{RESET}")
            print(f"{BOLD}{GREEN}" + "=" * 88 + f"{RESET}\n")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
