#!/usr/bin/env python3
"""
Interactive Analytics Dashboard Server for Steam Data Warehouse (DM_Project)
Lightweight HTTP server serving a modern responsive web dashboard with live SQL OLAP execution against PostgreSQL (or SQLite).
"""

import os
import sys
import json
import http.server
import socketserver
from urllib.parse import parse_qs, urlparse
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
PLOTS_DIR = os.path.join(BASE_DIR, "dashboard", "plots")
PORT = 8501

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

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="it">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Steam Games Analytics - Data Warehouse Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { background-color: #0b0f19; color: #f8fafc; font-family: ui-sans-serif, system-ui, -apple-system, sans-serif; }
        .glass-card { background: rgba(17, 24, 39, 0.75); backdrop-filter: blur(16px); border: 1px solid rgba(255,255,255,0.08); border-radius: 14px; }
        .gradient-text { background: linear-gradient(135deg, #38bdf8, #818cf8, #c084fc); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .btn-query { transition: all 0.2s ease; border: 1px solid rgba(255,255,255,0.06); }
        .btn-query:hover { border-color: rgba(56, 189, 248, 0.4); transform: translateY(-1px); }
    </style>
</head>
<body class="p-6 md:p-8">
    <div class="max-w-7xl mx-auto space-y-6">
        <!-- Header -->
        <div class="glass-card p-6 flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
            <div>
                <h1 class="text-3xl font-black tracking-tight gradient-text">Steam Games Data Warehouse</h1>
                <p class="text-slate-400 text-sm mt-1">Progetto Universitario di Data Management • Architettura a Stella (Star Schema) in PostgreSQL</p>
            </div>
            <div class="flex flex-wrap items-center gap-2">
                <span class="px-3 py-1 rounded-full text-xs font-semibold bg-emerald-500/20 text-emerald-400 border border-emerald-500/30">PostgreSQL Star Schema</span>
                <span class="px-3 py-1 rounded-full text-xs font-semibold bg-sky-500/20 text-sky-400 border border-sky-500/30">10 OLAP Queries Live</span>
                <span class="px-3 py-1 rounded-full text-xs font-semibold bg-purple-500/20 text-purple-400 border border-purple-500/30">ETL Pipeline Sync</span>
            </div>
        </div>

        <!-- KPI Cards -->
        <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
            <div class="glass-card p-5">
                <div class="text-slate-400 text-xs font-semibold uppercase tracking-wider">Giochi Analizzati</div>
                <div class="text-3xl font-extrabold mt-2 text-sky-400">__KPI_GAMES__</div>
                <div class="text-slate-500 text-xs mt-1">Dimensione `dim_game`</div>
            </div>
            <div class="glass-card p-5">
                <div class="text-slate-400 text-xs font-semibold uppercase tracking-wider">Recensioni nei Fatti</div>
                <div class="text-3xl font-extrabold mt-2 text-indigo-400">__KPI_REVIEWS__</div>
                <div class="text-slate-500 text-xs mt-1">Fatti `fact_game_reviews_analytics`</div>
            </div>
            <div class="glass-card p-5">
                <div class="text-slate-400 text-xs font-semibold uppercase tracking-wider">Fatturato Stimato Totale</div>
                <div class="text-3xl font-extrabold mt-2 text-emerald-400">$__KPI_REVENUE__M</div>
                <div class="text-slate-500 text-xs mt-1">Aggregazione Multidimensionale</div>
            </div>
            <div class="glass-card p-5">
                <div class="text-slate-400 text-xs font-semibold uppercase tracking-wider">Approval Rate Medio</div>
                <div class="text-3xl font-extrabold mt-2 text-amber-400">__KPI_APPROVAL__%</div>
                <div class="text-slate-500 text-xs mt-1">Sentiment Utenti Positivo</div>
            </div>
        </div>

        <!-- Interactive Charts Grid -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <!-- Chart 1 -->
            <div class="glass-card p-6">
                <div class="flex justify-between items-center mb-4">
                    <h3 class="text-lg font-bold text-slate-200">Fatturato per Genere ($ Millions)</h3>
                    <span class="text-xs text-slate-500">Slice & Dice (Generi)</span>
                </div>
                <canvas id="genreChart" class="max-h-72"></canvas>
            </div>
            <!-- Chart 2 -->
            <div class="glass-card p-6">
                <div class="flex justify-between items-center mb-4">
                    <h3 class="text-lg font-bold text-slate-200">Quote di Mercato: Tier Sviluppatori</h3>
                    <span class="text-xs text-slate-500">AAA vs AA vs Indie</span>
                </div>
                <canvas id="devChart" class="max-h-72"></canvas>
            </div>
        </div>

        <!-- Live OLAP SQL Query Runner -->
        <div class="glass-card p-6 space-y-4">
            <div class="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-2 border-b border-slate-800 pb-3">
                <div>
                    <h3 class="text-xl font-bold text-slate-100">Runner Query OLAP Live (PostgreSQL)</h3>
                    <p class="text-xs text-slate-400 mt-0.5">Esegui in tempo reale le query multidimensionali su Star Schema</p>
                </div>
                <span class="text-xs px-2.5 py-1 bg-slate-800 text-slate-300 rounded font-mono">ANSI SQL & Window Functions</span>
            </div>

            <!-- Query Buttons -->
            <div class="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-2.5">
                <button onclick="runQuery('q1')" class="p-3 text-left glass-card btn-query">
                    <div class="font-bold text-sky-400 text-xs">1. Rollup Temporale</div>
                    <div class="text-[11px] text-slate-400 mt-1">Anno & Trimestre</div>
                </button>
                <button onclick="runQuery('q2')" class="p-3 text-left glass-card btn-query">
                    <div class="font-bold text-indigo-400 text-xs">2. Slice & Dice</div>
                    <div class="text-[11px] text-slate-400 mt-1">AAA vs Indie per Genere</div>
                </button>
                <button onclick="runQuery('q3')" class="p-3 text-left glass-card btn-query">
                    <div class="font-bold text-amber-400 text-xs">3. DENSE_RANK()</div>
                    <div class="text-[11px] text-slate-400 mt-1">Top 3 Giochi per Genere</div>
                </button>
                <button onclick="runQuery('q4')" class="p-3 text-left glass-card btn-query">
                    <div class="font-bold text-emerald-400 text-xs">4. Elasticità Prezzo</div>
                    <div class="text-[11px] text-slate-400 mt-1">Fasce Prezzo vs Sentiment</div>
                </button>
                <button onclick="runQuery('q5')" class="p-3 text-left glass-card btn-query">
                    <div class="font-bold text-purple-400 text-xs">5. Profilo Utenti</div>
                    <div class="text-[11px] text-slate-400 mt-1">Casual vs Enthusiast vs Expert</div>
                </button>
                <button onclick="runQuery('q6')" class="p-3 text-left glass-card btn-query">
                    <div class="font-bold text-rose-400 text-xs">6. QoQ Growth LAG()</div>
                    <div class="text-[11px] text-slate-400 mt-1">Crescita % Trimestrale</div>
                </button>
                <button onclick="runQuery('q7')" class="p-3 text-left glass-card btn-query">
                    <div class="font-bold text-cyan-400 text-xs">7. Weekend Pattern</div>
                    <div class="text-[11px] text-slate-400 mt-1">Feriali vs Festivi</div>
                </button>
                <button onclick="runQuery('q8')" class="p-3 text-left glass-card btn-query">
                    <div class="font-bold text-teal-400 text-xs">8. Top Developers</div>
                    <div class="text-[11px] text-slate-400 mt-1">Market Share Sviluppatori</div>
                </button>
                <button onclick="runQuery('q9')" class="p-3 text-left glass-card btn-query">
                    <div class="font-bold text-yellow-400 text-xs">9. Cohort Rilascio</div>
                    <div class="text-[11px] text-slate-400 mt-1">Longevità per Anno</div>
                </button>
                <button onclick="runQuery('q10')" class="p-3 text-left glass-card btn-query">
                    <div class="font-bold text-orange-400 text-xs">10. Prezzo per Genere</div>
                    <div class="text-[11px] text-slate-400 mt-1">Pricing Power Incrociato</div>
                </button>
            </div>

            <!-- Execution Result Panel -->
            <div id="queryMeta" class="text-xs text-slate-400 flex justify-between items-center px-1">
                <span>Stato esecuzione: <strong id="queryStatusText" class="text-slate-300">In attesa di selezione</strong></span>
                <span id="queryLatency" class="font-mono text-emerald-400"></span>
            </div>
            <div id="queryResults" class="overflow-x-auto p-4 bg-slate-950/80 rounded-xl text-xs font-mono border border-slate-800 min-h-[140px] max-h-[420px]">
                <div class="text-slate-500 py-8 text-center">Clicca su uno dei pulsanti sopra per eseguire una query analitica OLAP in tempo reale su PostgreSQL.</div>
            </div>
        </div>

        <!-- Academic Footer -->
        <div class="text-center text-xs text-slate-500 py-2">
            Progetto di Data Management • Documentazione completa disponibile in <code class="text-sky-400">docs/DATA_WAREHOUSE_DOCUMENTATION.md</code>
        </div>
    </div>

    <script>
        // Chart 1: Revenue by Genre
        const genreCtx = document.getElementById('genreChart').getContext('2d');
        new Chart(genreCtx, {
            type: 'bar',
            data: {
                labels: __GENRE_LABELS__,
                datasets: [{
                    label: 'Fatturato ($ Millions)',
                    data: __GENRE_DATA__,
                    backgroundColor: 'rgba(56, 189, 248, 0.75)',
                    borderColor: 'rgba(56, 189, 248, 1)',
                    borderWidth: 1.5,
                    borderRadius: 6
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    x: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(255,255,255,0.05)' } },
                    y: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(255,255,255,0.05)' } }
                }
            }
        });

        // Chart 2: Developer Tier Breakdown
        const devCtx = document.getElementById('devChart').getContext('2d');
        new Chart(devCtx, {
            type: 'doughnut',
            data: {
                labels: __DEV_LABELS__,
                datasets: [{
                    data: __DEV_DATA__,
                    backgroundColor: ['#6366f1', '#10b981', '#f59e0b'],
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { labels: { color: '#cbd5e1' } }
                }
            }
        });

        async function runQuery(qId) {
            const resBox = document.getElementById('queryResults');
            const statusText = document.getElementById('queryStatusText');
            const latencyBox = document.getElementById('queryLatency');
            
            statusText.innerText = 'Esecuzione su PostgreSQL in corso...';
            latencyBox.innerText = '';
            resBox.innerHTML = '<div class="text-sky-400 py-6 text-center animate-pulse">Esecuzione query in corso sul Data Warehouse...</div>';
            
            const t0 = performance.now();
            try {
                const resp = await fetch('/api/query?id=' + qId);
                const data = await resp.json();
                const elapsed = (performance.now() - t0).toFixed(1);
                
                if (data.error) {
                    statusText.innerText = 'Errore esecuzione';
                    resBox.innerHTML = '<div class="text-rose-400 p-2">Errore: ' + data.error + '</div>';
                    return;
                }
                
                statusText.innerText = data.title + ' (' + data.rows.length + ' righe)';
                latencyBox.innerText = 'Latenza: ' + elapsed + ' ms';
                
                let html = '<table class="w-full text-left border-collapse"><thead class="text-slate-400 border-b border-slate-800 bg-slate-900/50 sticky top-0"><tr>';
                data.columns.forEach(c => html += '<th class="p-2.5 font-semibold text-sky-300">' + c + '</th>');
                html += '</tr></thead><tbody>';
                data.rows.forEach(r => {
                    html += '<tr class="border-b border-slate-900/80 hover:bg-slate-800/40 transition">';
                    r.forEach(v => html += '<td class="p-2.5 text-slate-300">' + (v !== null ? v : '<span class="text-slate-600">NULL</span>') + '</td>');
                    html += '</tr>';
                });
                html += '</tbody></table>';
                resBox.innerHTML = html;
            } catch(err) {
                statusText.innerText = 'Errore di connessione';
                resBox.innerHTML = '<div class="text-rose-400 p-2">Errore di rete: ' + err + '</div>';
            }
        }
    </script>
</body>
</html>
"""

def get_kpis():
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM dim_game;")
    games = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM fact_game_reviews_analytics;")
    reviews = cur.fetchone()[0]
    
    cur.execute("SELECT SUM(estimated_revenue_usd) FROM fact_game_reviews_analytics;")
    rev_raw = cur.fetchone()[0] or 0.0
    revenue_m = f"{float(rev_raw) / 1e6:,.2f}"
    
    cur.execute("SELECT AVG(positive_ratio) * 100 FROM fact_game_reviews_analytics;")
    approval = f"{float(cur.fetchone()[0] or 0.0):.1f}"
    
    cur.close()
    conn.close()
    return {"games": games, "reviews": reviews, "revenue": revenue_m, "approval": approval}

def get_chart_data():
    conn = get_connection()
    cur = conn.cursor()
    
    is_sqlite = isinstance(conn, sqlite3.Connection)
    cast_num = "" if is_sqlite else "::numeric"
    
    cur.execute(f"""
        SELECT g.genre_name, ROUND((SUM(f.estimated_revenue_usd)/1e6){cast_num}, 2)
        FROM fact_game_reviews_analytics f
        JOIN bridge_game_genre bgg ON f.game_key = bgg.game_key
        JOIN dim_genre g ON bgg.genre_key = g.genre_key
        GROUP BY g.genre_name
        ORDER BY SUM(f.estimated_revenue_usd) DESC;
    """)
    genre_rows = cur.fetchall()
    genre_labels = [r[0] for r in genre_rows]
    genre_data = [float(r[1]) for r in genre_rows]
    
    cur.execute("""
        SELECT dev.dev_tier, COUNT(*)
        FROM fact_game_reviews_analytics f
        JOIN dim_developer dev ON f.developer_key = dev.developer_key
        GROUP BY dev.dev_tier;
    """)
    dev_rows = cur.fetchall()
    dev_labels = [r[0] for r in dev_rows]
    dev_data = [int(r[1]) for r in dev_rows]
    
    cur.close()
    conn.close()
    return genre_labels, genre_data, dev_labels, dev_data

QUERIES_DICT = {
    "q1": ("1. Rollup Temporale (Anno & Trimestre)", """
        SELECT t.year AS Anno, t.quarter AS Trimestre, COUNT(f.fact_id) AS Reviews,
               ROUND(AVG(f.price_usd)::numeric, 2) AS Prezzo_Medio,
               ROUND(AVG(f.positive_ratio*100)::numeric, 1) AS Rating_Pct,
               ROUND((SUM(f.estimated_revenue_usd)/1e6)::numeric, 2) AS Fatturato_M
        FROM fact_game_reviews_analytics f
        JOIN dim_time t ON f.time_key = t.time_key
        GROUP BY t.year, t.quarter
        ORDER BY t.year DESC, t.quarter DESC LIMIT 12;
    """),
    "q2": ("2. Slice & Dice (AAA vs Indie per Genere)", """
        SELECT dev.dev_tier AS Tier, g.genre_name AS Genere,
               COUNT(DISTINCT dm.game_key) AS Titoli,
               ROUND(AVG(f.price_usd)::numeric, 2) AS Prezzo_Medio,
               ROUND(AVG(f.positive_ratio*100)::numeric, 1) AS Rating_Pct,
               ROUND((SUM(f.estimated_revenue_usd)/1e6)::numeric, 2) AS Fatturato_M
        FROM fact_game_reviews_analytics f
        JOIN dim_game dm ON f.game_key = dm.game_key
        JOIN bridge_game_genre bgg ON dm.game_key = bgg.game_key
        JOIN dim_genre g ON bgg.genre_key = g.genre_key
        JOIN dim_developer dev ON f.developer_key = dev.developer_key
        GROUP BY dev.dev_tier, g.genre_name
        ORDER BY Fatturato_M DESC LIMIT 15;
    """),
    "q3": ("3. DENSE_RANK() Top 3 per Genere", """
        WITH Ranked AS (
            SELECT g.genre_name, dm.game_title, dev.developer_name,
                   MAX(f.total_positive_reviews) AS positive_reviews,
                   DENSE_RANK() OVER (PARTITION BY g.genre_name ORDER BY MAX(f.total_positive_reviews) DESC) AS rnk
            FROM fact_game_reviews_analytics f
            JOIN dim_game dm ON f.game_key = dm.game_key
            JOIN bridge_game_genre bgg ON dm.game_key = bgg.game_key
            JOIN dim_genre g ON bgg.genre_key = g.genre_key
            JOIN dim_developer dev ON f.developer_key = dev.developer_key
            GROUP BY g.genre_name, dm.game_title, dev.developer_name
        )
        SELECT genre_name AS Genere, rnk AS Rank, game_title AS Titolo, developer_name AS Developer, positive_reviews AS Voti_Positivi
        FROM Ranked WHERE rnk <= 3 LIMIT 18;
    """),
    "q4": ("4. Elasticità del Prezzo & Fasce", """
        SELECT 
            CASE 
                WHEN f.price_usd = 0 THEN '1. Free to Play'
                WHEN f.price_usd < 15.00 THEN '2. Budget (<$15)'
                WHEN f.price_usd < 35.00 THEN '3. Mid ($15-$35)'
                WHEN f.price_usd < 60.00 THEN '4. Premium ($35-$60)'
                ELSE '5. AAA (>$60)'
            END AS Fascia_Prezzo,
            COUNT(f.fact_id) AS Reviews,
            ROUND(AVG(f.price_usd)::numeric, 2) AS Prezzo_Medio,
            ROUND(AVG(f.playtime_hours)::numeric, 1) AS Ore_Medie,
            ROUND(AVG(f.positive_ratio*100)::numeric, 1) AS Rating_Pct,
            ROUND((SUM(f.estimated_revenue_usd)/1e6)::numeric, 2) AS Fatturato_M
        FROM fact_game_reviews_analytics f
        GROUP BY Fascia_Prezzo
        ORDER BY Fascia_Prezzo;
    """),
    "q5": ("5. Livello Esperienza Utente", """
        SELECT u.reviewer_level AS Livello_Utente,
               COUNT(DISTINCT u.user_key) AS Utenti_Unici,
               COUNT(f.fact_id) AS Totale_Recensioni,
               ROUND(AVG(f.votes_up)::numeric, 2) AS Voti_Utili_Medi,
               ROUND(AVG(f.playtime_hours)::numeric, 1) AS Ore_Giocate_Medie,
               ROUND(AVG(f.review_score*100)::numeric, 1) AS Rating_Positivo_Pct
        FROM fact_game_reviews_analytics f
        JOIN dim_user u ON f.user_key = u.user_key
        GROUP BY u.reviewer_level
        ORDER BY Totale_Recensioni DESC;
    """),
    "q6": ("6. Crescita Trimestrale QoQ LAG()", """
        WITH QuarterlyMetrics AS (
            SELECT t.year, t.quarter,
                   ROUND((SUM(f.estimated_revenue_usd)/1e6)::numeric, 2) AS revenue_m
            FROM fact_game_reviews_analytics f
            JOIN dim_time t ON f.time_key = t.time_key
            GROUP BY t.year, t.quarter
        )
        SELECT year AS Anno, quarter AS Q, revenue_m AS Fatturato_M,
               LAG(revenue_m, 1) OVER (ORDER BY year, quarter) AS Fatturato_Prev_Q_M,
               ROUND(((revenue_m - LAG(revenue_m, 1) OVER (ORDER BY year, quarter)) / 
                      NULLIF(LAG(revenue_m, 1) OVER (ORDER BY year, quarter), 0) * 100)::numeric, 2) AS Crescita_QoQ_Pct
        FROM QuarterlyMetrics ORDER BY year DESC, quarter DESC LIMIT 12;
    """),
    "q7": ("7. Analisi Weekend vs Feriali", """
        SELECT 
            CASE WHEN t.is_weekend THEN 'Weekend' ELSE 'Feriale' END AS Giorno,
            COUNT(f.fact_id) AS Recensioni,
            ROUND(AVG(f.playtime_hours)::numeric, 1) AS Ore_Medie,
            ROUND(AVG(f.votes_up)::numeric, 2) AS Voti_Utili_Medi,
            ROUND(AVG(f.review_score*100)::numeric, 1) AS Rating_Positivo_Pct
        FROM fact_game_reviews_analytics f
        JOIN dim_time t ON f.time_key = t.time_key
        GROUP BY Giorno;
    """),
    "q8": ("8. Market Share Sviluppatori", """
        SELECT dev.developer_name AS Developer, dev.dev_tier AS Tier,
               COUNT(DISTINCT dm.game_key) AS Titoli,
               ROUND(AVG(f.positive_ratio*100)::numeric, 1) AS Rating_Pct,
               ROUND((SUM(f.estimated_revenue_usd)/1e6)::numeric, 2) AS Fatturato_M
        FROM fact_game_reviews_analytics f
        JOIN dim_developer dev ON f.developer_key = dev.developer_key
        JOIN dim_game dm ON f.game_key = dm.game_key
        GROUP BY dev.developer_name, dev.dev_tier
        ORDER BY Fatturato_M DESC LIMIT 15;
    """),
    "q9": ("9. Cohort per Anno di Rilascio", """
        SELECT dm.release_year AS Anno_Rilascio,
               COUNT(DISTINCT dm.game_key) AS Titoli_Rilasciati,
               COUNT(f.fact_id) AS Recensioni,
               ROUND(AVG(f.price_usd)::numeric, 2) AS Prezzo_Medio,
               ROUND(AVG(f.positive_ratio*100)::numeric, 1) AS Rating_Pct,
               ROUND((SUM(f.estimated_revenue_usd)/1e6)::numeric, 2) AS Fatturato_M
        FROM fact_game_reviews_analytics f
        JOIN dim_game dm ON f.game_key = dm.game_key
        GROUP BY dm.release_year
        ORDER BY dm.release_year DESC LIMIT 14;
    """),
    "q10": ("10. Performance per Genere", """
        SELECT g.genre_name AS Genere,
               COUNT(f.fact_id) AS Recensioni,
               ROUND(AVG(f.price_usd)::numeric, 2) AS Prezzo_Medio,
               ROUND(AVG(f.discount_pct)::numeric, 1) AS Sconto_Medio_Pct,
               ROUND(AVG(f.positive_ratio*100)::numeric, 1) AS Rating_Pct,
               ROUND((SUM(f.estimated_revenue_usd)/1e6)::numeric, 2) AS Fatturato_M
        FROM fact_game_reviews_analytics f
        JOIN bridge_game_genre bgg ON f.game_key = bgg.game_key
        JOIN dim_genre g ON bgg.genre_key = g.genre_key
        GROUP BY g.genre_name
        ORDER BY Fatturato_M DESC;
    """)
}

def execute_olap_api(q_id):
    query_item = QUERIES_DICT.get(q_id)
    if not query_item:
        return {"error": "Query ID non valido"}
        
    title, sql = query_item
    conn = get_connection()
    cur = conn.cursor()
    
    is_sqlite = isinstance(conn, sqlite3.Connection)
    if is_sqlite:
        sql = sql.replace("::numeric", "")
        
    try:
        cur.execute(sql)
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        # Convert numeric/decimal types to float/int for json serialization
        clean_rows = []
        for r in rows:
            clean_rows.append([float(v) if hasattr(v, 'as_tuple') else v for v in r])
        return {"title": title, "columns": cols, "rows": clean_rows}
    except Exception as e:
        return {"error": str(e)}
    finally:
        cur.close()
        conn.close()

class DashboardHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/" or parsed.path == "/index.html":
            kpis = get_kpis()
            g_labels, g_data, d_labels, d_data = get_chart_data()
            
            html = HTML_TEMPLATE.replace("__KPI_GAMES__", f"{kpis['games']:,}")
            html = html.replace("__KPI_REVIEWS__", f"{kpis['reviews']:,}")
            html = html.replace("__KPI_REVENUE__", str(kpis["revenue"]))
            html = html.replace("__KPI_APPROVAL__", str(kpis["approval"]))
            
            html = html.replace("__GENRE_LABELS__", json.dumps(g_labels))
            html = html.replace("__GENRE_DATA__", json.dumps(g_data))
            html = html.replace("__DEV_LABELS__", json.dumps(d_labels))
            html = html.replace("__DEV_DATA__", json.dumps(d_data))
            
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(html.encode("utf-8"))
            
        elif parsed.path == "/api/query":
            params = parse_qs(parsed.query)
            q_id = params.get("id", ["q1"])[0]
            result = execute_olap_api(q_id)
            
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode("utf-8"))
        else:
            super().do_GET()

def run_server():
    os.chdir(BASE_DIR)
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", PORT), DashboardHTTPRequestHandler) as httpd:
        print(f"[Dashboard Server] Serving Steam Analytics Web App on http://localhost:{PORT}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("[Dashboard Server] Shutting down.")

if __name__ == "__main__":
    run_server()
