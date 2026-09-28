-- ============================================================================
-- STEAM GAMES DATA WAREHOUSE - OLAP QUERY SUITE (DM_PROJECT)
-- Multidimensional Analytical Queries for Data Management Exam (PostgreSQL / SQLite)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- QUERY 1: Rollup / Time Hierarchy Analysis (Year -> Quarter)
-- Purpose: Drill-down and Roll-up performance analysis across time dimensions.
-- Demonstrates aggregation across the temporal hierarchy.
-- ----------------------------------------------------------------------------
SELECT 
    t.year,
    t.quarter,
    COUNT(f.fact_id) AS total_reviews_analyzed,
    ROUND(AVG(f.price_usd)::numeric, 2) AS avg_game_price,
    ROUND(AVG(f.positive_ratio * 100)::numeric, 2) AS avg_positive_rating_pct,
    ROUND(SUM(f.estimated_revenue_usd)::numeric, 2) AS total_quarterly_revenue
FROM fact_game_reviews_analytics f
JOIN dim_time t ON f.time_key = t.time_key
GROUP BY t.year, t.quarter
ORDER BY t.year DESC, t.quarter DESC;

-- ----------------------------------------------------------------------------
-- QUERY 2: Slice & Dice by Genre and Developer Tier
-- Purpose: Slice data for AAA vs AA vs Indie games across genres.
-- Analyzes differences in pricing power, playtime, and revenue across categories.
-- ----------------------------------------------------------------------------
SELECT 
    dev.dev_tier,
    g.genre_name,
    COUNT(DISTINCT dm.game_key) AS unique_games_count,
    ROUND(AVG(f.price_usd)::numeric, 2) AS avg_price,
    ROUND(AVG(f.playtime_hours)::numeric, 2) AS avg_playtime_hours,
    ROUND(AVG(f.positive_ratio * 100)::numeric, 2) AS positive_approval_pct,
    ROUND(SUM(f.estimated_revenue_usd)::numeric, 2) AS total_genre_revenue
FROM fact_game_reviews_analytics f
JOIN dim_game dm ON f.game_key = dm.game_key
JOIN bridge_game_genre bgg ON dm.game_key = bgg.game_key
JOIN dim_genre g ON bgg.genre_key = g.genre_key
JOIN dim_developer dev ON f.developer_key = dev.developer_key
GROUP BY dev.dev_tier, g.genre_name
ORDER BY total_genre_revenue DESC;

-- ----------------------------------------------------------------------------
-- QUERY 3: Window Function - Top 3 Rated Games Per Genre (DENSE_RANK)
-- Purpose: Rank games within each genre based on positive reviews count.
-- Demonstrates partitioning and ranking across dimensions.
-- ----------------------------------------------------------------------------
WITH RankedGames AS (
    SELECT 
        g.genre_name,
        dm.game_title,
        dev.developer_name,
        MAX(f.total_positive_reviews) AS positive_reviews_count,
        MAX(f.positive_ratio) AS positive_ratio,
        DENSE_RANK() OVER (
            PARTITION BY g.genre_name 
            ORDER BY MAX(f.total_positive_reviews) DESC
        ) AS rnk
    FROM fact_game_reviews_analytics f
    JOIN dim_game dm ON f.game_key = dm.game_key
    JOIN bridge_game_genre bgg ON dm.game_key = bgg.game_key
    JOIN dim_genre g ON bgg.genre_key = g.genre_key
    JOIN dim_developer dev ON f.developer_key = dev.developer_key
    GROUP BY g.genre_name, dm.game_title, dev.developer_name
)
SELECT genre_name, rnk, game_title, developer_name, positive_reviews_count, ROUND((positive_ratio * 100)::numeric, 1) AS rating_pct
FROM RankedGames
WHERE rnk <= 3
ORDER BY genre_name, rnk;

-- ----------------------------------------------------------------------------
-- QUERY 4: Price Elasticity & Bracket Analysis
-- Purpose: Evaluate user engagement and review scores across price tiers.
-- Demonstrates relation between price brackets, playtime, and user reviews.
-- ----------------------------------------------------------------------------
SELECT 
    CASE 
        WHEN f.price_usd = 0 THEN '1. Free to Play'
        WHEN f.price_usd < 15.00 THEN '2. Budget (<$15)'
        WHEN f.price_usd < 35.00 THEN '3. Mid-Tier ($15-$35)'
        WHEN f.price_usd < 60.00 THEN '4. Premium ($35-$60)'
        ELSE '5. AAA Premium (>$60)'
    END AS price_bracket,
    COUNT(f.fact_id) AS total_reviews,
    ROUND(AVG(f.price_usd)::numeric, 2) AS avg_price,
    ROUND(AVG(f.playtime_hours)::numeric, 2) AS avg_playtime_hours,
    ROUND(AVG(f.positive_ratio * 100)::numeric, 2) AS positive_approval_pct,
    ROUND(SUM(f.estimated_revenue_usd)::numeric, 2) AS total_bracket_revenue
FROM fact_game_reviews_analytics f
GROUP BY price_bracket
ORDER BY price_bracket;

-- ----------------------------------------------------------------------------
-- QUERY 5: Reviewer Experience Tier Breakdown
-- Purpose: Compare behavior of Casual vs Enthusiast vs Expert reviewers.
-- Analyzes user engagement, helpfulness votes, and sentiment.
-- ----------------------------------------------------------------------------
SELECT 
    u.reviewer_level,
    COUNT(DISTINCT u.user_key) AS unique_users_count,
    COUNT(f.fact_id) AS total_reviews_written,
    ROUND(AVG(f.votes_up)::numeric, 2) AS avg_helpfulness_votes,
    ROUND(AVG(f.playtime_hours)::numeric, 2) AS avg_user_playtime,
    ROUND(AVG(f.review_score * 100)::numeric, 2) AS positive_review_pct
FROM fact_game_reviews_analytics f
JOIN dim_user u ON f.user_key = u.user_key
GROUP BY u.reviewer_level
ORDER BY total_reviews_written DESC;

-- ----------------------------------------------------------------------------
-- QUERY 6: Quarter-over-Quarter (QoQ) Growth using LAG() Window Function
-- Purpose: Track revenue and review volume evolution quarter over quarter.
-- Demonstrates time-series financial trend analysis in DW.
-- ----------------------------------------------------------------------------
WITH QuarterlyMetrics AS (
    SELECT 
        t.year,
        t.quarter,
        COUNT(f.fact_id) AS quarterly_reviews,
        ROUND(SUM(f.estimated_revenue_usd)::numeric, 2) AS quarterly_revenue
    FROM fact_game_reviews_analytics f
    JOIN dim_time t ON f.time_key = t.time_key
    GROUP BY t.year, t.quarter
)
SELECT 
    year,
    quarter,
    quarterly_revenue,
    LAG(quarterly_revenue, 1) OVER (ORDER BY year, quarter) AS prev_quarter_revenue,
    ROUND(
        ((quarterly_revenue - LAG(quarterly_revenue, 1) OVER (ORDER BY year, quarter)) / 
        NULLIF(LAG(quarterly_revenue, 1) OVER (ORDER BY year, quarter), 0) * 100)::numeric, 2
    ) AS qoq_revenue_growth_pct
FROM QuarterlyMetrics
ORDER BY year, quarter;

-- ----------------------------------------------------------------------------
-- QUERY 7: Weekend vs Weekday User Behavior Analysis
-- Purpose: Analyze difference in review scores, helpfulness and playtime on weekends.
-- ----------------------------------------------------------------------------
SELECT 
    CASE WHEN t.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(f.fact_id) AS total_reviews,
    ROUND(AVG(f.playtime_hours)::numeric, 2) AS avg_playtime_hours,
    ROUND(AVG(f.votes_up)::numeric, 2) AS avg_helpful_votes,
    ROUND(AVG(f.review_score * 100)::numeric, 2) AS positive_review_pct
FROM fact_game_reviews_analytics f
JOIN dim_time t ON f.time_key = t.time_key
GROUP BY day_type;

-- ----------------------------------------------------------------------------
-- QUERY 8: Developer Market Share & Popularity Ranking
-- Purpose: Multidimensional breakdown of top developers by revenue, rating & game count.
-- ----------------------------------------------------------------------------
SELECT 
    dev.developer_name,
    dev.dev_tier,
    COUNT(DISTINCT dm.game_key) AS titles_released,
    SUM(f.total_positive_reviews) AS aggregate_positive_reviews,
    ROUND(AVG(f.positive_ratio * 100)::numeric, 2) AS avg_rating_pct,
    ROUND(SUM(f.estimated_revenue_usd)::numeric, 2) AS total_developer_revenue
FROM fact_game_reviews_analytics f
JOIN dim_developer dev ON f.developer_key = dev.developer_key
JOIN dim_game dm ON f.game_key = dm.game_key
GROUP BY dev.developer_name, dev.dev_tier
ORDER BY total_developer_revenue DESC;

-- ----------------------------------------------------------------------------
-- QUERY 9: Release Date Cohort & Game Longevity Analysis
-- Purpose: Correlates release year with current engagement and review sentiment.
-- ----------------------------------------------------------------------------
SELECT 
    dm.release_year,
    COUNT(DISTINCT dm.game_key) AS games_released,
    COUNT(f.fact_id) AS total_reviews_recorded,
    ROUND(AVG(f.playtime_hours)::numeric, 2) AS avg_playtime_hours,
    ROUND(AVG(f.price_usd)::numeric, 2) AS avg_launch_price,
    ROUND(AVG(f.positive_ratio * 100)::numeric, 2) AS avg_sentiment_approval_pct,
    ROUND(SUM(f.estimated_revenue_usd)::numeric, 2) AS cohort_cumulative_revenue
FROM fact_game_reviews_analytics f
JOIN dim_game dm ON f.game_key = dm.game_key
GROUP BY dm.release_year
ORDER BY dm.release_year DESC;

-- ----------------------------------------------------------------------------
-- QUERY 10: Multi-genre Popularity & Price Cross-Analysis
-- Purpose: Identifies most profitable genre combinations and pricing power.
-- ----------------------------------------------------------------------------
SELECT 
    g.genre_name,
    COUNT(f.fact_id) AS total_reviews,
    ROUND(AVG(f.price_usd)::numeric, 2) AS avg_genre_price,
    ROUND(AVG(f.discount_pct)::numeric, 2) AS avg_discount_offered_pct,
    ROUND(AVG(f.positive_ratio * 100)::numeric, 2) AS customer_satisfaction_pct,
    ROUND(SUM(f.estimated_revenue_usd)::numeric, 2) AS total_gross_revenue
FROM fact_game_reviews_analytics f
JOIN bridge_game_genre bgg ON f.game_key = bgg.game_key
JOIN dim_genre g ON bgg.genre_key = g.genre_key
GROUP BY g.genre_name
ORDER BY total_gross_revenue DESC;
