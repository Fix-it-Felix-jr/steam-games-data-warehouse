-- ============================================================================
-- STEAM GAMES ANALYTICS DATA WAREHOUSE (DM_PROJECT)
-- PostgreSQL / SQLite Compatible DDL Schema (Star Schema)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STAGING AREA TABLES
-- ----------------------------------------------------------------------------

DROP TABLE IF EXISTS stg_steam_games CASCADE;
CREATE TABLE stg_steam_games (
    app_id INT PRIMARY KEY,
    name VARCHAR(255),
    release_date VARCHAR(50),
    developer VARCHAR(255),
    publisher VARCHAR(255),
    genres TEXT,
    categories TEXT,
    original_price NUMERIC(10, 2),
    discount_price NUMERIC(10, 2),
    positive_reviews INT,
    negative_reviews INT,
    owners VARCHAR(100)
);

DROP TABLE IF EXISTS stg_steam_reviews CASCADE;
CREATE TABLE stg_steam_reviews (
    review_id BIGINT PRIMARY KEY,
    app_id INT,
    author_id VARCHAR(100),
    review_text TEXT,
    voted_up BOOLEAN,
    votes_up INT,
    votes_funny INT,
    playtime_forever INT,
    timestamp_created BIGINT
);

-- ----------------------------------------------------------------------------
-- DIMENSION TABLES
-- ----------------------------------------------------------------------------

DROP TABLE IF EXISTS bridge_game_genre CASCADE;
DROP TABLE IF EXISTS fact_game_reviews_analytics CASCADE;
DROP TABLE IF EXISTS dim_game CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;
DROP TABLE IF EXISTS dim_genre CASCADE;
DROP TABLE IF EXISTS dim_developer CASCADE;
DROP TABLE IF EXISTS dim_user CASCADE;

-- Dimension 1: Game
CREATE TABLE dim_game (
    game_key SERIAL PRIMARY KEY,
    app_id INT UNIQUE NOT NULL,
    game_title VARCHAR(255) NOT NULL,
    developer_name VARCHAR(255),
    publisher_name VARCHAR(255),
    age_rating VARCHAR(50),
    release_date DATE,
    release_year INT,
    is_indie BOOLEAN DEFAULT FALSE
);

-- Dimension 2: Time / Date
CREATE TABLE dim_time (
    time_key INT PRIMARY KEY, -- Formatted as YYYYMMDD
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Dimension 3: Genre
CREATE TABLE dim_genre (
    genre_key SERIAL PRIMARY KEY,
    genre_name VARCHAR(100) UNIQUE NOT NULL,
    genre_category VARCHAR(100) DEFAULT 'General'
);

-- Dimension 4: Developer
CREATE TABLE dim_developer (
    developer_key SERIAL PRIMARY KEY,
    developer_name VARCHAR(255) UNIQUE NOT NULL,
    country VARCHAR(100) DEFAULT 'Unknown',
    dev_tier VARCHAR(50) DEFAULT 'Indie' -- 'AAA', 'AA', 'Indie'
);

-- Dimension 5: User / Reviewer
CREATE TABLE dim_user (
    user_key SERIAL PRIMARY KEY,
    author_id VARCHAR(100) UNIQUE NOT NULL,
    total_reviews_written INT DEFAULT 1,
    reviewer_level VARCHAR(50) DEFAULT 'Casual' -- 'Casual', 'Enthusiast', 'Expert'
);

-- Bridge Table: Game <-> Genre (Many-to-Many)
CREATE TABLE bridge_game_genre (
    game_key INT REFERENCES dim_game(game_key) ON DELETE CASCADE,
    genre_key INT REFERENCES dim_genre(genre_key) ON DELETE CASCADE,
    PRIMARY KEY (game_key, genre_key)
);

-- ----------------------------------------------------------------------------
-- FACT TABLE
-- ----------------------------------------------------------------------------

CREATE TABLE fact_game_reviews_analytics (
    fact_id SERIAL PRIMARY KEY,
    game_key INT REFERENCES dim_game(game_key),
    time_key INT REFERENCES dim_time(time_key),
    user_key INT REFERENCES dim_user(user_key),
    developer_key INT REFERENCES dim_developer(developer_key),
    price_usd NUMERIC(10, 2),
    discount_pct NUMERIC(5, 2),
    review_score INT, -- 1 for positive, 0 for negative
    votes_up INT,
    playtime_hours NUMERIC(10, 2),
    total_positive_reviews INT,
    total_negative_reviews INT,
    positive_ratio NUMERIC(5, 2),
    estimated_revenue_usd NUMERIC(12, 2)
);

-- ----------------------------------------------------------------------------
-- ANALYTICAL INDEXES FOR OLAP OPTIMIZATION
-- ----------------------------------------------------------------------------

CREATE INDEX idx_fact_game_key ON fact_game_reviews_analytics(game_key);
CREATE INDEX idx_fact_time_key ON fact_game_reviews_analytics(time_key);
CREATE INDEX idx_fact_user_key ON fact_game_reviews_analytics(user_key);
CREATE INDEX idx_fact_dev_key ON fact_game_reviews_analytics(developer_key);
CREATE INDEX idx_dim_time_year_quarter ON dim_time(year, quarter);
CREATE INDEX idx_dim_game_dev ON dim_game(developer_name);
