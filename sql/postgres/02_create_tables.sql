-- ============================
-- Dimension Tables
-- ============================

-- Drop fact table first (depends on dimensions)
DROP TABLE IF EXISTS soccer_analytics.fact_matches CASCADE;

-- Drop dimension tables
DROP TABLE IF EXISTS soccer_analytics.dim_team CASCADE;
DROP TABLE IF EXISTS soccer_analytics.dim_league CASCADE;

CREATE TABLE IF NOT EXISTS soccer_analytics.dim_league (
    league_id INTEGER PRIMARY KEY,
    league_name TEXT NOT NULL,
    country_name TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS soccer_analytics.dim_team (
    team_id INTEGER PRIMARY KEY,
    team_name TEXT NOT NULL,
    league_id INTEGER,
    country_name TEXT
);

-- ============================
-- Fact Table
-- ============================

CREATE TABLE IF NOT EXISTS soccer_analytics.fact_matches (
    match_id INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    league_id INTEGER NOT NULL,
    home_team_id INTEGER NOT NULL,
    away_team_id INTEGER NOT NULL,
    home_goals INTEGER,
    away_goals INTEGER
);



