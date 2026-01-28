-- LEAGUES

CREATE TABLE IF NOT EXISTS soccer_staging.dim_league_staging (
    league_id INTEGER PRIMARY KEY,
    league_name TEXT,
    country_name TEXT
);

-- TEAMS

CREATE TABLE IF NOT EXISTS soccer_staging.dim_team_staging (
    team_id INTEGER PRIMARY KEY,
    team_long_name TEXT,
    team_short_name TEXT
);

-- PLAYERS

CREATE TABLE IF NOT EXISTS soccer_staging.dim_player_staging (
    player_id INTEGER PRIMARY KEY,
    player_name TEXT,
    birthday DATE,
    height FLOAT,
    weight FLOAT
);

-- MATCHES

CREATE TABLE IF NOT EXISTS soccer_staging.fact_match_staging (
    match_id INTEGER,
    league_id INTEGER,
    home_team_id INTEGER,
    away_team_id INTEGER,
    home_goals INTEGER,
    away_goals INTEGER,
    match_date DATE,
    season TEXT,
    goal_diff INTEGER,
    home_win SMALLINT,
    away_win SMALLINT,
    draw SMALLINT
);
