CREATE TABLE IF NOT EXISTS soccer_raw.country (
    id TEXT,
    name TEXT
);

CREATE TABLE IF NOT EXISTS soccer_raw.league (
    id TEXT,
    country_id TEXT,
    name TEXT
);

CREATE TABLE IF NOT EXISTS soccer_raw.team (
    id TEXT,
    team_api_id TEXT,
    team_fifa_api_id TEXT,
    team_long_name TEXT,
    team_short_name TEXT
);

CREATE TABLE IF NOT EXISTS soccer_raw.team_attributes (
    id TEXT,
    team_fifa_api_id TEXT,
    team_api_id TEXT,
    date TEXT,
    buildUpPlaySpeed TEXT,
    buildUpPlaySpeedClass TEXT,
    buildUpPlayDribbling TEXT,
    buildUpPlayDribblingClass TEXT,
    buildUpPlayPassing TEXT,
    buildUpPlayPassingClass TEXT,
    buildUpPlayPositioningClass TEXT,
    chanceCreationPassing TEXT,
    chanceCreationPassingClass TEXT,
    chanceCreationCrossing TEXT,
    chanceCreationCrossingClass TEXT,
    chanceCreationShooting TEXT,
    chanceCreationShootingClass TEXT,
    chanceCreationPositioningClass TEXT,
    defencePressure TEXT,
    defencePressureClass TEXT,
    defenceAggression TEXT,
    defenceAggressionClass TEXT,
    defenceTeamWidth TEXT,
    defenceTeamWidthClass TEXT,
    defenceDefenderLineClass TEXT
);

CREATE TABLE IF NOT EXISTS soccer_raw.player (
    id TEXT,
    player_api_id TEXT,
    player_name TEXT,
    player_fifa_api_id TEXT,
    birthday TEXT,
    height TEXT,
    weight TEXT
);

CREATE TABLE IF NOT EXISTS soccer_raw.player_attributes (
    id TEXT,
    player_fifa_api_id TEXT,
    player_api_id TEXT,
    date TEXT,
    overall_rating TEXT,
    potential TEXT,
    preferred_foot TEXT,
    attacking_work_rate TEXT,
    defensive_work_rate TEXT,
    crossing TEXT,
    finishing TEXT,
    heading_accuracy TEXT,
    short_passing TEXT,
    volleys TEXT,
    dribbling TEXT,
    curve TEXT,
    free_kick_accuracy TEXT,
    long_passing TEXT,
    ball_control TEXT,
    acceleration TEXT,
    sprint_speed TEXT,
    agility TEXT,
    reactions TEXT,
    balance TEXT,
    shot_power TEXT,
    jumping TEXT,
    stamina TEXT,
    strength TEXT,
    long_shots TEXT,
    aggression TEXT,
    interceptions TEXT,
    positioning TEXT,
    vision TEXT,
    penalties TEXT,
    marking TEXT,
    standing_tackle TEXT,
    sliding_tackle TEXT,
    gk_diving TEXT,
    gk_handling TEXT,
    gk_kicking TEXT,
    gk_positioning TEXT,
    gk_reflexes TEXT
);

CREATE TABLE IF NOT EXISTS soccer_raw.match (
    id TEXT,
    country_id TEXT,
    league_id TEXT,
    season TEXT,
    stage TEXT,
    date TEXT,
    match_api_id TEXT,
    home_team_api_id TEXT,
    away_team_api_id TEXT,
    home_team_goal TEXT,
    away_team_goal TEXT
);
