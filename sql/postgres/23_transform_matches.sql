TRUNCATE soccer_staging.fact_match_staging;

INSERT INTO soccer_staging.fact_match_staging (

    match_id,
    league_id,
    season,
    match_date,

    home_team_id,
    away_team_id,

    home_goals,
    away_goals,

    goal_diff,
    home_win,
    away_win,
    draw

)

SELECT
    m.match_api_id                 AS match_id,
    m.league_id,
    m.season,
    m.date::DATE                  AS match_date,

    m.home_team_api_id,
    m.away_team_api_id,

    m.home_team_goal,
    m.away_team_goal,

    (m.home_team_goal - m.away_team_goal) AS goal_diff,

    CASE 
        WHEN m.home_team_goal > m.away_team_goal THEN 1 
        ELSE 0 
    END AS home_win,

    CASE 
        WHEN m.away_team_goal > m.home_team_goal THEN 1 
        ELSE 0 
    END AS away_win,

    CASE 
        WHEN m.home_team_goal = m.away_team_goal THEN 1 
        ELSE 0 
    END AS draw

FROM soccer_raw.match m

WHERE
    m.match_api_id IS NOT NULL
    AND m.home_team_api_id IS NOT NULL
    AND m.away_team_api_id IS NOT NULL
    AND m.league_id IS NOT NULL
    AND m.home_team_goal IS NOT NULL
    AND m.away_team_goal IS NOT NULL;
