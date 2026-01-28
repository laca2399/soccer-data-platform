TRUNCATE soccer_staging.dim_league_staging;

INSERT INTO soccer_staging.dim_league_staging (
    league_id,
    league_name,
    country_name
)
SELECT
    l.id            AS league_id,
    l.name          AS league_name,
    c.name          AS country_name
FROM soccer_raw.league l
JOIN soccer_raw.country c
    ON l.country_id = c.id
WHERE l.name IS NOT NULL;
