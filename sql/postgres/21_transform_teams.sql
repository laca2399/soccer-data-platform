TRUNCATE soccer_staging.dim_team_staging;

WITH latest_team_attr AS (
    SELECT
        team_api_id,
        ROW_NUMBER() OVER (
            PARTITION BY team_api_id
            ORDER BY date DESC
        ) AS rn
    FROM soccer_raw.team_attributes
)

INSERT INTO soccer_staging.dim_team_staging (
    team_id,
    team_long_name,
    team_short_name
)
SELECT DISTINCT
    t.team_api_id       AS team_id,
    t.team_long_name,
    t.team_short_name
FROM soccer_raw.team t
LEFT JOIN latest_team_attr lta
    ON t.team_api_id = lta.team_api_id
WHERE lta.rn = 1 OR lta.rn IS NULL;
