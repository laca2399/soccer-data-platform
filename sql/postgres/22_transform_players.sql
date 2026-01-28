TRUNCATE soccer_staging.dim_player_staging;

INSERT INTO soccer_staging.dim_player_staging (
    player_id,
    player_name,
    birthday,
    height,
    weight
)

SELECT
    p.player_api_id                           AS player_id,
    trim(regexp_replace(p.player_name, ',.*$', '')) AS player_name,
    p.birthday::DATE,
    p.height,
    p.weight

FROM soccer_raw.player p

WHERE
    p.player_api_id IS NOT NULL
    AND p.player_name IS NOT NULL;