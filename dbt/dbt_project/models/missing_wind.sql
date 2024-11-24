{{ config(materialized='table', alias='Wind') }}

WITH monthly_averages AS (
    SELECT
        d.year,
        d.month,
        AVG(w.wind_direction) AS avg_wind_direction,
        AVG(w.avg_wind_speed) AS avg_avg_wind_speed,
        AVG(w.max_wind_speed) AS avg_max_wind_speed
    FROM
        Wind w
    JOIN
        Date d
    ON
        w.date_id = d.id
    WHERE
        w.wind_direction IS NOT NULL
        OR w.avg_wind_speed IS NOT NULL
        OR w.max_wind_speed IS NOT NULL
    GROUP BY
        d.year, d.month
),
filled_values AS (
    SELECT
        w.*,
        COALESCE(w.wind_direction, ma.avg_wind_direction) AS wind_direction_filled,
        COALESCE(w.avg_wind_speed, ma.avg_avg_wind_speed) AS avg_wind_speed_filled,
        COALESCE(w.max_wind_speed, ma.avg_max_wind_speed) AS max_wind_speed_filled
    FROM
        Wind w
    LEFT JOIN
        Date d
    ON
        w.date_id = d.id
    LEFT JOIN
        monthly_averages ma
    ON
        d.year = ma.year AND d.month = ma.month
)
SELECT
    id,
    date_id,
    wind_direction_filled AS wind_direction,
    avg_wind_speed_filled AS avg_wind_speed,
    max_wind_speed_filled AS max_wind_speed
FROM
    filled_values