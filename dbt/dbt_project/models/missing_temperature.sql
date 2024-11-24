{{ config(materialized='table', alias='Temperature') }}

WITH monthly_averages AS (
    SELECT
        d.year,
        d.month,
        AVG(t.air_temperature) AS avg_air_temperature,
        AVG(t.min_temperature) AS avg_min_temperature,
        AVG(t.max_temperature) AS avg_max_temperature
    FROM
        Temperature t
    JOIN
        Date d
    ON
        t.date_id = d.id
    WHERE
        t.air_temperature IS NOT NULL
        OR t.min_temperature IS NOT NULL
        OR t.max_temperature IS NOT NULL
    GROUP BY
        d.year, d.month
),
filled_values AS (
    SELECT
        t.*,
        COALESCE(t.air_temperature, ma.avg_air_temperature) AS air_temperature_filled,
        COALESCE(t.min_temperature, ma.avg_min_temperature) AS min_temperature_filled,
        COALESCE(t.max_temperature, ma.avg_max_temperature) AS max_temperature_filled
    FROM
        Temperature t
    LEFT JOIN
        Date d
    ON
        t.date_id = d.id
    LEFT JOIN
        monthly_averages ma
    ON
        d.year = ma.year AND d.month = ma.month
)
SELECT
    id,
    date_id,
    air_temperature_filled AS air_temperature,
    min_temperature_filled AS min_temperature,
    max_temperature_filled AS max_temperature
FROM
    filled_values