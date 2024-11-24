{{ config(materialized='table', alias='Weather') }}

WITH monthly_averages AS (
    SELECT
        d.year,
        d.month,
        AVG(w.solar_radiation) AS avg_solar_radiation,
        AVG(w.sea_level_pressure) AS avg_sea_level_pressure,
        AVG(w.station_level_pressure) AS avg_station_level_pressure,
        AVG(w.precipitation) AS avg_precipitation,
        AVG(w.relative_humidity) AS avg_relative_humidity
    FROM
        Weather w
    JOIN
        Date d
    ON
        w.date_id = d.id
    WHERE
        w.solar_radiation IS NOT NULL
        OR w.sea_level_pressure IS NOT NULL
        OR w.station_level_pressure IS NOT NULL
        OR w.precipitation IS NOT NULL
        OR w.relative_humidity IS NOT NULL
    GROUP BY
        d.year, d.month
),
filled_values AS (
    SELECT
        w.*,
        COALESCE(w.solar_radiation, ma.avg_solar_radiation) AS solar_radiation_filled,
        COALESCE(w.sea_level_pressure, ma.avg_sea_level_pressure) AS sea_level_pressure_filled,
        COALESCE(w.station_level_pressure, ma.avg_station_level_pressure) AS station_level_pressure_filled,
        COALESCE(w.precipitation, ma.avg_precipitation) AS precipitation_filled,
        COALESCE(w.relative_humidity, ma.avg_relative_humidity) AS relative_humidity_filled
    FROM
        Weather w
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
    solar_radiation_filled AS solar_radiation,
    sea_level_pressure_filled AS sea_level_pressure,
    station_level_pressure_filled AS station_level_pressure,
    precipitation_filled AS precipitation,
    relative_humidity_filled AS relative_humidity
FROM
    filled_values