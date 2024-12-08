{{ config(materialized='table', alias='Observation') }}

WITH monthly_averages AS (
    SELECT
        d.year,
        d.month,
        AVG(o.air_temperature_c) AS avg_air_temperature_c,
        AVG(o.min_temperature_c) AS avg_min_temperature_c,
        AVG(o.max_temperature_c) AS avg_max_temperature_c,
        AVG(o.wind_direction_deg) AS avg_wind_direction_deg,
        AVG(o.avg_wind_speed_m_s) AS avg_avg_wind_speed_m_s,
        AVG(o.max_wind_speed_m_s) AS avg_max_wind_speed_m_s,
        AVG(o.solar_radiation_w_m2) AS avg_solar_radiation_w_m2,
        AVG(o.sea_level_pressure_hPa) AS avg_sea_level_pressure_hPa,
        AVG(o.station_level_pressure_hPa) AS avg_station_level_pressure_hPa,
        AVG(o.precipitation_mm) AS avg_precipitation_mm,
        AVG(o.relative_humidity_pct) AS avg_relative_humidity_pct,
        AVG(o.pm25_10_ug_m3) AS avg_pm25_10_ug_m3,
        AVG(o.pm25_ug_m3) AS avg_pm25_ug_m3,
        AVG(o.pm10_ug_m3) AS avg_pm10_ug_m3,
        AVG(o.so2_ug_m3) AS avg_so2_ug_m3,
        AVG(o.no2_ug_m3) AS avg_no2_ug_m3,
        AVG(o.o3_ug_m3) AS avg_o3_ug_m3
    FROM
        Observation o
    JOIN Date d ON o.date_id = d.id
    GROUP BY
        d.year, d.month
),
filled_values AS (
    SELECT
        o.*,
        COALESCE(o.air_temperature_c, ma.avg_air_temperature_c) AS air_temperature_c_filled,
        COALESCE(o.min_temperature_c, ma.avg_min_temperature_c) AS min_temperature_c_filled,
        COALESCE(o.max_temperature_c, ma.avg_max_temperature_c) AS max_temperature_c_filled,
        COALESCE(o.wind_direction_deg, ma.avg_wind_direction_deg) AS wind_direction_deg_filled,
        COALESCE(o.avg_wind_speed_m_s, ma.avg_avg_wind_speed_m_s) AS avg_wind_speed_m_s_filled,
        COALESCE(o.max_wind_speed_m_s, ma.avg_max_wind_speed_m_s) AS max_wind_speed_m_s_filled,
        COALESCE(o.solar_radiation_w_m2, ma.avg_solar_radiation_w_m2) AS solar_radiation_w_m2_filled,
        COALESCE(o.sea_level_pressure_hPa, ma.avg_sea_level_pressure_hPa) AS sea_level_pressure_hPa_filled,
        COALESCE(o.station_level_pressure_hPa, ma.avg_station_level_pressure_hPa) AS station_level_pressure_hPa_filled,
        COALESCE(o.precipitation_mm, ma.avg_precipitation_mm) AS precipitation_mm_filled,
        COALESCE(o.relative_humidity_pct, ma.avg_relative_humidity_pct) AS relative_humidity_pct_filled,
        COALESCE(o.pm25_10_ug_m3, ma.avg_pm25_10_ug_m3) AS pm25_10_ug_m3_filled,
        COALESCE(o.pm25_ug_m3, ma.avg_pm25_ug_m3) AS pm25_ug_m3_filled,
        COALESCE(o.pm10_ug_m3, ma.avg_pm10_ug_m3) AS pm10_ug_m3_filled,
        COALESCE(o.so2_ug_m3, ma.avg_so2_ug_m3) AS so2_ug_m3_filled,
        COALESCE(o.no2_ug_m3, ma.avg_no2_ug_m3) AS no2_ug_m3_filled,
        COALESCE(o.o3_ug_m3, ma.avg_o3_ug_m3) AS o3_ug_m3_filled
    FROM
        Observation o
    JOIN Date d ON o.date_id = d.id
    LEFT JOIN monthly_averages ma
        ON d.year = ma.year AND d.month = ma.month
)
SELECT
    o.id,
    o.location_id,
    o.date_id,
    o.time_id,
    o.wind_id,
    o.rain_id,
    o.pressure_id,
    o.temperature_id,
    o.radiation_id,
    o.humidity_id,
    o.airpollution_id,
    air_temperature_c_filled AS air_temperature_c,
    min_temperature_c_filled AS min_temperature_c,
    max_temperature_c_filled AS max_temperature_c,
    wind_direction_deg_filled AS wind_direction_deg,
    avg_wind_speed_m_s_filled AS avg_wind_speed_m_s,
    max_wind_speed_m_s_filled AS max_wind_speed_m_s,
    solar_radiation_w_m2_filled AS solar_radiation_w_m2,
    sea_level_pressure_hPa_filled AS sea_level_pressure_hPa,
    station_level_pressure_hPa_filled AS station_level_pressure_hPa,
    precipitation_mm_filled AS precipitation_mm,
    relative_humidity_pct_filled AS relative_humidity_pct,
    pm25_10_ug_m3_filled AS pm25_10_ug_m3,
    pm25_ug_m3_filled AS pm25_ug_m3,
    pm10_ug_m3_filled AS pm10_ug_m3,
    so2_ug_m3_filled AS so2_ug_m3,
    no2_ug_m3_filled AS no2_ug_m3,
    o3_ug_m3_filled AS o3_ug_m3
FROM
    filled_values o