{{ config(materialized='table', alias='Airpollution') }}

WITH monthly_averages AS (
    SELECT
        d.year,
        d.month,
        AVG(a.pm25_10) AS avg_pm25_10,
        AVG(a.pm25) AS avg_pm25,
        AVG(a.pm10) AS avg_pm10,
        AVG(a.so2) AS avg_so2,
        AVG(a.no2) AS avg_no2,
        AVG(a.o3) AS avg_o3
    FROM
        Airpollution a
    JOIN
        Date d
    ON
        a.date_id = d.id
    WHERE
        a.pm25_10 IS NOT NULL
        OR a.pm25 IS NOT NULL
        OR a.pm10 IS NOT NULL
        OR a.so2 IS NOT NULL
        OR a.no2 IS NOT NULL
        OR a.o3 IS NOT NULL
    GROUP BY
        d.year, d.month
),
filled_values AS (
    SELECT
        a.*,
        COALESCE(a.pm25_10, ma.avg_pm25_10) AS pm25_10_filled,
        COALESCE(a.pm25, ma.avg_pm25) AS pm25_filled,
        COALESCE(a.pm10, ma.avg_pm10) AS pm10_filled,
        COALESCE(a.so2, ma.avg_so2) AS so2_filled,
        COALESCE(a.no2, ma.avg_no2) AS no2_filled,
        COALESCE(a.o3, ma.avg_o3) AS o3_filled
    FROM
        Airpollution a
    LEFT JOIN
        Date d
    ON
        a.date_id = d.id
    LEFT JOIN
        monthly_averages ma
    ON
        d.year = ma.year AND d.month = ma.month
)
SELECT
    id,
    date_id,
    pm25_10_filled AS pm25_10,
    pm25_filled AS pm25,
    pm10_filled AS pm10,
    so2_filled AS so2,
    no2_filled AS no2,
    o3_filled AS o3
FROM
    filled_values