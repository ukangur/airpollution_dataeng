{% snapshot wind_snapshot %}
{{ config(
    target_schema='main',
    unique_key='id',
    strategy='check',
    check_cols='all'
) }}

SELECT * FROM Wind

{% endsnapshot %}