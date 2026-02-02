{{ config (
    materialized = 'table',
    unique_key = 'id',
    partition_by = {
        'field': 'updated_at',
        'data_type': 'timestamp',
        'granularity': 'day'
    }
) }}

WITH gbp_rate AS (
    SELECT 
        usd_rate AS rate_from_usd_to_gbp
        , valid_from
    FROM {{ ref('stg_exchange_rate') }}
    WHERE currency = 'GBP'
)

SELECT 
    id
    , currency
    , usd_rate AS rate_from_usd
    , usd_rate / rate_from_usd_to_gbp  AS rate_from_gbp
    , valid_from
    , valid_to
    , updated_at
    , MIN(valid_from) OVER (PARTITION BY currency) = valid_from AS is_initial
FROM {{ ref('stg_exchange_rate') }}
LEFT JOIN gbp_rate 
    -- To get the correct GBP rate for the valid_from date
    USING(valid_from)
