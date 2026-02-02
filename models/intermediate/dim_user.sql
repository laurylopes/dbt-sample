{{ config (
    materialized = 'table',
    unique_key = 'id',
    partition_by = {
        'field': 'updated_at',
        'data_type': 'timestamp',
        'granularity': 'day'
    }
) }}

SELECT 
    id
    , user_id
    , created_at
    , platform
    , acquisition_channel
    , ip_country
    , updated_at
    , valid_from
    , valid_to
FROM {{ ref('stg_user') }} 
