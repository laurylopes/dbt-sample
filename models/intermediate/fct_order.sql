{{ config (
    materialized = 'table',
    unique_key = 'order_id',
    partition_by = {
        'field': 'updated_at',
        'data_type': 'timestamp',
        'granularity': 'day'
    }
) }}

SELECT 
    order_id
    , user_id
    , created_at
    , updated_at
    , amount
    , currency
    , esim_package
    , payment_method

    -- Assuming that the card country is same as IP country as a fallback
    , card_country
    , destination_country
    , status AS latest_status

    -- Creating status timestamp fields for completed, refunded, and failed
    , MIN(CASE WHEN status = 'completed' THEN updated_at END) OVER (PARTITION BY order_id) AS completed_at
    , MIN(CASE WHEN status = 'refunded' THEN updated_at END) OVER (PARTITION BY order_id) AS refunded_at
    , MIN(CASE WHEN status = 'failed' THEN updated_at END) OVER (PARTITION BY order_id) AS failed_at

FROM {{ ref('stg_order') }}
    
-- Keep only the latest status per order
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1
