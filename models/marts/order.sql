{{ config (
    materialized = 'incremental',
    unique_key = 'order_id',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
        'field': 'updated_at',
        'data_type': 'timestamp',
        'granularity': 'day'
    }
) }}

SELECT 
    order_id
    , orders.user_id
    , orders.created_at
    , orders.updated_at
    , orders.amount
    , orders.currency

    -- Creating USD and GBP amount fields
    -- If no exchange rate is found for the order date then use the initial exchange rate for that currency
    , orders.amount / COALESCE(exchange_rate.rate_from_usd, oldest_exchange_rate.rate_from_usd) AS usd_amount
    , orders.amount / COALESCE(exchange_rate.rate_from_gbp, oldest_exchange_rate.rate_from_gbp) AS gbp_amount

    , esim_package
    , payment_method

    -- Assuming that the card country is same as IP country as a fallback to fill nulls
    , COALESCE(orders.card_country, user.ip_country) AS card_country
    , destination_country
    , latest_status
    , completed_at
    , refunded_at
    , failed_at

FROM {{ ref('fct_order') }}  AS orders
LEFT JOIN {{ ref('fct_exchange_rate') }} AS exchange_rate
    ON orders.currency = exchange_rate.currency
    AND orders.created_at BETWEEN exchange_rate.valid_from AND exchange_rate.valid_to
LEFT JOIN {{ ref('fct_exchange_rate') }} AS oldest_exchange_rate
    ON orders.currency = oldest_exchange_rate.currency
    AND oldest_exchange_rate.is_initial
LEFT JOIN {{ ref('dim_user') }} AS user
    ON orders.user_id = user.user_id
    -- To ensure to get the correct ip country at the time the order was placed
    AND orders.created_at BETWEEN user.valid_from AND user.valid_to

-- Incremental filter
{% if is_incremental() %}
WHERE orders.updated_at > (SELECT MAX(orders.updated_at) FROM {{ this }})
{% endif %}
