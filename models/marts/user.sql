{{ config (
    materialized = 'incremental',
    unique_key = 'user_id',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
        'field': 'updated_at',
        'data_type': 'timestamp',
        'granularity': 'day'
    }
) }}


SELECT 
    user.user_id
    , user.created_at
    , metric.first_purchase_on
    , metric.last_purchase_on
    , user.platform
    , user.acquisition_channel
    , user.ip_country AS country
    , COALESCE(metric.total_orders, 0) AS total_orders
    , COALESCE(metric.total_usd_amount_spent, 0) AS total_usd_amount_spent
    , COALESCE(metric.total_gbp_amount_spent, 0) AS total_gbp_amount_spent
    , COALESCE(metric.usd_amount_spent_new, 0) AS usd_amount_spent_new
    , COALESCE(metric.gbp_amount_spent_new, 0) AS gbp_amount_spent_new
    , COALESCE(metric.usd_amount_spent_returned, 0) AS usd_amount_spent_returned
    , COALESCE(metric.gbp_amount_spent_returned, 0) AS gbp_amount_spent_returned
    , metric.days_between_first_and_last_purchase
    , metric.distinct_products_purchased
    , COALESCE(metric.is_new, 0) AS is_new
    , COALESCE(metric.has_returned, 0) AS has_returned
    , user.updated_at
FROM {{ ref('dim_user') }} AS user
LEFT JOIN {{ ref('_user_metrics_on_orders') }} AS metric
    USING (user_id)
-- Taking the latest record for each user, eg. latest country, platform 
WHERE user.valid_to IS null
 
-- Incremental filter
{% if is_incremental() %}
    AND user.updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
