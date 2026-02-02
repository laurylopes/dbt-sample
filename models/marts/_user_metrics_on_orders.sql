{{ config (
    materialized='ephemeral', 
    unique_key='user_id'
) }}

/* This model calculates user metrics based on orders such as total orders, 
first and last purchase dates, and user status flags */

with user_orders as (
    select 
        user_id,
        count(distinct order_id) as total_orders,
        sum(usd_amount) as usd_total_amount,
        sum(gbp_amount) as gbp_total_amount,
        cast(min(updated_at) as date) as first_purchase_on,
        cast(max(updated_at) as date) as last_purchase_on, 
        string_agg(distinct esim_package, ', ' order by esim_package) as distinct_products_purchased,
        count(distinct esim_package) as distinct_products_count
    from {{ ref('order') }}
    -- Only consider completed orders for user metrics
    where completed_at is not null
    group by user_id
), 

user_status as (
    select 
        user_id,
        -- is_new_user: 1 if user has only 1 completed order
        case when total_orders = 1 then 1 else 0 end as is_new,
        -- has_returned: 1 if user purchased on different days (days_between_first_and_last_purchase > 0)
        case when date_diff(last_purchase_on, first_purchase_on, DAY) > 0 then 1 else 0 end as has_returned     
    from user_orders
)

select 
    user_id,
    first_purchase_on,
    last_purchase_on,
    date_diff(last_purchase_on, first_purchase_on, DAY) as days_between_first_and_last_purchase,
    total_orders,
    round(usd_total_amount, 2) as total_usd_amount_spent,
    round(gbp_total_amount, 2) as total_gbp_amount_spent,
    round(case when is_new = 1 then usd_total_amount else 0 end, 2) as usd_amount_spent_new,
    round(case when is_new = 1 then gbp_total_amount else 0 end, 2) as gbp_amount_spent_new,
    round(case when has_returned = 1 then usd_total_amount else 0 end, 2) as usd_amount_spent_returned,
    round(case when has_returned = 1 then gbp_total_amount else 0 end, 2) as gbp_amount_spent_returned,
    distinct_products_purchased,
    case when distinct_products_count > 1 then 1 else 0 end as has_purchased_different_products, 
    is_new,
    has_returned          
from user_orders
inner join user_status
    using (user_id)


