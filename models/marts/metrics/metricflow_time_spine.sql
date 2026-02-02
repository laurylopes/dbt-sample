{{config(
    materialized='table'
    )
}}

with days as (
    {{dbt.date_spine(
        'day',
        "date(2023,01,01)",
        "current_date"
    )
    }}
),

final as (
    select cast(date_day as date) as date_day
    from days
)

select *
from final