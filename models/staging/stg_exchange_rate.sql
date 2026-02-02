WITH snapshot AS (

    SELECT * FROM {{ ref('snp_raw_exchange_rate') }}

)

, renamed AS (

    SELECT
        dbt_scd_id AS id
        , currency
        , usd_rate
        , dbt_updated_at AS updated_at
        , dbt_valid_from AS valid_from
        , dbt_valid_to AS valid_to

FROM snapshot

)

SELECT * FROM renamed
