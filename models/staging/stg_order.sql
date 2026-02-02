WITH source AS (

    SELECT * FROM {{ source('raw', 'order') }}

)

, renamed AS (

    SELECT
        order_id
        , created_at
        , updated_at
        , LOWER(status) AS status
        , amount
        , UPPER(currency) AS currency
        , esim_package
        , payment_method
        , UPPER(card_country) AS card_country
        , UPPER(destination_country) AS destination_country
        , CAST(user_id AS string) AS user_id

    FROM source

)

SELECT * FROM renamed
