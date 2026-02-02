WITH snapshot AS (

    SELECT * FROM {{ ref('snp_raw_user') }}

)

, renamed AS (

    SELECT
	
        dbt_scd_id AS id
        , CAST(user_id AS string) AS user_id
        , created_at
        , platform
        , acquisition_channel
        , CASE WHEN ip_country IS null THEN 'UNKNOWN' ELSE UPPER(ip_country) END AS ip_country
        , dbt_updated_at AS updated_at
        , dbt_valid_from AS valid_from
        , dbt_valid_to AS valid_to
        
    FROM snapshot

)

SELECT * FROM renamed
