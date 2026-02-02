{% snapshot snp_raw_user %}

{{
    config(
      target_schema='snapshots',
      unique_key='user_id',
      strategy='check',
      check_cols=['ip_country', 'platform']
    )
}}

select * from {{ source('raw', 'user') }}

{% endsnapshot %}