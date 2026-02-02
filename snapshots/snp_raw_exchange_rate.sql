{% snapshot snp_raw_exchange_rate %}

{{
    config(
      target_schema='snapshots',
      unique_key='currency',
      strategy='check',
      check_cols=['usd_rate', 'currency']
    )
}}

select * from {{ source('raw', 'exchange_rate') }}

{% endsnapshot %}