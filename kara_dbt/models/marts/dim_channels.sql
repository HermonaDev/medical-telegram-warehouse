with unique_channels as (
    select distinct
        channel as channel_name
    from {{ ref('stg_telegram_data') }}
)

select
    row_number() over (order by channel_name) as channel_key,
    channel_name
from unique_channels