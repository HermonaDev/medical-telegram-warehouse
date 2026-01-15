with messages as (
    select * from {{ ref('stg_telegram_data') }}
),
channels as (
    select * from {{ ref('dim_channels') }}
)

select
    m.message_id,
    c.channel_key,
    m.message_date,
    m.message_text,
    m.views,
    m.forwards,
    m.image_path
from messages m
join channels c on m.channel = c.channel_name