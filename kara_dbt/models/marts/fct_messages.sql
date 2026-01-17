with messages as (
    select * from {{ ref('stg_telegram_data') }}
),
channels as (
    select * from {{ ref('dim_channels') }}
),
dates as (
    select * from {{ ref('dim_dates') }}
)

select
    -- Primary Key
    m.message_id,
    
    -- Foreign Keys for the Star Schema
    c.channel_key,
    d.date_key,
    
    -- Fact Attributes
    m.message_text,
    m.message_length,
    m.has_image,
    m.views,
    m.forwards,
    m.image_path
from messages m
left join channels c on m.channel = c.channel_name
left join dates d on m.date_key = d.date_key