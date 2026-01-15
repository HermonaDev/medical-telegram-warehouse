with raw_data as (
    select * from {{ source('telegram_raw', 'telegram_messages') }}
)

select
    id as internal_id,
    channel,
    (content->>'id')::int as message_id,
    (content->>'date')::timestamp as message_date,
    content->>'text' as message_text,
    (content->>'views')::int as views,
    (content->>'forwards')::int as forwards,
    content->>'image_path' as image_path
from raw_data
where content->>'text' is not null