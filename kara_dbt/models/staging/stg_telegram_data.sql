with raw_data as (
    select * from {{ source('telegram_raw', 'telegram_messages') }}
)

select
    (content->>'id')::int as message_id,
    channel,
    (content->>'date')::timestamp as message_timestamp,
    (content->>'date')::date as date_key,
    content->>'text' as message_text,
    length(content->>'text') as message_length,
    case 
        when content->>'image_path' is not null then true 
        else false 
    end as has_image,
    (content->>'views')::int as views,
    (content->>'forwards')::int as forwards,
    content->>'image_path' as image_path -- <--- ENSURE THIS ALIAS IS HERE
from raw_data