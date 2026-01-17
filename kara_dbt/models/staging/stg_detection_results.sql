with raw_detections as (
    select * from {{ source('telegram_raw', 'detection_results') }}
)
select
    channel,
    image_path,
    label as detected_item,
    confidence
from raw_detections
