{{ config(materialized='table') }}

WITH detections AS (
    SELECT 
        image_path,
        label AS detected_item,
        confidence,
        -- Extract message_id from path if possible, or join on path
        CAST(SPLIT_PART(image_path, '_', 1) AS INTEGER) as message_id 
    FROM {{ ref('stg_detection_results') }} -- Update to your actual staging table name
),
messages AS (
    SELECT * FROM {{ ref('fct_messages') }}
)

SELECT 
    m.message_id,
    m.channel_key,
    m.date_key,
    d.detected_item,
    d.confidence,
    m.message_text
FROM messages m
INNER JOIN detections d ON m.message_id = d.message_id