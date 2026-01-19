{{ config(materialized='table') }}

SELECT 
    m.message_id,
    m.channel_key,
    d.label as detected_item,
    CASE 
        WHEN d.label IN ('pill', 'capsule', 'syrup') THEN 'Medication'
        WHEN d.label IN ('syringe', 'gloves', 'mask') THEN 'Medical Equipment'
        ELSE 'Other'
    END AS image_category,
    d.confidence,
    m.message_text
FROM {{ ref('fct_messages') }} m
JOIN {{ ref('stg_detection_results') }} d ON m.message_id = d.message_id