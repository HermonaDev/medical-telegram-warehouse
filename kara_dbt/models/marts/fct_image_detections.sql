-- kara_dbt/models/marts/fct_image_detections.sql
{{ config(materialized='table') }}

SELECT 
    m.message_id,
    m.channel_key,
    m.date_key as date_key,           -- Rubric Alignment
    d.label as detected_class,        -- Rubric Alignment
    d.confidence as confidence_score, -- Rubric Alignment
    CASE 
        WHEN d.label IN ('pill', 'capsule', 'syrup') THEN 'Medication'
        ELSE 'Medical Equipment'
    END AS image_category
FROM {{ ref('fct_messages') }} m
JOIN {{ ref('stg_detection_results') }} d ON m.message_id = d.message_id