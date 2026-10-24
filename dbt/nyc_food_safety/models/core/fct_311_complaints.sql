{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='complaint_id',
        on_schema_change='fail'
    )
}}

select
    complaint_id,
    complaint_created_at,
    complaint_created_date,
    complaint_closed_at,
    complaint_closed_date,

    date_trunc(
        complaint_created_date, month
    ) as month_start,

    agency,
    complaint_type,
    complaint_detail,
    location_type,

    complaint_type = 'Food Poisoning' as is_food_poisoning,

    borough,
    zip_code,
    incident_address,
    street_name,
    city,
    status,
    resolution_description,
    latitude,
    longitude,
    source_extracted_at

from {{ ref('stg_311_complaints') }}
