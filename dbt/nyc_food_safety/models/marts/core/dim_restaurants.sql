select
    camis,
    restaurant_name,
    cuisine_description,
    borough,
    zip_code,
    building,
    street,
    latitude,
    longitude,
    record_date,
    source_extracted_at

from {{ ref('int_restaurants__latest') }}
