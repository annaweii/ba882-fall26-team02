with ranked_records as (

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
        source_extracted_at,

        row_number() over (
            partition by camis
            order by
                record_date desc,
                source_extracted_at desc,
                inspection_date desc,
                row_key desc
        ) as record_rank

    from {{ ref('stg_inspections') }}
    where camis is not null

)

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

from ranked_records
where record_rank = 1
