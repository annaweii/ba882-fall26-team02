with source_data as (

    select *
    from {{ source('nyc_raw', 'raw_311_complaints') }}

),

typed as (

    select
        nullif(trim(unique_key), '') as complaint_id,

        safe_cast(
            nullif(trim(created_date), '') as datetime
        ) as complaint_created_at,

        safe_cast(
            nullif(trim(closed_date), '') as datetime
        ) as complaint_closed_at,

        nullif(trim(agency), '') as agency,
        nullif(trim(complaint_type), '') as complaint_type,
        nullif(trim(descriptor), '') as complaint_detail,
        nullif(trim(location_type), '') as location_type,

        case
            when upper(trim(borough)) in (
                'MANHATTAN',
                'BRONX',
                'BROOKLYN',
                'QUEENS',
                'STATEN ISLAND'
            )
            then upper(trim(borough))
            else null
        end as borough,

        regexp_extract(
            trim(incident_zip),
            r'^([0-9]{5})(?:-[0-9]{4})?$'
        ) as zip_code,

        nullif(trim(incident_address), '') as incident_address,
        nullif(trim(street_name), '') as street_name,
        nullif(trim(city), '') as city,
        upper(nullif(trim(status), '')) as status,

        nullif(
            trim(resolution_description), ''
        ) as resolution_description,

        safe_cast(
            nullif(trim(latitude), '') as float64
        ) as latitude,

        safe_cast(
            nullif(trim(longitude), '') as float64
        ) as longitude,

        extracted_at as source_extracted_at

    from source_data

)

select
    complaint_id,
    complaint_created_at,
    date(complaint_created_at) as complaint_created_date,
    complaint_closed_at,
    date(complaint_closed_at) as complaint_closed_date,
    agency,
    complaint_type,
    complaint_detail,
    location_type,
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

from typed
