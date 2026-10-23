with source_data as (

    select *
    from {{ source('nyc_raw', 'raw_inspections') }}

),

typed as (

    select
        nullif(trim(row_key), '') as row_key,
        nullif(trim(camis), '') as camis,
        nullif(trim(dba), '') as restaurant_name,
        nullif(trim(cuisine_description), '') as cuisine_description,

        case upper(trim(boro))
            when '1' then 'MANHATTAN'
            when 'MANHATTAN' then 'MANHATTAN'
            when '2' then 'BRONX'
            when 'BRONX' then 'BRONX'
            when '3' then 'BROOKLYN'
            when 'BROOKLYN' then 'BROOKLYN'
            when '4' then 'QUEENS'
            when 'QUEENS' then 'QUEENS'
            when '5' then 'STATEN ISLAND'
            when 'STATEN ISLAND' then 'STATEN ISLAND'
            else null
        end as borough,

        regexp_extract(
            trim(zipcode),
            r'^([0-9]{5})(?:-[0-9]{4})?$'
        ) as zip_code,

        nullif(trim(building), '') as building,
        nullif(trim(street), '') as street,

        safe_cast(
            nullif(trim(latitude), '') as float64
        ) as latitude,

        safe_cast(
            nullif(trim(longitude), '') as float64
        ) as longitude,

        date(
            safe_cast(
                nullif(trim(inspection_date), '') as datetime
            )
        ) as source_inspection_date,

        nullif(trim(inspection_type), '') as inspection_type,
        nullif(trim(action), '') as action,
        upper(nullif(trim(violation_code), '')) as violation_code,
        nullif(trim(violation_description), '') as violation_description,
        upper(nullif(trim(critical_flag), '')) as critical_flag,

        safe_cast(
            nullif(trim(score), '') as float64
        ) as inspection_score,

        upper(nullif(trim(grade), '')) as grade,

        date(
            safe_cast(
                nullif(trim(grade_date), '') as datetime
            )
        ) as grade_date,

        date(
            safe_cast(
                nullif(trim(record_date), '') as datetime
            )
        ) as record_date,

        extracted_at as source_extracted_at

    from source_data

)

select
    row_key,
    camis,
    restaurant_name,
    cuisine_description,
    borough,
    zip_code,
    building,
    street,
    latitude,
    longitude,

    nullif(
        source_inspection_date, date '1900-01-01'
    ) as inspection_date,

    source_inspection_date is not null
        and source_inspection_date != date '1900-01-01'
        as is_inspected,

    inspection_type,
    action,
    violation_code,
    violation_description,
    critical_flag,

    case
        when critical_flag = 'CRITICAL' then true
        when critical_flag = 'NOT CRITICAL' then false
        else null
    end as is_critical,

    inspection_score,
    grade,
    grade_date,
    record_date,
    source_extracted_at

from typed
