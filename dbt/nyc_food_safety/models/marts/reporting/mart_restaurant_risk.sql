{{ config(materialized='table') }}

with reporting_date as (

    select {{ as_of_date() }} as as_of_date

),

restaurants as (

    select
        camis,
        restaurant_name,
        cuisine_description,
        borough,
        zip_code

    from {{ ref('dim_restaurants') }}

),

inspection_summary as (

    select * from {{ ref('int_restaurants__inspection_summary') }}

),

-- TODO: move to an int_ model once the 311 staging/fact SQL has been reviewed.
complaints_by_zip_90d as (

    select
        c.zip_code,

        count(*) as zip_food_complaint_count_90d,

        max(c.source_extracted_at)
            as complaints_source_extracted_at

    from {{ ref('fct_311_complaints') }} as c
    cross join reporting_date as d

    where c.zip_code is not null
      and c.complaint_created_date >=
          date_sub(d.as_of_date, interval 90 day)
      and c.complaint_created_date < d.as_of_date

    group by c.zip_code

)

select
    r.camis,
    r.restaurant_name,
    r.cuisine_description,
    r.borough,
    r.zip_code,

    d.as_of_date,

    s.last_inspection_date,
    s.last_inspection_score,
    s.last_inspection_type,

    date_diff(
        d.as_of_date,
        s.last_inspection_date,
        day
    ) as days_since_last_inspection,

    s.last_graded_grade,
    s.last_graded_date,

    coalesce(s.inspection_count_12m, 0)
        as inspection_count_12m,

    s.average_inspection_score_12m,

    coalesce(
        s.inspections_with_critical_count_12m, 0
    ) as inspections_with_critical_count_12m,

    coalesce(
        s.critical_outcome_known_count_12m, 0
    ) as critical_outcome_known_count_12m,

    coalesce(
        s.unknown_critical_outcome_count_12m, 0
    ) as unknown_critical_outcome_count_12m,

    coalesce(
        s.critical_violation_count_12m, 0
    ) as critical_violation_count_12m,

    case
        when r.zip_code is null then null
        else coalesce(
            c.zip_food_complaint_count_90d, 0
        )
    end as zip_food_complaint_count_90d,

    current_timestamp() as built_at,

    s.inspections_source_extracted_at,
    c.complaints_source_extracted_at

from restaurants as r

cross join reporting_date as d

left join inspection_summary as s
    on r.camis = s.camis

left join complaints_by_zip_90d as c
    on r.zip_code = c.zip_code
