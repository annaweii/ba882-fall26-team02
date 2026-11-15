
{{ config(materialized='table') }}

with reporting_date as (

    select current_date() as as_of_date

),

restaurants as (

    select
        camis,
        restaurant_name,
        cuisine_description,
        borough,
        zip_code,
        source_extracted_at
    from {{ ref('dim_restaurants') }}

),

inspections as (

    select
        i.*
    from {{ ref('fct_inspections') }} as i
    cross join reporting_date as d
    where i.inspection_date < d.as_of_date

),

latest_inspection as (

    select
        camis,
        inspection_date as last_inspection_date,
        inspection_score as last_inspection_score,
        inspection_type as last_inspection_type

    from inspections

    qualify row_number() over (
        partition by camis
        order by
            inspection_date desc,
            inspection_id desc
    ) = 1

),

latest_grade as (

    select
        camis,
        grade as last_graded_grade,
        grade_date as last_graded_date

    from inspections
    cross join reporting_date as d

    where grade in ('A', 'B', 'C')
      and grade_date is not null
      and grade_date <= d.as_of_date

    qualify row_number() over (
        partition by camis
        order by
            grade_date desc,
            inspection_date desc,
            inspection_id desc
    ) = 1

),

inspection_history_12m as (

    select
        i.camis,

        count(*) as inspection_count_12m,

        avg(i.inspection_score)
            as average_inspection_score_12m,

        countif(i.has_critical_violation is true)
            as inspections_with_critical_count_12m,

        countif(i.has_critical_violation is not null)
            as critical_outcome_known_count_12m,

        countif(i.has_critical_violation is null)
            as unknown_critical_outcome_count_12m,

        sum(i.critical_violation_count)
            as critical_violation_count_12m

    from inspections as i
    cross join reporting_date as d

    where i.inspection_date >=
        date_sub(d.as_of_date, interval 12 month)

    group by i.camis

),

inspection_freshness as (

    select
        camis,
        max(source_extracted_at)
            as inspections_source_extracted_at

    from {{ ref('fct_inspections') }}

    group by camis

),

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

    li.last_inspection_date,
    li.last_inspection_score,
    li.last_inspection_type,

    date_diff(
        d.as_of_date,
        li.last_inspection_date,
        day
    ) as days_since_last_inspection,

    lg.last_graded_grade,
    lg.last_graded_date,

    coalesce(h.inspection_count_12m, 0)
        as inspection_count_12m,

    h.average_inspection_score_12m,

    coalesce(
        h.inspections_with_critical_count_12m, 0
    ) as inspections_with_critical_count_12m,

    coalesce(
        h.critical_outcome_known_count_12m, 0
    ) as critical_outcome_known_count_12m,

    coalesce(
        h.unknown_critical_outcome_count_12m, 0
    ) as unknown_critical_outcome_count_12m,

    coalesce(
        h.critical_violation_count_12m, 0
    ) as critical_violation_count_12m,

    case
        when r.zip_code is null then null
        else coalesce(
            c.zip_food_complaint_count_90d, 0
        )
    end as zip_food_complaint_count_90d,

    current_timestamp() as built_at,

    f.inspections_source_extracted_at,
    c.complaints_source_extracted_at

from restaurants as r

cross join reporting_date as d

left join latest_inspection as li
    on r.camis = li.camis

left join latest_grade as lg
    on r.camis = lg.camis

left join inspection_history_12m as h
    on r.camis = h.camis

left join inspection_freshness as f
    on r.camis = f.camis

left join complaints_by_zip_90d as c
    on r.zip_code = c.zip_code
