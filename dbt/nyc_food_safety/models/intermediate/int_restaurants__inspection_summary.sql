-- Grain: one row per CAMIS that has at least one inspection.
-- Latest inspection, latest letter grade, trailing 12-month history and
-- freshness, all computed from reconstructed inspections.
-- Restaurants with no inspections are absent here; the mart left joins.

with reporting_date as (

    select {{ as_of_date() }} as as_of_date

),

all_inspections as (

    select
        camis,
        inspection_id,
        inspection_date,
        inspection_type,
        inspection_score,
        grade,
        grade_date,
        critical_violation_count,
        has_critical_violation,
        source_extracted_at

    from {{ ref('int_inspections__reconstructed') }}

),

inspections as (

    select i.*
    from all_inspections as i
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

-- Intentionally NOT date-filtered (matches the original mart).
inspection_freshness as (

    select
        camis,
        max(source_extracted_at) as inspections_source_extracted_at

    from all_inspections

    group by camis

)

select
    f.camis,

    li.last_inspection_date,
    li.last_inspection_score,
    li.last_inspection_type,

    lg.last_graded_grade,
    lg.last_graded_date,

    h.inspection_count_12m,
    h.average_inspection_score_12m,
    h.inspections_with_critical_count_12m,
    h.critical_outcome_known_count_12m,
    h.unknown_critical_outcome_count_12m,
    h.critical_violation_count_12m,

    f.inspections_source_extracted_at

from inspection_freshness as f

left join latest_inspection as li
    on f.camis = li.camis

left join latest_grade as lg
    on f.camis = lg.camis

left join inspection_history_12m as h
    on f.camis = h.camis
