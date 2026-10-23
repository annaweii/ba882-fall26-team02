with citation_totals as (

    select
        inspection_id,
        count(*) as violation_count,
        countif(is_critical is true) as critical_violation_count,
        countif(is_critical is null) as unknown_critical_status_count

    from {{ ref('fct_violations') }}

    group by inspection_id

)

select
    i.inspection_id,
    i.violation_count,
    coalesce(v.violation_count, 0) as detailed_violation_count

from {{ ref('fct_inspections') }} as i

left join citation_totals as v
    on i.inspection_id = v.inspection_id

where
    i.violation_count != coalesce(v.violation_count, 0)

    or i.critical_violation_count
        != coalesce(v.critical_violation_count, 0)

    or i.unknown_critical_status_count
        != coalesce(v.unknown_critical_status_count, 0)
