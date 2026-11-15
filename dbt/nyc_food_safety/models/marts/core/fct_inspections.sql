select
    i.inspection_id,
    i.camis,
    i.inspection_date,
    date_trunc(i.inspection_date, month) as month_start,
    i.inspection_type,

    r.borough,
    r.zip_code,

    i.inspection_score,
    i.grade,
    i.grade_date,
    i.violation_count,
    i.critical_violation_count,
    i.unknown_critical_status_count,
    i.has_critical_violation,
    i.source_extracted_at

from {{ ref('int_inspections__reconstructed') }} as i

left join {{ ref('int_restaurants__latest') }} as r
    on i.camis = r.camis
