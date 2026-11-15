select
    s.row_key as violation_id,
    i.inspection_id,
    s.camis,
    s.inspection_date,
    s.inspection_type,
    s.violation_code,
    s.violation_description,
    s.critical_flag,
    s.is_critical,
    s.source_extracted_at

from {{ ref('stg_inspections') }} as s

left join {{ ref('fct_inspections') }} as i
    on s.camis = i.camis
    and s.inspection_date = i.inspection_date
    and s.inspection_type is not distinct from i.inspection_type

where
    s.is_inspected
    and s.violation_code is not null
