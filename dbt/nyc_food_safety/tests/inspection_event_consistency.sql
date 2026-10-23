select
    camis,
    inspection_date,
    inspection_type,
    count(distinct inspection_score) as distinct_scores,
    count(distinct grade) as distinct_grades

from {{ ref('stg_inspections') }}

where is_inspected

group by
    camis,
    inspection_date,
    inspection_type

having
    count(distinct inspection_score) > 1
    or count(distinct grade) > 1
