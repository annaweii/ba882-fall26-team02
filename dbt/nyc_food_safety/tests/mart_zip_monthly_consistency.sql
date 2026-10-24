select
    zip_code,
    month_start
from {{ ref('mart_zip_monthly') }}
where
    inspection_count != (
        critical_outcome_known_count
        + unknown_critical_outcome_count
    )
    or inspections_with_critical_count
        > critical_outcome_known_count
    or critical_inspection_rate < 0
    or critical_inspection_rate > 1
    or grade_a_rate < 0
    or grade_a_rate > 1
    or graded_inspection_count != (
        grade_a_count + grade_b_count + grade_c_count
    )
    or food_poisoning_complaint_count > food_complaint_count
    or inspection_count < 0
    or food_complaint_count < 0
