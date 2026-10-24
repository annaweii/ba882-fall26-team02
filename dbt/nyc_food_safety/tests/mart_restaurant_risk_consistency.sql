select
    camis
from {{ ref('mart_restaurant_risk') }}
where
    inspection_count_12m != (
        critical_outcome_known_count_12m
        + unknown_critical_outcome_count_12m
    )
    or inspections_with_critical_count_12m
        > critical_outcome_known_count_12m
    or inspection_count_12m < 0
    or critical_violation_count_12m < 0
    or inspections_with_critical_count_12m < 0
    or zip_food_complaint_count_90d < 0
    or last_inspection_date >= as_of_date
    or days_since_last_inspection < 0
