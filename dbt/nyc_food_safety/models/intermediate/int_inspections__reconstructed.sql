-- Grain: one row per CAMIS + inspection_date + inspection_type.
-- Collapses citation-level staging rows into inspection events, so
-- scores and grades are counted once per event. inspection_id is defined
-- here and only here; keep the struct field names unchanged or every
-- hash changes.

with inspection_groups as (

    select
        camis,
        inspection_date,
        inspection_type,

        max(inspection_score) as inspection_score,
        max(grade) as grade,
        max(grade_date) as grade_date,

        countif(
            violation_code is not null
        ) as violation_count,

        countif(
            violation_code is not null
            and is_critical is true
        ) as critical_violation_count,

        countif(
            violation_code is not null
            and is_critical is null
        ) as unknown_critical_status_count,

        max(source_extracted_at) as source_extracted_at

    from {{ ref('stg_inspections') }}

    where
        is_inspected
        and camis is not null

    group by
        camis,
        inspection_date,
        inspection_type

)

select
    to_hex(
        sha256(
            to_json_string(
                struct(
                    camis as camis,
                    inspection_date as inspection_date,
                    inspection_type as inspection_type
                )
            )
        )
    ) as inspection_id,

    camis,
    inspection_date,
    inspection_type,
    inspection_score,
    grade,
    grade_date,
    violation_count,
    critical_violation_count,
    unknown_critical_status_count,

    case
        when critical_violation_count > 0 then true
        when unknown_critical_status_count > 0 then cast(null as bool)
        else false
    end as has_critical_violation,

    source_extracted_at

from inspection_groups
