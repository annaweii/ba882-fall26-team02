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
                    i.camis as camis,
                    i.inspection_date as inspection_date,
                    i.inspection_type as inspection_type
                )
            )
        )
    ) as inspection_id,

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

    case
        when i.critical_violation_count > 0 then true
        when i.unknown_critical_status_count > 0 then cast(null as bool)
        else false
    end as has_critical_violation,

    i.source_extracted_at

from inspection_groups as i

left join {{ ref('dim_restaurants') }} as r
    on i.camis = r.camis
