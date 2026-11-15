
{{ config(materialized='table') }}

with zip_codes as (

    select zip_code
    from {{ ref('fct_inspections') }}
    where zip_code is not null
      and inspection_date >= date '2023-01-01'

    union distinct

    select zip_code
    from {{ ref('fct_311_complaints') }}
    where zip_code is not null
      and complaint_created_date >= date '2023-01-01'

),

calendar_months as (

    select month_start
    from unnest(
        generate_date_array(
            date '2023-01-01',
            date_trunc(current_date(), month),
            interval 1 month
        )
    ) as month_start

),

zip_month_calendar as (

    select
        z.zip_code,
        m.month_start
    from zip_codes as z
    cross join calendar_months as m

),

inspections_monthly as (

    select
        zip_code,
        month_start,

        count(*) as inspection_count,
        count(distinct camis) as restaurants_inspected_count,

        countif(has_critical_violation is not null)
            as critical_outcome_known_count,

        countif(has_critical_violation is true)
            as inspections_with_critical_count,

        countif(has_critical_violation is null)
            as unknown_critical_outcome_count,

        sum(critical_violation_count)
            as critical_violation_count,

        safe_divide(
            countif(has_critical_violation is true),
            countif(has_critical_violation is not null)
        ) as critical_inspection_rate,

        avg(inspection_score) as average_inspection_score,

        countif(grade = 'A') as grade_a_count,
        countif(grade = 'B') as grade_b_count,
        countif(grade = 'C') as grade_c_count,

        countif(grade in ('A', 'B', 'C'))
            as graded_inspection_count,

        safe_divide(
            countif(grade = 'A'),
            countif(grade in ('A', 'B', 'C'))
        ) as grade_a_rate,

        max(source_extracted_at)
            as inspections_source_extracted_at

    from {{ ref('fct_inspections') }}

    where zip_code is not null
      and inspection_date >= date '2023-01-01'
      and inspection_date <= current_date()

    group by zip_code, month_start

),

complaints_monthly as (

    select
        zip_code,
        month_start,

        count(*) as food_complaint_count,

        countif(is_food_poisoning is true)
            as food_poisoning_complaint_count,

        max(source_extracted_at)
            as complaints_source_extracted_at

    from {{ ref('fct_311_complaints') }}

    where zip_code is not null
      and complaint_created_date >= date '2023-01-01'
      and complaint_created_date <= current_date()

    group by zip_code, month_start

),

borough_by_zip as (

    select
        zip_code,

        case
            when count(distinct borough) = 1
                then max(borough)
            else cast(null as string)
        end as borough

    from {{ ref('dim_restaurants') }}

    where zip_code is not null

    group by zip_code

),

combined as (

    select
        cal.zip_code,
        cal.month_start,

        b.borough,

        coalesce(i.inspection_count, 0)
            as inspection_count,

        coalesce(i.restaurants_inspected_count, 0)
            as restaurants_inspected_count,

        coalesce(i.critical_outcome_known_count, 0)
            as critical_outcome_known_count,

        coalesce(i.inspections_with_critical_count, 0)
            as inspections_with_critical_count,

        coalesce(i.unknown_critical_outcome_count, 0)
            as unknown_critical_outcome_count,

        coalesce(i.critical_violation_count, 0)
            as critical_violation_count,

        i.critical_inspection_rate,
        i.average_inspection_score,

        coalesce(i.grade_a_count, 0)
            as grade_a_count,

        coalesce(i.grade_b_count, 0)
            as grade_b_count,

        coalesce(i.grade_c_count, 0)
            as grade_c_count,

        coalesce(i.graded_inspection_count, 0)
            as graded_inspection_count,

        i.grade_a_rate,

        coalesce(c.food_complaint_count, 0)
            as food_complaint_count,

        coalesce(c.food_poisoning_complaint_count, 0)
            as food_poisoning_complaint_count,

        i.inspections_source_extracted_at,
        c.complaints_source_extracted_at,

        cal.month_start < date_trunc(current_date(), month)
            as is_complete_month

    from zip_month_calendar as cal

    left join inspections_monthly as i
        on cal.zip_code = i.zip_code
       and cal.month_start = i.month_start

    left join complaints_monthly as c
        on cal.zip_code = c.zip_code
       and cal.month_start = c.month_start

    left join borough_by_zip as b
        on cal.zip_code = b.zip_code

),

previous_month_values as (

    select
        *,

        lag(critical_inspection_rate) over (
            partition by zip_code
            order by month_start
        ) as previous_critical_inspection_rate,

        lag(food_complaint_count) over (
            partition by zip_code
            order by month_start
        ) as previous_food_complaint_count,

        lag(is_complete_month) over (
            partition by zip_code
            order by month_start
        ) as previous_month_is_complete

    from combined

)

select
    * except (
        previous_critical_inspection_rate,
        previous_food_complaint_count,
        previous_month_is_complete
    ),

    case
        when is_complete_month
         and previous_month_is_complete
         and critical_inspection_rate is not null
         and previous_critical_inspection_rate is not null
        then
            100 * (
                critical_inspection_rate
                - previous_critical_inspection_rate
            )
        else null
    end as critical_rate_change_pp,

    case
        when is_complete_month
         and previous_month_is_complete
        then
            food_complaint_count
            - previous_food_complaint_count
        else null
    end as food_complaint_change_count,

    current_timestamp() as built_at

from previous_month_values
