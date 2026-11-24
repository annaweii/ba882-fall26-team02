{{ config(materialized='table') }}

-- ML training table: one row per routine initial inspection.
-- Every feature uses ONLY inspections dated before that inspection.
-- Column names match mart_restaurant_risk so the same model can score it.
-- Label: label_not_a = score of 14 or more (about 40% of inspections).

with inspections as (
    select inspection_id, camis, inspection_date, inspection_type,
           inspection_score, grade, grade_date,
           critical_violation_count, has_critical_violation
    from {{ ref('int_inspections__reconstructed') }}
),
targets as (
    select * from inspections
    where inspection_type = 'Cycle Inspection / Initial Inspection'
      and inspection_score is not null
      and inspection_date >= '2023-04-01'
      and inspection_date < date_sub({{ as_of_date() }}, interval 30 day)
),
history as (
    select t.inspection_id,
           count(p.inspection_id)                       as inspection_count_12m,
           avg(p.inspection_score)                      as average_inspection_score_12m,
           countif(p.has_critical_violation is true)    as inspections_with_critical_count_12m,
           coalesce(sum(p.critical_violation_count), 0) as critical_violation_count_12m
    from targets t
    left join inspections p
      on  p.camis = t.camis
      and p.inspection_date <  t.inspection_date
      and p.inspection_date >= date_sub(t.inspection_date, interval 12 month)
    group by t.inspection_id
),
last_prior as (
    select t.inspection_id,
           p.inspection_date  as last_inspection_date,
           p.inspection_score as last_inspection_score
    from targets t
    join inspections p on p.camis = t.camis and p.inspection_date < t.inspection_date
    qualify row_number() over (partition by t.inspection_id
                               order by p.inspection_date desc, p.inspection_id desc) = 1
),
last_grade as (
    select t.inspection_id, p.grade as last_graded_grade
    from targets t
    join inspections p
      on p.camis = t.camis and p.grade in ('A','B','C') and p.grade_date < t.inspection_date
    qualify row_number() over (partition by t.inspection_id
                               order by p.grade_date desc, p.inspection_date desc, p.inspection_id desc) = 1
),
restaurants as (
    select camis, borough, cuisine_description, zip_code
    from {{ ref('int_restaurants__latest') }}
),
zip_complaints as (
    select t.inspection_id, count(c.complaint_id) as complaint_count
    from targets t
    join restaurants r on r.camis = t.camis
    left join {{ ref('fct_311_complaints') }} c
      on  c.zip_code = r.zip_code
      and c.complaint_created_date >= date_sub(t.inspection_date, interval 90 day)
      and c.complaint_created_date <  t.inspection_date
    group by t.inspection_id
)
select
    t.inspection_id, t.camis, t.inspection_date,
    r.borough, r.cuisine_description, r.zip_code,
    date_diff(t.inspection_date, lp.last_inspection_date, day) as days_since_last_inspection,
    lp.last_inspection_score,
    lg.last_graded_grade,
    h.inspection_count_12m,
    h.average_inspection_score_12m,
    h.inspections_with_critical_count_12m,
    h.critical_violation_count_12m,
    case when r.zip_code is null then null else z.complaint_count end as zip_food_complaint_count_90d,
    t.inspection_score >= 14 as label_not_a
from targets t
left join history        h  on h.inspection_id  = t.inspection_id
left join last_prior     lp on lp.inspection_id = t.inspection_id
left join last_grade     lg on lg.inspection_id = t.inspection_id
left join zip_complaints z  on z.inspection_id  = t.inspection_id
left join restaurants    r  on r.camis          = t.camis
