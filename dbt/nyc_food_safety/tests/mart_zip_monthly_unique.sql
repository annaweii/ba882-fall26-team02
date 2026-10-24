select
    zip_code,
    month_start,
    count(*) as row_count
from {{ ref('mart_zip_monthly') }}
group by zip_code, month_start
having count(*) > 1
