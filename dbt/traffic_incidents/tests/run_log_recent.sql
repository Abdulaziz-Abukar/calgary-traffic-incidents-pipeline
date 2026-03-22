-- Fail only if the table has data but nothing recent
with stats as (
  select
    count(*) as total_rows,
    countif(snapshot_ts >= timestamp_sub(current_timestamp(), interval 36 hour)) as recent_rows
  from {{ ref('pipeline_run_log') }}
)
select *
from stats
where total_rows > 0
  and recent_rows = 0
