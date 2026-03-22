with latest_day as (
  select timestamp_trunc(max(snapshot_ts), day) as day
  from {{ ref('fact_incident_snapshot') }}
),
cnt as (
  select count(*) as n
  from {{ ref('fact_incident_snapshot') }} f
  join latest_day d
    on timestamp_trunc(f.snapshot_ts, day) = d.day
)

select n
from cnt
where n = 0