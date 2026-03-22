{% set hours = var('freshness_hours', 36) %}

with max_ts as (
  select max(snapshot_ts) as max_snapshot_ts
  from {{ ref('fact_incident_snapshot') }}
)
select max_snapshot_ts
from max_ts
where max_snapshot_ts is not null
  and max_snapshot_ts < timestamp_sub(current_timestamp(), interval {{ hours }} hour)
