{{ config(materialized='table') }}

with bronze as (
  select
    snapshot_id,
    snapshot_ts,
    run_type,
    query_name,
    incident_id,
    source_updated_at
  from {{ ref('stg_traffic_incidents_raw') }}
),

agg as (
  select
    snapshot_id,
    any_value(snapshot_ts) as snapshot_ts,
    any_value(run_type) as run_type,
    any_value(query_name) as query_name,

    count(*) as rows_loaded,
    count(distinct incident_id) as distinct_incidents,

    min(source_updated_at) as min_source_updated_at,
    max(source_updated_at) as max_source_updated_at,

    current_timestamp() as loaded_at
  from bronze
  group by snapshot_id
)

select *
from agg
