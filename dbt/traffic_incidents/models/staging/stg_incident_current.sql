{{ config(materialized='view') }}

select
  cast(incident_id as string) as incident_id,

  trim(cast(incident_info as string)) as incident_info,
  cast(description as string) as description,

  cast(start_ts as timestamp) as start_ts,
  cast(modified_ts as timestamp) as modified_ts,

  upper(cast(quadrant as string)) as quadrant,
  cast(longitude as float64) as longitude,
  cast(latitude as float64) as latitude,
  cast(count as int64) as count,

  -- source lineage
  cast(source_row_id as string) as source_row_id,
  cast(source_version as string) as source_version,
  cast(source_created_at as timestamp) as source_created_at,
  cast(source_updated_at as timestamp) as source_updated_at,

  -- ingestion lineage (the bronze row that won)
  cast(last_snapshot_id as string) as last_snapshot_id,
  cast(last_snapshot_ts as timestamp) as last_snapshot_ts,
  cast(last_run_type as string) as last_run_type,
  cast(last_query_name as string) as last_query_name,

  cast(loaded_at as timestamp) as loaded_at

from {{ source('traffic_silver', 'incident_current') }}
