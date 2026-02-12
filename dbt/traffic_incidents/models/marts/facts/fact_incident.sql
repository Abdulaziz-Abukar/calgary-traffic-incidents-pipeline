{{ config(
  materialized='table',
  partition_by={"field": "loaded_at", "data_type": "timestamp"},
  cluster_by=["incident_id","location_key"]
) }}

select
    -- grain: 1 row per incident (current/latest state)
    incident_id,
    location_key,

    -- timestamps
    start_ts,
    modified_ts,
    source_created_at,
    source_updated_at,

    -- metric
    count,

    -- lineage
    source_row_id,
    source_version,
    last_snapshot_id,
    last_snapshot_ts,
    last_run_type,
    last_query_name,
    loaded_at

from {{ ref('int_incident_location_keyed') }}