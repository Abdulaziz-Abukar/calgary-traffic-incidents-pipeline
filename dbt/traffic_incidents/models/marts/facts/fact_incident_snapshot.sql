{{ config(
    materialized='table',
    partition_by={"field": "snapshot_ts", "data_type": "timestamp"},
    cluster_by=["incident_id", "location_key"]
) }}

select
    snapshot_id,
    snapshot_ts,

    incident_id,
    location_key,

    -- time fields (useful for filtering)
    start_ts,
    modified_ts,
    source_updated_at,

    -- metric
    count,

    -- run metadata
    run_type,
    query_name

from {{ ref('int_snapshot_incident_location_keyed') }}