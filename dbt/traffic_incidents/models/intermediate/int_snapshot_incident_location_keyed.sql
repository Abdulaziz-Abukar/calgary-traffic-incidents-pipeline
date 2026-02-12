{{ config(materialized='view') }}

select
    -- snapshot_grain
    snapshot_id,
    snapshot_ts,
    run_type,
    query_name,

    -- incident grain
    incident_id,

    -- timestamps (business + source)
    start_ts,
    modified_ts,
    source_created_at,
    source_updated_at,

    -- measures
    count,

    -- raw location fields
    quadrant,
    latitude,
    longitude,

    -- same location_key logic as dim_location
    concat(
        quadrant, '_',
        cast(round(latitude, 3) as string), '_',
        cast(round(longitude, 3) as string)
    ) as location_key

from {{ ref('stg_traffic_incidents_raw') }}
where incident_id is not null
    and snapshot_id is not null
    and quadrant is not null
    and latitude is not null
    and longitude is not null