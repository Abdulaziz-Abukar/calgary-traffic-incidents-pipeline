with base as (
    select
        incident_id,
        incident_info,
        description,
        source_updated_at,
        snapshot_ts,
        snapshot_id
    from {{ ref('stg_traffic_incidents_raw') }}
    where incident_id is not null
        and source_updated_at is not null
),

-- if multiple records land with the same source_updated_at for an incident,
-- pick a single canonical one determinstically
dedup_same_updated_at as (
    select * except(rn)
    from (
        select
            *,
            row_number() over (
                partition by incident_id, source_updated_at
                order by snapshot_ts desc, snapshot_id desc
            ) as rn
        from base
    )
    where rn = 1
),

-- identify actual change events (including the first observed state).
changes as (
    select
        *,
        lag(incident_info) over (
            partition by incident_id
            order by source_updated_at, snapshot_ts, snapshot_id
        ) as prev_incident_info,
        lag(description) over (
            partition by incident_id
            order by source_updated_at, snapshot_ts, snapshot_id
        ) as prev_description
    from dedup_same_updated_at
)

select
    incident_id,
    incident_info,
    description,
    source_updated_at as valid_from
from changes
    where prev_incident_info is null
    or prev_description is null
    or incident_info != prev_incident_info
    or description != prev_description