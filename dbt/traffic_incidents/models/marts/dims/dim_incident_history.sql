{{ config(
    materialized='table',
    partition_by={"field": "valid_from", "data_type": "timestamp"},
    cluster_by=["incident_id"]
) }}

with events as (
    select
        incident_id,
        incident_info,
        description,
        valid_from
    from {{ ref('int_incident_attr_change_events') }}
),

scd as (
    select
        incident_id,
        incident_info,
        description,
        valid_from,
        lead(valid_from) over (
            partition by incident_id
            order by valid_from
        ) as valid_to
    from events
)

select
    incident_id,
    incident_info,
    description,
    valid_from,
    valid_to,
    valid_to is null as is_current
from scd