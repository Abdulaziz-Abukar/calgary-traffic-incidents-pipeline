{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['snapshot_id','incident_id'],
    partition_by={"field": "snapshot_ts", "data_type": "timestamp"},
    cluster_by=["incident_id","location_key"]
) }}

with f as (
  select *
  from {{ ref('fact_incident_snapshot') }}
  {% if is_incremental() %}
    -- reprocess a small lookback window to handle late arrivals / reruns
    where snapshot_ts >= timestamp_sub(
      (select max(snapshot_ts) from {{ this }}),
      interval 2 day
    )
  {% endif %}
),

scd as (
  select
    incident_id,
    incident_info,
    description,
    valid_from,
    valid_to
  from {{ ref('dim_incident_history') }}
),

cur as (
  select
    incident_id,
    incident_info,
    description
  from {{ ref('dim_incident') }}
)

select
  f.snapshot_id,
  f.incident_id,
  f.snapshot_ts,
  f.location_key,
  f.run_type,
  f.query_name,

  (scd.incident_id is null) as used_current_fallback,
  coalesce(scd.incident_info, cur.incident_info) as incident_info_asof,
  coalesce(scd.description,   cur.description)   as description_asof,

  scd.valid_from,
  scd.valid_to

from f
left join scd
  on f.incident_id = scd.incident_id
 and f.snapshot_ts >= scd.valid_from
 and (f.snapshot_ts < scd.valid_to or scd.valid_to is null)

left join cur
  on f.incident_id = cur.incident_id
