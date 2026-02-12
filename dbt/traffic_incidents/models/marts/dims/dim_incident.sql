{{ config(materialized='table') }}

select
    incident_id,
    incident_info,
    description
from {{ref('stg_incident_current') }}