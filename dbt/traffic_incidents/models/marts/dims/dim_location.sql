{{ config(materialized='table') }}

select distinct
    location_key,
    quadrant,
    latitude_bucket as latitude,
    longitude_bucket as longitude
from {{ ref('int_incident_location_keyed') }}
where latitude_bucket is not null
    and longitude_bucket is not null
    and quadrant is not null