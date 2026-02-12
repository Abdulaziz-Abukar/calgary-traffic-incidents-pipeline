{{ config(materialized='view') }}

select
    *,
    concat(
        quadrant, '_',
        cast(round(latitude, 3) as string), '_',
        cast(round(longitude, 3) as string)
    ) as location_key,
    round(latitude, 3) as latitude_bucket,
    round(longitude, 3) as longitude_bucket
from {{ ref('stg_incident_current') }}