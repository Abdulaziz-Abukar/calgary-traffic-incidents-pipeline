select
    snapshot_id,
    incident_id
from {{ ref('fact_incident_snapshot_enriched') }}
where incident_info_asof is null
    or description_asof is null