select incident_id
from {{ ref('dim_incident_history') }}
where is_current = true
group by incident_id
having count(*) > 1