with x as (
    select
        count(*) as total_rows,
        sum(case when used_current_fallback then 1 else 0 end) as fallback_rows
    from {{ ref('fact_incident_snapshot_enriched') }}
)

select *
from x
where safe_divide(fallback_rows, total_rows) > 0.05