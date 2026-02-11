CREATE TABLE IF NOT EXISTS `PROJECT_ID-incident.traffic_silver.incident_current` (
  incident_id STRING NOT NULL,

  incident_info STRING,
  description STRING,
  start_ts TIMESTAMP,
  modified_ts TIMESTAMP,
  quadrant STRING,
  longitude FLOAT64,
  latitude FLOAT64,
  count INT64,

  -- source system lineage
  source_row_id STRING,
  source_version STRING,
  source_created_at TIMESTAMP,
  source_updated_at TIMESTAMP,   -- latest known update time from Socrata

  -- ingestion lineage (from bronze row that “won”)
  last_snapshot_id STRING,
  last_snapshot_ts TIMESTAMP,
  last_run_type STRING,
  last_query_name STRING,

  -- warehouse metadata
  loaded_at TIMESTAMP NOT NULL
)
CLUSTER BY incident_id, quadrant;