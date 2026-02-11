MERGE `PROJECT_ID.traffic_silver.incident_current` T
USING (
  WITH candidates AS (
    SELECT
      incident_id,
      incident_info,
      description,
      start_ts,
      modified_ts,
      quadrant,
      longitude,
      latitude,
      count,
      source_row_id,
      source_version,
      source_created_at,
      source_updated_at,
      snapshot_id AS last_snapshot_id,
      snapshot_ts AS last_snapshot_ts,
      run_type AS last_run_type,
      query_name AS last_query_name
    FROM `PROJECT_ID.traffic_bronze.traffic_incidents_raw`
    WHERE snapshot_id = @snapshot_id    -- for now update to specific snapshot_id to pull
  ),
  dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        c.*,
        ROW_NUMBER() OVER (
          PARTITION BY incident_id
          ORDER BY source_updated_at DESC, last_snapshot_ts DESC, source_version DESC
        ) AS rn
      FROM candidates c
    )
    WHERE rn = 1
  )
  SELECT * FROM dedup
) S
ON T.incident_id = S.incident_id

WHEN MATCHED AND (
  T.source_updated_at IS NULL OR S.source_updated_at > T.source_updated_at
  OR (S.source_updated_at = T.source_updated_at AND S.last_snapshot_ts > T.last_snapshot_ts)
) THEN
  UPDATE SET
    incident_info      = S.incident_info,
    description        = S.description,
    start_ts           = S.start_ts,
    modified_ts        = S.modified_ts,
    quadrant           = S.quadrant,
    longitude          = S.longitude,
    latitude           = S.latitude,
    count              = S.count,
    source_row_id      = S.source_row_id,
    source_version     = S.source_version,
    source_created_at  = S.source_created_at,
    source_updated_at  = S.source_updated_at,
    last_snapshot_id   = S.last_snapshot_id,
    last_snapshot_ts   = S.last_snapshot_ts,
    last_run_type      = S.last_run_type,
    last_query_name    = S.last_query_name,
    loaded_at          = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (
    incident_id,
    incident_info,
    description,
    start_ts,
    modified_ts,
    quadrant,
    longitude,
    latitude,
    count,
    source_row_id,
    source_version,
    source_created_at,
    source_updated_at,
    last_snapshot_id,
    last_snapshot_ts,
    last_run_type,
    last_query_name,
    loaded_at
  )
  VALUES (
    S.incident_id,
    S.incident_info,
    S.description,
    S.start_ts,
    S.modified_ts,
    S.quadrant,
    S.longitude,
    S.latitude,
    S.count,
    S.source_row_id,
    S.source_version,
    S.source_created_at,
    S.source_updated_at,
    S.last_snapshot_id,
    S.last_snapshot_ts,
    S.last_run_type,
    S.last_query_name,
    CURRENT_TIMESTAMP()
  );
