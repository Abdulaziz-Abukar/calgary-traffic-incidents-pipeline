-- traffic_incidents_raw

CREATE TABLE IF NOT EXISTS `PROJECT_ID.traffic_bronze.traffic_incidents_raw` (
    snapshot_id         STRING NOT NULL,
    snapshot_ts         TIMESTAMP NOT NULL,
    run_type            STRING NOT NULL,
    query_name          STRING NOT NULL,

    incident_id         STRING NOT NULL,
    incident_info       STRING,
    description         STRING,


    start_ts            TIMESTAMP,
    modified_ts         TIMESTAMP,

    quadrant            STRING,
    longitude           FLOAT64,
    latitude            FLOAT64,
    count               INT64,

    source_row_id       STRING,
    source_version      STRING,
    source_created_at   TIMESTAMP,
    source_updated_at   TIMESTAMP
)
PARTITION BY DATE(snapshot_ts)
CLUSTER BY run_type, quadrant, incident_id;