-- WATERMARK TABLE DDL

CREATE TABLE IF NOT EXISTS `PROJECT_ID.traffic_control.watermark` (
  source_name               STRING NOT NULL,
  last_source_updated_at    TIMESTAMP,
  updated_at                TIMESTAMP NOT NULL
);
