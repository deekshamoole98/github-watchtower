-- Athena external table over the raw GitHub Events JSON in S3.
-- Field names match the verified GitHub Events API v3 response structure.
-- Run this once, then MSCK REPAIR TABLE after each new date= partition appears.
--
-- Note: Athena reads each file in raw/date=YYYY-MM-DD/ as a JSON array.
-- The SerDe expects one JSON object per line (NDJSON), so the Lambda
-- must write events as newline-delimited JSON, not a bare array.
-- See lambda_function.py: events are written one per line via json.dumps.

CREATE EXTERNAL TABLE IF NOT EXISTS github_events_raw (
    id          STRING,          -- event id (string, not int, per API)
    type        STRING,          -- e.g. PushEvent, CreateEvent, DeleteEvent
    actor       STRUCT<
                    id:             BIGINT,
                    login:          STRING,
                    display_login:  STRING,
                    gravatar_id:    STRING,
                    url:            STRING,
                    avatar_url:     STRING
                >,
    repo        STRUCT<
                    id:             BIGINT,
                    name:           STRING,   -- "owner/repo-name" format
                    url:            STRING
                >,
    payload     STRING,          -- kept as raw STRING; shape varies by event type
    public      BOOLEAN,
    created_at  TIMESTAMP,
    org         STRUCT<          -- optional; absent for personal (non-org) repos
                    id:             BIGINT,
                    login:          STRING,
                    gravatar_id:    STRING,
                    url:            STRING,
                    avatar_url:     STRING
                >
)
PARTITIONED BY (`date` STRING)   -- matches S3 prefix raw/date=YYYY-MM-DD/
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'TRUE'
)
STORED AS TEXTFILE
LOCATION 's3://YOUR_BUCKET_NAME/raw/'
TBLPROPERTIES (
    'has_encrypted_data' = 'false'
);

-- Discover partitions after each new date= prefix is written:
MSCK REPAIR TABLE github_events_raw;

-- Quick sanity check — should return rows:
-- SELECT id, type, actor.login, repo.name, created_at
-- FROM github_events_raw
-- WHERE date = '2026-01-19'
-- LIMIT 10;
