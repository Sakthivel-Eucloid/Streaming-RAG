-- Creates the necessary tables for the Flink pipeline.
-- This script is run by the Flink client on initialization.

CREATE TABLE live_user_events (
    event_id BIGINT,
    user_id STRING,
    event_type STRING,
    `timestamp` BIGINT,
    page_url STRING,
    session_id STRING,
    amount DOUBLE,
    generated_at STRING
) WITH (
    'connector' = 'filesystem',
    'path' = '/opt/flink/data',
    'format' = 'json'
);

CREATE TABLE user_activity_summary (
    user_id STRING,
    total_events BIGINT,
    total_amount DOUBLE,
    avg_amount DOUBLE,
    last_event_type STRING
) WITH (
    'connector' = 'print'
);

CREATE TABLE windowed_activity (
    user_id STRING,
    event_count BIGINT,
    total_amount DOUBLE,
    window_start STRING,
    window_end STRING
) WITH (
    'connector' = 'print'
);
