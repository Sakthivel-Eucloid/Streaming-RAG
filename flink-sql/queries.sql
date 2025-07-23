-- Contains continuous queries to run against the live data stream.
-- Run these commands one by one in the Flink SQL client AFTER running setup.sql.

-- Query 1: Continuously updating aggregation of events by user.
-- This is a long-running job.
SELECT 
    user_id,
    COUNT(*) as event_count,
    SUM(amount) as total_spent
FROM live_user_events
GROUP BY user_id;


-- Query 2: Continuous user activity summary.
-- This is a long-running INSERT job.
INSERT INTO user_activity_summary
SELECT 
    user_id,
    COUNT(*) as total_events,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MAX(event_type) as last_event_type
FROM live_user_events
GROUP BY user_id;


-- Query 3: Real-time dashboard query.
-- This is a long-running job.
SELECT 
    user_id,
    COUNT(*) as total_events,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchases,
    COUNT(CASE WHEN event_type = 'click' THEN 1 END) as clicks,
    SUM(amount) as total_spent,
    MAX(generated_at) as last_activity
FROM live_user_events
GROUP BY user_id
HAVING COUNT(*) > 1;