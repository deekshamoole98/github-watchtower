-- Four Athena views over github_events_raw.
-- Run db/athena_schema.sql first, then MSCK REPAIR TABLE, then these.
-- All views exclude bot actors (login ending in [bot]) from counts;
-- add AND actor.login NOT LIKE '%[bot]%' to any view to restore that.

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Event type distribution over time
--    "What kinds of events are happening, and is the mix shifting?"
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_event_type_by_day AS
SELECT
    date,
    type                                         AS event_type,
    COUNT(*)                                     AS event_count,
    ROUND(
        COUNT(*) * 100.0
        / SUM(COUNT(*)) OVER (PARTITION BY date),
        2
    )                                            AS pct_of_day_total
FROM github_events_raw
GROUP BY date, type
ORDER BY date DESC, event_count DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Most active repos and orgs by event volume
--    "Which repos and orgs are the busiest in the public event stream?"
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_top_repos AS
SELECT
    date,
    repo.name                                    AS repo_name,
    -- org.login is NULL for personal repos
    COALESCE(org.login, SPLIT_PART(repo.name, '/', 1)) AS owner,
    COUNT(*)                                     AS event_count,
    COUNT(DISTINCT actor.login)                  AS unique_actors,
    COUNT(DISTINCT type)                         AS distinct_event_types
FROM github_events_raw
GROUP BY date, repo.name, org.login
ORDER BY date DESC, event_count DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Hourly activity pattern (UTC)
--    "When is the public event stream busiest? Are there dead hours?"
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_hourly_activity AS
SELECT
    date,
    HOUR(created_at)                             AS hour_utc,
    COUNT(*)                                     AS event_count,
    COUNT(DISTINCT actor.login)                  AS unique_actors,
    -- breakdown of the two most common event types by hour
    SUM(CASE WHEN type = 'PushEvent'   THEN 1 ELSE 0 END) AS push_events,
    SUM(CASE WHEN type = 'CreateEvent' THEN 1 ELSE 0 END) AS create_events
FROM github_events_raw
GROUP BY date, HOUR(created_at)
ORDER BY date DESC, hour_utc;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Day-over-day event volume change
--    "Is activity trending up or down week over week?"
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_daily_volume_trend AS
WITH daily AS (
    SELECT
        date,
        COUNT(*)                                 AS event_count,
        COUNT(DISTINCT actor.login)              AS unique_actors,
        COUNT(DISTINCT repo.name)                AS unique_repos
    FROM github_events_raw
    GROUP BY date
),
with_lag AS (
    SELECT
        date,
        event_count,
        unique_actors,
        unique_repos,
        LAG(event_count)     OVER (ORDER BY date) AS prev_day_events,
        LAG(unique_actors)   OVER (ORDER BY date) AS prev_day_actors,
        LAG(unique_repos)    OVER (ORDER BY date) AS prev_day_repos
    FROM daily
)
SELECT
    date,
    event_count,
    unique_actors,
    unique_repos,
    prev_day_events,
    CASE
        WHEN prev_day_events IS NULL OR prev_day_events = 0 THEN NULL
        ELSE ROUND((event_count - prev_day_events) * 100.0 / prev_day_events, 2)
    END                                          AS event_count_pct_change,
    CASE
        WHEN prev_day_actors IS NULL OR prev_day_actors = 0 THEN NULL
        ELSE ROUND((unique_actors - prev_day_actors) * 100.0 / prev_day_actors, 2)
    END                                          AS actors_pct_change
FROM with_lag
ORDER BY date DESC;
