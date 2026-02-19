-- psql note:
--   CREATE DATABASE IF NOT EXISTS is not supported
--   create the DB once from psql/pgAdmin, then run this script
--   or, drop the db if it exists, then create it.

DROP DATABASE IF EXISTS wikipedia_events
CREATE DATABASE wikipedia_events

-- connect to the target database before creating schema objects
\c wikipedia_events

-- store raw Wikimedia events used by pipeline and analytics
CREATE TABLE IF NOT EXISTS raw_events (
    id TEXT PRIMARY KEY,
    domain TEXT,
    dt TIMESTAMP WITH TIME ZONE NOT NULL,
    type TEXT,
    namespace INTEGER,
    title TEXT,
    comment TEXT,
    "user" TEXT,
    bot BOOLEAN,
    wiki TEXT,
    minor BOOLEAN,
    patrolled BOOLEAN,
    log_type TEXT,
    length INTEGER
);

-- indexes for common time-bounded and grouped analytics queries
CREATE INDEX IF NOT EXISTS idx_raw_events_dt ON raw_events (dt);
CREATE INDEX IF NOT EXISTS idx_raw_events_user_dt ON raw_events ("user", dt);
CREATE INDEX IF NOT EXISTS idx_raw_events_type_dt ON raw_events (type, dt);
CREATE INDEX IF NOT EXISTS idx_raw_events_wiki_dt ON raw_events (wiki, dt);

-- views can be added here as analytics requirements solidify


-- REMOVE BLOCKERS IF TRUNCATE LOCKS
SELECT
  pid,
  usename,
  application_name,
  state,
  wait_event_type,
  wait_event,
  xact_start,
  query_start,
  query
FROM pg_stat_activity
WHERE datname = current_database()
ORDER BY query_start;

SELECT
  blocked.pid AS blocked_pid,
  blocker.pid AS blocker_pid,
  blocked.query AS blocked_query,
  blocker.query AS blocker_query
FROM pg_stat_activity blocked
JOIN pg_locks bl ON bl.pid = blocked.pid AND NOT bl.granted
JOIN pg_locks kl ON kl.locktype = bl.locktype
                AND kl.database IS NOT DISTINCT FROM bl.database
                AND kl.relation IS NOT DISTINCT FROM bl.relation
                AND kl.page IS NOT DISTINCT FROM bl.page
                AND kl.tuple IS NOT DISTINCT FROM bl.tuple
                AND kl.virtualxid IS NOT DISTINCT FROM bl.virtualxid
                AND kl.transactionid IS NOT DISTINCT FROM bl.transactionid
                AND kl.classid IS NOT DISTINCT FROM bl.classid
                AND kl.objid IS NOT DISTINCT FROM bl.objid
                AND kl.objsubid IS NOT DISTINCT FROM bl.objsubid
                AND kl.pid <> bl.pid
JOIN pg_stat_activity blocker ON blocker.pid = kl.pid;

SELECT pg_terminate_backend(<blocker_pid);
