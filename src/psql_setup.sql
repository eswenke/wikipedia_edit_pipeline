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
    patrolled TEXT,
    log_type TEXT,
    length INTEGER
);

-- indexes for common time-bounded and grouped analytics queries
CREATE INDEX IF NOT EXISTS idx_raw_events_dt ON raw_events (dt);
CREATE INDEX IF NOT EXISTS idx_raw_events_user_dt ON raw_events ("user", dt);
CREATE INDEX IF NOT EXISTS idx_raw_events_type_dt ON raw_events (type, dt);
CREATE INDEX IF NOT EXISTS idx_raw_events_wiki_dt ON raw_events (wiki, dt);

-- views can be added here as analytics requirements solidify
