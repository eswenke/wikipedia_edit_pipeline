-- create database if needed
CREATE DATABASE IF NOT EXISTS wikipedia_events;

-- connect to wiki analytics regardless, no error thrown if already connected
\c wikipedia_events

-- create raw_events table
CREATE TABLE IF NOT EXISTS raw_events (
    id TEXT PRIMARY KEY,
    uri TEXT NOT NULL,
    domain TEXT NOT NULL,
    dt TIMESTAMP WITH TIME ZONE NOT NULL,
    "type" TEXT NOT NULL,
    "namespace" INTEGER NOT NULL,
    title TEXT NOT NULL,
    title_url TEXT NOT NULL,
    comment TEXT NOT NULL,
    "user" TEXT NOT NULL,
    bot BOOLEAN NOT NULL,
    wiki TEXT NOT NULL,
    minor BOOLEAN,
    patrolled TEXT,
    log_type TEXT,
    length_change INTEGER,
);

-- create indexes for performance (TBD)


-- create views for common queries (TBD)


-- Create view for hourly metrics (TBD)
