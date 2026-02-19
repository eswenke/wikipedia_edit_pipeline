SELECT * FROM raw_events LIMIT 100;

-- events per minute (based on interval in time series)
WITH minutes AS (
	SELECT generate_series(
		date_trunc('minute', now() - interval '2 hours'),
		date_trunc('minute', now()),
		interval '1 minute'
	) AS minutes_ts
)
SELECT
	minutes_ts,
	COALESCE(count(r.id), 0) AS events
FROM minutes m
LEFT JOIN raw_events r
	ON date_trunc('minute', r.dt) = m.minutes_ts
GROUP BY m.minutes_ts
ORDER BY m.minutes_ts;

-- edit type count
SELECT
	date_trunc('minute', dt) AS dt_minute,
	COUNT(*) AS category_count
FROM raw_events
WHERE "type" = 'categorize'
GROUP BY dt_minute;

-- edit type mix per minute in last hour (insert unfilled rows)
WITH minutes AS (
	SELECT generate_series(
		date_trunc('minute', now() - interval '1 hour'),
		date_trunc('minute', now()),
		interval '1 minute'
	) AS minutes_ts
), per_minute AS (
    SELECT
        date_trunc('minute', dt) AS minute_ts,
        COUNT(*) FILTER (WHERE "type" = 'edit') AS edit_count,
        COUNT(*) FILTER (WHERE "type" = 'new') AS new_count,
        COUNT(*) FILTER (WHERE "type" = 'log') AS log_count,
        COUNT(*) FILTER (WHERE "type" = 'categorize') AS categorize_count
    FROM raw_events
    WHERE dt >= now() - interval '1 hour'
    GROUP BY minute_ts
)
SELECT
	m.minutes_ts,
	COALESCE(p.categorize_count, 0) AS category_count,
	COALESCE(p.edit_count, 0) AS edit_count,
	COALESCE(p.log_count, 0) AS log_count,
	COALESCE(p.new_count, 0) AS new_count
FROM minutes m
LEFT JOIN per_minute p ON p.minute_ts = m.minutes_ts
ORDER BY m.minutes_ts;

-- edit type mix per minute (without unfilled rows)
SELECT
	date_trunc('minute', dt) AS minutes_ts,
	COUNT(*) FILTER (WHERE "type" = 'edit') AS edit_count,
	COUNT(*) FILTER (WHERE "type" = 'new') AS new_count,
	COUNT(*) FILTER (WHERE "type" = 'log') AS log_count,
	COUNT(*) FILTER (WHERE "type" = 'categorize') AS categorize_count
FROM raw_events
GROUP BY minutes_ts
ORDER BY minutes_ts;

-- distinct users per minute
SELECT
	date_trunc('minute', dt) AS minutes,
	COUNT(DISTINCT("user")) AS user_count
FROM raw_events
GROUP BY minutes
ORDER BY minutes DESC;

-- bot vs. human events per minute
SELECT
	date_trunc('minute', dt) AS minutes,
	COUNT(*) FILTER (WHERE bot = TRUE) AS bots,
	COUNT(*) FILTER (WHERE bot = FALSE) AS humans
FROM raw_events
GROUP BY minutes
ORDER BY minutes DESC;

-- wiki event type aggregates
SELECT
	wiki,
	COUNT(*) AS total_count,
	COUNT(*) FILTER (WHERE "type" = 'edit') AS edit_count,
	COUNT(*) FILTER (WHERE "type" = 'new') AS new_count,
	COUNT(*) FILTER (WHERE "type" = 'log') AS log_count,
	COUNT(*) FILTER (WHERE "type" = 'categorize') AS categorize_count
FROM raw_events
GROUP BY wiki
ORDER BY total_count DESC;

-- user event type aggregates
SELECT
	"user",
	COUNT(*) AS total_count,
	COUNT(*) FILTER (WHERE "type" = 'edit') AS edit_count,
	COUNT(*) FILTER (WHERE "type" = 'new') AS new_count,
	COUNT(*) FILTER (WHERE "type" = 'log') AS log_count,
	COUNT(*) FILTER (WHERE "type" = 'categorize') AS categorize_count
FROM raw_events
GROUP BY "user"
ORDER BY total_count DESC;

-- event type percentiles
SELECT
	"type",
	ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw_events
WHERE "type" IN ('edit', 'categorize', 'log', 'new')
GROUP BY "type"
ORDER BY pct DESC;

-- minor vs major edit percentiles
SELECT
	CASE
		WHEN minor = 'true' THEN 'minor'
		WHEN minor = 'false' THEN 'major'
	END AS change_size,
	ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw_events
WHERE "minor" IS NOT NULL
GROUP BY "minor"
ORDER BY pct DESC;

-- change size avg (all time, bot, human)
SELECT
	ROUND(AVG("length"), 2) AS all_avg_length,
	ROUND(AVG("length") FILTER (WHERE bot = TRUE), 2) AS bot_avg_length,
	ROUND(AVG("length") FILTER (WHERE bot = FALSE), 2) AS human_avg_length
FROM raw_events
WHERE "length" IS NOT NULL;

-- change size avg per minute
SELECT
	date_trunc('minute', dt) AS minutes,
	ROUND(AVG("length"), 2) AS avg_length
FROM raw_events
WHERE "length" IS NOT NULL
GROUP BY minutes
ORDER BY minutes;

-- users with the largest average change size (min 10 events)
SELECT
	"user",
	COUNT(*) AS user_events,
	ROUND(AVG("length"), 2) AS avg_length
FROM raw_events
WHERE "length" IS NOT NULL
GROUP BY "user"
HAVING COUNT(*) > 10
ORDER BY user_events DESC, avg_length DESC;

-- namespace counts
SELECT
	"namespace",
	COUNT(*) AS namespace_count
FROM raw_events
WHERE "namespace" IN (SELECT DISTINCT("namespace") FROM raw_events)
GROUP BY "namespace"
ORDER BY namespace_count DESC;

-- patrolled vs not patrolled events
SELECT
	DISTINCT("type") AS event_type,
	COUNT(*) AS event_count,
	COUNT(*) FILTER (WHERE patrolled = 'true') as patrolled_count,
	COUNT(*) FILTER (WHERE patrolled = 'false') as unpatrolled_count,
FROM raw_events
WHERE patrolled IS NOT NULL AND bot IS NOT NULL
GROUP BY event_type
ORDER BY event_count DESC;

-- patrolled vs not patrolled events by bot vs human
SELECT
	CASE
		WHEN bot = TRUE THEN 'bot'
		WHEN bot = FALSE THEN 'human'
	END AS user_type,
	COUNT(*) AS event_count,
	COUNT(*) FILTER (WHERE patrolled = 'true') as patrolled_count,
	COUNT(*) FILTER (WHERE patrolled = 'false') as unpatrolled_count
FROM raw_events
WHERE patrolled IS NOT NULL AND bot IS NOT NULL
GROUP BY bot
ORDER BY event_count DESC;

-- log type counts
SELECT
	log_type,
	COUNT(log_type) as log_type_count
FROM raw_events
WHERE log_type IS NOT NULL
GROUP BY log_type
ORDER BY log_type_count DESC;

-- log type inspection per user
SELECT
	*
FROM raw_events
WHERE log_type = 'abusefilter'
ORDER BY dt DESC;






