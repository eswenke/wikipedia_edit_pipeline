import pandas as pd

# metrics:
#   - event type counts per minute
#   - event type counts per wiki
#   - event type counts per user
#   - distinct user counts per minute
#   - bot vs human event counts per minute
#   - event type percentiles - can be done via redis as well
#   - event size (minor vs major) percentiles - can be done via redis as well
#   - event size per minute
#   - event size avg (all time, bot, human)
#   - users with the largest avg event size and/or most events
#   - patrolled vs unpatrolled events by event type
#   - patrolled vs unpatrolled events by bots/humans

# can do many version of all these queries regarding time bounds / bot vs. human / etc.


class PSQLAnalytics:
    def __init__(self, psql_manager):
        self.psql = psql_manager

    def _run_query(self, query, params=None):
        if not self.psql.conn:
            raise RuntimeError("psql connection is not initialized")

        cur = self.psql.conn.cursor()
        cur.execute(query, params or ())
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()
        return pd.DataFrame(rows, columns=columns)

    def _top_users_per_minute(self):
        query = """
            SELECT
                date_trunc('minute', dt) AS minute_ts,
                "user",
                COUNT(*) AS event_count
            FROM raw_events
            WHERE "user" IS NOT NULL
              AND dt::date = CURRENT_DATE
            GROUP BY minute_ts, "user"
            ORDER BY minute_ts DESC, event_count DESC;
        """
        return self._run_query(query)

    def top_users_per_minute_today(self):
        return self._top_users_per_minute()

    def top_users_today(self, limit=10, user_type="all"):
        user_type = (user_type or "all").lower()
        if user_type not in {"all", "bot", "human"}:
            user_type = "all"

        query = """
            SELECT
                "user",
                COUNT(*) AS event_count
            FROM raw_events
            WHERE "user" IS NOT NULL
              AND dt::date = CURRENT_DATE
              AND (
                    %s = 'all'
                    OR (%s = 'bot' AND bot IS TRUE)
                    OR (%s = 'human' AND bot IS FALSE)
              )
            GROUP BY "user"
            ORDER BY event_count DESC
            LIMIT %s;
        """
        return self._run_query(query, (user_type, user_type, user_type, limit))

    def _top_wikis(self):
        query = """
            SELECT
                wiki,
                COUNT(*) AS event_count
            FROM raw_events
            WHERE wiki IS NOT NULL
              AND dt::date = CURRENT_DATE
            GROUP BY wiki
            ORDER BY event_count DESC
            LIMIT 10;
        """
        return self._run_query(query)

    def top_wikis_today(self, limit=10):
        query = """
            SELECT
                wiki,
                COUNT(*) AS event_count
            FROM raw_events
            WHERE wiki IS NOT NULL
              AND dt::date = CURRENT_DATE
            GROUP BY wiki
            ORDER BY event_count DESC
            LIMIT %s;
        """
        return self._run_query(query, (limit,))

    def _gap_filled_time_series(self):
        # analysis on missed minutes over a time period
        # analysis on uptime of the pipeline
        # for visualization purposes bc we need each minute?
        query = """
            WITH minutes AS (
                SELECT generate_series(
                    date_trunc('minute', now() - (%s * interval '1 hour')),
                    date_trunc('minute', now()),
                    interval '1 minute'
                ) AS minutes_ts
            )
            SELECT
                m.minutes_ts,
                COALESCE(count(r.id), 0) AS events
            FROM minutes m
            LEFT JOIN raw_events r
                ON date_trunc('minute', r.dt) = m.minutes_ts
            GROUP BY m.minutes_ts
            ORDER BY m.minutes_ts;
        """
        return self._run_query(query, (1,))

    def gap_filled_time_series(self, window_hours=1):
        query = """
            WITH minutes AS (
                SELECT generate_series(
                    date_trunc('minute', now() - (%s * interval '1 hour')),
                    date_trunc('minute', now()),
                    interval '1 minute'
                ) AS minutes_ts
            )
            SELECT
                m.minutes_ts,
                COALESCE(count(r.id), 0) AS events
            FROM minutes m
            LEFT JOIN raw_events r
                ON date_trunc('minute', r.dt) = m.minutes_ts
            GROUP BY m.minutes_ts
            ORDER BY m.minutes_ts;
        """
        return self._run_query(query, (window_hours,))

    def _event_size_distribution(self):
        query = """
            SELECT
                ROUND(AVG("length"), 2) AS all_avg_length,
                ROUND(AVG("length") FILTER (WHERE bot = TRUE), 2) AS bot_avg_length,
                ROUND(AVG("length") FILTER (WHERE bot = FALSE), 2) AS human_avg_length
            FROM raw_events
            WHERE "length" IS NOT NULL;
        """
        return self._run_query(query)

    def event_size_distribution(self):
        return self._event_size_distribution()

    def _event_type_distribution(self):
        query = """
            SELECT
                "type",
                COUNT(*) AS event_count,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
            FROM raw_events
            WHERE "type" IN ('edit', 'categorize', 'log', 'new')
              AND dt::date = CURRENT_DATE
            GROUP BY "type"
            ORDER BY event_count DESC;
        """
        return self._run_query(query)

    def patrolled_bot_distribution_today(self):
        return self._patrolled_bot_distribution()

    def event_type_distribution_today(self):
        return self._event_type_distribution()

    def _wiki_event_type_distribution(self):
        query = """
            SELECT
                wiki,
                COUNT(*) AS total_count,
                COUNT(*) FILTER (WHERE "type" = 'edit') AS edit_count,
                COUNT(*) FILTER (WHERE "type" = 'new') AS new_count,
                COUNT(*) FILTER (WHERE "type" = 'log') AS log_count,
                COUNT(*) FILTER (WHERE "type" = 'categorize') AS categorize_count
            FROM raw_events
            WHERE wiki IS NOT NULL
              AND dt::date = CURRENT_DATE
            GROUP BY wiki
            ORDER BY total_count DESC;
        """
        return self._run_query(query)

    def wiki_event_type_distribution_today(self):
        return self._wiki_event_type_distribution()

    def _patrolled_bot_distribution(self):
        query = """
            SELECT
                CASE
                    WHEN bot = TRUE THEN 'bot'
                    WHEN bot = FALSE THEN 'human'
                END AS user_type,
                COUNT(*) AS event_count,
                COUNT(*) FILTER (WHERE patrolled = 'true') AS patrolled_count,
                COUNT(*) FILTER (WHERE patrolled = 'false') AS unpatrolled_count
            FROM raw_events
            WHERE patrolled IS NOT NULL
              AND bot IS NOT NULL
              AND dt::date = CURRENT_DATE
            GROUP BY bot
            ORDER BY event_count DESC;
        """
        return self._run_query(query)

    def print_sql_analytics(self):
        # master print function to test in pipeline.py with
        metric_frames = {
            "top_users_per_minute": self._top_users_per_minute(),
            "top_users_today": self.top_users_today(),
            "top_wikis_today": self.top_wikis_today(),
            "gap_filled_time_series": self.gap_filled_time_series(),
            "event_size_distribution": self._event_size_distribution(),
            "event_type_distribution": self._event_type_distribution(),
            "wiki_event_type_distribution": self._wiki_event_type_distribution(),
            "patrolled_bot_distribution": self._patrolled_bot_distribution(),
        }

        for name, df in metric_frames.items():
            print(f"\n=== {name} ===")
            if df.empty:
                print("no rows")
            else:
                print(df.head(20).to_string(index=False))
