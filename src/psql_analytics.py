from psql_manager import PSQLManager

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

    def _top_users_per_minute(self):
        # also include user behavior? event count, types, event sizes, bot/human, patrolled, etc.

        # if self.psql.conn
        #   create cursor
        #   execute query
        #   fetch results
        #   visualize (or wait for this step until all queries are programmed)
        #   close cursor
        # catch errors
        # print some results to verify output
        pass

    def _top_wikis(self):
        # per minute, hour, or all time? 
        # do this with redis?
        pass

    def _gap_filled_time_series(self):
        # analysis on missed minutes over a time period
        # analysis on uptime of the pipeline
        # for visualization purposes bc we need each minute?
        pass

    def _event_size_distribution(self):
        pass

    def _event_type_distribution(self):
        pass

    def _wiki_event_type_distribution(self):
        pass

    def _patrolled_bot_distribution(self):
        pass

    def _

    def print_sql_analytics(self):
        # master print function to test in pipeline.py with
        pass