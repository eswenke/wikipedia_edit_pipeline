from dotenv import load_dotenv
import redis
import os
from datetime import datetime


# metrics:
#   - bot vs human
#   - patrolled bot vs unpatrolled bot
#   - minor vs major
#   - event type occurances
#   - log type occurances
#   - namespace occurances
#   - top 10 editors or so


class RedisManager:
    """Manages Redis counters used for real-time pipeline analytics."""

    def __init__(self):
        """Load connection settings and initialize Redis client state."""
        load_dotenv()
        self.host = os.getenv("REDIS_HOST", "localhost")
        self.port = int(os.getenv("REDIS_PORT", 6379))
        self.user = os.getenv("REDIS_USER", "default")
        self.password = os.getenv("REDIS_PASSWORD", None)
        self.minute_ttl_seconds = 7200
        self.top_users_minute_ttl_seconds = 7200
        self.client = None

    def _get_today(self):
        """Return current date string used for day-scoped keys."""
        return datetime.now().strftime("%m-%d-%Y")

    def _get_minute_bucket(self):
        """Return current unix-minute bucket for rolling-window metrics."""
        return int(datetime.now().timestamp() // 60)

    def _increment_metric(self, metric_group, metric_name):
        """Increment day, all-time, and minute-bucket counters atomically."""
        if metric_name is None:
            return

        today = self._get_today()
        minute_bucket = self._get_minute_bucket()
        minute_key = f"minute:{minute_bucket}:{metric_group}:{metric_name}"

        pipe = self.client.pipeline()
        # day-level metrics
        pipe.incr(f"{today}:{metric_group}:{metric_name}")
        # all-time metrics
        pipe.incr(f"all:{metric_group}:{metric_name}")
        # rolling-window metrics
        pipe.incr(minute_key)
        pipe.expire(minute_key, self.minute_ttl_seconds)
        pipe.execute()

    def _increment_top_user(self, username):
        """Increment top-user sorted sets for minute/day/all scopes."""
        if not username:
            return

        today = self._get_today()
        minute_bucket = self._get_minute_bucket()
        minute_key = f"top_users:minute:{minute_bucket}"

        pipe = self.client.pipeline()
        pipe.zincrby(f"{today}:top_users", 1, username)
        pipe.zincrby("all:top_users", 1, username)
        pipe.zincrby(minute_key, 1, username)
        pipe.expire(minute_key, self.top_users_minute_ttl_seconds)
        pipe.execute()

    def connect(self):
        """Create and validate Redis connection."""
        try:
            self.client = redis.Redis(
                host=self.host, port=self.port, username=self.user, password=self.password, decode_responses=True
            )
            self.client.ping()
        except redis.ConnectionError as e:
            print(f"redis connection error: {e}")
            exit(1)

    def process_event(self, json_data):
        """Update Redis analytics counters for one Wikimedia event."""

        try:
            event_type = json_data.get("type")
            if event_type is None:
                return False

            self._increment_metric("events", "total")
            self._increment_metric("type", event_type)

            namespace = json_data.get("namespace")
            if namespace is not None:
                self._increment_metric("namespace", str(namespace))

            if event_type == "log" and json_data.get("log_type"):
                self._increment_metric("log_type", json_data.get("log_type"))

            self._increment_top_user(json_data.get("user"))

            # edit events include additional bot/human and minor/major slices
            if event_type == "edit":
                if json_data.get("bot") is True:
                    self._increment_metric("edits", "bot")
                elif json_data.get("bot") is False:
                    self._increment_metric("edits", "human")

                if json_data.get("minor") is True:
                    self._increment_metric("edits", "minor")
                elif json_data.get("minor") is False:
                    self._increment_metric("edits", "major")

            if json_data.get("bot") is True:
                if json_data.get("patrolled") is True:
                    self._increment_metric("patrolled", "patrolled_bot")
                elif json_data.get("patrolled") is False:
                    self._increment_metric("patrolled", "unpatrolled_bot")

        except Exception as e:
            print(f"error processing JSON: {e}")
            return False

        return True

    def print_metrics(self, option):
        """Print metrics for today, rolling windows (5m/1h), or all-time."""

        def print_aggregates(aggregates, title):
            print(f"\n=== {title} ===")
            for key in sorted(aggregates.keys()):
                print(f"{key}: {aggregates[key]}")

        def aggregate_window(window_minutes):
            # sum minute-bucket keys over the requested rolling window
            aggregates = {}
            current_minute = self._get_minute_bucket()

            for minute in range(current_minute - window_minutes + 1, current_minute + 1):
                minute_keys = self.client.keys(f"minute:{minute}:*:*")
                for key in minute_keys:
                    _, _, metric_group, metric_name = key.split(":", 3)
                    agg_key = f"{metric_group}:{metric_name}"
                    aggregates[agg_key] = aggregates.get(agg_key, 0) + int(self.client.get(key) or 0)

            return aggregates

        def aggregate_top_users_window(window_minutes):
            top_users = {}
            current_minute = self._get_minute_bucket()

            for minute in range(current_minute - window_minutes + 1, current_minute + 1):
                key = f"top_users:minute:{minute}"
                for user, score in self.client.zrevrange(key, 0, -1, withscores=True):
                    top_users[user] = top_users.get(user, 0) + int(score)

            return sorted(top_users.items(), key=lambda x: x[1], reverse=True)[:10]

        def print_top_users(entries, title):
            print(f"\n=== {title} ===")
            if not entries:
                print("no data")
                return
            for user, score in entries:
                print(f"{user}: {int(score)}")

        if self.client:
            if option == "today":
                aggregates = {}
                today = self._get_today()
                today_keys = self.client.keys(f"{today}:*:*")
                for key in today_keys:
                    _, metric_group, metric_name = key.split(":", 2)
                    aggregates[f"{metric_group}:{metric_name}"] = int(self.client.get(key) or 0)

                print_aggregates(aggregates, "TODAY")

            elif option == "5m":
                aggregates = aggregate_window(5)
                print_aggregates(aggregates, "LAST 5 MINUTES")
                top_users = aggregate_top_users_window(5)
                print_top_users(top_users, "TOP USERS (LAST 5 MINUTES)")
                one_hour_total = aggregate_window(60).get("events:total", 0)
                five_min_total = aggregates.get("events:total", 0)
                if one_hour_total > 0:
                    baseline_five_min = one_hour_total / 12
                    spike_score = five_min_total / baseline_five_min if baseline_five_min > 0 else 0
                    print(f"spike_score_5m_vs_1h_baseline: {spike_score:.2f}x")

            elif option == "1h":
                aggregates = aggregate_window(60)
                print_aggregates(aggregates, "LAST 1 HOUR")
                top_users = aggregate_top_users_window(60)
                print_top_users(top_users, "TOP USERS (LAST 1 HOUR)")

            elif option == "all":
                aggregates = {}
                all_keys = self.client.keys("all:*:*")
                for key in all_keys:
                    _, metric_group, metric_name = key.split(":", 2)
                    aggregates[f"{metric_group}:{metric_name}"] = int(self.client.get(key) or 0)

                print_aggregates(aggregates, "ALL TIME")

                top_users = self.client.zrevrange("all:top_users", 0, 9, withscores=True)
                print_top_users(top_users, "TOP USERS (ALL TIME)")

            else:
                print(f"invalid option: {option}")
                print("usage: print_metrics(<'today'>|<'5m'>|<'1h'>|<'all'>)")
        else:
            print("error: not connected to redis db")

    def flush_db(self):
        """Delete all Redis keys in the current Redis database."""
        try:
            self.connect()
            self.client.flushdb()
            print("redis db flushed successfully")

            self.client.close()
        except Exception as e:
            print(f"error flushing redis db: {e}")
            self.client.close()
            exit(1)
