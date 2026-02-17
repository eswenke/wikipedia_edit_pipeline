from dotenv import load_dotenv
import redis
import os
import json
from datetime import datetime

# NOTES:
#
# printing contains hardcoded string slicing, might need to be changed
#
# at some point when we run this pipeline for more than a day, the naming convention
# needs to change because the redis manager client will be active for more than a day,
# but the today variable is only set upon initialization.
# either:
#   1. call it in process_json so that it is called each time and is updated as such
#   2. look up a good way to orchestrate / poll to make sure we accurately change the
#      date for metrics when the next day starts, ideally in a way that doesn't require
#      resetting it every call like in option 1 (just takes less resources in general)


class RedisManager:
    def __init__(self):
        load_dotenv()
        self.host = os.getenv("REDIS_HOST", "localhost")
        self.port = int(os.getenv("REDIS_PORT", 6379))
        self.user = os.getenv("REDIS_USER", "default")
        self.password = os.getenv("REDIS_PASSWORD", None)
        self.today = datetime.now().strftime("%m-%d-%Y")  # check notes on this
        self.client = None

    def connect(self):
        try:
            self.client = redis.Redis(
                host=self.host, port=self.port, username=self.user, password=self.password, decode_responses=True
            )
            self.client.ping()
        except redis.ConnectionError as e:
            print(f"redis connection error: {e}")
            exit(1)

    def process_event(self, json_data):
        # process the json data
        # THERE ARE 4 TYPES OF EVENTS:
        #   1. edit
        #   2. categorize
        #   3. log
        #   4. new
        
        try:
            # if type of event is an edit, count metrics
            if json_data["type"] == "edit":
                self.client.incr(f"{self.today}:type:edit")
                if json_data["bot"] is True:
                    self.client.incr(f"{self.today}:edits:bot")
                elif json_data["bot"] is False:
                    self.client.incr(f"{self.today}:edits:human")

                if json_data["minor"] is True:
                    self.client.incr(f"{self.today}:edits:minor")
                elif json_data["minor"] is False:
                    self.client.incr(f"{self.today}:edits:major")

            # otherwise, count the number of that type of edit
            else:
                self.client.incr(f"{self.today}:type:{json_data['type']}")

        except Exception as e:
            print(f"error processing JSON: {e}")
            return False

        return True

    def bot_human_count(self, json_data):
        # bot edit logic function
        pass

    def minor_major_count(self, json_data):
        # general edit size function
        pass

    def print_metrics(self, option):
        if self.client:
            if option == "today":
                metric_keys = self.client.keys(f"{self.today}:*")
                metric_keys = set([metric.split(":")[1] for metric in metric_keys])
                for metric in metric_keys:
                    print(f"\n=== {metric.upper()} ===")
                    specific_keys = self.client.keys(f"{self.today}:{metric}:*")
                    for key in specific_keys:
                        print(f"{key}: {self.client.get(f'{key}')}")

            elif option == "all":
                # might not be the most efficient way to do this... consider changing instead of going from each date
                aggregates = {}
                all_keys = self.client.keys("*:*:*")
                dates = set([key.split(":")[0] for key in all_keys])

                for date in sorted(dates):
                    # sum edits metrics
                    edit_keys = self.client.keys(f"{date}:edits:*")
                    if edit_keys:
                        for key in edit_keys:
                            aggregates[f"edits:{key.split(":")[2]}"] = aggregates.get(f"edits:{key.split(":")[2]}", 0) + int(self.client.get(key))

                    # sum type metrics
                    type_keys = self.client.keys(f"{date}:type:*")
                    if type_keys:
                        for key in type_keys:
                            aggregates[f"type:{key.split(":")[2]}"] = aggregates.get(f"type:{key.split(":")[2]}", 0) + int(self.client.get(key))

                print("\n=== ALL TIME ===")
                for k, v in aggregates.items():
                    print(f"{k}: {v}")

            else:
                print(f"invalid option: {option}")
                print(f"usage: print_metrics(<'today'>|<'all'>)")
        else:
            print("error: not connected to redis db")

    def flush_db(self):
        # flush out all db metrics
        try:
            self.connect()
            self.client.flushdb()
            print("redis db flushed successfully")

            self.client.close()
        except Exception as e:
            print(f"error flushing redis db: {e}")
            self.client.close()
            exit(1)
