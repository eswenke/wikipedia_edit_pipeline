from dotenv import load_dotenv
import redis
import os
import json
from datetime import datetime


class RedisManager:
    def __init__(self):
        load_dotenv()
        self.host = os.getenv("REDIS_HOST", "localhost")
        self.port = int(os.getenv("REDIS_PORT", 6379))
        self.user = os.getenv("REDIS_USER", "default")
        self.password = os.getenv("REDIS_PASSWORD", None)
        self.client = None
        self.today = datetime.now().strftime("%m-%d-%Y")


    def connect(self):
        try:
            self.client = redis.Redis(
                host=self.host, port=self.port, username=self.user, password=self.password, decode_responses=True
            )
            self.client.ping()
        except redis.ConnectionError as e:
            print(f"redis connection error: {e}")
            exit(1)

    def process_json(self, json_data):
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
                self.client.incr(f"{self.today}:type:{json_data["type"]}")

        except Exception as e:
            print(f"error processing JSON: {e}")
            exit(1)

    def bot_human_count(self, json_data):
        # bot edit logic function
        pass

    def minor_major_count(self, json_data):
        # boolean edit size function
        pass

    def print_metrics(self):
        # verify that counters are working
        # print(f"bot edits: {self.client.get(f"{self.today}:edits:bot")}")
        # print(f"human edits: {self.client.get(f"{self.today}:edits:human")}")
        # print(f"minor edits: {self.client.get(f"{self.today}:edits:minor")}")
        # print(f"major edits: {self.client.get(f"{self.today}:edits:major")}")
        
        metric_keys = self.client.keys(f"{self.today}:*")
        metric_keys = set([metric.split(":")[1] for metric in metric_keys])
        for metric in metric_keys:
            print()
            specific_keys = self.client.keys(f"{self.today}:{metric}:*")
            for key in specific_keys:
                print(f"{key}: {self.client.get(f"{key}")}")

    def flush_db(self):
        # flush out all db metrics
        try:
            self.client.flushdb()
            print("redis db flushed successfully")
        except Exception as e:
            print(f"error flushing redis db: {e}")
            exit(1)

    def close(self):
        self.client.close()
