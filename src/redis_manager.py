import redis
import os
import json
from dotenv import load_dotenv

# if i need an explicit class for my redis logic that makes things cleaner


class RedisManager:
    def __init__(self):
        load_dotenv()
        self.host = os.getenv("REDIS_HOST", "localhost")
        self.port = int(os.getenv("REDIS_PORT", 6379))
        self.user = os.getenv("REDIS_USER", "default")
        self.password = os.getenv("REDIS_PASSWORD", None)
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

    def process_json(self, json_data):
        try:
            # if type of event is an edit, count metrics
            if json_data["type"] == "edit":
                self.client.incr("type:edit")
                if json_data["bot"] is True:
                    self.client.incr("edits:bot")
                elif json_data["bot"] is False:
                    self.client.incr("edits:human")

                if json_data["minor"] is True:
                    self.client.incr("edits:minor")
                elif json_data["minor"] is False:
                    self.client.incr("edits:major")

            # otherwise, count the number of that type of edit
            else:
                self.client.incr(f"type:{json_data["type"]}")

        except Exception as e:
            print(f"error processing JSON: {e}")

    def bot_human_count(self, json_data):
        # bot edit logic function
        pass

    def minor_major_count(self, json_data):
        # boolean edit size function
        pass

    def close(self):
        self.client.close()
