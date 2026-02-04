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
                host=self.host, port=self.port, user=self.user, password=self.password, decode_responses=True
            )
            self.client.ping()
        except redis.ConnectionError as e:
            print(f"redis connection failed: {e}")
        except Exception as e:
            print(f"unexpected error connecting to redis: {e}")

        return self.client

    def process_json(json_data):
        try:
            if json_data["bot"] == "True":
                self.client.incr("edits:bot")
            elif json_data["bot"] == "False":
                self.client.incr("edits:human")

            if json_data["minor"] == "True":
                self.client.incr("edits:minor")
            elif json_data["minor"] == "False":
                self.client.incr("edits:major")

        except Exception as e:
            print(f"error processing JSON: {e}")
            raise

    def bot_human_count(json_data):
        # bot edit logic function
        pass

    def minor_major_count(json_data):
        # boolean edit size function
        pass

    def close(self):
        self.client.close()
