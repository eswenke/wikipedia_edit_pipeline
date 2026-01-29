import redis
import os
from dotenv import load_dotenv

load_dotenv()

r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    decode_responses=True,
    username=os.getenv("REDIS_USER"),
    password=os.getenv("REDIS_PW"),
)
