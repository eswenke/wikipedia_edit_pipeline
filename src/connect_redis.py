import redis
import os
from dotenv import load_dotenv

load_dotenv()

# create RedisManager class that can be called from change_proc
# change_proc is the primary processing file, so should we make that
# file into a class as well, powering the engine of the pipeline?
# then we add a postgresql connection class? seems to be the most
# production-like and logical. then we create a visualization class
# of sorts?

r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    decode_responses=True,
    username=os.getenv("REDIS_USER"),
    password=os.getenv("REDIS_PW"),
)
