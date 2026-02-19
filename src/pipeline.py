from redis_manager import RedisManager
from psql_manager import PSQLManager
import aiohttp
import asyncio
import json
import time
import random

"""
Main Wikimedia stream ingestion pipeline.

This module ingests recent-change events, updates Redis real-time counters,
stores raw events in PostgreSQL, and enforces a time-bounded run window.
"""


async def wiki_connect(run_seconds, retention_hours):
    """Stream events for run_seconds while keeping only retention_hours of raw rows."""

    # wikimedia SSE endpoint and required user-agent policy header
    uri = "https://stream.wikimedia.org/v2/stream/recentchange"
    headers = {
        "User-Agent": "WikipediaEditPipeline/1.0 (https://github.com/eswenke; swenke.ethan.us@gmail.com) aiohttp/3.13.3",
    }

    # connect analytics/cache service
    redis_manager = RedisManager()
    redis_manager.connect()

    # connect durable raw-event storage
    psql_manager = PSQLManager()
    psql_manager.connect()

    # one-time prune at startup to keep table bounded for local runs
    if not psql_manager.prune_old_raw_events(retention_hours):
        print("failed to prune old raw events")
        redis_manager.client.close()
        psql_manager.conn.close()
        return

    # stop processing once the requested runtime window has passed
    deadline = time.monotonic() + run_seconds
    i = 0

    while time.monotonic() < deadline:
        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(uri) as resp:
                    # raise on non-200 response codes
                    resp.raise_for_status()

                    # consume SSE lines and parse payloads from `data: ...` records
                    async for line in resp.content:
                        now = time.monotonic()
                        if now >= deadline:
                            print("run window reached. stopping stream processing.")
                            break

                        if line:
                            # decode bytes from the stream and remove trailing whitespace
                            clean_line = line.decode().strip()

                            # parse JSON payload from SSE data lines
                            if clean_line.startswith("data: "):
                                try:
                                    i += 1
                                    json_data = json.loads(clean_line[6:])

                                    if "type" not in json_data or "meta" not in json_data:
                                        continue

                                    # update Redis real-time analytics
                                    if not redis_manager.process_event(json_data):
                                        print(f"failed to process event with redis: \n{json_data}")
                                        continue

                                    # persist raw event for deeper historical analysis
                                    if not psql_manager.process_event(json_data):
                                        print(f"failed to process event with psql: \n{json_data}")
                                        continue

                                    # lightweight throughput heartbeat
                                    if i % 1000 == 0:
                                        print(f"processed events: {i}")

                                except json.JSONDecodeError:
                                    print(f"invalid JSON for line: {clean_line}")
                                    continue

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if time.monotonic() >= deadline:
                break

            reconnect_delay_seconds = random.randint(2, 10)
            # in case of rate limiting / backoff, per wiki sse stream policy
            if getattr(e, "status", None) == 429:
                retry_after = e.headers.get("Retry-After") if hasattr(e, "headers") else None
                reconnect_delay_seconds = int(retry_after) if retry_after else reconnect_delay_seconds

            print(f"connection issue: {e}. reconnecting in {reconnect_delay_seconds}s...")
            await asyncio.sleep(reconnect_delay_seconds)
            continue

        except Exception as e:
            print(f"unexpected error: {e}")
            break

    # print end-of-run metrics and close resources
    try:
        psql_manager.print_events()
        redis_manager.print_metrics("today")
        redis_manager.print_metrics("1h")
        redis_manager.print_metrics("5m")
        # redis_manager.print_metrics("all") # retention is in hours, not necessary as of now
    except Exception as e:
        print(f"error printing final metrics: {e}")

    if redis_manager.client:
        redis_manager.client.close()
    if psql_manager.conn:
        psql_manager.conn.close()


if __name__ == "__main__":
    RUN_SECONDS = 7200  # 8 hours = 28800 seconds
    RETENTION_HOURS = 8  # keep raw events for 6 hours
    asyncio.run(wiki_connect(RUN_SECONDS, RETENTION_HOURS))
