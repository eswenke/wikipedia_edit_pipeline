from redis_manager import RedisManager
import aiohttp
import asyncio
import json
import redis

# NOTES:
# - separate wiki_connect function into more managable, separated functions?


async def wiki_connect():
    # establish uri and headers in accordance to wikimedia robot policy: https://wikitech.wikimedia.org/wiki/Robot_policy
    uri = "https://stream.wikimedia.org/v2/stream/recentchange"
    headers = {
        "User-Agent": "WikipediaEditPipeline/1.0 (https://github.com/eswenke; swenke.ethan.us@gmail.com) aiohttp/3.13.3",
        "Accept-Encoding": "gzip",
    }

    redis_manager = RedisManager()
    redis_manager.connect()
    # redis_manager.flush_db() BE REALLY CAREFUL AND COGNISCENT THAT THIS IS HERE

    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(uri) as resp:
                # raise exception for non-200 codes
                resp.raise_for_status()

                # process lines, stripping event metadata and converting useful data to json
                i = 0
                async for line in resp.content:
                    if line:
                        # decode from bytes and strip whitespace
                        clean_line = line.decode().strip()

                        # find useful data about the change and convert to json
                        if clean_line.startswith("data: "):
                            try:
                                i += 1
                                json_data = json.loads(clean_line[6:])

                                # for k, v in json_data.items():
                                #     print(f"{k}: {v}")

                                # process the json data
                                redis_manager.process_json(json_data)

                                # process 100 events
                                if i >= 100:
                                    redis_manager.print_metrics()
                                    exit(0)

                            except json.JSONDecodeError:
                                print(f"invalid JSON for line: {clean_line}")
                                continue

    except aiohttp.ClientError as e:
        print(f"connection failed: {e}")
    except Exception as e:
        print(f"unexpected error: {e}")

    finally:
        redis_manager.close()


if __name__ == "__main__":
    asyncio.run(wiki_connect())
