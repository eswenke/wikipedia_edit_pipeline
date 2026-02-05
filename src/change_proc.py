import aiohttp
import asyncio
import json
import redis
from redis_manager import RedisManager

# NOTES:
# - make this object oriented -> classes for recent changes / streams?


async def wiki_connect():
    # establish uri and headers in accordance to wikimedia robot policy: https://wikitech.wikimedia.org/wiki/Robot_policy
    uri = "https://stream.wikimedia.org/v2/stream/recentchange"
    headers = {
        "User-Agent": "WikipediaEditPipeline/1.0 (https://github.com/eswenke; swenke.ethan.us@gmail.com) aiohttp/3.13.3",
        "Accept-Encoding": "gzip",
    }

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

                                # create redis manager object and intialize connection
                                redis_manager = RedisManager()
                                redis_manager.connect()

                                # process the json data
                                redis_manager.process_json(json_data)

                                # process 100 events
                                if i >= 100:
                                    # verify that counters are working
                                    print(f"bot edits: {redis_manager.client.get("edits:bot")}")
                                    print(f"human edits: {redis_manager.client.get("edits:human")}")
                                    print(f"minor edits: {redis_manager.client.get("edits:minor")}")
                                    print(f"major edits: {redis_manager.client.get("edits:major")}")
                                    
                                    type_keys = redis_manager.client.keys("type:*")
                                    print()
                                    for key in type_keys:
                                        print(f"{key}: {redis_manager.client.get(f"{key}")}")
                                    exit(0)

                            except json.JSONDecodeError:
                                print(f"invalid JSON for line: {clean_line}")
                                continue

    except aiohttp.ClientError as e:
        print(f"connection failed: {e}")
    except Exception as e:
        print(f"unexpected error: {e}")


if __name__ == "__main__":
    asyncio.run(wiki_connect())
