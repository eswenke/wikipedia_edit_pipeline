import aiohttp
import asyncio
import json


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
                # show status of request
                print(resp.status)

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
                                print(json_data["id"])
                                print(json_data["type"])
                                print(json_data["title"])
                                print()

                                if i > 10:
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
