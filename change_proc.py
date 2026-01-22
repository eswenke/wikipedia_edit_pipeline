import aiohttp
import asyncio
import json


async def wiki_connect():
    uri = "https://stream.wikimedia.org/v2/stream/recentchange"
