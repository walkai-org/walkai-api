import json
import os

import redis

r = redis.from_url(os.getenv("REDIS_URL"))
TTL = 600


def save_oauth_tx(state: str, data: dict):
    r.setex(f"oauth:{state}", TTL, json.dumps(data))


def load_oauth_tx(state: str) -> dict | None:
    raw = r.get(f"oauth:{state}")
    if not raw:
        return None
    r.delete(f"oauth:{state}")
    return json.loads(raw)
