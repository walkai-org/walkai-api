import json

import redis

from app.core.config import get_settings

settings = get_settings()
r = redis.from_url(settings.redis_url)
TTL = 600


def save_oauth_tx(state: str, data: dict):
    r.setex(f"oauth:{state}", TTL, json.dumps(data))


def load_oauth_tx(state: str) -> dict | None:
    raw = r.get(f"oauth:{state}")
    if not raw:
        return None
    r.delete(f"oauth:{state}")
    return json.loads(raw)
