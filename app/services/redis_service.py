import json

from redis import Redis

TTL = 600


def save_oauth_tx(redis_client: Redis, state: str, data: dict):
    redis_client.setex(f"oauth:{state}", TTL, json.dumps(data))


def load_oauth_tx(redis_client: Redis, state: str) -> dict | None:
    raw = redis_client.get(f"oauth:{state}")
    if not raw:
        return None
    redis_client.delete(f"oauth:{state}")
    return json.loads(raw)
