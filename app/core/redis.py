import redis
from fastapi import Request
from redis import Redis

from app.core.config import get_settings


def _build_client() -> Redis:
    settings = get_settings()
    return redis.from_url(settings.redis_url)


def create_redis_client() -> Redis:
    return _build_client()


def get_redis(request: Request) -> Redis:
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        raise RuntimeError("Redis client is not configured on application state")
    return redis_client
