from redis.asyncio import Redis

_redis: Redis | None = None


def set_redis(redis: Redis | None) -> None:
    global _redis
    _redis = redis


def get_redis() -> Redis:
    return _redis
