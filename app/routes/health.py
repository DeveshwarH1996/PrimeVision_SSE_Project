from fastapi import APIRouter, Depends
from redis.asyncio import Redis

from app.dependencies import get_redis
from app.models import HealthResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health(redis: Redis = Depends(get_redis)):
    try:
        await redis.ping()
        redis_status = "ok"
    except Exception:
        redis_status = "unavailable"
    return HealthResponse(status="ok", redis=redis_status)
