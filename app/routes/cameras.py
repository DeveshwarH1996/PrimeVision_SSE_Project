from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from redis.asyncio import Redis

from app.config import settings
from app.dependencies import get_redis
from app.models import CameraInfo
from app import state as state_store

router = APIRouter()


@router.get("/cameras", response_model=list[CameraInfo])
async def get_cameras(redis: Redis = Depends(get_redis)) -> list[CameraInfo]:
    data = await state_store.get_cameras(redis)
    now = datetime.now(timezone.utc)
    result = []
    for camera_id, last_seen_str in data.items():
        last_seen = datetime.fromisoformat(last_seen_str)
        if last_seen.tzinfo is None:
            last_seen = last_seen.replace(tzinfo=timezone.utc)
        stale = (now - last_seen).total_seconds() > settings.staleness_threshold_seconds
        result.append(CameraInfo(camera_id=camera_id, last_seen=last_seen_str, stale=stale))
    return result
