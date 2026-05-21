import json
from datetime import datetime, timezone

from redis.asyncio import Redis

from app.models import PackageState

_PACKAGE_PREFIX = "package:"
_ROBOTS_KEY = "robots"
_CAMERAS_KEY = "cameras"
_EVENTS_CHANNEL = "package_events"


async def get_package(redis: Redis, piece_id: str) -> PackageState | None:
    data = await redis.hgetall(f"{_PACKAGE_PREFIX}{piece_id}")
    if not data:
        return None
    return _deserialize(data)


async def set_package(redis: Redis, state: PackageState) -> None:
    await redis.hset(f"{_PACKAGE_PREFIX}{state.piece_id}", mapping=_serialize(state))


async def get_all_packages(redis: Redis) -> list[PackageState]:
    keys = await redis.keys(f"{_PACKAGE_PREFIX}*")
    packages = []
    for key in keys:
        data = await redis.hgetall(key)
        if data:
            packages.append(_deserialize(data))
    return packages


async def publish_event(redis: Redis, state: PackageState) -> None:
    await redis.publish(_EVENTS_CHANNEL, state.model_dump_json())


async def update_robot_seen(redis: Redis, robot_id: str) -> None:
    await redis.hset(_ROBOTS_KEY, robot_id, datetime.now(timezone.utc).isoformat())


async def update_camera_seen(redis: Redis, camera_id: str) -> None:
    await redis.hset(_CAMERAS_KEY, camera_id, datetime.now(timezone.utc).isoformat())


async def get_robots(redis: Redis) -> dict[str, str]:
    return await redis.hgetall(_ROBOTS_KEY)


async def get_cameras(redis: Redis) -> dict[str, str]:
    return await redis.hgetall(_CAMERAS_KEY)


def _serialize(state: PackageState) -> dict:
    data = state.model_dump()
    data["history"] = json.dumps(data["history"])
    data["enrichment_metadata"] = json.dumps(data["enrichment_metadata"])
    data["created_at"] = data["created_at"].isoformat()
    data["updated_at"] = data["updated_at"].isoformat()
    return {k: ("" if v is None else str(v)) for k, v in data.items()}


def _deserialize(data: dict) -> PackageState:
    d = {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in data.items()
    }
    d["history"] = json.loads(d["history"])
    d["enrichment_metadata"] = json.loads(d["enrichment_metadata"])
    d["retry_count"] = int(d["retry_count"])
    d["route"] = d["route"] or None
    d["operator_id"] = d["operator_id"] or None
    return PackageState(**d)
