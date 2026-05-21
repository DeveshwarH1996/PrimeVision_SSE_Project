import asyncio
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from redis.asyncio import Redis

from app.dependencies import get_redis
from app.models import PackageScanRequest, PackageState
from app.pipeline import run_pipeline
from app import state as state_store

router = APIRouter()


@router.post("/events/package-scan")
async def package_scan(
    payload: PackageScanRequest,
    redis: Redis = Depends(get_redis),
):
    existing = await state_store.get_package(redis, payload.piece_id)
    if existing:
        return existing

    now = datetime.now(timezone.utc)
    pkg = PackageState(
        piece_id=payload.piece_id,
        status="RECEIVED",
        barcode=payload.barcode,
        robot_id=payload.robot_id,
        camera_id=payload.camera_id,
        history=["RECEIVED"],
        created_at=now,
        updated_at=now,
    )
    await state_store.set_package(redis, pkg)
    await state_store.publish_event(redis, pkg)
    await state_store.update_robot_seen(redis, payload.robot_id)
    await state_store.update_camera_seen(redis, payload.camera_id)
    asyncio.create_task(run_pipeline(redis, payload.piece_id))
    return pkg
