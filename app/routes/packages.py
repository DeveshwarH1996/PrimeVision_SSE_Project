from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from redis.asyncio import Redis

from app.config import settings
from app.dependencies import get_redis
from app.models import PackageState, ReviewRequest, StatsResponse
from app.pipeline import resume_pipeline
from app import state as state_store

router = APIRouter(prefix="/packages")


@router.get("/manual-review")
async def get_manual_review(redis: Redis = Depends(get_redis)) -> list[PackageState]:
    packages = await state_store.get_all_packages(redis)
    return [p for p in packages if p.status == "MANUAL_REVIEW"]


@router.get("/stuck")
async def get_stuck(redis: Redis = Depends(get_redis)) -> list[PackageState]:
    packages = await state_store.get_all_packages(redis)
    now = datetime.now(timezone.utc)
    return [
        p for p in packages
        if p.status == "MANUAL_REVIEW"
        and (now - p.updated_at).total_seconds() > settings.staleness_threshold_seconds
    ]


@router.get("/failed")
async def get_failed(redis: Redis = Depends(get_redis)) -> list[PackageState]:
    packages = await state_store.get_all_packages(redis)
    return [p for p in packages if p.status == "FAILED"]


@router.get("/stats", response_model=StatsResponse)
async def get_stats(redis: Redis = Depends(get_redis)) -> StatsResponse:
    packages = await state_store.get_all_packages(redis)
    by_status: dict[str, int] = {}
    for p in packages:
        by_status[p.status] = by_status.get(p.status, 0) + 1
    total = len(packages)
    error_rate = by_status.get("FAILED", 0) / total if total else 0.0
    return StatsResponse(total=total, by_status=by_status, error_rate=error_rate)


@router.get("")
async def get_packages(
    status: str | None = None,
    robot_id: str | None = None,
    camera_id: str | None = None,
    redis: Redis = Depends(get_redis),
) -> list[PackageState]:
    packages = await state_store.get_all_packages(redis)
    if status:
        packages = [p for p in packages if p.status == status]
    if robot_id:
        packages = [p for p in packages if p.robot_id == robot_id]
    if camera_id:
        packages = [p for p in packages if p.camera_id == camera_id]
    return packages


@router.get("/{piece_id}", response_model=PackageState)
async def get_package(piece_id: str, redis: Redis = Depends(get_redis)) -> PackageState:
    pkg = await state_store.get_package(redis, piece_id)
    if not pkg:
        raise HTTPException(status_code=404, detail="Package not found")
    return pkg


@router.post("/{piece_id}/review")
async def review_package(
    piece_id: str,
    review: ReviewRequest,
    redis: Redis = Depends(get_redis),
) -> dict:
    pkg = await state_store.get_package(redis, piece_id)
    if not pkg:
        raise HTTPException(status_code=404, detail="Package not found")
    if pkg.status != "MANUAL_REVIEW":
        raise HTTPException(
            status_code=409,
            detail=f"Package is not in MANUAL_REVIEW state (current: {pkg.status})",
        )
    ok = resume_pipeline(piece_id, review.route, review.operator_id)
    if not ok:
        raise HTTPException(status_code=409, detail="No active review event for this package")
    return {"message": "Review submitted, pipeline resuming"}
