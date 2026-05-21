import json
import pytest
import pytest_asyncio
from datetime import datetime, timezone
import fakeredis
import fakeredis.aioredis as aioredis

from app.models import PackageState
from app.state import (
    get_package,
    set_package,
    get_all_packages,
    publish_event,
    update_robot_seen,
    update_camera_seen,
    get_robots,
    get_cameras,
)


def _pkg(piece_id: str = "PKG-001") -> PackageState:
    now = datetime.now(timezone.utc)
    return PackageState(
        piece_id=piece_id,
        status="RECEIVED",
        barcode="92055901755477000000000001",
        robot_id="R-01",
        camera_id="CAM-01",
        history=["RECEIVED"],
        created_at=now,
        updated_at=now,
    )


@pytest_asyncio.fixture
async def redis():
    server = fakeredis.FakeServer()
    r = fakeredis.FakeAsyncRedis(server=server, decode_responses=True)
    yield r
    await r.aclose()


async def test_set_and_get_roundtrip(redis):
    pkg = _pkg()
    await set_package(redis, pkg)
    result = await get_package(redis, pkg.piece_id)
    assert result is not None
    assert result.piece_id == "PKG-001"
    assert result.status == "RECEIVED"
    assert result.history == ["RECEIVED"]
    assert result.route is None
    assert result.operator_id is None


async def test_get_package_unknown_returns_none(redis):
    result = await get_package(redis, "DOES-NOT-EXIST")
    assert result is None


async def test_get_all_packages_returns_all(redis):
    await set_package(redis, _pkg("PKG-001"))
    await set_package(redis, _pkg("PKG-002"))
    results = await get_all_packages(redis)
    ids = {p.piece_id for p in results}
    assert ids == {"PKG-001", "PKG-002"}


async def test_publish_event_writes_to_channel(redis):
    pkg = _pkg()
    pubsub = redis.pubsub()
    await pubsub.subscribe("package_events")
    await pubsub.get_message(timeout=0.1)  # drain subscribe confirmation
    await publish_event(redis, pkg)
    msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
    assert msg is not None
    data = json.loads(msg["data"])
    assert data["piece_id"] == "PKG-001"
    assert data["status"] == "RECEIVED"


async def test_robot_seen_tracked(redis):
    await update_robot_seen(redis, "R-01")
    robots = await get_robots(redis)
    assert "R-01" in robots


async def test_camera_seen_tracked(redis):
    await update_camera_seen(redis, "CAM-01")
    cameras = await get_cameras(redis)
    assert "CAM-01" in cameras


async def test_set_package_with_route_and_operator(redis):
    pkg = _pkg()
    pkg.route = "BIN-A"
    pkg.operator_id = "OP-01"
    await set_package(redis, pkg)
    result = await get_package(redis, pkg.piece_id)
    assert result.route == "BIN-A"
    assert result.operator_id == "OP-01"
