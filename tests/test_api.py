import asyncio
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
import pytest_asyncio

from app.models import PackageState
from app import state as state_store


_SCAN_PAYLOAD = {
    "piece_id": "PKG-001",
    "robot_id": "R-01",
    "camera_id": "CAM-01",
    "barcode": "92055901755477000000000001",
}


async def test_scan_returns_received_state(client, redis):
    with patch("app.routes.events.asyncio.create_task"):
        resp = await client.post("/events/package-scan", json=_SCAN_PAYLOAD)
    assert resp.status_code == 200
    data = resp.json()
    assert data["piece_id"] == "PKG-001"
    assert data["status"] == "RECEIVED"
    assert "RECEIVED" in data["history"]


async def test_scan_idempotency_returns_existing_state(client, redis):
    with patch("app.routes.events.asyncio.create_task"):
        await client.post("/events/package-scan", json=_SCAN_PAYLOAD)
        resp = await client.post("/events/package-scan", json=_SCAN_PAYLOAD)
    assert resp.status_code == 200
    assert resp.json()["status"] == "RECEIVED"


async def test_scan_invalid_payload_returns_422(client):
    resp = await client.post("/events/package-scan", json={"piece_id": "PKG-001"})
    assert resp.status_code == 422


async def test_get_package_returns_state(client, redis):
    with patch("app.routes.events.asyncio.create_task"):
        await client.post("/events/package-scan", json=_SCAN_PAYLOAD)
    resp = await client.get("/packages/PKG-001")
    assert resp.status_code == 200
    assert resp.json()["piece_id"] == "PKG-001"


async def test_get_package_unknown_returns_404(client):
    resp = await client.get("/packages/UNKNOWN")
    assert resp.status_code == 404


async def test_get_all_packages(client, redis):
    with patch("app.routes.events.asyncio.create_task"):
        await client.post("/events/package-scan", json=_SCAN_PAYLOAD)
    resp = await client.get("/packages")
    assert resp.status_code == 200
    assert len(resp.json()) == 1


async def test_get_packages_filter_by_status(client, redis):
    with patch("app.routes.events.asyncio.create_task"):
        await client.post("/events/package-scan", json=_SCAN_PAYLOAD)
    resp = await client.get("/packages?status=RECEIVED")
    assert resp.status_code == 200
    assert all(p["status"] == "RECEIVED" for p in resp.json())


async def test_get_packages_filter_by_robot(client, redis):
    with patch("app.routes.events.asyncio.create_task"):
        await client.post("/events/package-scan", json=_SCAN_PAYLOAD)
    resp = await client.get("/packages?robot_id=R-01")
    assert len(resp.json()) == 1
    resp2 = await client.get("/packages?robot_id=R-99")
    assert len(resp2.json()) == 0


def _seeded_pkg(redis_fixture, piece_id="PKG-REVIEW", status="MANUAL_REVIEW"):
    now = datetime.now(timezone.utc)
    return PackageState(
        piece_id=piece_id,
        status=status,
        barcode="bar",
        robot_id="R-01",
        camera_id="CAM-01",
        history=["RECEIVED", "ENRICHED_METADATA", "ENRICHED_OCR",
                 "ENRICHED_LLM1", "ENRICHED_LLM2", "MANUAL_REVIEW"],
        created_at=now,
        updated_at=now,
    )


async def test_manual_review_queue(client, redis):
    pkg = _seeded_pkg(redis)
    await state_store.set_package(redis, pkg)
    resp = await client.get("/packages/manual-review")
    assert resp.status_code == 200
    assert any(p["piece_id"] == "PKG-REVIEW" for p in resp.json())


async def test_review_wrong_status_returns_409(client, redis):
    pkg = _seeded_pkg(redis, status="RECEIVED")
    await state_store.set_package(redis, pkg)
    resp = await client.post(
        f"/packages/{pkg.piece_id}/review",
        json={"action": "approve", "route": "BIN-A", "operator_id": "OP-01"},
    )
    assert resp.status_code == 409


async def test_review_unknown_package_returns_404(client):
    resp = await client.post(
        "/packages/GHOST/review",
        json={"action": "approve", "route": "BIN-A", "operator_id": "OP-01"},
    )
    assert resp.status_code == 404


async def test_stats_endpoint(client, redis):
    with patch("app.routes.events.asyncio.create_task"):
        await client.post("/events/package-scan", json=_SCAN_PAYLOAD)
    resp = await client.get("/packages/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert "total" in data
    assert "by_status" in data
    assert "error_rate" in data


async def test_failed_endpoint(client, redis):
    pkg = _seeded_pkg(redis, status="FAILED")
    await state_store.set_package(redis, pkg)
    resp = await client.get("/packages/failed")
    assert resp.status_code == 200
    assert any(p["piece_id"] == "PKG-REVIEW" for p in resp.json())


async def test_stuck_endpoint(client, redis):
    resp = await client.get("/packages/stuck")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


async def test_health_returns_ok(client, redis):
    resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["redis"] == "ok"


async def test_robots_endpoint(client, redis):
    with patch("app.routes.events.asyncio.create_task"):
        await client.post("/events/package-scan", json=_SCAN_PAYLOAD)
    resp = await client.get("/robots")
    assert resp.status_code == 200
    robots = resp.json()
    assert any(r["robot_id"] == "R-01" for r in robots)


async def test_cameras_endpoint(client, redis):
    with patch("app.routes.events.asyncio.create_task"):
        await client.post("/events/package-scan", json=_SCAN_PAYLOAD)
    resp = await client.get("/cameras")
    assert resp.status_code == 200
    cameras = resp.json()
    assert any(c["camera_id"] == "CAM-01" for c in cameras)
