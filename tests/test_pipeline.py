import asyncio
from datetime import datetime, timezone

import fakeredis.aioredis as aioredis
import pytest
import pytest_asyncio

from app.models import PackageState
from app.pipeline import resume_pipeline, run_pipeline
from app.state import get_package, set_package


def _pkg(piece_id: str = "PKG-001") -> PackageState:
    now = datetime.now(timezone.utc)
    return PackageState(
        piece_id=piece_id,
        status="RECEIVED",
        barcode="bar",
        robot_id="R-01",
        camera_id="CAM-01",
        history=["RECEIVED"],
        created_at=now,
        updated_at=now,
    )


@pytest_asyncio.fixture
async def redis():
    r = aioredis.FakeRedis(decode_responses=True)
    yield r
    await r.aclose()


@pytest.fixture(autouse=True)
def fast_pipeline(monkeypatch):
    monkeypatch.setattr("app.pipeline.settings.enrichment_step_delay", 0.0)
    monkeypatch.setattr("app.pipeline.settings.confidence_threshold", 0.0)


async def test_pipeline_reaches_finalized(redis):
    pkg = _pkg()
    await set_package(redis, pkg)
    await run_pipeline(redis, pkg.piece_id)
    result = await get_package(redis, pkg.piece_id)
    assert result.status == "FINALIZED"


async def test_pipeline_history_contains_all_states(redis):
    pkg = _pkg()
    await set_package(redis, pkg)
    await run_pipeline(redis, pkg.piece_id)
    result = await get_package(redis, pkg.piece_id)
    expected = [
        "RECEIVED", "ENRICHED_METADATA", "ENRICHED_OCR",
        "ENRICHED_LLM1", "ENRICHED_LLM2", "ROUTED", "FINALIZED",
    ]
    for state in expected:
        assert state in result.history


async def test_pipeline_sets_route(redis):
    pkg = _pkg()
    await set_package(redis, pkg)
    await run_pipeline(redis, pkg.piece_id)
    result = await get_package(redis, pkg.piece_id)
    assert result.route in ["BIN-A", "BIN-B", "BIN-C"]


async def test_pipeline_marks_failed_after_retries(redis, monkeypatch):
    async def _bad_enrich(redis, pkg, status):
        raise RuntimeError("simulated failure")

    monkeypatch.setattr("app.pipeline._enrich", _bad_enrich)
    monkeypatch.setattr("app.pipeline.settings.enrichment_retry_count", 1)

    pkg = _pkg()
    await set_package(redis, pkg)
    await run_pipeline(redis, pkg.piece_id)
    result = await get_package(redis, pkg.piece_id)
    assert result.status == "FAILED"
    assert "FAILED" in result.history


async def test_pipeline_manual_review_pause_and_resume(redis, monkeypatch):
    monkeypatch.setattr("app.pipeline.settings.confidence_threshold", 1.1)

    pkg = _pkg()
    await set_package(redis, pkg)
    task = asyncio.create_task(run_pipeline(redis, pkg.piece_id))

    for _ in range(50):
        await asyncio.sleep(0.05)
        result = await get_package(redis, pkg.piece_id)
        if result and result.status == "MANUAL_REVIEW":
            break

    assert result.status == "MANUAL_REVIEW"

    ok = resume_pipeline(pkg.piece_id, "BIN-A", "OP-01")
    assert ok is True

    await task
    result = await get_package(redis, pkg.piece_id)
    assert result.status == "FINALIZED"
    assert result.route == "BIN-A"
    assert result.operator_id == "OP-01"
    assert "MANUAL_REVIEW" in result.history


async def test_concurrent_pipelines_dont_interfere(redis):
    pkgs = [_pkg(f"PKG-{i:03d}") for i in range(3)]
    for pkg in pkgs:
        await set_package(redis, pkg)

    await asyncio.gather(*[run_pipeline(redis, pkg.piece_id) for pkg in pkgs])

    for pkg in pkgs:
        result = await get_package(redis, pkg.piece_id)
        assert result.status == "FINALIZED"


async def test_pipeline_does_nothing_for_unknown_piece_id(redis):
    await run_pipeline(redis, "UNKNOWN-PKG")
