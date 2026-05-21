import asyncio
from datetime import datetime, timezone

from redis.asyncio import Redis

from app import state as state_store
from app.config import settings
from app.models import PackageState
from app.router import make_routing_decision

_review_events: dict[str, asyncio.Event] = {}
_review_routes: dict[str, str] = {}
_review_operators: dict[str, str | None] = {}


async def _update_state(redis: Redis, pkg: PackageState, status: str) -> PackageState:
    pkg.status = status
    pkg.history.append(status)
    pkg.updated_at = datetime.now(timezone.utc)
    await state_store.set_package(redis, pkg)
    await state_store.publish_event(redis, pkg)
    return pkg


async def _enrich(redis: Redis, pkg: PackageState, status: str) -> PackageState:
    await asyncio.sleep(settings.enrichment_step_delay)
    return await _update_state(redis, pkg, status)


async def _run_pipeline_step(redis: Redis, pkg: PackageState, status: str) -> PackageState:
    last_exc: Exception | None = None
    for _ in range(settings.enrichment_retry_count):
        try:
            return await _enrich(redis, pkg, status)
        except Exception as exc:
            last_exc = exc
    raise last_exc


async def run_pipeline(redis: Redis, piece_id: str) -> None:
    pkg = await state_store.get_package(redis, piece_id)
    if not pkg:
        return

    try:
        for status in ["ENRICHED_METADATA", "ENRICHED_OCR", "ENRICHED_LLM1", "ENRICHED_LLM2"]:
            pkg = await _run_pipeline_step(redis, pkg, status)

        route, confidence = make_routing_decision(pkg)

        if confidence < settings.confidence_threshold:
            pkg = await _update_state(redis, pkg, "MANUAL_REVIEW")
            event = asyncio.Event()
            _review_events[piece_id] = event
            await event.wait()
            route = _review_routes.pop(piece_id, route)
            pkg.operator_id = _review_operators.pop(piece_id, None)
            _review_events.pop(piece_id, None)

        pkg.route = route
        pkg = await _update_state(redis, pkg, "ROUTED")
        await _update_state(redis, pkg, "FINALIZED")

    except Exception:
        pkg.status = "FAILED"
        pkg.history.append("FAILED")
        pkg.updated_at = datetime.now(timezone.utc)
        await state_store.set_package(redis, pkg)
        await state_store.publish_event(redis, pkg)


def resume_pipeline(piece_id: str, route: str, operator_id: str) -> bool:
    event = _review_events.get(piece_id)
    if not event:
        return False
    _review_routes[piece_id] = route
    _review_operators[piece_id] = operator_id
    event.set()
    return True
