# SSE Package Event Orchestrator — Design Spec
**Date:** 2026-05-21
**Status:** Approved

---

## Context

PrimeVision interview assignment: build a real-time package processing backend simulating a warehouse/robotics sorting system. Assignment doc: `SSE_PROIJECT.md`. Full design rationale: Notion page (PrimeVision SSE project).

---

## Environment

- Conda env: `primevision`, Python 3.11 — must be created before any development begins
- Docker: Python 3.11 base image, independent of conda
- Local dev: `uvicorn app.main:app --reload` inside activated conda env
- Full system: `docker compose up --build` → API at `http://localhost:8000`

---

## Build Order (Option A — Core pipeline first)

1. Environment setup (conda + requirements)
2. Docker + docker-compose scaffolding
3. Config, models, state.py (Redis layer)
4. Core scan endpoint + enrichment pipeline + WebSocket broadcasting — verify end-to-end
5. Manual review flow (pause/resume)
6. Observability endpoints
7. Tests (written alongside each layer)
8. README

---

## Project Structure

```
app/
  main.py          # FastAPI app, lifespan handler (starts Redis pub/sub listener)
  config.py        # pydantic-settings: Redis URL, retry count, staleness threshold, confidence threshold
  models.py        # all Pydantic models: request, response, PackageState
  state.py         # ONLY file that talks to Redis directly
  pipeline.py      # enrichment steps, retry logic, asyncio.Event pause for MANUAL_REVIEW
  router.py        # make_routing_decision() → (route, confidence) — isolated, swappable
  websocket.py     # ConnectionManager + Redis pub/sub subscriber task
  routes/
    events.py      # POST /events/package-scan
    packages.py    # all /packages/* endpoints including manual review
    robots.py      # GET /robots
    cameras.py     # GET /cameras
tests/
  conftest.py      # pytest fixtures: AsyncClient, fakeredis
  test_api.py      # endpoint behavior tests
  test_pipeline.py # state machine, retry, manual review pause/resume
  test_state.py    # Redis layer in isolation
docs/superpowers/specs/
Dockerfile
docker-compose.yml
requirements.txt
README.md
```

---

## Data Model

```python
class PackageState(BaseModel):
    piece_id: str
    status: str
    barcode: str
    robot_id: str
    camera_id: str
    route: str | None
    history: list[str]
    enrichment_metadata: dict
    retry_count: int
    created_at: datetime
    updated_at: datetime
    operator_id: str | None
```

Stored in Redis as a hash keyed by `piece_id`. `state.py` exposes named functions (`get_package`, `set_package`, `publish_event`, `update_robot_seen`, `update_camera_seen`, etc.) — no raw Redis calls outside this module.

---

## Enrichment Pipeline

**Trigger:** `POST /events/package-scan` → idempotency check → write `RECEIVED` → return `202` → `asyncio.create_task(run_pipeline(piece_id))`

**State machine:**
```
RECEIVED
  → ENRICHED_METADATA
  → ENRICHED_OCR
  → ENRICHED_LLM1
  → ENRICHED_LLM2
  → routing decision
      confidence >= threshold  → ROUTED → FINALIZED
      confidence < threshold   → MANUAL_REVIEW (pipeline pauses on asyncio.Event)
                                    operator POST /review sets event + injects route
                                  → ROUTED → FINALIZED
```

Each step: `async def` with `asyncio.sleep()` to simulate latency. After each step: append to `history`, write to Redis, publish to pub/sub channel.

**Retry logic:** each step retries up to N times (configurable). All retries exhausted → status `FAILED`, published, pipeline stops. Package visible in `/packages/failed`.

**Idempotency:** if `piece_id` already exists in Redis, return current state with `200`, do not re-run pipeline.

---

## Router Module

```python
# router.py
def make_routing_decision(package: PackageState) -> tuple[str, float]:
    route = random.choice(["BIN-A", "BIN-B", "BIN-C"])
    confidence = random.random()
    return route, confidence
```

Threshold in `config.py`. Replacing this function with an ML model requires no changes to the pipeline.

---

## MANUAL_REVIEW Pause/Resume

`pipeline.py` holds `_review_events: dict[str, asyncio.Event]` at module level.

- When confidence < threshold: create event for `piece_id`, `await` it
- `POST /packages/{piece_id}/review` calls `resume_pipeline(piece_id, route, operator_id)` → sets event, injects chosen route
- Pipeline continues to `ROUTED → FINALIZED`

---

## WebSocket Broadcasting

```
pipeline.py → state.publish_event() → Redis PUBLISH "package_events" <json>

websocket.py (background task from lifespan)
  → Redis SUBSCRIBE "package_events"
  → on message: ConnectionManager.broadcast() to all active WS clients
```

`ConnectionManager`: `list[WebSocket]`. Dead connections caught on send and removed silently. Subscriber task started via `asyncio.create_task` in lifespan, cancelled cleanly on shutdown.

Scales horizontally: multiple app instances all subscribe to the same Redis channel — every client gets every update regardless of which instance their WebSocket connects to.

---

## API Endpoints

**Core (required):**
| Method | Path | Notes |
|--------|------|-------|
| `POST` | `/events/package-scan` | `202` on new, `200` on duplicate |
| `GET` | `/packages` | `?status=&robot_id=&camera_id=` filters |
| `GET` | `/packages/{piece_id}` | `404` if not found |
| `WS` | `/ws/events` | real-time state broadcast |

**Manual review:**
| Method | Path | Notes |
|--------|------|-------|
| `GET` | `/packages/manual-review` | all packages in `MANUAL_REVIEW` status |
| `POST` | `/packages/{piece_id}/review` | `{ action, route, operator_id }` |

**Observability:**
| Method | Path | Notes |
|--------|------|-------|
| `GET` | `/packages/stats` | count by status, throughput, error rate |
| `GET` | `/packages/stuck` | `MANUAL_REVIEW` older than threshold |
| `GET` | `/packages/failed` | all `FAILED` packages |
| `GET` | `/robots` | last seen + staleness flag |
| `GET` | `/cameras` | last seen + staleness flag |
| `GET` | `/health` | app up + Redis ping |

**Route ordering note:** literal paths (`/packages/manual-review`, `/packages/stuck`, etc.) must be registered before `/packages/{piece_id}` in the router.

---

## Testing

**Stack:** `pytest-asyncio`, `httpx.AsyncClient`, `fakeredis` (async) — no real Redis or Docker needed.

**`test_api.py`:** happy path all endpoints, idempotency, `422` invalid payload, `404` unknown piece_id, manual review queue, observability endpoint shapes, health Redis check.

**`test_pipeline.py`:** full pipeline to `FINALIZED`, all history entries in order, retry then `FAILED`, `MANUAL_REVIEW` pause triggered by low confidence, `POST /review` resumes to `FINALIZED`, concurrent pipelines non-interfering.

**`test_state.py`:** get/set round-trip, pub/sub publish, robot/camera last-seen, staleness flag logic.

```bash
pytest                          # full suite
pytest tests/test_pipeline.py  # single file
pytest -k "test_manual_review" # single test
```

---

## README Sections (required by assignment)

1. Architecture decisions
2. Scalability considerations
3. Failure handling strategy
4. Observability/monitoring approach
5. Production improvements with more time

All design rationale and thought process lives on the Notion page, not in the README.
