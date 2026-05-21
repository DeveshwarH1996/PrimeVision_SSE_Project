# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Project Overview

This is a **PrimeVision SSE interview assignment**: a real-time package event orchestrator simulating a warehouse/robotics sorting system. The assignment is documented in `SSE_PROIJECT.md`. The implementation plan is in `PRELIMINARY.md`. The design rationale is in `NOTION.md` (also mirrored to the PrimeVision SSE project Notion page).

---

## Stack

- **Python 3.11**, **FastAPI**, **asyncio**
- **Redis** — dual role: state store (hashes) + pub/sub for WebSocket broadcasting
- **Docker + Docker Compose** — single command to run everything: `docker compose up --build`
- **pytest + httpx** — testing
- API served at `http://localhost:8000`

---

## Environment

- **Conda env:** `primevision` (Python 3.11) — created at `/home/ecoprt/miniconda3/envs/primevision`
- **Always activate before local work:** `conda activate primevision`
- **Run commands inside env without activating:** `conda run -n primevision <command>`

## Commands

```bash
# Run everything (Docker, no conda needed)
docker compose up --build

# Local dev — activate env first, then:
conda activate primevision
uvicorn app.main:app --reload

# Tests (run inside conda env)
conda run -n primevision pytest
conda run -n primevision pytest tests/test_api.py::test_name   # single test
conda run -n primevision pytest tests/ -v                      # verbose

# Install deps (if env needs refreshing)
conda run -n primevision pip install -r requirements.txt -r requirements-dev.txt
```

---

## Architecture

### Data Flow

```
POST /events/package-scan
  → validate (Pydantic) → write RECEIVED to Redis → return 202
  → background task: enrichment pipeline
      ENRICHED_METADATA → ENRICHED_OCR → ENRICHED_LLM1 → ENRICHED_LLM2
      → routing decision (confidence score)
          low confidence  → MANUAL_REVIEW (paused, awaits POST /packages/{id}/review)
          high confidence → ROUTED → FINALIZED
  → every state change: write to Redis + publish to Redis channel
  → WebSocket handler: subscribes to Redis channel, broadcasts to all clients
```

### Key Design Decisions

1. **FastAPI background tasks** (not Celery) — returns 202 immediately, pipeline runs async. Tradeoff: job lost if server crashes. Documented as production limitation.

2. **Redis pub/sub for WebSocket** — pipeline publishes state changes to a Redis channel; WebSocket handler subscribes. Application layer is stateless, horizontally scalable. In-memory connection lists were explicitly rejected.

3. **Swappable router module** (`app/router.py`) — routing logic is isolated so an ML-based router can replace the rule-based one without touching the pipeline. Confidence score drives the MANUAL_REVIEW flow.

4. **MANUAL_REVIEW as first-class state** — low-confidence packages pause here; floor operators act via `POST /packages/{piece_id}/review`. Not an edge case.

## Implementation Progress

Tracked in the Notion Project Tracker: `https://www.notion.so/3671ec9df1ac81b4a4e2cc9c9e6e2d57`

Full implementation plan: `docs/superpowers/plans/2026-05-21-sse-orchestrator.md` (13 tasks, Tasks 0–12)

| Task | Status | Description |
|------|--------|-------------|
| Task 0 | ✅ Done | Environment setup — conda env, requirements, pytest.ini |
| Task 1 | ⏳ Next | Docker + docker-compose.yml |
| Task 2 | ⏳ | Config + Models |
| Task 3 | ⏳ | Dependencies module + State layer (Redis) |
| Task 4 | ⏳ | Router module |
| Task 5 | ⏳ | WebSocket + Connection Manager |
| Task 6 | ⏳ | Main app + lifespan + conftest.py |
| Task 7 | ⏳ | Scan endpoint + enrichment pipeline |
| Task 8 | ⏳ | Package endpoints + manual review |
| Task 9 | ⏳ | Observability endpoints (robots, cameras) |
| Task 10 | ⏳ | WebSocket endpoint wired into main |
| Task 11 | ⏳ | README |
| Task 12 | ⏳ | Full test run + Docker smoke test |

**To resume:** Invoke `superpowers:subagent-driven-development` and start from Task 1. Use `conda run -n primevision` prefix for all Python/pytest commands inside subagents.

---

### Project Structure (planned)

```
app/
  main.py          # FastAPI app, mounts routers
  models.py        # Pydantic models
  state.py         # all Redis reads/writes
  pipeline.py      # enrichment steps, retry logic, failure handling
  router.py        # routing decision — designed to be swappable
  websocket.py     # WS connection manager + Redis pub/sub listener
  config.py        # Redis URL, timeouts, thresholds
  routes/
    events.py      # POST /events/package-scan
    packages.py    # all /packages/* endpoints
    robots.py      # GET /robots
    cameras.py     # GET /cameras
tests/
  test_api.py
  test_pipeline.py
  test_state.py
Dockerfile
docker-compose.yml
README.md
```

---

## API Surface

### Required
| Method | Path | Notes |
|--------|------|-------|
| POST | `/events/package-scan` | accepts `piece_id`, `robot_id`, `camera_id`, `barcode` |
| GET | `/packages` | supports `?status=&robot_id=&camera_id=` filters |
| GET | `/packages/{piece_id}` | single package state |
| WS | `/ws/events` | broadcasts all state changes |

### Extended (differentiators)
| Method | Path | Notes |
|--------|------|-------|
| GET | `/packages/manual-review` | queue awaiting operator action |
| POST | `/packages/{piece_id}/review` | operator approve/override (`action`, `route`, `operator_id`) |
| GET | `/packages/stuck` | MANUAL_REVIEW items older than threshold |
| GET | `/packages/failed` | failed enrichment with no retries remaining |
| GET | `/packages/stats` | throughput, status breakdown, error rate |
| GET | `/robots` | active robots, last seen, staleness flag |
| GET | `/cameras` | active cameras, last seen, staleness flag |
| GET | `/health` | app health + Redis reachability |

---

## Package State Machine

`RECEIVED` → `ENRICHED_METADATA` → `ENRICHED_OCR` → `ENRICHED_LLM1` → `ENRICHED_LLM2` → `MANUAL_REVIEW` (conditional) → `ROUTED` → `FINALIZED`

Failed enrichment → `FAILED` (after N retries). State and full history stored in Redis.

---

## Error Handling Rules

- **Duplicate `piece_id`** — idempotency check, return current state, do not re-run pipeline
- **Failed enrichment** — retry up to N times, then mark `FAILED`, publish to pub/sub
- **Invalid payload** — Pydantic returns 422 automatically
- **Redis down** — surfaced via `/health`; log structured error
