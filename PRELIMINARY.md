# Prime Vision SSE Project Plan
## Real-Time Package Event Orchestrator

---

## Stack

- **FastAPI** — all REST endpoints, WebSocket, Pydantic validation
- **asyncio** — enrichment pipeline runs as background task
- **Redis** — dual role: state store + pub/sub for WebSocket broadcasting
- **Docker Compose** — two services: app + Redis
- **pytest + httpx** — testing
- **Conda env** — local dev (Python 3.11)

---

## Architecture Decisions

1. **Background tasks for enrichment pipeline** — FastAPI returns 202 immediately, pipeline runs async in background. Not a separate worker process (overkill for this scope, honest tradeoff to document).

2. **Redis as state store** — all package state lives in Redis as hashes. Survives restarts, fast reads.

3. **Redis pub/sub for WebSocket broadcasting** — pipeline publishes state changes to a Redis channel, WebSocket handler subscribes and forwards to connected clients. This means multiple server instances can all broadcast correctly — the key scalability win.

4. **Manual review pause** — routing decision includes a confidence score. Low confidence pauses pipeline at `MANUAL_REVIEW`, waits for operator input via API before resuming.

5. **Extensible routing** — router.py is designed as a swappable module. Current implementation is rule-based. README notes how an ML-based router would plug in without touching the pipeline.

---

## Data Flow

```
POST /events/package-scan
        ↓
  Validate payload (Pydantic)
        ↓
  Write RECEIVED to Redis
        ↓
  Return 202 immediately
        ↓ (background task)
  Run enrichment pipeline:
    → ENRICHED_METADATA
    → ENRICHED_OCR
    → ENRICHED_LLM1
    → ENRICHED_LLM2
        ↓
  Routing decision (confidence check)
    ↓ low confidence           ↓ high confidence
  MANUAL_REVIEW              ROUTED
    ↓                           ↓
  Wait for operator          FINALIZED
  POST /review
    ↓
  ROUTED → FINALIZED

  Every state change:
    → writes to Redis
    → publishes to Redis pub/sub
    → WebSocket handler broadcasts to all connected clients
```

---

## Full Endpoint List

### Core (required)
```
POST /events/package-scan          # submit a package scan event
GET  /packages                     # get all packages
GET  /packages/{piece_id}          # get single package state
WS   /ws/events                    # real-time state broadcast
```

### Filtering
```
GET  /packages?status=FAILED&robot_id=R-12    # filter by status, robot, camera
```

### Manual Review (floor operator)
```
GET  /packages/manual-review                  # queue of packages awaiting operator action
POST /packages/{piece_id}/review              # operator approves or overrides route
```

Review payload:
```json
{
  "action": "approve",
  "route": "BIN-B",
  "operator_id": "OP-01"
}
```

### Observability / Floor Awareness
```
GET  /packages/stats      # throughput, status breakdown, error rate
GET  /robots              # active robots, last seen timestamp, staleness flag
GET  /cameras             # active cameras, last seen timestamp, staleness flag
GET  /packages/stuck      # MANUAL_REVIEW items older than threshold
GET  /packages/failed     # failed enrichment with no retry remaining
GET  /health              # app health + Redis reachability check
```

---

## Project Structure

```
primevision/
├── app/
│   ├── main.py              # FastAPI app init, mounts routers
│   ├── models.py            # Pydantic models (request/response/state)
│   ├── state.py             # all Redis reads and writes in one place
│   ├── pipeline.py          # enrichment steps, failure handling, retry logic
│   ├── router.py            # routing decision logic, designed to be swappable
│   ├── websocket.py         # WS connection manager + Redis pub/sub listener
│   ├── config.py            # settings (Redis URL, timeouts, thresholds)
│   └── routes/
│       ├── events.py        # POST /events/package-scan
│       ├── packages.py      # all /packages/* endpoints
│       ├── robots.py        # GET /robots
│       └── cameras.py       # GET /cameras
├── tests/
│   ├── test_api.py          # endpoint tests with httpx
│   ├── test_pipeline.py     # state transition and failure tests
│   └── test_state.py        # Redis state management tests
├── Dockerfile
├── docker-compose.yml
└── README.md
```

---

## Error Handling Requirements

- **Duplicate package events** — idempotency check on piece_id, return current state if already exists
- **Failed enrichment** — retry up to N times, then mark as FAILED, publish to pub/sub
- **Invalid payloads** — Pydantic handles this, return 422 with clear error
- **Redis unavailable** — /health endpoint surfaces this, structured logs capture it
- **Packages stuck in MANUAL_REVIEW** — /packages/stuck surfaces these for operator attention

---

## README Sections to Write

1. **Architecture decisions** — background tasks vs worker, Redis dual role, pub/sub WebSocket design
2. **Scalability considerations** — Redis pub/sub means multi-instance works, stateless app layer, routing is swappable
3. **Failure handling strategy** — enrichment retries, dead letter visibility, duplicate idempotency
4. **Observability/monitoring approach** — structured JSON logs with piece_id/robot_id/status/timestamp on every transition, /health endpoint, /stats endpoint
5. **Production improvements with more time** — Celery/Kafka for resilient pipeline, ML-based router, persistent storage (Postgres) alongside Redis, authentication on operator endpoints, rate limiting on scan endpoint

---

## Key Differentiators vs Other Submissions

- Manual review queue with operator action endpoint (explicitly in their example history, most will skip it)
- Floor-facing endpoints (stats, robots, cameras, stuck packages)
- Redis pub/sub WebSocket architecture that genuinely scales across instances
- /health endpoint (expected in any real deployment, often missed in exercises)
- README written from warehouse/robotics domain knowledge, not generic boilerplate
- Structured logging framed around operational visibility

---

## Notes on Scope

- This is a 3-hour exercise. Code should be clean and correct, not production-complete.
- Additional endpoints are lightweight — robots/cameras just track last seen from incoming scan events, no extra modeling needed.
- Confidence score for routing can be a simple random float for simulation purposes — the architecture around it is what matters.
- Enrichment steps are simulated with async sleep — structure should look real even if logic is placeholder.
