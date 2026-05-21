# PrimeVision SSE — Real-Time Package Event Orchestrator

## Setup

```bash
docker compose up --build
```

API available at `http://localhost:8000`. Docs at `http://localhost:8000/docs`.

---

## Architecture Decisions

**FastAPI background tasks over a separate worker:** When a scan arrives, the endpoint returns 202 immediately and the enrichment pipeline runs as an asyncio background task. Celery or a dedicated worker process would add infrastructure complexity (broker, worker config, separate Docker service) not justified at this scope. The explicit tradeoff is resilience — if the server crashes mid-pipeline, the job is lost. This is documented rather than hidden.

**Redis as state store and pub/sub backbone:** All package state lives in Redis hashes for fast reads and persistence across restarts. Every state change publishes to a Redis channel. The WebSocket handler subscribes to that channel and forwards messages to connected clients. The application layer holds no authoritative state — multiple app instances can run behind a load balancer and every WebSocket client gets every update regardless of which instance they connect to.

**Isolated router module:** Routing logic lives in `router.py` and returns `(route, confidence)`. The pipeline depends on the interface, not the implementation. Swapping in an ML-based router requires changing one function with no pipeline changes.

---

## Scalability Considerations

The application layer is stateless — all state is in Redis. Horizontal scaling requires only adding app instances; no coordination layer needed. The Redis pub/sub WebSocket architecture ensures broadcast correctness across instances. The enrichment pipeline is the current bottleneck — replacing background tasks with Celery workers would allow independent scaling of processing capacity.

---

## Failure Handling Strategy

**Duplicate events:** idempotency check on `piece_id` at the scan endpoint. If the package already exists, the current state is returned and the pipeline is not re-run.

**Enrichment failures:** each step retries up to N times (configurable). After all retries are exhausted the package is marked `FAILED`, published to pub/sub, and surfaced at `GET /packages/failed`.

**Invalid payloads:** Pydantic validates all request bodies and returns 422 with field-level error detail.

**Low-confidence routing:** packages route to `MANUAL_REVIEW` when the confidence score falls below the configured threshold. The pipeline pauses until a floor operator acts via `POST /packages/{piece_id}/review`. Packages stuck in this state beyond a configurable threshold appear at `GET /packages/stuck`.

**Redis unavailable:** surfaced immediately at `GET /health`. All state operations will fail with logged errors.

---

## Observability / Monitoring Approach

Every state transition writes to Redis and publishes to the WebSocket channel, providing a real-time event stream at `WS /ws/events`. The following endpoints provide operational visibility:

- `GET /health` — app availability and Redis reachability
- `GET /packages/stats` — throughput, status breakdown, error rate
- `GET /robots` / `GET /cameras` — last-seen timestamps and staleness flags
- `GET /packages/stuck` — MANUAL_REVIEW items exceeding the staleness threshold
- `GET /packages/failed` — failed enrichments with no retries remaining

In production, structured JSON logs keyed on `piece_id`, `robot_id`, `status`, and `timestamp` would feed Datadog or CloudWatch for alerting and dashboarding.

---

## Production Improvements

- **Celery or Kafka for the pipeline:** resilient job processing that survives server restarts and enables horizontal worker scaling
- **Postgres alongside Redis:** durable audit trail and historical queries; Redis handles hot state and pub/sub
- **ML-based router:** plug into `router.py`; reduces manual review rate over time without pipeline changes
- **Rate limiting on the scan endpoint:** protect against sensor misconfiguration flooding the system
- **Role-based access on operator endpoints:** in a floor deployment, operator actions should be attributable and access-controlled
