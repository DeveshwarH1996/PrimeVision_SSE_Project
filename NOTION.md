# Design Thought Process — Prime Vision SSE Assignment
## Real-Time Package Event Orchestrator

---

## Why I Approached It This Way

When I first read the assignment I noticed it wasn't really asking me to build a CRUD API. The core challenge was designing an async pipeline with multiple state transitions, real-time broadcasting, and a system that a human operator could actually interact with on a floor. That framing shaped every decision I made.

I also noticed the assignment explicitly listed "ability to explain tradeoffs during review discussion" as an evaluation criterion. So I treated architecture decisions as first-class deliverables, not afterthoughts.

---

## Stack Decisions

### Why I stuck with FastAPI + Redis
The preferred stack was listed explicitly. Deviating without a strong reason doesn't read as creativity — it reads as not following requirements. I considered MongoDB and GraphQL and made deliberate choices about both.

MongoDB adds infrastructure complexity with no real gain for an event-driven, fast-changing state workload. Redis is the right tool here — fast reads, pub/sub built in, no extra service needed.

GraphQL I considered more seriously once the endpoint surface expanded. With filtering, manual review, stats, robots, cameras, and health endpoints, there's a reasonable case for it — a floor dashboard could use flexible querying across multiple dimensions without me building every combination as a REST endpoint. I still chose REST because the endpoints are well-defined, the query patterns are predictable, and GraphQL adds a resolver layer that doesn't earn its complexity here. If the frontend requirements became more dynamic that calculus would change.

### Why Python 3.11
Better asyncio performance, improved typing support, and cleaner error messages. No real downside for this stack.

---

## Architecture Decisions

### Background tasks over a separate worker process
When a scan comes in, FastAPI returns a 202 immediately and the enrichment pipeline runs as a background task. I considered using Celery or a separate worker process and rejected it. A worker process adds infrastructure complexity — another Docker service, a broker, worker configuration — that isn't justified here. The honest tradeoff is resilience: if the server crashes mid-pipeline the job is lost. I documented this explicitly and noted Celery/Kafka as the natural next step in production.

### Redis as state store AND pub/sub
Redis plays two roles here. First, it stores all package state as hashes — fast reads, survives restarts. Second, and more importantly, it's the backbone of the WebSocket broadcasting architecture. Every state change publishes to a Redis channel. The WebSocket handler subscribes to that channel and forwards updates to connected clients. This means the application layer is stateless — you can run multiple instances and every client still gets every update. Storing WebSocket connections in memory is simpler but breaks the moment you scale horizontally. The Redis pub/sub approach costs very little extra and the architecture win is significant.

### Extensible routing via a swappable module
The routing logic lives in its own module designed to be replaced without touching the pipeline. Current implementation is rule-based with a confidence score. The confidence score is what drives the manual review flow. I designed it this way because the natural production evolution is to swap in an ML-based router — the pipeline doesn't need to change, just the routing module.

---

## The Manual Review Decision

This was one of the most deliberate decisions I made. Looking at the example final state in the assignment brief, MANUAL_REVIEW was right there in the history array. I treated it as a first-class state with its own floor-facing endpoints rather than an edge case.

When routing confidence is low the pipeline pauses at MANUAL_REVIEW. A floor worker sees it in the queue, reviews it, and POSTs an action to resume. This makes the system actually usable on a floor — packages don't silently fall into a bad bin, they get flagged for human review.

This also made the routing extensibility story concrete. Improving the ML router over time directly reduces the manual review rate — the pipeline doesn't change, just the confidence threshold coming out of the router.

---

## Thinking About the Floor

The assignment is framed around a warehouse or sorting facility. I've worked on deployed autonomous vehicle systems and the thing that's always underspecified in engineering exercises is who actually interacts with the running system. On a floor that's not just a developer with API access — it's someone who needs visibility into what's happening right now and a clear path to action when something goes wrong.

That drove a set of additional endpoints beyond the core requirements:

- A stats endpoint so a supervisor can see throughput, error rates, and status breakdown at a glance
- Robot and camera endpoints that track last seen timestamps and flag staleness — if a sensor goes offline you want to know before it becomes a problem
- A stuck packages endpoint for MANUAL_REVIEW items sitting unresolved beyond a threshold
- A failed packages endpoint for enrichment failures with no retry remaining
- A health endpoint that checks both app availability and Redis connectivity — expected in any deployed system

None of these are complex to implement. But they reflect thinking about the system as something that runs continuously with real people depending on it, not something that gets demoed once.

---

## What I Considered and Rejected

**MongoDB** — adds infrastructure with no real gain for this workload. Redis handles state perfectly.

**GraphQL** — considered seriously with the expanded endpoint surface, still chose REST. Query patterns are predictable and well-defined. GraphQL would make sense if frontend querying requirements became more dynamic.

**In-memory WebSocket connection list** — simple but breaks in multi-instance deployment. Redis pub/sub costs little extra and scales correctly.

**Separate Celery worker** — right answer for production, documented as a future improvement rather than included here. The tradeoff is resilience vs complexity and complexity loses at this scope.

---

## What I Would Do Differently With More Time

- **Celery or Kafka for the pipeline** — resilient job processing, survives server restarts, horizontal scaling of workers
- **Postgres alongside Redis** — Redis for hot state and pub/sub, Postgres for durable history and audit trail
- **ML-based router** — plug into the existing router module, reduce manual review rate over time
- **Role-based access if exposed beyond internal network** — in a floor deployment this is typically handled at the network or application layer above this service, but if the API were ever externally accessible, attributing review actions to specific operators becomes important
- **Rate limiting on the scan endpoint** — protect against sensor misconfiguration flooding the system
- **Structured JSON logging** — every state transition logs piece_id, robot_id, status, timestamp in a queryable format for Datadog or CloudWatch

---

## On Using AI in This Process

I used Claude to help think through architecture decisions and tradeoffs. The decisions themselves — stack choices, the Redis pub/sub approach, the manual review flow, the floor-facing endpoints — came from understanding the domain and reading the assignment carefully. AI accelerates implementation. It doesn't replace knowing what to build and why. I'm comfortable explaining every decision in this document in a review conversation.

---
