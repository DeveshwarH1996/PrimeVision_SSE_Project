# Prime Vision SSE Interview Assignment  
## Real-Time Package Event Orchestrator

## Objective

Build a small backend system that simulates a real-time package processing workflow similar to an event-driven robotics or warehouse orchestration platform.

The goal of this exercise is to evaluate:

- software engineering fundamentals
- asynchronous programming
- API design
- containerization skills
- architecture decisions
- production engineering mindset
- code quality and maintainability

---

# Assignment Requirements

Build a backend service exposing the following interfaces.

---

## API Endpoints

### Submit Package Event

```http
POST /events/package-scan
```

Example payload:

```json
{
  "piece_id": "PKG-001",
  "robot_id": "R-12",
  "camera_id": "CAM-04",
  "barcode": "92055901755477000000000001"
}
```

---

### Get Package State

```http
GET /packages/{piece_id}
```

---

### Get All Packages

```http
GET /packages
```

---

### WebSocket Event Stream

```http
WS /ws/events
```

The WebSocket endpoint should broadcast package state updates to connected clients in real time.

---

# Workflow Requirements

When a package scan event is received, the system should:

1. accept the event
2. create or update package state
3. asynchronously simulate an enrichment operations (e.g., fetching additional metadata from external services)
4. update package state to `ENRICHED`
5. make a routing decision
6. update package state to `FINALIZED` after completing all operations
7. broadcast all state changes over WebSocket

Example final state:

```json
{
  "piece_id": "PKG-001",
  "status": "ROUTED",
  "barcode": "92055901755477000000000001",
  "robot_id": "R-12",
  "route": "BIN-A",
  "history": [
    "RECEIVED",
    "ENRICHED_METADATA",
    "ENRICHED_OCR",
    "ENRICHED_LLM1",
    "ENRICHED_LLM2",
    "MANUAL_REVIEW",
    "...",
    "FINALIZED"
  ]
}
```

---

# Technical Expectations

Preferred stack:

- Python
- FastAPI
- asyncio

Optional technologies:

- Redis
- Docker / Docker Compose
- pytest

Candidates may use AI-assisted development tools if desired.

---

# Containerization Requirement

The project must include:

```text
Dockerfile
docker-compose.yml
```

The entire system should run using:

```bash
docker compose up --build
```

The API should be accessible at:

```text
http://localhost:8000
```

If Redis is used, it should be configured through Docker Compose.

---

# Additional Requirements

The system should gracefully handle:

- duplicate package events
- failed enrichment operations
- invalid payloads

Routing decisions may be simple, but the implementation should be designed so additional routing logic could be added later.

---

# Deliverables

Please provide:

- source code
- Dockerfile
- docker-compose.yml
- README with setup instructions
- brief architecture overview
- tests for core functionality
- notes describing tradeoffs and future improvements

---

# Required README Sections

The README should include short sections describing:

- architecture decisions
- scalability considerations
- failure handling strategy
- observability/monitoring approach
- production improvements you would make with more time

---

# Time Expectation

Expected completion time:

```text
~3 hours
```

This is intentionally scoped as a limited exercise. We are more interested in engineering judgment, architecture decisions, and implementation quality than in building a production-complete system.

---

# Evaluation Criteria

We will evaluate:

- code quality and maintainability
- API design
- asynchronous programming approach
- state management
- error handling
- containerization
- testing approach
- architecture decisions
- scalability and production considerations
- ability to explain tradeoffs during review discussion

---
