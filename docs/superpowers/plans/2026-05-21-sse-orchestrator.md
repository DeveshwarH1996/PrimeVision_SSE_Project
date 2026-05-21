# SSE Package Event Orchestrator — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a real-time package event orchestrator with FastAPI, Redis pub/sub, and WebSocket broadcasting, including a MANUAL_REVIEW flow and floor-facing observability endpoints.

**Architecture:** Scan events trigger an async enrichment pipeline that runs in the background and broadcasts every state change through Redis pub/sub to WebSocket clients. Redis is the single state store; `state.py` is the only file that touches it directly. The `router.py` module is intentionally isolated so an ML-based router can replace it without touching the pipeline.

**Tech Stack:** Python 3.11 (conda env `primevision`), FastAPI, asyncio, redis-py (async), pydantic-settings, pytest-asyncio, httpx, fakeredis, Docker Compose.

**Notion Tracker:** `https://www.notion.so/3671ec9df1ac81b4a4e2cc9c9e6e2d57` (page ID: `3671ec9df1ac81b4a4e2cc9c9e6e2d57`)
After completing each task, mark it done using `mcp__claude_ai_Notion__notion-update-page` with `command: update_content`. Replace `- [ ] **Task N:**` → `- [x] **Task N:**` for the matching line. The exact `old_str` for each task is included at the end of every task section below.

---

## File Map

| File | Responsibility |
|------|---------------|
| `app/dependencies.py` | Module-level Redis singleton + `get_redis` / `set_redis` |
| `app/config.py` | pydantic-settings: Redis URL, thresholds, delays |
| `app/models.py` | All Pydantic models: request, response, PackageState |
| `app/state.py` | All Redis reads/writes — nothing else touches Redis |
| `app/router.py` | `make_routing_decision(pkg) → (route, confidence)` |
| `app/websocket.py` | ConnectionManager + `pubsub_listener` coroutine |
| `app/pipeline.py` | Enrichment steps, retry logic, MANUAL_REVIEW pause/resume |
| `app/main.py` | FastAPI app, lifespan, route mounting |
| `app/routes/events.py` | `POST /events/package-scan` |
| `app/routes/packages.py` | All `/packages/*` endpoints including manual review |
| `app/routes/robots.py` | `GET /robots` |
| `app/routes/cameras.py` | `GET /cameras` |
| `app/routes/health.py` | `GET /health` |
| `tests/conftest.py` | fakeredis fixture, test client with lifespan bypass |
| `tests/test_state.py` | Redis layer in isolation |
| `tests/test_pipeline.py` | State machine, retry, manual review pause/resume |
| `tests/test_api.py` | All endpoint behavior |
| `Dockerfile` | Python 3.11-slim, copies app/, runs uvicorn |
| `docker-compose.yml` | Two services: redis:7-alpine + app |
| `requirements.txt` | Runtime dependencies |
| `requirements-dev.txt` | Test/dev dependencies |
| `pytest.ini` | asyncio_mode = auto |
| `README.md` | Five required sections from assignment brief |

---

## Task 0: Environment Setup

**Files:**
- Create: `requirements.txt`
- Create: `requirements-dev.txt`
- Create: `pytest.ini`

> **STOP:** Do not write any code until the conda environment is created and activated.

- [ ] **Step 1: Create and activate the conda environment**

```bash
conda create -n primevision python=3.11 -y
conda activate primevision
```

Expected: shell prompt shows `(primevision)`.

- [ ] **Step 2: Create `requirements.txt`**

```
fastapi==0.115.5
uvicorn[standard]==0.32.1
redis==5.2.1
pydantic-settings==2.6.1
```

- [ ] **Step 3: Create `requirements-dev.txt`**

```
pytest==8.3.4
pytest-asyncio==0.24.0
httpx==0.28.1
fakeredis==2.26.2
```

- [ ] **Step 4: Install dependencies**

```bash
pip install -r requirements.txt -r requirements-dev.txt
```

Expected: all packages install without errors.

- [ ] **Step 5: Create `pytest.ini`**

```ini
[pytest]
asyncio_mode = auto
```

- [ ] **Step 6: Commit**

```bash
git add requirements.txt requirements-dev.txt pytest.ini
git commit -m "chore: add requirements and pytest config"
```

- [ ] **Step 7: Mark Task 0 complete in Notion**

`mcp__claude_ai_Notion__notion-update-page` → `page_id: 3671ec9df1ac81b4a4e2cc9c9e6e2d57` → `command: update_content`:
```json
[{"old_str": "- [ ] **Task 0:** Environment Setup — conda env `primevision` (Python 3.11), requirements.txt, pytest.ini", "new_str": "- [x] **Task 0:** Environment Setup — conda env `primevision` (Python 3.11), requirements.txt, pytest.ini"}]
```

---

## Task 1: Docker + Docker Compose

**Files:**
- Create: `Dockerfile`
- Create: `docker-compose.yml`

- [ ] **Step 1: Create `Dockerfile`**

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 2: Create `docker-compose.yml`**

```yaml
services:
  redis:
    image: redis:7-alpine
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  app:
    build: .
    ports:
      - "8000:8000"
    environment:
      - REDIS_URL=redis://redis:6379
    depends_on:
      redis:
        condition: service_healthy
```

- [ ] **Step 3: Commit**

```bash
git add Dockerfile docker-compose.yml
git commit -m "chore: add Dockerfile and docker-compose"
```

- [ ] **Step 4: Mark Task 1 complete in Notion**

`mcp__claude_ai_Notion__notion-update-page` → `page_id: 3671ec9df1ac81b4a4e2cc9c9e6e2d57` → `command: update_content`:
```json
[{"old_str": "- [ ] **Task 1:** Docker + Docker Compose — Dockerfile, docker-compose.yml", "new_str": "- [x] **Task 1:** Docker + Docker Compose — Dockerfile, docker-compose.yml"}]
```

---

## Task 2: Config + Models

**Files:**
- Create: `app/__init__.py`
- Create: `app/config.py`
- Create: `app/models.py`

- [ ] **Step 1: Create `app/__init__.py`** (empty)

```bash
mkdir -p app/routes
touch app/__init__.py app/routes/__init__.py
```

- [ ] **Step 2: Create `app/config.py`**

```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_url: str = "redis://localhost:6379"
    enrichment_retry_count: int = 3
    enrichment_step_delay: float = 0.5
    confidence_threshold: float = 0.5
    staleness_threshold_seconds: int = 60

    model_config = {"env_file": ".env"}


settings = Settings()
```

- [ ] **Step 3: Create `app/models.py`**

```python
from datetime import datetime
from pydantic import BaseModel


class PackageScanRequest(BaseModel):
    piece_id: str
    robot_id: str
    camera_id: str
    barcode: str


class ReviewRequest(BaseModel):
    action: str
    route: str
    operator_id: str


class PackageState(BaseModel):
    piece_id: str
    status: str
    barcode: str
    robot_id: str
    camera_id: str
    route: str | None = None
    history: list[str] = []
    enrichment_metadata: dict = {}
    retry_count: int = 0
    created_at: datetime
    updated_at: datetime
    operator_id: str | None = None


class RobotInfo(BaseModel):
    robot_id: str
    last_seen: str
    stale: bool


class CameraInfo(BaseModel):
    camera_id: str
    last_seen: str
    stale: bool


class StatsResponse(BaseModel):
    total: int
    by_status: dict[str, int]
    error_rate: float


class HealthResponse(BaseModel):
    status: str
    redis: str
```

- [ ] **Step 4: Commit**

```bash
git add app/
git commit -m "feat: add config and Pydantic models"
```

- [ ] **Step 5: Mark Task 2 complete in Notion**

`mcp__claude_ai_Notion__notion-update-page` → `page_id: 3671ec9df1ac81b4a4e2cc9c9e6e2d57` → `command: update_content`:
```json
[{"old_str": "- [ ] **Task 2:** Config + Models — app/config.py, app/models.py", "new_str": "- [x] **Task 2:** Config + Models — app/config.py, app/models.py"}]
```

---

## Task 3: Dependencies Module + State Layer

**Files:**
- Create: `app/dependencies.py`
- Create: `app/state.py`
- Create: `tests/__init__.py`
- Create: `tests/test_state.py`

- [ ] **Step 1: Create `app/dependencies.py`**

```python
from redis.asyncio import Redis

_redis: Redis | None = None


def set_redis(redis: Redis | None) -> None:
    global _redis
    _redis = redis


def get_redis() -> Redis:
    return _redis
```

- [ ] **Step 2: Write failing tests in `tests/test_state.py`**

```bash
touch tests/__init__.py tests/test_state.py
```

```python
import json
import pytest
import pytest_asyncio
from datetime import datetime, timezone
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
    r = aioredis.FakeRedis(decode_responses=True)
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
```

- [ ] **Step 3: Run tests — expect failure (module not found)**

```bash
pytest tests/test_state.py -v
```

Expected: `ModuleNotFoundError: No module named 'app.state'`

- [ ] **Step 4: Create `app/state.py`**

```python
import json
from datetime import datetime, timezone

from redis.asyncio import Redis

from app.models import PackageState

_PACKAGE_PREFIX = "package:"
_ROBOTS_KEY = "robots"
_CAMERAS_KEY = "cameras"
_EVENTS_CHANNEL = "package_events"


async def get_package(redis: Redis, piece_id: str) -> PackageState | None:
    data = await redis.hgetall(f"{_PACKAGE_PREFIX}{piece_id}")
    if not data:
        return None
    return _deserialize(data)


async def set_package(redis: Redis, state: PackageState) -> None:
    await redis.hset(f"{_PACKAGE_PREFIX}{state.piece_id}", mapping=_serialize(state))


async def get_all_packages(redis: Redis) -> list[PackageState]:
    keys = await redis.keys(f"{_PACKAGE_PREFIX}*")
    packages = []
    for key in keys:
        data = await redis.hgetall(key)
        if data:
            packages.append(_deserialize(data))
    return packages


async def publish_event(redis: Redis, state: PackageState) -> None:
    await redis.publish(_EVENTS_CHANNEL, state.model_dump_json())


async def update_robot_seen(redis: Redis, robot_id: str) -> None:
    await redis.hset(_ROBOTS_KEY, robot_id, datetime.now(timezone.utc).isoformat())


async def update_camera_seen(redis: Redis, camera_id: str) -> None:
    await redis.hset(_CAMERAS_KEY, camera_id, datetime.now(timezone.utc).isoformat())


async def get_robots(redis: Redis) -> dict[str, str]:
    return await redis.hgetall(_ROBOTS_KEY)


async def get_cameras(redis: Redis) -> dict[str, str]:
    return await redis.hgetall(_CAMERAS_KEY)


def _serialize(state: PackageState) -> dict:
    data = state.model_dump()
    data["history"] = json.dumps(data["history"])
    data["enrichment_metadata"] = json.dumps(data["enrichment_metadata"])
    data["created_at"] = data["created_at"].isoformat()
    data["updated_at"] = data["updated_at"].isoformat()
    return {k: ("" if v is None else str(v)) for k, v in data.items()}


def _deserialize(data: dict) -> PackageState:
    d = {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in data.items()
    }
    d["history"] = json.loads(d["history"])
    d["enrichment_metadata"] = json.loads(d["enrichment_metadata"])
    d["retry_count"] = int(d["retry_count"])
    d["route"] = d["route"] or None
    d["operator_id"] = d["operator_id"] or None
    return PackageState(**d)
```

- [ ] **Step 5: Run tests — expect all pass**

```bash
pytest tests/test_state.py -v
```

Expected: 7 tests pass.

- [ ] **Step 6: Commit**

```bash
git add app/dependencies.py app/state.py tests/
git commit -m "feat: add dependencies module and Redis state layer"
```

- [ ] **Step 7: Mark Task 3 complete in Notion**

`mcp__claude_ai_Notion__notion-update-page` → `page_id: 3671ec9df1ac81b4a4e2cc9c9e6e2d57` → `command: update_content`:
```json
[{"old_str": "- [ ] **Task 3:** Dependencies Module + State Layer — app/dependencies.py, app/state.py, tests/test_state.py", "new_str": "- [x] **Task 3:** Dependencies Module + State Layer — app/dependencies.py, app/state.py, tests/test_state.py"}]
```

---

## Task 4: Router Module

**Files:**
- Create: `app/router.py`

- [ ] **Step 1: Create `app/router.py`**

```python
import random

from app.models import PackageState

_ROUTES = ["BIN-A", "BIN-B", "BIN-C"]


def make_routing_decision(package: PackageState) -> tuple[str, float]:
    route = random.choice(_ROUTES)
    confidence = random.random()
    return route, confidence
```

- [ ] **Step 2: Verify import**

```bash
python -c "from app.router import make_routing_decision; print(make_routing_decision.__doc__)"
```

Expected: no import error.

- [ ] **Step 3: Commit**

```bash
git add app/router.py
git commit -m "feat: add swappable routing module"
```

- [ ] **Step 4: Mark Task 4 complete in Notion**

`mcp__claude_ai_Notion__notion-update-page` → `page_id: 3671ec9df1ac81b4a4e2cc9c9e6e2d57` → `command: update_content`:
```json
[{"old_str": "- [ ] **Task 4:** Router Module — app/router.py", "new_str": "- [x] **Task 4:** Router Module — app/router.py"}]
```

---

## Task 5: WebSocket + Connection Manager

**Files:**
- Create: `app/websocket.py`

- [ ] **Step 1: Create `app/websocket.py`**

```python
import asyncio

from fastapi import WebSocket
from redis.asyncio import Redis

_EVENTS_CHANNEL = "package_events"


class ConnectionManager:
    def __init__(self) -> None:
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str) -> None:
        dead: list[WebSocket] = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                dead.append(connection)
        for conn in dead:
            self.disconnect(conn)


manager = ConnectionManager()


async def pubsub_listener(redis: Redis) -> None:
    pubsub = redis.pubsub()
    await pubsub.subscribe(_EVENTS_CHANNEL)
    async for message in pubsub.listen():
        if message["type"] == "message":
            data = message["data"]
            if isinstance(data, bytes):
                data = data.decode()
            await manager.broadcast(data)
```

- [ ] **Step 2: Verify import**

```bash
python -c "from app.websocket import manager, pubsub_listener; print('ok')"
```

Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add app/websocket.py
git commit -m "feat: add WebSocket connection manager and Redis pub/sub listener"
```

- [ ] **Step 4: Mark Task 5 complete in Notion**

`mcp__claude_ai_Notion__notion-update-page` → `page_id: 3671ec9df1ac81b4a4e2cc9c9e6e2d57` → `command: update_content`:
```json
[{"old_str": "- [ ] **Task 5:** WebSocket + Connection Manager — app/websocket.py", "new_str": "- [x] **Task 5:** WebSocket + Connection Manager — app/websocket.py"}]
```

---

## Task 6: Main App + Lifespan

**Files:**
- Create: `app/main.py`
- Create: `app/routes/health.py`
- Create: `tests/conftest.py`

- [ ] **Step 1: Create `app/routes/health.py`**

```python
from fastapi import APIRouter, Depends
from redis.asyncio import Redis

from app.dependencies import get_redis
from app.models import HealthResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health(redis: Redis = Depends(get_redis)):
    try:
        await redis.ping()
        redis_status = "ok"
    except Exception:
        redis_status = "unavailable"
    return HealthResponse(status="ok", redis=redis_status)
```

- [ ] **Step 2: Create `app/main.py`**

```python
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis

from app.config import settings
from app.dependencies import set_redis, get_redis
from app.websocket import pubsub_listener
from app.routes import events, packages, robots, cameras, health


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    set_redis(redis)
    listener = asyncio.create_task(pubsub_listener(redis))
    yield
    listener.cancel()
    try:
        await listener
    except asyncio.CancelledError:
        pass
    await redis.aclose()
    set_redis(None)


app = FastAPI(title="PrimeVision SSE", lifespan=lifespan)

app.include_router(events.router)
app.include_router(packages.router)
app.include_router(robots.router)
app.include_router(cameras.router)
app.include_router(health.router)
```

- [ ] **Step 3: Create stub route files so main.py imports don't fail**

`app/routes/events.py`:
```python
from fastapi import APIRouter
router = APIRouter()
```

`app/routes/packages.py`:
```python
from fastapi import APIRouter
router = APIRouter(prefix="/packages")
```

`app/routes/robots.py`:
```python
from fastapi import APIRouter
router = APIRouter()
```

`app/routes/cameras.py`:
```python
from fastapi import APIRouter
router = APIRouter()
```

- [ ] **Step 4: Create `tests/conftest.py`**

```python
from contextlib import asynccontextmanager

import fakeredis.aioredis as aioredis
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.dependencies import get_redis, set_redis
from app.main import app


@pytest_asyncio.fixture
async def redis():
    r = aioredis.FakeRedis(decode_responses=True)
    set_redis(r)
    yield r
    set_redis(None)
    await r.aclose()


@pytest_asyncio.fixture
async def client(redis):
    app.dependency_overrides[get_redis] = lambda: redis

    @asynccontextmanager
    async def _noop_lifespan(app):
        yield

    original = app.router.lifespan_context
    app.router.lifespan_context = _noop_lifespan

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac

    app.router.lifespan_context = original
    app.dependency_overrides.clear()
```

- [ ] **Step 5: Verify app starts**

```bash
python -c "from app.main import app; print('app created ok')"
```

Expected: `app created ok`

- [ ] **Step 6: Commit**

```bash
git add app/main.py app/routes/ tests/conftest.py
git commit -m "feat: add FastAPI app with lifespan and test client fixture"
```

- [ ] **Step 7: Mark Task 6 complete in Notion**

`mcp__claude_ai_Notion__notion-update-page` → `page_id: 3671ec9df1ac81b4a4e2cc9c9e6e2d57` → `command: update_content`:
```json
[{"old_str": "- [ ] **Task 6:** Main App + Lifespan — app/main.py, app/routes/health.py, tests/conftest.py", "new_str": "- [x] **Task 6:** Main App + Lifespan — app/main.py, app/routes/health.py, tests/conftest.py"}]
```

---

## Task 7: Core Scan Endpoint + Enrichment Pipeline

**Files:**
- Create: `app/pipeline.py`
- Modify: `app/routes/events.py`
- Create: `tests/test_pipeline.py`

- [ ] **Step 1: Write failing pipeline tests in `tests/test_pipeline.py`**

```python
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
```

- [ ] **Step 2: Run tests — expect failure (module not found)**

```bash
pytest tests/test_pipeline.py -v
```

Expected: `ModuleNotFoundError: No module named 'app.pipeline'`

- [ ] **Step 3: Create `app/pipeline.py`**

```python
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
```

- [ ] **Step 4: Run pipeline tests — expect all pass**

```bash
pytest tests/test_pipeline.py -v
```

Expected: 7 tests pass.

- [ ] **Step 5: Implement `app/routes/events.py`**

```python
import asyncio
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from redis.asyncio import Redis

from app.dependencies import get_redis
from app.models import PackageScanRequest, PackageState
from app.pipeline import run_pipeline
from app import state as state_store

router = APIRouter()


@router.post("/events/package-scan")
async def package_scan(
    payload: PackageScanRequest,
    redis: Redis = Depends(get_redis),
):
    existing = await state_store.get_package(redis, payload.piece_id)
    if existing:
        return existing

    now = datetime.now(timezone.utc)
    pkg = PackageState(
        piece_id=payload.piece_id,
        status="RECEIVED",
        barcode=payload.barcode,
        robot_id=payload.robot_id,
        camera_id=payload.camera_id,
        history=["RECEIVED"],
        created_at=now,
        updated_at=now,
    )
    await state_store.set_package(redis, pkg)
    await state_store.publish_event(redis, pkg)
    await state_store.update_robot_seen(redis, payload.robot_id)
    await state_store.update_camera_seen(redis, payload.camera_id)
    asyncio.create_task(run_pipeline(redis, payload.piece_id))
    return pkg
```

- [ ] **Step 6: Commit**

```bash
git add app/pipeline.py app/routes/events.py tests/test_pipeline.py
git commit -m "feat: add enrichment pipeline and scan endpoint"
```

- [ ] **Step 7: Mark Task 7 complete in Notion**

`mcp__claude_ai_Notion__notion-update-page` → `page_id: 3671ec9df1ac81b4a4e2cc9c9e6e2d57` → `command: update_content`:
```json
[{"old_str": "- [ ] **Task 7:** Core Scan Endpoint + Enrichment Pipeline — app/pipeline.py, app/routes/events.py, tests/test_pipeline.py", "new_str": "- [x] **Task 7:** Core Scan Endpoint + Enrichment Pipeline — app/pipeline.py, app/routes/events.py, tests/test_pipeline.py"}]
```

---

## Task 8: Package Endpoints + Manual Review

**Files:**
- Modify: `app/routes/packages.py`
- Create: `tests/test_api.py` (partial — scan + packages tests)

- [ ] **Step 1: Write failing tests for scan and packages endpoints in `tests/test_api.py`**

```python
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
```

- [ ] **Step 2: Run tests — expect failures (routes return 404)**

```bash
pytest tests/test_api.py -v
```

Expected: most tests fail with 404 or missing routes.

- [ ] **Step 3: Implement `app/routes/packages.py`**

```python
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from redis.asyncio import Redis

from app.config import settings
from app.dependencies import get_redis
from app.models import PackageState, ReviewRequest, StatsResponse
from app.pipeline import resume_pipeline
from app import state as state_store

router = APIRouter(prefix="/packages")


@router.get("/manual-review")
async def get_manual_review(redis: Redis = Depends(get_redis)) -> list[PackageState]:
    packages = await state_store.get_all_packages(redis)
    return [p for p in packages if p.status == "MANUAL_REVIEW"]


@router.get("/stuck")
async def get_stuck(redis: Redis = Depends(get_redis)) -> list[PackageState]:
    packages = await state_store.get_all_packages(redis)
    now = datetime.now(timezone.utc)
    return [
        p for p in packages
        if p.status == "MANUAL_REVIEW"
        and (now - p.updated_at).total_seconds() > settings.staleness_threshold_seconds
    ]


@router.get("/failed")
async def get_failed(redis: Redis = Depends(get_redis)) -> list[PackageState]:
    packages = await state_store.get_all_packages(redis)
    return [p for p in packages if p.status == "FAILED"]


@router.get("/stats", response_model=StatsResponse)
async def get_stats(redis: Redis = Depends(get_redis)) -> StatsResponse:
    packages = await state_store.get_all_packages(redis)
    by_status: dict[str, int] = {}
    for p in packages:
        by_status[p.status] = by_status.get(p.status, 0) + 1
    total = len(packages)
    error_rate = by_status.get("FAILED", 0) / total if total else 0.0
    return StatsResponse(total=total, by_status=by_status, error_rate=error_rate)


@router.get("")
async def get_packages(
    status: str | None = None,
    robot_id: str | None = None,
    camera_id: str | None = None,
    redis: Redis = Depends(get_redis),
) -> list[PackageState]:
    packages = await state_store.get_all_packages(redis)
    if status:
        packages = [p for p in packages if p.status == status]
    if robot_id:
        packages = [p for p in packages if p.robot_id == robot_id]
    if camera_id:
        packages = [p for p in packages if p.camera_id == camera_id]
    return packages


@router.get("/{piece_id}", response_model=PackageState)
async def get_package(piece_id: str, redis: Redis = Depends(get_redis)) -> PackageState:
    pkg = await state_store.get_package(redis, piece_id)
    if not pkg:
        raise HTTPException(status_code=404, detail="Package not found")
    return pkg


@router.post("/{piece_id}/review")
async def review_package(
    piece_id: str,
    review: ReviewRequest,
    redis: Redis = Depends(get_redis),
) -> dict:
    pkg = await state_store.get_package(redis, piece_id)
    if not pkg:
        raise HTTPException(status_code=404, detail="Package not found")
    if pkg.status != "MANUAL_REVIEW":
        raise HTTPException(
            status_code=409,
            detail=f"Package is not in MANUAL_REVIEW state (current: {pkg.status})",
        )
    ok = resume_pipeline(piece_id, review.route, review.operator_id)
    if not ok:
        raise HTTPException(status_code=409, detail="No active review event for this package")
    return {"message": "Review submitted, pipeline resuming"}
```

- [ ] **Step 4: Run all tests — expect all pass**

```bash
pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add app/routes/packages.py tests/test_api.py
git commit -m "feat: add package endpoints and manual review flow"
```

- [ ] **Step 6: Mark Task 8 complete in Notion**

`mcp__claude_ai_Notion__notion-update-page` → `page_id: 3671ec9df1ac81b4a4e2cc9c9e6e2d57` → `command: update_content`:
```json
[{"old_str": "- [ ] **Task 8:** Package Endpoints + Manual Review — app/routes/packages.py, tests/test_api.py", "new_str": "- [x] **Task 8:** Package Endpoints + Manual Review — app/routes/packages.py, tests/test_api.py"}]
```

---

## Task 9: Observability Endpoints

**Files:**
- Modify: `app/routes/robots.py`
- Modify: `app/routes/cameras.py`

- [ ] **Step 1: Implement `app/routes/robots.py`**

```python
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from redis.asyncio import Redis

from app.config import settings
from app.dependencies import get_redis
from app.models import RobotInfo
from app import state as state_store

router = APIRouter()


@router.get("/robots", response_model=list[RobotInfo])
async def get_robots(redis: Redis = Depends(get_redis)) -> list[RobotInfo]:
    data = await state_store.get_robots(redis)
    now = datetime.now(timezone.utc)
    result = []
    for robot_id, last_seen_str in data.items():
        last_seen = datetime.fromisoformat(last_seen_str)
        if last_seen.tzinfo is None:
            last_seen = last_seen.replace(tzinfo=timezone.utc)
        stale = (now - last_seen).total_seconds() > settings.staleness_threshold_seconds
        result.append(RobotInfo(robot_id=robot_id, last_seen=last_seen_str, stale=stale))
    return result
```

- [ ] **Step 2: Implement `app/routes/cameras.py`**

```python
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from redis.asyncio import Redis

from app.config import settings
from app.dependencies import get_redis
from app.models import CameraInfo
from app import state as state_store

router = APIRouter()


@router.get("/cameras", response_model=list[CameraInfo])
async def get_cameras(redis: Redis = Depends(get_redis)) -> list[CameraInfo]:
    data = await state_store.get_cameras(redis)
    now = datetime.now(timezone.utc)
    result = []
    for camera_id, last_seen_str in data.items():
        last_seen = datetime.fromisoformat(last_seen_str)
        if last_seen.tzinfo is None:
            last_seen = last_seen.replace(tzinfo=timezone.utc)
        stale = (now - last_seen).total_seconds() > settings.staleness_threshold_seconds
        result.append(CameraInfo(camera_id=camera_id, last_seen=last_seen_str, stale=stale))
    return result
```

- [ ] **Step 3: Run all tests**

```bash
pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add app/routes/robots.py app/routes/cameras.py
git commit -m "feat: add robots and cameras observability endpoints"
```

- [ ] **Step 5: Mark Task 9 complete in Notion**

`mcp__claude_ai_Notion__notion-update-page` → `page_id: 3671ec9df1ac81b4a4e2cc9c9e6e2d57` → `command: update_content`:
```json
[{"old_str": "- [ ] **Task 9:** Observability Endpoints — app/routes/robots.py, app/routes/cameras.py", "new_str": "- [x] **Task 9:** Observability Endpoints — app/routes/robots.py, app/routes/cameras.py"}]
```

---

## Task 10: WebSocket Endpoint

**Files:**
- Modify: `app/main.py`

- [ ] **Step 1: Add WebSocket route to `app/main.py`**

Add the following after the existing imports and before `lifespan`:

```python
from fastapi import WebSocket, WebSocketDisconnect
from app.websocket import manager
```

Add this route after `app.include_router(health.router)`:

```python
@app.websocket("/ws/events")
async def websocket_events(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
```

Full updated `app/main.py`:

```python
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from redis.asyncio import Redis

from app.config import settings
from app.dependencies import set_redis, get_redis
from app.websocket import manager, pubsub_listener
from app.routes import events, packages, robots, cameras, health


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    set_redis(redis)
    listener = asyncio.create_task(pubsub_listener(redis))
    yield
    listener.cancel()
    try:
        await listener
    except asyncio.CancelledError:
        pass
    await redis.aclose()
    set_redis(None)


app = FastAPI(title="PrimeVision SSE", lifespan=lifespan)

app.include_router(events.router)
app.include_router(packages.router)
app.include_router(robots.router)
app.include_router(cameras.router)
app.include_router(health.router)


@app.websocket("/ws/events")
async def websocket_events(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
```

- [ ] **Step 2: Run all tests**

```bash
pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add app/main.py
git commit -m "feat: add WebSocket /ws/events endpoint"
```

- [ ] **Step 4: Mark Task 10 complete in Notion**

`mcp__claude_ai_Notion__notion-update-page` → `page_id: 3671ec9df1ac81b4a4e2cc9c9e6e2d57` → `command: update_content`:
```json
[{"old_str": "- [ ] **Task 10:** WebSocket Endpoint — WS /ws/events wired into app/main.py", "new_str": "- [x] **Task 10:** WebSocket Endpoint — WS /ws/events wired into app/main.py"}]
```

---

## Task 11: README

**Files:**
- Create: `README.md`

- [ ] **Step 1: Create `README.md`**

```markdown
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
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add README with required architecture sections"
```

- [ ] **Step 3: Mark Task 11 complete in Notion**

`mcp__claude_ai_Notion__notion-update-page` → `page_id: 3671ec9df1ac81b4a4e2cc9c9e6e2d57` → `command: update_content`:
```json
[{"old_str": "- [ ] **Task 11:** README — five required sections from assignment brief", "new_str": "- [x] **Task 11:** README — five required sections from assignment brief"}]
```

---

## Task 12: Full Test Run + Docker Smoke Test

- [ ] **Step 1: Run full test suite**

```bash
pytest tests/ -v --tb=short
```

Expected: all tests pass, no warnings about unclosed resources.

- [ ] **Step 2: Build and start Docker Compose**

```bash
docker compose up --build -d
```

Expected: both services start, app logs show `Application startup complete`.

- [ ] **Step 3: Smoke test the API**

```bash
# Submit a scan
curl -s -X POST http://localhost:8000/events/package-scan \
  -H "Content-Type: application/json" \
  -d '{"piece_id":"PKG-001","robot_id":"R-12","camera_id":"CAM-04","barcode":"92055901755477000000000001"}' | python3 -m json.tool

# Wait for pipeline to complete
sleep 3

# Check final state
curl -s http://localhost:8000/packages/PKG-001 | python3 -m json.tool

# Check health
curl -s http://localhost:8000/health | python3 -m json.tool
```

Expected: `PKG-001` reaches `FINALIZED` with full history, health returns `{"status": "ok", "redis": "ok"}`.

- [ ] **Step 4: Shut down**

```bash
docker compose down
```

- [ ] **Step 5: Final commit**

```bash
git add .
git commit -m "chore: verify full test suite and Docker smoke test pass"
```

- [ ] **Step 6: Mark Task 12 complete in Notion**

`mcp__claude_ai_Notion__notion-update-page` → `page_id: 3671ec9df1ac81b4a4e2cc9c9e6e2d57` → `command: update_content`:
```json
[{"old_str": "- [ ] **Task 12:** Full Test Run + Docker Smoke Test — all tests pass, docker compose up --build verified", "new_str": "- [x] **Task 12:** Full Test Run + Docker Smoke Test — all tests pass, docker compose up --build verified"}]
```
