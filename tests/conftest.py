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
