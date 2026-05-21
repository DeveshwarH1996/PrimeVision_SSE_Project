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
