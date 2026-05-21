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
