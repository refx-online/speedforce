from __future__ import annotations

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

import app.settings as settings

# aiomysql driver. pool_pre_ping guards against MySQL dropping idle connections,
# which it does by default after wait_timeout.
engine: AsyncEngine = create_async_engine(
    f"mysql+aiomysql://{settings.MYSQL_USER}:{settings.MYSQL_PASSWORD}"
    f"@{settings.MYSQL_HOST}:{settings.MYSQL_PORT}/{settings.MYSQL_DATABASE}",
    pool_pre_ping=True,
    pool_recycle=280,
)

session_factory = async_sessionmaker(engine, expire_on_commit=False)

redis: Redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)


async def dispose() -> None:
    await engine.dispose()
    await redis.aclose()
