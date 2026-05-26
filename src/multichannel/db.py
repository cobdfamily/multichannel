"""SQLAlchemy async engine + session factory."""

from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy.pool import StaticPool
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from multichannel.config import Settings


class Database:
    def __init__(self, settings: Settings) -> None:
        url = str(settings.DATABASE_URL)
        kwargs = {"echo": False, "pool_pre_ping": True}
        if url.startswith("sqlite"):
            kwargs = {
                "echo": False,
                "connect_args": {"check_same_thread": False},
                "poolclass": StaticPool,
            }
        else:
            kwargs["pool_size"] = settings.DATABASE_POOL_MAX
        self.engine: AsyncEngine = create_async_engine(
            url,
            **kwargs,
        )
        self.session_maker: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
            autoflush=False,
        )

    async def dispose(self) -> None:
        await self.engine.dispose()

    async def close(self) -> None:
        await self.dispose()


async def session_dependency(database: Database) -> AsyncIterator[AsyncSession]:
    async with database.session_maker() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
