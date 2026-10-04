from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.route import router as route_router

# importing the hub module is what registers /signalr/metadata; the import sits
# below the host import it depends on
from app.signalr import metadata as _metadata_hub  # noqa: F401
from app.signalr.host import router as signalr_router
from app.signalr.metadata import start_stable_presence
from app.signalr.metadata import stop_stable_presence
from app.state.services import dispose


@asynccontextmanager
async def lifespan(app: FastAPI):
    # subscribe to bakenohana's presence channel so stable users show up for lazer
    await start_stable_presence()
    try:
        yield
    finally:
        await stop_stable_presence()
        await dispose()


asgi_app = FastAPI(
    title="speedforce",
    lifespan=lifespan,
)

asgi_app.include_router(route_router)
asgi_app.include_router(signalr_router)
