from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.route import router as route_router

# importing the hub module is what registers /signalr/metadata; the import sits
# below the host import it depends on
from app.signalr import metadata as _metadata_hub  # noqa: F401
from app.signalr.host import router as signalr_router
from app.state.services import dispose


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await dispose()


asgi_app = FastAPI(
    title="speedforce",
    lifespan=lifespan,
)

asgi_app.include_router(route_router)
asgi_app.include_router(signalr_router)
