from __future__ import annotations

from fastapi import APIRouter

from app.route.beatmaps import router as beatmaps_router
from app.route.comments import router as comments_router
from app.route.me import router as me_router
from app.route.oauth import router as oauth_router
from app.route.replays import router as replays_router
from app.route.scores import router as scores_router
from app.route.social import router as social_router

router = APIRouter()

router.include_router(oauth_router)
router.include_router(me_router)
router.include_router(beatmaps_router)
router.include_router(comments_router)
router.include_router(scores_router)
router.include_router(replays_router)
router.include_router(social_router)
