from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


def read_bool(value: str) -> bool:
    return value.lower() in ("true", "1")


def getenv(name: str, default: str | None = None) -> str:
    # NOTE: plain os.environ[name] dies with a bare KeyError at import time,
    # which tells you nothing. fail fast with the actual var name instead.
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"missing required env var: {name}")
    return value


DEBUG = read_bool(getenv("DEBUG", "false"))
HOST = getenv("HOST", "0.0.0.0")
PORT = int(getenv("PORT", "9090"))

MYSQL_HOST = getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(getenv("MYSQL_PORT", "3307"))
MYSQL_USER = getenv("MYSQL_USER", "bancho")
MYSQL_PASSWORD = getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = getenv("MYSQL_DATABASE", "bancho")

REDIS_URL = getenv("REDIS_URL", "redis://localhost:6379")

# forlorn owns the score WRITE path (usecases::score). speedforce validates the
# lazer submission and hands off, so pb-rejection/placement/xp/pp/leaderboard
# logic stays in one place for both clients.
# same env var name forlorn reads (LAZER_INTERNAL_TOKEN) so the two sides can
# never drift out of sync
FORLORN_URL = getenv("FORLORN_URL", "http://localhost:3030")

# owns .osz generation; speedforce proxies downloads rather than reading the
# shared volume, whose path differs between the host and containers.
BEATMAP_SERVICE_URL = getenv("BEATMAP_SERVICE_URL", "http://localhost:3700")
LAZER_INTERNAL_TOKEN = getenv("LAZER_INTERNAL_TOKEN", "")

# where beatmap-service caches full .osz archives (shared union_data volume)
OSZ_PATH = getenv("OSZ_PATH", "/srv/root/.data/osz")

# HS256 secret for the tokens we mint. The client treats the access token as
# opaque, so the algorithm is entirely our choice -- it only has to match
# whatever ends up validating it (our own realtime server). If we ever adopt
# ppy's osu-server-spectator instead, that validates RS256 against a public
# key and this needs revisiting.
JWT_SECRET_KEY = getenv("JWT_SECRET_KEY", "")
JWT_ALGORITHM = getenv("JWT_ALGORITHM", "HS256")

# seconds. the client refuses any token with <= 30s left (OAuthToken.IsValid),
# so don't set this anywhere near that.
ACCESS_TOKEN_TTL = int(getenv("ACCESS_TOKEN_TTL", "3600"))
REFRESH_TOKEN_TTL = int(getenv("REFRESH_TOKEN_TTL", "2592000"))

# must match RefxEndpointConfiguration.APIClientID in refx-lazer. also used as
# the `aud` claim, so a single value covers both.
OAUTH_CLIENT_ID = getenv("OAUTH_CLIENT_ID", "5")
OAUTH_CLIENT_SECRET = getenv("OAUTH_CLIENT_SECRET", "")

# lazer is limited to the four base rulesets -- no custom rulesets. It still
# plays RX/AP, but those arrive as mods and are mapped to dense mode ids
# (4-7) by app/models/modes.py, exactly as forlorn does. So "vanilla only"
# constrains the ruleset, not the mode id.
LAZER_RULESETS = (0, 1, 2, 3)
