from __future__ import annotations

import hashlib
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import bcrypt
import jwt

import app.settings as settings


# Bancho stores bcrypt(md5_hex(plaintext)) -- cost 10. dorchadas
# src/lib/user.ts is the reference implementation; bakenohana verifies the same
# way. Hashing the md5 first is what lets a 32-char hex digest satisfy bcrypt's
# 72-byte input limit without the original password ever being stored.
def hash_password(plaintext: str) -> str:
    digest = hashlib.md5(plaintext.encode()).hexdigest()  # noqa: S324 - required by the bancho schema
    return bcrypt.hashpw(digest.encode(), bcrypt.gensalt(10)).decode()


def verify_password(plaintext: str, stored: str) -> bool:
    if not stored:
        return False
    digest = hashlib.md5(plaintext.encode()).hexdigest()  # noqa: S324 - required by the bancho schema
    try:
        return bcrypt.checkpw(digest.encode(), stored.encode())
    except ValueError:
        # malformed hash in the row; treat as a failed login rather than a 500
        return False


def mint_access_token(user_id: int, username: str) -> tuple[str, int]:
    """returns (token, expires_in_seconds).

    The client stores `access_token|expiry|refresh_token` and refuses any token
    with 30s or less remaining, so TTL is comfortably long by default.
    """
    now = datetime.now(UTC)
    expires = now + timedelta(seconds=settings.ACCESS_TOKEN_TTL)

    token = jwt.encode(
        {
            "aud": settings.OAUTH_CLIENT_ID,
            "sub": str(user_id),
            "osu_user_id": user_id,
            "username": username,
            "scopes": ["*"],
            "typ": "access",
            "iat": int(now.timestamp()),
            "nbf": int(now.timestamp()),
            "exp": int(expires.timestamp()),
            # jti is what the realtime server looks up for revocation.
            "jti": f"{user_id}.{int(now.timestamp())}",
        },
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
    )
    return token, settings.ACCESS_TOKEN_TTL


def mint_refresh_token(user_id: int, username: str) -> tuple[str, int]:
    now = datetime.now(UTC)
    expires = now + timedelta(seconds=settings.REFRESH_TOKEN_TTL)
    token = jwt.encode(
        {
            "aud": settings.OAUTH_CLIENT_ID,
            "sub": str(user_id),
            "osu_user_id": user_id,
            "username": username,
            "scopes": ["*"],
            # marks it so a refresh token can never be replayed as an access token
            "typ": "refresh",
            "iat": int(now.timestamp()),
            "nbf": int(now.timestamp()),
            "exp": int(expires.timestamp()),
        },
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
    )
    return token, settings.REFRESH_TOKEN_TTL


def decode_token(token: str) -> dict | None:
    """returns claims, or None if the token is invalid/expired."""
    try:
        return jwt.decode(
            token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM],
            options={"verify_aud": False},
        )
    except jwt.PyJWTError:
        return None
