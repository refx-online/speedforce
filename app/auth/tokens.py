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
    """Check a password, accepting either a plaintext or an already-hashed form.

    Two clients log in to the same table with different credentials, and this is
    the only place that reconciles them:

    * **lazer** sends the **plaintext** password, so ``md5`` it to match the
      stored form.
    * **stable** never holds the plaintext. ``Options_Login.cs:84`` stores
      ``CryptoHelper.GetMd5String(password)`` and every bancho call sends that
      digest as ``h=``, because bancho's schema *is* the md5. So the value
      arriving here is already md5'd, and md5-ing it again computes
      ``bcrypt(md5(md5(pw)))``, which never matches.

    The md5-hex form is tried second rather than first: the canonical path stays
    one bcrypt check, so lazer (the majority) pays nothing, and only stable pays
    for the extra comparison.

    Trying both is preferred over sniffing "looks like 32 hex chars", which would
    misfire on a genuinely hex-looking plaintext password, and over a form flag,
    which would add a wire parameter both clients have to know about. Nothing is
    weakened: both branches test the same secret, since stable already puts the
    md5 hex into every bancho request in cleartext.
    """
    if not stored:
        return False

    try:
        if bcrypt.checkpw(hashlib.md5(plaintext.encode()).hexdigest().encode(), stored.encode()):  # noqa: S324
            return True

        return bcrypt.checkpw(plaintext.encode(), stored.encode())
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
