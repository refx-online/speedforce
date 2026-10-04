from __future__ import annotations

from fastapi import APIRouter
from fastapi import Form
from fastapi.responses import JSONResponse

import app.settings as settings
from app.auth.tokens import decode_token
from app.auth.tokens import mint_access_token
from app.auth.tokens import mint_refresh_token
from app.auth.tokens import verify_password
from app.models.repository import find_user_by_name
from app.models.repository import get_password_hash

router = APIRouter(tags=["OAuth"])


def _error(error: str, description: str, status: int = 400) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        # lazer surfaces `hint` verbatim on the login failure screen
        # (OAuth.UserDisplayableError), so keep it human-readable.
        content={"error": error, "hint": description, "message": description},
    )


def _token_response(user_id: int, username: str) -> JSONResponse:
    access, access_ttl = mint_access_token(user_id, username)
    refresh, _ = mint_refresh_token(user_id, username)
    return JSONResponse(
        content={
            "access_token": access,
            "token_type": "bearer",
            # MUST be a JSON number of seconds. lazer computes ExpiresIn from it
            # and refuses anything with <=30s left (OAuthToken.IsValid).
            "expires_in": access_ttl,
            # also effectively mandatory: the client persists
            # `access_token|expiry|refresh_token` and loses the session on expiry
            # if this is empty.
            "refresh_token": refresh,
        }
    )


# NOTE on content type: the client builds this with WebRequest.AddParameter, and
# osu-framework serialises ANY request carrying form parameters as
# multipart/form-data (WebRequest.cs, the MultipartFormDataContent branch) -- not
# application/x-www-form-urlencoded. FastAPI's Form() parses multipart, but only
# when python-multipart is installed; without it every field reads as empty and
# this endpoint returns invalid_grant forever.
@router.post("/oauth/token")
async def oauth_token(
    grant_type: str = Form(...),
    client_id: str = Form(...),
    client_secret: str = Form(...),
    scope: str = Form("*"),
    username: str | None = Form(None),
    password: str | None = Form(None),
    refresh_token: str | None = Form(None),
) -> JSONResponse:
    # Client credentials arrive in the form body only -- the client sends no
    # Basic auth header on this request.
    if client_id != settings.OAUTH_CLIENT_ID or client_secret != settings.OAUTH_CLIENT_SECRET:
        return _error("invalid_client", "Client authentication failed.", 401)

    if grant_type == "password":
        if not username or password is None:
            return _error("invalid_request", "username and password are required.")

        user = await find_user_by_name(username)
        stored_hash = await get_password_hash(user.id) if user else None
        # Identical response for unknown user and wrong password, so this can't
        # be used to enumerate accounts.
        if not user or not stored_hash or not verify_password(password, stored_hash):
            return _error("invalid_grant", "Invalid username or password.", 401)

        return _token_response(user.id, user.name)

    if grant_type == "refresh_token":
        if not refresh_token:
            return _error("invalid_request", "refresh_token is required.")

        claims = decode_token(refresh_token)
        # NOTE: lazer treats a failed refresh as fatal -- OAuth.cs nulls the whole
        # token on any non-network error and the user is logged out. So only 401
        # when the refresh token is genuinely unusable, never for transient faults.
        if not claims or claims.get("typ") != "refresh":
            return _error("invalid_grant", "The refresh token is invalid or expired.", 401)

        user = await find_user_by_name(str(claims.get("username") or ""))
        if not user or user.id != int(claims["osu_user_id"]):
            return _error("invalid_grant", "The refresh token is invalid or expired.", 401)

        return _token_response(user.id, user.name)

    return _error("unsupported_grant_type", f"Unsupported grant_type: {grant_type}")
