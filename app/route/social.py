"""The endpoints lazer calls on the way to ``APIState.Online`` that are not scores,
beatmaps or replays.

None of these are implemented in `me.py`/`beatmaps.py`/`scores.py`. Each one was
a `404` in the live client's log, and each one is a request the login path makes
unconditionally, so they all had to exist before login could finish:

| endpoint | client request | response DTO |
|---|---|---|
| `/notifications` | `GetNotificationsRequest` | `APINotificationsBundle` |
| `/friends` | `GetFriendsRequest` | `List<APIRelation>` |
| `/blocks` | `GetBlocksRequest` | `List<APIRelation>` |
| `/me/beatmapset-favourites` | `GetMyFavouriteBeatmapSetsRequest` | `GetMyFavouriteBeatmapSetsResponse` |
| `/chat/ack` | `ChatAckRequest` | `ChatAckResponse` |
| `/chat/updates` | `GetUpdatesRequest` | `GetUpdatesResponse` |
| `/users/{id}/{ruleset}` | `GetUserRequest` | `APIUser` |
| `/seasonal-backgrounds` | `GetSeasonalBackgroundsRequest` | `APISeasonalBackgrounds` |

**Why empty-but-valid beats 404, every time.** `APINotificationBundle` is the
worst case: `WebSocketNotificationsClientConnector.BuildConnectionAsync` does
`req.Response!.Endpoint`, so a 404 leaves `Response` null and the connector dies
with a `NullReferenceException` on *every* retry, forever. A `[]` keeps the
socket connector alive.

The relation/favourite/silence lists are empty because bancho has no friends or
silences model at all -- there is nowhere to read them from yet. Returning an
empty list is honest; inventing rows would not be.

`/users/{id}/{ruleset}` is the exception and is fully served: the online-user
list and the score-screen player panel call it for every visible user, so a 404
there leaves the panels blank.
"""

from __future__ import annotations

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from app.models.api_user import APIUser
from app.models.api_user import GlobalRank
from app.models.api_user import Grades
from app.models.api_user import LevelInfo
from app.models.api_user import UserStatistics
from app.models.repository import UserRow
from app.models.repository import find_user_by_id
from app.models.repository import find_user_by_name
from app.models.repository import get_stats
from app.route.me import RULESET_SHORT_NAMES
from app.route.me import require_user
from app.state.mods import resolve_mode

router = APIRouter(prefix="/api/v2", tags=["osu! API v2"])

# Same 50-id cap the client uses for its `ids[]` batch requests.
MAX_BATCH_IDS = 50

SHORT_NAME_TO_MODE = {name: mode for mode, name in RULESET_SHORT_NAMES.items()}


class APIRelation(BaseModel):
    """`APIRelation` -- `target_user` is null when the target no longer exists."""

    target_id: int
    relation_type: int = 0
    mutual: bool = False
    target_user: APIUser | None = None


class FavouriteBeatmapSets(BaseModel):
    beatmapset_ids: list[int] = Field(default_factory=list)


class ChatSilence(BaseModel):
    id: int
    user_id: int


class ChatAckResponse(BaseModel):
    silences: list[ChatSilence] = Field(default_factory=list)


class SeasonalBackground(BaseModel):
    url: str


class SeasonalBackgrounds(BaseModel):
    ends_at: datetime
    backgrounds: list[SeasonalBackground] = Field(default_factory=list)


class NotificationsBundle(BaseModel):
    """`APINotificationsBundle`.

    `notification_endpoint` is the URL the notifications client opens as a **raw**
    websocket -- not a SignalR hub. It authenticates with an `Authorization`
    header, which a browser websocket cannot set and this client can, so it is
    the one connection where the header is the only option.
    """

    has_more: bool = False
    notifications: list[dict] = Field(default_factory=list)
    notification_endpoint: str = ""


class ChatChannel(BaseModel):
    """`Channel`, as `ListChannelsRequest` binds it.

    Defined so the shape is documented even though the list is always empty: the
    snake_case names are the contract, and there is no `Channel` model to inherit
    them from here. Unused fields are omitted rather than sent as null, since
    `Channel` has a `JsonConstructor` and treats missing and null differently.
    """

    channel_id: int
    name: str
    # `Channel.Type` is a `ChannelType` enum with no StringEnumConverter and no
    # global converter configured, so Newtonsoft expects the **ordinal**, not a
    # name. `str` here type-checks fine against our own models and only fails
    # when a real channel is ever returned -- returning `[]` hides it.
    type: int
    description: str = ""
    users: list[int] = Field(default_factory=list)


class ChatUpdates(BaseModel):
    """`GetUpdatesResponse`.

    Aliased to the client's PascalCase field names. Newtonsoft matches property
    names case-insensitively so snake_case would in fact bind, but the wire
    format is what other clients read, so it is reproduced exactly.

    `Presence` and `Messages` are empty because bancho's chat is a separate
    service we do not read from. Empty is honest here -- unlike most of this
    file, a chat 404 does not wedge login, it just leaves chat silent.
    """

    presence: list[dict] = Field(default_factory=list, alias="Presence")
    messages: list[dict] = Field(default_factory=list, alias="Messages")

    model_config = ConfigDict(populate_by_name=True)


def _statistics(pp: float, accuracy: float, mode: int, stats: dict) -> UserStatistics:
    return UserStatistics(
        level=LevelInfo(current=int(pp**0.5 / 1.25) + 1 if pp > 0 else 0, progress=0),
        is_ranked=pp > 0,
        pp=pp,
        ranked_score=int(stats.get("rscore") or 0),
        hit_accuracy=accuracy,
        play_count=int(stats.get("plays") or 0),
        play_time=int(stats.get("playtime") or 0),
        total_score=int(stats.get("tscore") or 0),
        total_hits=int(stats.get("total_hits") or 0),
        maximum_combo=int(stats.get("max_combo") or 0),
        replays_watched_by_others=int(stats.get("replay_views") or 0),
        grade_counts=Grades(
            ssh=int(stats.get("xh_count") or 0),
            ss=int(stats.get("x_count") or 0),
            sh=int(stats.get("sh_count") or 0),
            s=int(stats.get("s_count") or 0),
            a=int(stats.get("a_count") or 0),
        ),
        variants=[],
        rank_history={},
    )


def _iso(timestamp: int) -> str | None:
    if not timestamp:
        return None
    return datetime.fromtimestamp(timestamp, tz=UTC).isoformat()


async def _public_user(user: UserRow, mode: int) -> APIUser:
    stats = await get_stats(user.id, mode)
    pp = float(stats.get("pp") or 0)

    return APIUser(
        id=user.id,
        username=user.name,
        country_code=user.country or "xx",
        avatar_url=f"https://a.041095.xyz/{user.id}",
        # see me.py: covers are not served anywhere we control yet
        cover_url="",
        playmode=mode,
        is_online=True,
        join_date=_iso(user.creation_time),
        last_visit=_iso(user.latest_activity),
        statistics=_statistics(pp, float(stats.get("acc") or 0.0), mode, stats),
        global_rank=GlobalRank(rank=None, ruleset_id=mode),
    )


@router.get("/notifications")
async def get_notifications(_: UserRow = Depends(require_user)) -> NotificationsBundle:
    # Re-hosting the connection point at our own public host: the client opens it
    # verbatim, so it has to be a URL reachable from the player's machine.
    return NotificationsBundle(notification_endpoint="wss://api.041095.xyz/signalr/notifications")


@router.get("/search")
async def search(
    mode: str = Query("user"),
    query: str = Query(""),
    _: UserRow | None = Depends(require_user),
) -> JSONResponse:
    """`SearchUsersRequest` -- `GET /api/v2/search?mode=user&query=...`.

    The response is **nested**, which is easy to get wrong:

        {"total": N, "user": {"data": [...]}}

    not a flat `{"total": N, "users": [...]}`. `SearchUsersResponse.Users` is a
    computed property reading `data.Users`, and `data` is the object bound to
    `user`. A flat shape binds nothing and the list stays empty with no error.

    Always empty: bancho has no user-search index to query. An empty `data` list
    is the honest answer, and unlike `/notifications` this 404 does not wedge
    login -- but it does re-poll, so existing is much quieter.
    """
    return JSONResponse(content={"total": 0, "user": {"data": []}})


@router.get("/chat/channels")
async def list_channels(_: UserRow = Depends(require_user)) -> list[ChatChannel]:
    """Empty channel list.

    `ListChannelsRequest` binds `Channel`, which wants `channel_id`, `name`,
    `type` and the rest as literal snake_case. Returning `[]` rather than 404
    matters more here than it looks: the client re-polls this repeatedly while
    the lounge is open, so a 404 is a permanent retry loop in the log.
    """
    return []


@router.get("/chat/channels/{channel_id}")
async def get_channel(channel_id: int, _: UserRow = Depends(require_user)) -> ChatChannel:
    raise HTTPException(status_code=404, detail="no such channel")


@router.get("/chat/channels/{channel_id}/users/{user_id}")
async def leave_channel(channel_id: int, user_id: int, _: UserRow = Depends(require_user)) -> JSONResponse:
    return JSONResponse(content={})


@router.post("/chat/channels/{channel_id}/users/{user_id}")
async def join_channel(channel_id: int, user_id: int, _: UserRow = Depends(require_user)) -> JSONResponse:
    return JSONResponse(content={})


@router.post("/chat/updates")
async def ack_chat_updates(_: UserRow = Depends(require_user)) -> JSONResponse:
    return JSONResponse(content={})


@router.get("/chat/updates")
async def get_chat_updates(_: UserRow = Depends(require_user)) -> ChatUpdates:
    # The client polls this with `includes[]=presence` on its way to online, so
    # it has to exist even though there is no chat backend to read from.
    return ChatUpdates()


@router.get("/friends")
async def get_friends(_: UserRow = Depends(require_user)) -> list[APIRelation]:
    return []


@router.get("/blocks")
async def get_blocks(_: UserRow = Depends(require_user)) -> list[APIRelation]:
    return []


@router.get("/me/beatmapset-favourites")
async def get_favourites(_: UserRow = Depends(require_user)) -> FavouriteBeatmapSets:
    return FavouriteBeatmapSets()


@router.post("/chat/ack")
async def chat_ack(_: UserRow = Depends(require_user)) -> ChatAckResponse:
    return ChatAckResponse()


@router.get("/seasonal-backgrounds")
async def get_seasonal_backgrounds() -> SeasonalBackgrounds:
    # No seasonal content configured. `ends_at` must be a real timestamp or the
    # client parses a DateTimeOffset from null; the value itself is unused
    # because the list is empty.
    return SeasonalBackgrounds(ends_at=datetime.now(tz=UTC) + timedelta(days=1), backgrounds=[])


async def _users_by_ids(ids: list[int], ruleset_id: int | None) -> dict[str, Any]:
    """Shared body of the two batch user routes.

    Both `GetUsersRequest` (`users/?ids[]=`) and `LookupUsersRequest`
    (`users/lookup/?ids[]=`) bind `GetUsersResponse`, which is
    `{"users": [...], "cursor": ...}` -- a bare array is not accepted.
    """
    batch = ids[:MAX_BATCH_IDS]

    users = []
    for user_id in batch:
        row = await find_user_by_id(user_id)
        if row is None:
            continue

        # An explicit `ruleset_id` is the caller's choice and wins; otherwise fall
        # back to the user's effective mode, which is how the single-user route
        # resolves stat views (and honours stable's relax/autopilot preferences).
        # lazer sends `ruleset_id` from `RealtimeUserList` purely to get
        # `global_rank` back for sorting.
        mode = int(ruleset_id) if ruleset_id is not None else resolve_mode(user_id, row.preferred_mode)
        users.append((await _public_user(row, mode)).model_dump())

    return {"users": users, "cursor": None}


# Declared before `/users/{lookup}/{ruleset}` so the literal `lookup` segment is
# never captured as a username.
@router.get("/users/lookup/")
async def lookup_users(
    ids: list[int] = Query(default_factory=list, alias="ids[]"),
    ruleset_id: int | None = Query(None),
) -> JSONResponse:
    """`LookupUsersRequest` -- `users/lookup/?ids[]=`.

    This is what feeds lazer's online-users list. `RealtimeUserList` collects the
    ids from the presence stream and resolves them in batches of 50 through this
    route before creating a panel per user.

    The route did not exist, so every request 404'd. That failed *silently* on the
    client: `OnlineLookupCache` resolves a missing batch to `null`, `RealtimeUserList`
    then does `if (user == null) continue;`, and the panel list simply stays empty
    with no toast, no error dialog and no failed-request log. Presence pushes were
    arriving correctly the whole time and being thrown away one layer up.
    """
    return JSONResponse(content=await _users_by_ids(ids, ruleset_id))


@router.get("/users/")
async def batch_get_users(
    ids: list[int] = Query(default_factory=list, alias="ids[]"),
    ruleset_id: int | None = Query(None),
) -> JSONResponse:
    """`GetUsersRequest` -- `users/?ids[]=`.

    Both routes have a literal trailing slash, so the single-user
    `/users/{lookup}/{ruleset}` route cannot match them. Public rather than
    authenticated, matching the single-user route, since a rendered user list is
    not private.
    """
    return JSONResponse(content=await _users_by_ids(ids, ruleset_id))


@router.get("/users/{lookup}/{ruleset}")
async def get_user(lookup: str, ruleset: str, key: str = Query("id")) -> APIUser:
    """`GetUserRequest` -- `/users/{Lookup}/{ruleset}?key={key}`.

    The first path segment is the *lookup*, not necessarily an id: lazer sends
    `key=id` with a numeric segment or `key=username` with a name, and the real
    request is `/users/3/osu?key=id`. The second segment is the ruleset short
    name. An unrecognised `key` is rejected rather than answered as an id
    lookup -- silently treating it as one would return the wrong user.
    """
    if key == "id":
        if not lookup.lstrip("-").isdigit():
            raise HTTPException(status_code=400, detail="expected a numeric id when key=id")
        user = await find_user_by_id(int(lookup))
    elif key == "username":
        user = await find_user_by_name(lookup)
    else:
        raise HTTPException(status_code=400, detail=f"unsupported lookup key: {key}")

    if not user:
        raise HTTPException(status_code=404, detail="user not found")

    mode = SHORT_NAME_TO_MODE.get(ruleset)
    if mode is None:
        # lazer omits the ruleset segment for some callers; treat anything else
        # as the user's own preferred mode rather than guessing wrong stats
        mode = resolve_mode(user.id, user.preferred_mode)
    else:
        mode = resolve_mode(user.id, mode)

    return await _public_user(user, mode)
