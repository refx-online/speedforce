"""Beatmapset comments -- `GET /api/v2/comments`.

Read by `CommentSectionDrawable` through `GetCommentsRequest`, which the beatmap
overlay loads as soon as a beatmapset's detail page is opened.

## why this shape is fiddly

`GetCommentsRequest` binds `CommentBundle`, and every one of its list properties
has a **setter that dereferences the other lists**:

```csharp
public List<long> UserVotes
{
    set { userVotes = value; Comments.ForEach(...); IncludedComments.ForEach(...); }
}
```

A null in `comments`, `included_comments` or `pinned_comments` is therefore an
immediate `NullReferenceException` in the client, not an empty screen. All four
lists are declared `[]` rather than left out or sent as null.

The bundle also expects **`comments` to be non-null before anything else**, so it
is populated even when empty.

## mapping bancho's `comments` table

The table is shared with the stable client and stores both beatmaps
(`target_type='map'`) and beatmapsets (`target_type='song'`) -- the enum reads
`song` where lazer reads `beatmapset`. Only `beatmapset` is served here; the
stable client is the only reader of the per-map comments, and it reads them
straight from MySQL rather than through the API.

There is no parent/reply column and no votes column, so every comment is a
top-level comment with zero votes. `replies_count` and `votes_count` are still
sent because the client renders vote and reply buttons from them.
"""

from __future__ import annotations

from datetime import UTC
from datetime import datetime

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pydantic import Field

from app.models.repository import UserRow
from app.models.repository import find_user_by_id
from app.route.me import require_user
from app.route.social import _public_user
from app.state.mods import resolve_mode
from app.state.services import session_factory

router = APIRouter(prefix="/api/v2", tags=["osu! API v2"])

# The client's comment section asks for 20 at a time; capping at 100 keeps a
# hand-rolled `page=999999` from pulling an entire beatmapset's history.
PAGE_SIZE = 20
MAX_PAGE_SIZE = 100


class Comment(BaseModel):
    """`Comment`, as `GetCommentsRequest` binds it."""

    id: int
    parent_id: int | None = None
    user_id: int | None = None
    message: str
    message_html: str | None = None
    replies_count: int = 0
    votes_count: int = 0
    commentable_type: str
    commentable_id: int
    legacy_name: str | None = None
    created_at: datetime
    updated_at: datetime | None = None
    deleted_at: datetime | None = None
    edited_at: datetime | None = None
    edited_by_id: int | None = None
    pinned: bool = False


class CommentableMeta(BaseModel):
    """`CommentableMeta` -- one entry per commentable id in the response."""

    id: int
    owner_id: int | None = None
    owner_title: str | None = None
    title: str
    type: str
    url: str = ""


class CommentBundle(BaseModel):
    """`CommentBundle` -- see the module docstring for why no list may be null."""

    total: int = 0
    top_level_count: int = 0
    has_more: bool = False
    has_more_id: int | None = None
    user_follow: bool = False
    commentable_meta: list[CommentableMeta] = Field(default_factory=list)
    comments: list[Comment] = Field(default_factory=list)
    pinned_comments: list[Comment] = Field(default_factory=list)
    included_comments: list[Comment] = Field(default_factory=list)
    user_votes: list[int] = Field(default_factory=list)
    users: list[dict] = Field(default_factory=list)


# `CommentableType` -> the `target_type` stored in bancho's `comments` table.
# lazer snake_cases the enum name, so `Beatmapset` -> `beatmapset`.
COMMENTABLE_TYPES = {
    "beatmapset": "song",
    "news_post": None,
    "build": None,
}


async def _load_comments(commentable_type: str, commentable_id: int, page: int, limit: int):
    """Return `(comments, total)` for one page of a commentable."""
    from sqlalchemy import text

    target_type = COMMENTABLE_TYPES.get(commentable_type)
    if target_type is None:
        # An unknown type is not an error the client can act on; an empty bundle
        # renders as "no comments", which is what an unsupported type means here.
        return [], 0

    offset = max(page - 1, 0) * limit

    async with session_factory() as session:
        total = await session.execute(
            text("SELECT COUNT(*) FROM comments WHERE target_type = :t AND target_id = :i"),
            {"t": target_type, "i": commentable_id},
        )
        total_count = int(total.scalar() or 0)

        result = await session.execute(
            text(
                "SELECT c.id, c.userid, c.comment, c.time, u.name "
                "FROM comments c "
                "LEFT JOIN users u ON u.id = c.userid "
                "WHERE c.target_type = :t AND c.target_id = :i "
                "ORDER BY c.time DESC, c.id DESC "
                "LIMIT :limit OFFSET :offset"
            ),
            {"t": target_type, "i": commentable_id, "limit": limit, "offset": offset},
        )
        rows = result.fetchall()

    return rows, total_count


@router.get("/comments")
async def get_comments(
    commentable_id: int = Query(...),
    commentable_type: str = Query("beatmapset"),
    page: int = Query(1, ge=1),
    sort: str = Query("new"),
    _: UserRow = Depends(require_user),
) -> JSONResponse:
    """`GetCommentsRequest` -- `GET /api/v2/comments`.

    `sort` is accepted and ignored: the table has no vote count to sort `top` by
    and no like count for `old`, so every ordering the client asks for collapses
    to newest-first. Taking the parameter rather than rejecting it keeps the
    client's re-poll quiet.
    """
    limit = min(PAGE_SIZE * max(page, 1), MAX_PAGE_SIZE) if page > 1 else PAGE_SIZE
    rows, total = await _load_comments(commentable_type, commentable_id, page, limit)

    comments = [
        Comment(
            id=int(row[0]),
            user_id=int(row[1]) if row[1] is not None else None,
            message=str(row[2] or ""),
            commentable_type=commentable_type,
            commentable_id=commentable_id,
            legacy_name=str(row[4]) if row[4] else None,
            created_at=datetime.fromtimestamp(int(row[3] or 0), tz=UTC),
        )
        for row in rows
    ]

    # `users` is how the client resolves `user_id` to a display name. Served from
    # the same source as the rest of the API so a commenter shows up even if they
    # have since been deleted.
    authors = {int(row[1]) for row in rows if row[1] is not None}
    users: list[dict] = []
    for author in sorted(authors):
        user = await find_user_by_id(author)
        if user is None:
            continue
        users.append((await _public_user(user, resolve_mode(user.id, user.preferred_mode))).model_dump(mode="json"))

    meta = [
        CommentableMeta(
            id=commentable_id,
            title=f"beatmapset {commentable_id}",
            type=commentable_type,
        )
    ]

    return JSONResponse(
        content=CommentBundle(
            total=total,
            top_level_count=total,
            has_more=page * limit < total,
            has_more_id=comments[-1].id if comments and page * limit < total else None,
            commentable_meta=meta,
            comments=comments,
            users=users,
        ).model_dump(mode="json")
    )
