from __future__ import annotations

from app.models.beatmap import Beatmap
from app.models.beatmap import BeatmapSet
from app.models.beatmap import status_from_mask

from .beatmap_repo import MapRow

# osu!'s own CDN. Our CDN (assets.041095.xyz/{id}) serves avatars, not covers,
# and b.041095.xyz/beatmapset/... 404s, so the old URLs here resolved to nothing
# and every beatmap cover was blank. assets.ppy.sh is also already covered by the
# client's *.ppy.sh allowlist, so no client patch is needed for covers.
ASSETS_BASE = "https://assets.ppy.sh/beatmaps"


def row_to_beatmap(row: MapRow) -> Beatmap:
    return Beatmap(
        id=row.id,
        beatmapset_id=row.set_id,
        # per-mode status, not the legacy single `status`
        status=status_from_mask(row.status_mask, row.mode),
        mode_int=row.mode,
        version=row.version,
        difficulty_rating=row.diff,
        accuracy=row.od,  # bancho `od` is lazer's `accuracy`
        drain=row.hp,
        cs=row.cs,
        ar=row.ar,
        bpm=row.bpm,
        total_length=row.total_length,
        # lazer needs the real values: hit_length drives the progress bar's fill
        # and the counts populate the difficulty picker bar, which renders empty
        # without them.
        hit_length=row.hit_length,
        count_circles=row.count_normal,
        count_sliders=row.count_slider,
        count_spinners=row.count_spinner,
        playcount=row.plays,
        passcount=row.passes,
        max_combo=row.max_combo,
        checksum=row.md5,
        last_updated=row.last_update.isoformat() if hasattr(row.last_update, "isoformat") else None,
    )


def rows_to_set(rows: list[MapRow]) -> BeatmapSet | None:
    """Fold the difficulty rows of one set into a BeatmapSet.

    The schema stores artist/title/creator per difficulty, so the first row is
    authoritative for set-level metadata.
    """
    if not rows:
        return None

    first = rows[0]
    beatmaps = [row_to_beatmap(r) for r in rows]

    # set-level status: a set is "ranked" if any difficulty is approved, else
    # loved if any is loved, else the lowest-ranked status present.
    codes = [status_from_mask(r.status_mask, r.mode) for r in rows]
    for candidate in (1, 3, 2, 0, -1):
        if candidate in codes:
            set_status = candidate
            break
    else:
        set_status = 0

    return BeatmapSet(
        id=first.set_id,
        artist=first.artist,
        title=first.title,
        creator=first.creator,
        status=set_status,
        play_count=sum(r.plays for r in rows),
        bpm=first.bpm,
        covers={
            # no "header": osu! does not publish one (404s), so advertising it
            # would just be a second broken image.
            "list": [f"{ASSETS_BASE}/{first.set_id}/covers/list.jpg"],
            "cover": f"{ASSETS_BASE}/{first.set_id}/covers/cover.jpg",
            "cover@2x": f"{ASSETS_BASE}/{first.set_id}/covers/cover@2x.jpg",
            "card": f"{ASSETS_BASE}/{first.set_id}/covers/card.jpg",
            "card@2x": f"{ASSETS_BASE}/{first.set_id}/covers/card@2x.jpg",
            "list@2x": f"{ASSETS_BASE}/{first.set_id}/covers/list@2x.jpg",
        },
        beatmaps=beatmaps,
        last_updated=first.last_update.isoformat() if hasattr(first.last_update, "isoformat") else None,
        submitted_date=first.last_update.isoformat() if hasattr(first.last_update, "isoformat") else None,
    )
