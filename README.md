# speedforce

osu!(lazer) API v2 and realtime services for the re;fx server.

This is the lazer-facing half of the stack. `bakenohana` serves the stable client
over bancho; speedforce speaks the osu! web API v2 that lazer expects. Both read
and write the **same `bancho` database**, so a user has one identity, one set of
scores, and one leaderboard regardless of which client they use.

> **Status: in progress.** `/oauth/token` and `/api/v2/me/` are implemented and
> verified against a live client login. Everything else is not built yet.

## Scope

lazer is limited to the **four base rulesets** (osu/taiko/catch/mania) — no
custom rulesets. That constrains the *ruleset*, not the mode: lazer still plays
Relax and Autopilot, and those must land on the right leaderboards.

They arrive as **mods**, never as separate rulesets — `ModRelax.Acronym` is
`"RX"` — so lazer always sends `ruleset_id` 0-3 and puts RX/AP in the mod list.
Deriving the effective mode is therefore our job. `app/models/modes.py` is a
faithful port of forlorn's `GameMode::from_params`, so lazer and stable scores
land on the same dense ids (CLAUDE.md in the monorepo):

| lazer | mods | effective mode |
|---|---|---|
| osu | — | 0 `VN_OSU` |
| taiko / catch / mania | — | 1 / 2 / 3 |
| osu + RX | `RX` | 4 `RX_OSU` |
| taiko + RX | `RX` | 5 `RX_TAIKO` |
| catch + RX | `RX` | 6 `RX_CATCH` |
| mania + RX | `RX` | **3** — there is no relax mania; ids 12-14 are cheat-rx |
| osu + AP | `AP` | 7 `AP_OSU` |
| taiko + AP | `AP` | 1 — AP is osu-only |
| any + TD | `TD` | unchanged — the mod bit stays on the score and pp is computed with it |

`app/state/mods.py` resolves which mode to report, in precedence order: live
lazer mod state (fed by the metadata hub's `UpdateStatus`/`UpdateActivity`, not
built yet) → `users.preferred_mode` → vanilla osu. The middle case matters: a
stable player who prefers relax should see their relax stats in lazer too, even
though lazer never told us about them.

Implemented:

| Endpoint | Purpose |
|---|---|
| `POST /oauth/token` | password + refresh_token grants |
| `GET /api/v2/me/` | login gate; returns `APIMe` |
| `GET /api/v2/beatmapsets/search` | song select |
| `GET /api/v2/beatmapsets/lookup?beatmap_id=` | set from a beatmap id |
| `GET /api/v2/beatmapsets/{id}` | set detail |
| `GET /api/v2/beatmaps/lookup?id=&checksum=` | difficulty lookup |
| `GET /api/v2/beatmapsets/{id}/download` | streams the `.osz` |
| `POST /api/v2/beatmaps/{id}/solo/scores` | mints a score submission token |
| `PUT /api/v2/beatmaps/{id}/solo/scores/{token}` | submits a lazer score |

Not yet built (see `speedforce-api.md` in the monorepo root for the full
inventory derived from the client source):

- `/signalr/metadata`, `/signalr/spectator`, `/signalr/multiplayer`
- chat and the notifications WebSocket
- wiring live lazer mod state into `app/state/mods.py` (fed by the metadata
  hub's `UpdateStatus`, so it can't work until that hub exists)

## Score submission

The token lifecycle, validation and payload parsing live here; **the write itself
does not**. forlorn owns it via `usecases::score` — the same path stable scores go
through — because that usecase is where personal-best rejection, placement, xp, pp
calculation, leaderboard ZADDs, first-place webhooks and the Redis events live.
Reimplementing any of that in Python would let lazer and stable stats drift apart,
which defeats the point of sharing one schema.

So speedforce validates, normalises, derives the effective mode from the mods, and
POSTs to `FORLORN_URL/api/v1/lazer/scores`. **That forlorn endpoint does not exist
yet** — it needs adding, accepting the JSON shape in `app/route/scores.py` and
running the existing usecase chain. Until then submissions return 503.

Two design points worth keeping:

- **`user_id` is taken from the score token, never from the payload.** A client
  cannot submit a score as another user.
- **The token is only spent after the write succeeds.** `score_tokens.score_id` is
  what makes it single-use (the consume query only matches rows where it's still
  NULL), so a failed hand-off leaves the submission retryable instead of losing it.

## Tests

```bash
python tests/test_beatmaps.py
python tests/test_scores.py
```

`test_scores.py` stubs the forlorn hand-off, so it tests only speedforce's half.

Integration tests over the real ASGI stack via `httpx.ASGITransport` — no port
bound. They assert against **real database rows**, so they need `MYSQL_*` and
`OSZ_PATH` pointed at a populated instance; there are no fixtures.

## Beatmap schema notes

The schema fights osu-web's model in ways that matter here:

- **There is no beatmapsets table.** `mapsets` exists but is a 3-column osu-api
  sync-tracking table and is empty. All set metadata — artist, title, creator —
  is denormalised onto every difficulty row, so a beatmapset is just `maps` rows
  sharing `set_id`, and the set is folded up from them at response time.
- **`maps` PK is `(server, id)`.** Beatmap ids are namespaced by a `server`
  enum (`'osu!'` / `'private'`); every row currently in the dev database is
  `'osu!'`. `maps.id` also has a separate unique index, so lookups by id are
  unambiguous today, but a `'private'` row reusing an id would need the server
  column disambiguated.
- **`status` is legacy.** Per-mode approval lives in `status_mask` as 3 bits per
  mode; `status_from_mask()` in `app/models/beatmap.py` decodes it. Searching by
  ranked/qualified/loved must filter on the mask, not `status` — see CLAUDE.md.
- **Fields lazer wants that we don't store** are defaulted rather than invented:
  `count_circles`/`count_sliders`/`count_spinners`, `hit_length`, `*_unicode`
  titles, `source`, tags, genre/language, video/storyboard, ratings. lazer
  renders these as 0/empty and recomputes difficulty locally after import, so
  they're cosmetic — but the difficulty-picker bar will show no hit counts.
- **`bpm` and `diff` are mostly 0** in the database because nothing backfills
  them. `omajinai` computes star rating and could populate `diff`.
- **`.osz` archives are served from disk** (`OSZ_PATH`, the shared `union_data`
  volume that `beatmap-service` writes). Entries over 100 MB inside an archive
  make the client throw, which is why the `.osz` files must exclude video.

## Running

```bash
poetry install
poetry run python main.py          # or: uvicorn app:asgi_app
```

Configuration is env-based; copy `.env.example` to `.env`. It shares
`MYSQL_*` / `REDIS_URL` with the rest of the stack.

## Client contract notes

Things that are easy to get wrong and produce a client that silently fails to log
in. All verified against the client source, not assumed.

- **`/oauth/token` arrives as `multipart/form-data`**, not
  `application/x-www-form-urlencoded`. The client uses `WebRequest.AddParameter`,
  and osu-framework serialises *any* request with form parameters as multipart
  (`WebRequest.cs`). `python-multipart` is a hard dependency for this reason.
- **`expires_in` must be a JSON number of seconds.** The client computes expiry
  from it and treats anything with 30s or less as invalid, which triggers an
  immediate logout.
- **`refresh_token` is effectively mandatory.** The client persists
  `access_token|expiry|refresh_token` and loses the session on access-token
  expiry without it. A failed refresh is *fatal* — it clears the whole token.
- **`GET /api/v2/me/` has a trailing slash.** `GetMeRequest.Target` is
  `me/{Ruleset?.ShortName}` and lazer sends it with no ruleset.
- **`session_verification_method` must stay null** unless
  `POST /api/v2/session/verify` is implemented, or the client parks in
  `RequiresSecondFactorAuth` and never finishes logging in.
- **Declare literal paths before path-parameter routes.** FastAPI matches in
  declaration order, so `/beatmapsets/lookup` registered after
  `/beatmapsets/{beatmapset_id}` is swallowed by the int path param and returns
  `422 int_parsing` instead of the set.
- **Return 401, never 500, on auth failure.** Three consecutive failures put the
  client into `APIState.Failing`, after which it stops retrying and the UI is
  effectively bricked until restart.
- **`statistics` is a flat object for one ruleset**, not a dictionary keyed by
  ruleset — the ruleset comes from the request path.
- Field names are bound literally from `[JsonProperty]`; lazer does no case
  conversion. The `stats` table names do not match the wire names
  (`tscore` → `total_score`, `acc` → `hit_accuracy`, `plays` → `play_count`,
  `xh_count` → `grade_counts.ssh`), and `stats.id` *is* the user id — there is
  no `user_id` column.

## Schema notes

Written against the existing `bancho` schema, not a new one. Two things that
surprise people:

- `stats` primary key is `(id, mode)`. **`id` is the user id.**
- `users.priv` is a bitmask (`bakenohana/src/shared/constants/priv.cr`), not a
  level. Verified against live rows: `qabot` = 2051 = NOMINATOR|VERIFIED|
  UNRESTRICTED.

## Acknowledgements

Built from the osu!(lazer) client source (`ppy/osu`), which is the authoritative
statement of the protocol, and cross-checked against `osu-framework` and ppy's
`osu-server-spectator`.

Prior art consulted while understanding how a lazer backend fits together:

- **[GooGuTeam/g0v0-server](https://github.com/GooGuTeam/g0v0-server)** —
  an existing lazer server implementation, used to sanity-check behaviour that
  the client leaves ambiguous (for example, that `/api/v2/me` must return
  `session_verification_method: null` to skip 2FA). No code was taken from it.
  It is AGPL-3.0 and none of it is vendored here.
- **[GooGuTeam/g0v0.Server.Realtime](https://github.com/GooGuTeam/g0v0.Server.Realtime)**
  — noted as prior art for the realtime services. MIT.

## License

See the monorepo's licensing. Portions of the protocol behaviour are derived from
reading MIT-licensed upstream projects; no third-party code is vendored.