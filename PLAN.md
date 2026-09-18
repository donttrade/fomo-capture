# capture_leaderboard.py — build plan

The data-collection instrument for the 60-day FOMO rank-persistence measurement.
Not an analysis tool. Not a trading tool.

## GOAL

Once per UTC day, capture the leaderboard at 1D, 7D, 14D and 30D, and append it to a
local record that is (a) durable, (b) re-derivable from raw bytes if the parser turns
out to have been wrong, and (c) loud when a capture fails or a day is missed.

The record's job is to make one analysis possible later: rank persistence between
NON-OVERLAPPING windows — 1D vs next-day 1D, 7D vs 7-days-later 7D, 30D vs
30-days-later 30D. Consecutive 30D captures share 29/30 days of underlying data;
their overlap is mechanical and means nothing. All timeframes are captured daily
anyway, because a day not captured cannot be captured later.

Success is not "the script runs." Success is: on day 61, the raw bytes of every
capture are on disk, each hash-pinned, every gap known and named, and the parsed CSV
regenerates from the raw bytes identically.

## NOT IN SCOPE

The most important section. Everything here is a thing not to build.

- **Analysis of any kind.** No transition matrix, no persistence rate, no p-values,
  no null cohort, no charts, no statistics beyond row counts. If you find yourself
  computing a mean, stop.
- **Anything about trading.** No wallet following, no PnL recomputation, no on-chain
  calls, no signals, no position sizing.
- **Anything Maximus touches.** Never opens, reads, writes or imports journal.csv,
  positions.csv, passport.py or positions.py. Lives in a sibling directory with its
  own venv — structural, not a promise.
- **Committing.** The script never calls git. Reine commits.
- **"Load more."** Capture the first page as served. Record N; never assume it.
- **Entity resolution across days.** Store identity verbatim. Do not dedupe or merge.
  That is an analysis decision and belongs in the analysis.
- **A database.** CSV. 18,000 rows is not a database problem.
- **Retry sophistication.** Two attempts, fixed pause, then fail loudly. No backoff,
  no queue. A missed day is an honest recorded gap; a retry framework is a place for
  failures to hide.
- **Alerting infrastructure.** No email, push, webhook or Telegram.
- **A config file.** Constants at the top of the one file.
- **Abstraction.** One file, no package, no classes, no plugin interface.
- **Backfill.** The source says its own pre-Aug-28 data is wrong. Forward-only.

## RESOLVED BY MEASUREMENT (2026-09-18) — do not re-litigate

1. **Stable identifier: YES.** Every row links to `/wallet/<base58 address>`.
   `trader_id` = that address, never a display name. Names and @handles are editable;
   addresses are not.
2. **Capture method: headless browser.** The `/dapi/v2/pnl/leaderboard/top` endpoint
   returns 403 Forbidden even when fetched from inside the page's own JS context with
   credentials. `requests` will not work. Playwright, in `fomo/.venv`.
3. **Parse by LABEL, not by position.** Each row's text carries its own labels
   (`REALIZED`, `ROI`, `VOLUME`, `WIN RATE`, `TRADES`, `TOKENS`, `Last active`).
   The DOM field order is `REALIZED, ROI, VOLUME, WIN RATE` — which is NOT the
   visible header order (`Realized, Volume, ROI, Win Rate`). A positional parser
   written against the header silently swaps ROI and Volume on every row forever.
   Rows are also variable height: rank 38 carries an extra `+1` badge line, and rows
   without an X account show a truncated address (`Cqu5...QSsR`) where the handle
   goes — that is not a handle and must be stored as empty.
4. **Timeframes: 1, 7, 14, 30.** Not 90 (yields no non-overlapping pair in 60 days).
5. **Platforms: all of them.** `/leaderboard/<slug>` for fomo, axiom, photon, bloom,
   gmgn, pumpfun, terminal, kolscan — same layout, one parser, a list of slugs.
6. **Scheduling:** macOS cron does not run while asleep and does not catch up. Use a
   launchd agent with `StartCalendarInterval` AND `RunAtLoad`, and make the script
   idempotent per UTC date so firing on wake, on unlock and by hand all produce
   exactly one capture. Record the actual capture timestamp; jitter matters for the
   1D board (a rolling 24h window) and the analysis can discard pairs that overlap
   too much. Better to measure the jitter than to pretend a laptop is a server.

## DATA MODEL

Three artifacts. Raw is primary; CSV is derived.

**`raw/<YYYY-MM-DD>/<capture_id>.html`** — the rendered page source, byte-for-byte,
written before anything is parsed. Plus one `<date>-page.txt` per day for the
disclaimer tripwire. Never rewritten.

**`captures.csv`** — the manifest. Append-only, hash-chained. One row per
(platform, timeframe, attempt):

```
capture_id, captured_at_utc, capture_date_utc, platform, timeframe_days,
sort, direction, min_trades, min_days, source_url, method,
status (OK|FAIL), row_count, raw_path, raw_sha256, page_text_sha256,
parser_version, error, prev_hash, row_hash
```

`min_trades=20` / `min_days=3` are a SELECTION RULE, not metadata — they define the
population being measured. Record them per capture alongside the URL observed on the
wire, so a silent change by the site is detectable after the fact.

**`leaderboard.csv`** — the rows. Append-only, not chained:

```
capture_id, captured_at_utc, capture_date_utc, platform, timeframe_days, rank,
trader_id, display_name, handle, badges,
realized_pnl_usd, realized_pnl_src, volume_usd, volume_src,
roi_pct, roi_src, win_rate_pct, win_rate_src,
trades, trades_src, tokens, tokens_src,
last_active_hours, last_active_src, parser_version
```

**Every derived numeric column keeps its source string beside it.** `"$1.33M"` next
to `1330000.0`. If the suffix parser is wrong about K/M, the original is right there
and the damage is repairable without touching raw files.

The chain goes on the manifest, not the rows: the manifest pins each raw file's
sha256 and row count, so an altered leaderboard.csv is already detectable by
re-deriving it. One chain, over the thing that cannot be regenerated.

## STRUCTURE

```
~/Trading with Claude Code/
  maximus/                     <- UNTOUCHED
  fomo/
    .venv/                     <- its own venv
    capture_leaderboard.py     <- ONE file, ~250 lines
    captures.csv
    leaderboard.csv
    raw/2026-09-18/...
    logs/capture.log
    com.reine.fomocapture.plist -> symlinked into ~/Library/LaunchAgents/
```

Verbs: `capture` (default), `status`, `verify`, `rebuild`.

Dependencies: stdlib (`csv`, `json`, `hashlib`, `datetime`, `pathlib`, `argparse`)
plus `playwright`. Try `channel="chrome"` to drive the installed Chrome and skip the
~300MB Chromium download.

## BUILD ORDER

1. **Capture one board, raw only, no parsing.** (~45 min) Fetch fomo/30D with
   Playwright, write raw HTML to `raw/<date>/<capture_id>.html`, append one manifest
   row with `status=OK`, row_count blank, chain hash, and the URL observed.
   *Works when:* run twice -> two raw files, two manifest rows, second row's
   `prev_hash` equals the first's `row_hash`.

2. **Parse it, label-anchored.** (~60 min) raw -> rows -> leaderboard.csv with every
   `*_src` populated. `trader_id` from the row's `/wallet/<addr>` href.
   *Works when:* manifest `row_count` == lines added == anchors found; ranks run
   1..N contiguous, no duplicates; `trader_id` unique within the capture.

3. **All timeframes and platforms, all-or-nothing per capture.** (~45 min) Loop
   slugs x timeframes. A capture that fails writes ZERO rows and one FAIL manifest
   row with non-empty `error`. Any row that fails to parse fails the whole capture —
   never a partial board. Exit nonzero if any capture is not OK.
   *Works when:* point one at a broken URL -> others land, that one is FAIL with zero
   rows, exit code nonzero.

4. **`verify` and `rebuild`.** (~60 min) `verify` walks the chain and re-hashes every
   raw file. `rebuild` reparses every raw file with the current parser and diffs
   against leaderboard.csv.
   *Works when:* edit one byte of a raw file -> verify names it. Edit a manifest row
   -> verify names it. Change the parser -> rebuild reports a diff instead of
   silently rewriting.

5. **Idempotence, page-text tripwire, `status`.** (~45 min) Skip any capture already
   OK for today's UTC date. Save and hash the page text once a day. `status` prints
   first capture date, every date since with a missing capture, and any day the
   page-text hash changed.
   *Works when:* run three times in one day -> one capture. Delete a day's rows ->
   status names that date.

6. **launchd.** (~45 min) Plist with `StartCalendarInterval` (07:30 local) AND
   `RunAtLoad`, logs to `logs/capture.log`. Absolute paths only — launchd gets a
   minimal environment with no .zshrc.
   *Works when:* `launchctl kickstart` produces a capture; then shut the lid before
   07:30, open at 16:00, confirm it fired on wake and status shows no gap.

7. **Day-3 eyeball.** (~15 min, three days later) Open three days of raw files and
   read them. Check one trader's rank moved in a way a human finds plausible. A
   subtly wrong parser is usually obvious to a person and invisible to an assertion.

## DEFINITION OF DONE

Invariants. Not one is a number measured in a pre-run.

1. For every `OK` manifest row, the file at `raw_path` exists and its sha256 equals
   `raw_sha256`.
2. `rebuild` reparses every raw file and produces a leaderboard.csv identical to the
   one on disk.
3. For every OK capture: rows bearing that `capture_id` == manifest `row_count` ==
   wallet anchors in that raw file.
4. Within a capture, `rank` is contiguous 1..N with no duplicates, and `trader_id` is
   unique.
5. Every numeric column has a non-empty `*_src` beside it, and re-parsing that
   `*_src` yields the stored value exactly.
6. Every `trader_id` is a base58 address extracted from a row link — never a display
   name.
7. Every parsed field is read from a labelled anchor, never a positional offset.
8. The manifest hash chain verifies unbroken end to end.
9. A FAIL capture contributes zero rows and exactly one manifest row with non-empty
   `error`.
10. Every manifest row records a `source_url` observed on the wire whose `days`
    parameter equals that row's `timeframe_days`.
11. The process exits nonzero iff at least one capture in the run is not OK.
12. Running `capture` again the same UTC date adds no second capture for anything
    already OK that date.
13. `status` names every UTC date since the first capture lacking an OK capture.
14. No path outside `fomo/` is written — verified directly: mtimes of journal.csv and
    positions.csv unchanged across a run.
15. No git command is executed by the script, ever.

Done is all fifteen holding on a record with at least three distinct capture dates —
not on the first successful run.

## ONE THING OUTSIDE THE SCRIPT, ON DAY 1

The raw record is irreplaceable and lives on one laptop. Get `fomo/raw/` into the
GitHub remote on day one. A 60-day experiment with a single point of failure at the
storage layer is not a 60-day experiment.
