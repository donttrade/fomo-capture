#!/usr/bin/env python3
"""capture_leaderboard.py — FOMO leaderboard capture instrument.

BUILD STEP 3: capture every board in the matrix -- 7 platforms x 3 timeframes
= 21 measured captures, plus ONE canary that is not part of the measurement,
so 22 per run -- raw bytes only, no parsing. Each capture writes the XHR
response body to raw/<date>/<capture_id>.json and the rendered page source to
raw/<date>/<capture_id>.html, then appends exactly ONE hash-chained row to
captures.csv. Nothing is extracted from the page: the wallet-anchor count and
the "No data yet" check below are a validity gate, not parsing, and row_count
stays blank on every row (step 2 fills it).

The matrix, the (page_slug, wire_platform) pair that a platform now is, which
of those two names lands in the manifest, and the canary are all defined and
justified in the constants block below.

Step 3's three obligations, and where each lives:

  * ONE manifest row per capture, OK or FAIL -- capture_one().
  * A FAIL row carries a non-empty, specific `error` -- failure_reasons().
  * One capture failing never aborts the run, and the process exits nonzero
    iff at least one capture is not OK -- run_captures().

PLUS ONE PIECE OF STEP 5: idempotence per UTC date. A capture that already has
an OK row for the date its bytes would be stamped with writes nothing at all
-- no raw file, no manifest row -- and is reported as a SKIP, which is a thing
that happens in the log and never a status in captures.csv. So "exactly one
row per capture" above holds for every capture this run actually takes, and a
run in which all 22 are skipped writes nothing and exits 0. launchd fires this
job at 07:30, at load and on every wake (PLAN.md decision 6); without the
guard, a laptop opened three times in a day would record 66 rows for 22
boards. See the IDEMPOTENCE block and the guard in capture_one().

That guard is checked TWICE, and it takes three things to be safe:

  * A cheap pre-flight before the browser exists (preflight_skip), so the
    ordinary already-done run costs milliseconds instead of 332 seconds of
    visible Chrome. It refuses to decide within one capture's worth of time of
    UTC midnight, because a skip decided on the wrong side of midnight loses a
    day -- see the trap described in preflight_skip().
  * The authoritative check after the harvest (capture_one), on the date the
    bytes actually arrived. That one is the last word.
  * An OK row only counts as proof if the files it names are on disk and
    non-empty (missing_evidence). Otherwise deleting one raw file would make
    that board un-recapturable for its date, for ever.

And ONE RUN AT A TIME, enforced by run_lock() over the whole run. Two
overlapping runs each read a stale picture of what today already has, and
both capture everything: 44 rows, 88 files, one day, and a hash chain that
verifies perfectly. A second process logs one line saying so and exits 0.

`error` answers one question only: why is this capture not usable. Everything
else a reader must not miss -- a tab mechanism that misbehaved on a capture
whose bytes are nonetheless confirmed on the wire, a refused body kept as
evidence, the canary's verdict -- goes in `notes`, a separate hashed column,
so neither cell dilutes the other. See add_note().

The manifest is the only thing here that cannot be regenerated, so it is
defended harder than anything else in the file:

  * The tail of captures.csv is checked at the START of every run AND before
    every append. A file that does not end in a newline, whose last row is
    short or unhashed, whose last row does not reproduce its own row_hash, or
    whose last row does not chain to the one before it, fails the run with
    exit 1 and NO append at all -- see check_manifest_tail() for the rules and
    check_manifest_tail_before_run() for why once per append was not enough.
  * A blank schema_version means the v2 columns are EMPTY. It is never a
    licence to carry unhashed evidence -- see manifest_row_problems().
  * The header may be widened, never narrowed -- see migrate_manifest().

The raw bytes are the only thing here that cannot be RE-COLLECTED, and PLAN.md
calls them irreplaceable, so they are fsynced -- the files AND their parent
directory -- BEFORE the manifest row that pins them is appended. The ordering
guarantee is: bytes durable, then the row naming them. See write_durably() and
fsync_dir(). Getting that backwards would let a crash leave a chain-verifying
row pointing at a file that is short or absent, which is invariant 1 broken
while the chain reports all-clear. Every directory in the chain of names is
committed, this one included: raw/<date>/, raw/, and fomo/ itself. An fsync on
a file commits that file's bytes and says nothing about the directory entry
that gives it a name.

Not an analysis tool. Not a trading tool. Never calls git. Writes only inside
this directory.
"""

import argparse
import csv
import errno
import fcntl
import hashlib
import logging
import os
import re
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse, parse_qsl

# ---------------------------------------------------------------- constants --
BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "raw"
LOG_DIR = BASE_DIR / "logs"
MANIFEST_PATH = BASE_DIR / "captures.csv"

# TWO LOCK FILES, and they are not interchangeable -- see manifest_lock() and
# run_lock(). LOCK_PATH is held for milliseconds around a single append so two
# writers cannot fork the hash chain. RUN_LOCK_PATH is held for the WHOLE run
# so two runs cannot both decide, from the same stale picture of captures.csv,
# that today's boards still need capturing. Conflating them would deadlock:
# the run-long holder would still need the append lock 22 times.
LOCK_PATH = BASE_DIR / ".captures.lock"
RUN_LOCK_PATH = BASE_DIR / ".capture_run.lock"

PARSER_VERSION = "0.1.0"
SCHEMA_VERSION = "3"

# ------------------------------------------------------------ the matrix --
# PLAN.md, RESOLVED BY MEASUREMENT 4 and 5, as amended 2026-09-18.
#
# A PLATFORM IS A PAIR, not a string: (page_slug, wire_platform).
#
#   page_slug     -- what goes in /leaderboard/<slug>; the page we navigate to.
#   wire_platform -- what that page's own XHR carries as `platform=`, and what
#                    url_matches_capture() compares against.
#
# The two are the same word for every platform but pump.fun, whose page lives
# at /leaderboard/pumpfun while its API calls the board `pumpfun-app`.
# /leaderboard/pumpfun-app is a 404, so neither name is a typo for the other:
# each is correct for its own layer. Getting this wrong is what made all four
# pumpfun captures of 2026-09-18 FAIL -- the page loaded, the board rendered,
# a good 100-wallet body went past on the wire, and every one of them was
# refused because the capture was asking for platform="pumpfun".
#
# THE MANIFEST RECORDS THE WIRE NAME. A row's `platform` column is the same
# string that appears inside that row's own `source_url`, so the two can be
# checked against each other by eye with no lookup table, and step 2 can match
# a row to the `platform` field inside its own JSON body. Recording the page
# slug instead would leave a row reading platform=pumpfun sitting beside a
# source_url reading platform=pumpfun-app -- which is indistinguishable, to a
# reader or to a checker, from exactly the mismatch invariant 10 exists to
# catch. The page slug is not lost by this choice: it is one entry in the
# table below, and the page it names is the row's own raw HTML artifact.
PLATFORMS = [
    ("fomo", "fomo"),
    ("axiom", "axiom"),
    ("photon", "photon"),
    ("bloom", "bloom"),
    ("gmgn", "gmgn"),
    ("pumpfun", "pumpfun-app"),
    ("terminal", "terminal"),
]

# kolscan is deliberately NOT in that table, and gets no special path anywhere
# in this file. Measured 2026-09-18: its page carries no /dapi/ reference of
# any kind, no timeframe tabs, and 50 server-rendered wallet addresses instead
# of 100 delivered by XHR; its 1D and 30D pages render an identical list, so
# the timeframe control is inert. There is nothing to observe on the wire and
# no timeframe to select, so it cannot join a rank-persistence measurement
# between timeframed windows. It is a different instrument, not a missing
# board. PLAN.md decision 5.

# 14 is NOT in this list. Measured 2026-09-18: `days=14` returns the 7D board
# byte for byte -- same sha256, same wallet order, same nextCursor -- on all
# six platforms that serve both, and in-band max(period.tradingDays) is 7 on
# every 14D body against 30 on every 30D body. The request genuinely carries
# days=14 and the tab click genuinely lands, so no request-side check can see
# this; only the payload can. 90 is absent for the original reason: it yields
# no non-overlapping pair in 60 days. PLAN.md decision 4.
TIMEFRAMES = [1, 7, 30]

# ------------------------------------------------------------- the canary --
# ONE capture per run that IS NOT PART OF THE MEASUREMENT.
#
# 14 was dropped because days=14 serves the 7D board. This single extra
# capture exists only to notice if that ever stops being true. fomo only, once
# per run.
#
# It is marked as not-a-measurement four ways, none of which relies on a
# future reader being careful:
#   1. capture_role="canary" on its manifest row -- a hashed column, so the
#      marking is signed by the row's own row_hash. Every measured row says
#      "measurement"; no row is silent on the question.
#   2. Its capture_id, and therefore both its raw filenames, contain
#      "-canary-": fomo-14d-canary-<stamp>-<uuid>.
#   3. Its `notes` cell opens with "canary: NOT PART OF THE MEASUREMENT".
#   4. It is built by name in build_matrix() as a separate item. There is no
#      product of PLATFORMS and TIMEFRAMES anywhere in this file that yields
#      a 14, so nothing can pick it up by looping the matrix.
#
# HOW IT SIGNALS A CHANGE, and why this way and not the other way:
#
# By comparing its raw_json_sha256 against the SAME RUN's fomo 7D
# raw_json_sha256 -- a comparison of two hashes this script already computes.
# It reads nothing out of either body. Reading `period.tradingDays` in-band
# would be parsing, and parsing belongs to step 2; this stays a hash
# comparison so no parser is smuggled into step 3.
#
# The alternative offered was to record the canary faithfully and leave every
# comparison to step 2 or `status`. Rejected: neither exists yet, so the
# canary would sit mute for as long as that takes, and a canary that cannot
# chirp is just an extra page load.
#
# The comparison is sound because it is precisely the measurement that retired
# 14: PLAN.md decision 4 records the two bodies as byte-identical. It was
# re-measured before being adopted here -- fomo's 30D payload hashed
# identically across eight captures spanning 71 minutes on 2026-09-18 -- so a
# payload does not churn within the span of a run and a same-run comparison is
# not a coin flip. The canary is placed immediately after fomo's 7D capture
# (build_matrix) to keep that span as short as the run allows.
#
# A CHANGED CANARY DOES NOT CHANGE THE EXIT CODE. Invariant 11 says the
# process exits nonzero iff at least one capture is not OK, and a canary that
# captured cleanly IS OK whatever its bytes turn out to say. The change is
# recorded in `notes` on the hash-pinned row and logged at ERROR level, where
# `status` (step 5) and any reader will find it. Making it an exit code would
# break invariant 11 and would put a nonzero exit in front of the only thing
# the exit code is for: whether the 21 measured boards landed.
#
# Its OK/FAIL, on the other hand, counts exactly like any other capture's. A
# canary behaving as currently expected returns 7D bytes, writes an OK row,
# matches the hash and costs the run nothing -- which is the requirement that
# it not fail every day. If it genuinely stops capturing, that is not "as
# expected" either, and invariant 11 is left intact rather than carved out.
CANARY_SLUG = "fomo"
CANARY_TIMEFRAME = 14
CANARY_PAIRED_TIMEFRAME = 7   # the measured board its hash is compared to

ROLE_MEASUREMENT = "measurement"
ROLE_CANARY = "canary"

BOARD_URL_TEMPLATE = "https://www.solanatracker.io/leaderboard/%s"

# Politeness, not a retry mechanism. PLAN.md NOT IN SCOPE forbids backoff and
# queues; this is a flat pause between consecutive captures so 22 page loads
# do not arrive as a burst. There is no second attempt anywhere in this file.
INTER_CAPTURE_PAUSE_S = 2.5

# Measured 2026-09-18: HEADED is required -- headless renders the shell and the
# leaderboard XHR never fires. Re-measured, 4 trials each: single-nav 4/4,
# 2.2-6.3s; warm-up hop + 8s settle 4/4, 10.0-10.3s. The hop bought nothing.
HEADLESS = False
BROWSER_ARGS = ["--disable-blink-features=AutomationControlled"]
VIEWPORT = {"width": 1512, "height": 982}

# The XHR whose request URL is the wire-observed source_url and whose response
# body is the primary raw artifact. Never fetched directly (it 403s); only
# observed via a Playwright response listener.
API_URL_FRAGMENT = "/dapi/v2/pnl/leaderboard/top"

# Validity gate only. Anchors are counted, never read; the text is tested for
# one literal, never parsed.
BOARD_READY_SELECTOR = 'a[href^="/wallet/"]'
EMPTY_BOARD_TEXT = "No data yet"

# The timeframe tabs. Measured 2026-09-18 from a captured raw file: the page
# carries two tablists, and the periods live in the one labelled below, as
# <button role="tab"><span>30D</span></button> for 1D/7D/14D/30D/90D, with
# aria-selected="true" on the active one (30D on a cold load). The page still
# offers a 14D tab; TIMEFRAMES no longer asks for it, and only the canary
# clicks it.
#
# THE TIMEFRAME IS UI STATE, NOT A URL. Selecting a tab changes what the page
# REQUESTS; the page URL is the same for every timeframe. So a click is never
# taken on trust: select_timeframe() clicks and then blocks until an XHR whose
# own `days` parameter equals the timeframe being captured is observed, and the
# observed-response list is scoped to a single capture (a fresh page per
# capture, its listener attached before navigation) so no capture can ever
# consume another's response. A 7D row carrying 30D bytes would be invisible
# in the data and would poison the whole analysis; it is the one silent
# failure this step could plausibly introduce.
#
# The same listener is what catches a paginated response: a "load more" fires
# another XHR for the same days and platform, and choose_source_url() has to
# tell the first page from a continuation. See CURSOR_KEY_CANDIDATES.
PERIOD_TABLIST_SELECTOR = 'div[role="tablist"][aria-label="Leaderboard period"]'
PERIOD_TAB_SELECTOR = PERIOD_TABLIST_SELECTOR + ' button[role="tab"]'
TIMEFRAME_LABEL = "%dD"           # 1 -> "1D", 30 -> "30D"

NAV_TIMEOUT_MS = 60_000
API_SETTLE_MS = 3_000
TAB_TIMEOUT_MS = 15_000           # finding/clicking the tab itself
MATCHING_XHR_TIMEOUT_MS = 30_000  # waiting for a response whose days match
# Waiting for the board to RENDER. This ran at 15s, and 15s was wrong -- the
# reasoning behind it ("a shorter wait here loses nothing") missed that the
# two outcomes cost wildly different amounts.
#
# wait_for_selector returns the instant the first anchor appears, so a
# generous timeout costs a board that renders NOTHING AT ALL. The full wait is
# only ever paid by a board with no rows, which is a FAIL either way and, at
# one or two such boards a day, is seconds of wall clock.
#
# What 15s did cost was a board that rendered slowly: its XHR body is already
# fsynced to disk and pinned by the row, and the row nonetheless says FAIL
# "zero anchors", so step 2 drops a board whose correct bytes it is sitting
# on. PLAN.md's NOT IN SCOPE forbids a retry, and a day not captured cannot be
# captured later, so that board is gone for good. 45 seconds on a day a board
# happens to be empty against a board lost forever is not a close call.
#
# 45s: three times the old budget, room for a slow React render on a laptop
# driving 22 headed page loads. Still well under NAV_TIMEOUT_MS, because this
# wait covers rendering only -- the fetch has already landed by the time it
# starts.
READY_TIMEOUT_MS = 45_000
LOCK_TIMEOUT_S = 120

# THE LONGEST ONE CAPTURE CAN TAKE, in seconds, before its own timeouts kill
# it. Composed from the constants above and never guessed, because this number
# is what lets the cheap pre-flight skip (preflight_skip) be SURE that the
# capture it is declining to run would have been stamped with the same UTC
# date it just read from the clock.
#
# fetch_page() pays these waits one after another, and every one is a ceiling:
#
#   page.goto                       NAV_TIMEOUT_MS             60s
#   wait for the period tablist     TAB_TIMEOUT_MS             15s
#   click that tab                  TAB_TIMEOUT_MS             15s
#   wait for a matching days= XHR   MATCHING_XHR_TIMEOUT_MS    30s
#   wait for the board to render    READY_TIMEOUT_MS           45s
#   the post-render settle          API_SETTLE_MS               3s
#   content + anchors + inner_text  3 x NAV_TIMEOUT_MS        180s
#                                                            -----
#                                                             348s
#
# The last line is page.content(), query_selector_all() and inner_text(),
# which take no timeout of their own and inherit
# page.set_default_timeout(NAV_TIMEOUT_MS).
#
# A measured capture takes about 15s, so this bound is roughly 23x the real
# thing. That slack is deliberate and it is asymmetric on purpose: being too
# generous here costs at most one extra page load near UTC midnight, while
# being too tight costs a board its day, permanently.
MAX_CAPTURE_SECONDS = (
    NAV_TIMEOUT_MS
    + 2 * TAB_TIMEOUT_MS
    + MATCHING_XHR_TIMEOUT_MS
    + READY_TIMEOUT_MS
    + API_SETTLE_MS
    + 3 * NAV_TIMEOUT_MS
) / 1000.0

GENESIS_PREV_HASH = "0" * 64

# The original 19-hashed-field layout. Frozen forever: rows written before the
# schema_version column existed are hashed over exactly these columns.
MANIFEST_COLUMNS_V1 = [
    "capture_id", "captured_at_utc", "capture_date_utc", "platform",
    "timeframe_days", "sort", "direction", "min_trades", "min_days",
    "source_url", "method", "status", "row_count", "raw_path", "raw_sha256",
    "page_text_sha256", "parser_version", "error", "prev_hash", "row_hash",
]

# v2 inserted three columns between `error` and `prev_hash`; v3 inserts two
# more directly after them. Every layout is derived from the frozen v1 list,
# and each one is spelled out in full, so they cannot drift apart and a row
# written under any of them still reproduces its stored row_hash for ever.
V2_ADDED_COLUMNS = ["schema_version", "raw_json_path", "raw_json_sha256"]
V3_ADDED_COLUMNS = ["capture_role", "notes"]
_CUT = MANIFEST_COLUMNS_V1.index("prev_hash")
MANIFEST_COLUMNS_V2 = (MANIFEST_COLUMNS_V1[:_CUT] + V2_ADDED_COLUMNS
                       + MANIFEST_COLUMNS_V1[_CUT:])
MANIFEST_COLUMNS = (MANIFEST_COLUMNS_V1[:_CUT] + V2_ADDED_COLUMNS
                    + V3_ADDED_COLUMNS + MANIFEST_COLUMNS_V1[_CUT:])

SCHEMA_COLUMNS = {"": MANIFEST_COLUMNS_V1, "2": MANIFEST_COLUMNS_V2,
                  "3": MANIFEST_COLUMNS}

# WHY THE TWO v3 COLUMNS ARE COLUMNS AND NOT LOG LINES.
#
# capture_role ("measurement" | "canary"): the canary must be impossible to
# mistake for a measured board, by a person or by step 2. logs/capture.log is
# neither hash-pinned nor committed -- it is in .gitignore -- so a marking
# that lives only there is a marking that does not survive the week. In a
# hashed column it is signed by the row's own row_hash, and step 2 can filter
# on it without knowing anything about canaries.
#
# notes: things a reader must not miss about a capture that are NOT reasons it
# failed. Three kinds land here -- a nav/readiness problem on a capture that
# nevertheless produced wire-confirmed bytes (nav_note), a refused body kept
# as evidence and named nowhere else (capture_one), and the canary's verdict
# (canary_verdict). They are kept out of `error` deliberately: `error` answers
# "why is this row not usable", invariant 9 leans on a FAIL row having a
# non-empty one, and a cell that also carries observations which are not
# failures stops meaning anything. Every entry is tagged with its kind, so one
# cell holding several stays readable.

# Which query-string keys on the observed source_url feed which manifest
# column. Matched case-insensitively, first hit wins; blank if absent.
# Values are recorded as observed -- never hardcoded -- so that a silent
# change of the site's selection rule is detectable after the fact.
QUERY_KEY_CANDIDATES = {
    "sort": ("sort", "sortby", "sort_by", "orderby", "order_by"),
    "direction": ("direction", "dir", "order", "sortdirection", "sort_direction"),
    "min_trades": ("mintrades", "min_trades", "minimumtrades"),
    "min_days": ("mindays", "min_days", "minimumdays"),
}
DAYS_KEY_CANDIDATES = ("days", "timeframe", "period", "window")
PLATFORM_KEY_CANDIDATES = ("platform", "platforms", "source")

# PAGINATION. Every leaderboard body captured so far carries "hasMore": true
# and a "nextCursor", so this endpoint DOES paginate and one page load can
# produce more than one response for the same board. PLAN.md, NOT IN SCOPE:
# "Load more -- capture the first page as served." A response fetched with a
# cursor is a continuation -- a different slice of the same board -- not this
# capture's artifact. See is_continuation() and choose_source_url().
#
# Cursor keys first, because that is the mechanism actually in use. The
# ordinal keys are here in case the site ever swaps mechanism, and each names
# the values that still mean page one, so a future `page=1` is not mistaken
# for a continuation.
CURSOR_KEY_CANDIDATES = ("cursor", "nextcursor", "next_cursor", "after")
FIRST_PAGE_VALUES = {"page": ("", "1"), "offset": ("", "0")}

log = logging.getLogger("capture")


class ManifestIntegrityError(RuntimeError):
    """The manifest on disk, or a row about to be written, is not appendable.

    Always fatal, never a warning. When this is raised nothing has been
    written to captures.csv and nothing will be: the run logs the specific
    problem and exits 1. Raw artifacts already on disk stay exactly where they
    are -- they are the evidence that a capture happened, and they are
    re-parseable later once the manifest is repaired by hand.
    """


# ------------------------------------------------------- row-level validity --
def row_label(row, line_num=None):
    """A specific, human-findable name for one manifest row.

    Every integrity message routes through this, because "a row is invalid" is
    useless and "line 37 (capture_id=fomo-30d-...)" is actionable.
    """
    bits = []
    if line_num is not None:
        bits.append("line %d" % line_num)
    capture_id = str(row.get("capture_id") or "").strip()
    if capture_id:
        bits.append("capture_id=%s" % capture_id)
    if not bits:
        return "manifest row"
    return "manifest row (%s)" % ", ".join(bits)


def unknown_schema_message(row, version, line_num=None):
    """Why an unrecognised schema_version is fatal FOR THAT ROW, by name."""
    known = ", ".join(repr(v) for v in sorted(SCHEMA_COLUMNS))
    return ("%s declares schema_version %r, which this script has no column "
            "list for (known versions: %s). Without a column list its row_hash "
            "can neither be computed nor checked, so the chain cannot be "
            "verified across it. Either this file was written by a newer "
            "capture_leaderboard.py, or the cell was edited."
            % (row_label(row, line_num), version, known))


def manifest_row_problems(row, line_num=None):
    """Every rule this one manifest row breaks, named. NEVER raises.

    Returns a list of human-readable strings; empty means the row is valid.
    Three callers: append_manifest_row() before writing a row,
    check_manifest_tail() before chaining from the last row on disk, and
    (step 4) `verify`, which walks every row and REPORTS each problem rather
    than dying on the first one.

    The rules:

    1. `schema_version` must be a version this script knows -- a key of
       SCHEMA_COLUMNS. See unknown_schema_message() for why an unknown one is
       not "probably fine".

    2. A row declaring an OLDER schema must leave every column that schema
       predates EMPTY -- see unhashed_columns(). A blank `schema_version`
       means ONE thing, and only one: this is a schema-v1 row and its v2 and
       v3 columns are empty. It is NOT a declaration that later columns may be
       ignored. The v1 hash does not cover them, so a blank-version row
       carrying a non-empty raw_json_path, capture_role or notes would be
       evidence that no hash signs -- and a row shaped exactly like that is
       how a forged capture would walk in through the front door and still
       verify clean. Such a row is invalid: either it declares the schema that
       covers those columns, or they are empty.

       The rule is stated over unhashed_columns(version) rather than over one
       hard-coded list, so adding a v4 tomorrow extends it to v1, v2 AND v3
       rows without anyone remembering to.

    The four rows this file was born with have every later column empty, so
    they remain valid v1 rows under this reading and their stored row_hash
    values are untouched by it.
    """
    version = row.get("schema_version") or ""
    if version not in SCHEMA_COLUMNS:
        return [unknown_schema_message(row, version, line_num)]

    problems = []
    unhashed = unhashed_columns(version)
    label = row_label(row, line_num)
    for name in unhashed:
        value = str(row.get(name) or "")
        if value.strip():
            problems.append(
                "%s declares schema_version %r (blank means v1), whose hash "
                "does NOT cover %s -- yet it carries %s=%r. That is unhashed "
                "evidence. A row on that schema must leave every column the "
                "schema predates (%s) empty; a row with data in them must "
                "declare the schema that covers them (current: %r)."
                % (label, version, name, name, value,
                   ", ".join(unhashed), SCHEMA_VERSION))
    return problems


def unhashed_columns(schema_version):
    """Columns in TODAY's CSV layout that THIS row's schema does not hash.

    A row declares its schema; that schema names a frozen column list; the
    row's row_hash covers exactly that list. Anything the current header
    carries which is not on it is a cell no hash signs, so on that row it must
    be empty.

    `schema_version` itself is excluded: it is the declaration, not evidence,
    and a v1 row leaves it blank by definition -- which is what makes it a v1
    row, not an unsigned cell.
    """
    covered = set(SCHEMA_COLUMNS[schema_version])
    return [name for name in MANIFEST_COLUMNS
            if name not in covered and name != "schema_version"]


# -------------------------------------------------------------- hash chain --
def row_hash(row, schema_version=None):
    """Canonical row serialisation, so `verify` (step 4) can reproduce it.

    RULE, complete and sufficient to reimplement from this comment alone:

      1. Read the row's `schema_version` cell. A MISSING OR EMPTY cell means
         schema v1; any other value names that schema version.
      2. That version selects a fixed, frozen column list:
           v1 ("")  -> MANIFEST_COLUMNS_V1  (20 names, ending prev_hash,
                       row_hash) -- the layout the file was born with. Columns
                       added to the CSV after v1 are NOT part of a v1 row's
                       hash. Because they are not hashed, a v1 row is only
                       valid when they are EMPTY: blank schema_version means
                       "the later columns are empty", never "ignore whatever
                       is in them" (manifest_row_problems(), rule 2).
           v2 ("2") -> MANIFEST_COLUMNS_V2  (23 names; schema_version,
                       raw_json_path, raw_json_sha256 inserted between `error`
                       and `prev_hash`). Frozen the same way v1 is: the 39
                       rows already written under it must keep reproducing
                       their stored hashes, so v3's columns are appended AFTER
                       these, never mixed into them.
           v3 ("3") -> MANIFEST_COLUMNS     (25 names; capture_role and notes
                       inserted after raw_json_sha256).
         A version that is neither -- anything not a key of SCHEMA_COLUMNS --
         is UNKNOWN. There is no column list for it, so there is no defined
         hash for that row: this function raises ManifestIntegrityError naming
         the row and the version, and `verify` must report that row as
         unverifiable and carry on with the rest. It must never be treated as
         v1 by default, and it must never take down a whole pass with a bare
         KeyError.
      3. Hash over every column in that list EXCEPT `row_hash` itself, in list
         order, prev_hash INCLUDED. A column named in the list but absent from
         the row is the empty string.
      4. Each field is emitted as its UTF-8 bytes prefixed by its BYTE length
         and a colon: b"<len>:<utf-8 bytes>". Length-prefixing makes field
         boundaries unambiguous, so no two distinct rows serialise identically.
      5. The emitted fields are joined by a single NUL byte (b"\\x00") and the
         sha256 hexdigest of that byte string is the row_hash.

    The chain is version-independent: every row's `prev_hash` is the previous
    row's stored `row_hash`, whatever schema either row uses. So a v1 row and a
    v2 row chain together with no transition rule.
    """
    if schema_version is None:
        schema_version = (row.get("schema_version") or "")
    if schema_version not in SCHEMA_COLUMNS:
        raise ManifestIntegrityError(
            unknown_schema_message(row, schema_version))
    columns = SCHEMA_COLUMNS[schema_version]
    parts = []
    for name in columns:
        if name == "row_hash":
            continue
        blob = str(row.get(name, "")).encode("utf-8")
        parts.append(str(len(blob)).encode("ascii") + b":" + blob)
    return hashlib.sha256(b"\x00".join(parts)).hexdigest()


@contextmanager
def manifest_lock():
    """Exclusive advisory lock over read-prev-hash + append, as one unit.

    fcntl.flock is held by an open file descriptor, so the kernel drops it the
    instant the holding process dies -- a killed capture cannot strand a lock.
    The lock FILE is never removed for the same reason (unlinking it would let
    a second process lock a different inode). Blocking with a deadline so a
    wedged holder surfaces as a loud failure instead of a hang.
    """
    fh = LOCK_PATH.open("a+")
    deadline = time.monotonic() + LOCK_TIMEOUT_S
    try:
        while True:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as exc:
                if exc.errno not in (errno.EACCES, errno.EAGAIN):
                    raise
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        "could not acquire %s within %ds; another capture is "
                        "still running" % (LOCK_PATH.name, LOCK_TIMEOUT_S))
                time.sleep(0.05)
        yield
    finally:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        finally:
            fh.close()


@contextmanager
def run_lock():
    """ONE capture run at a time on this machine. Yields True if we got it.

    THE DEFECT THIS CLOSES, which manifest_lock() could not. That lock is held
    for milliseconds around one append, so it keeps the hash chain in one
    piece and nothing more. Two overlapping runs each take it 22 times,
    politely, one after another -- and each of them read captures.csv at
    START-up, before the other had written anything, so both believe today's
    boards still need capturing. The result is 44 OK rows, 88 raw files, two
    of everything for one day, AND A HASH CHAIN THAT VERIFIES PERFECTLY. There
    is no number anywhere in the record that looks wrong. launchd cannot cause
    this on its own (it will not start a second copy of one label), but a
    person typing ./run_capture.sh during the five-and-a-half-minute run can,
    and that is the ordinary way it would happen.

    So the decision "does today still need this board" and the writes that
    follow from it have to be one critical section, and that section is the
    whole run.

    NON-BLOCKING, unlike manifest_lock(). Waiting would mean a second process
    sitting silent for up to 332 seconds and then doing the work all over
    again against a map that is now stale in the other direction. Standing
    down immediately is both cheaper and more correct: the run that holds the
    lock is already capturing exactly the boards this one would have.

    A SEPARATE LOCK FILE from LOCK_PATH, deliberately. Reusing it would mean
    holding the append lock for the whole run, and then the run's own first
    append would block on a lock it already holds -- via a second file
    descriptor, which flock does not treat as the same holder. That is a
    guaranteed deadlock, not a risk of one.

    Like manifest_lock(), the lock lives on an open descriptor, so the kernel
    releases it the instant a killed run dies, and the file is never unlinked.
    """
    fh = RUN_LOCK_PATH.open("a+")
    try:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in (errno.EACCES, errno.EAGAIN):
                raise
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    finally:
        fh.close()


def check_manifest_tail_before_run():
    """Run the tail check ONCE at startup, before anything else happens.

    THE DEFECT THIS CLOSES: check_manifest_tail() was reachable only through
    append_row(), so a run that skipped every capture never called it at all.
    Tear the last row of captures.csv and re-run on the same UTC date and the
    old code exited 0 without a word -- and worse, the torn, unchained row was
    still read by read_ok_captures() as proof that its board had landed, so it
    SUPPRESSED the capture that would have replaced it. The damage then sat
    there until the next day's run tried to append, by which time a day of
    boards had been lost to it.

    A torn manifest is not a condition to discover half an hour into a browser
    session. It is a reason not to start one.

    The call inside append_row() stays. It guards a different moment -- the
    instant before a specific row is chained onto the tail, which may be 22
    appends and six minutes after this one -- and this check cannot speak for
    that one.
    """
    with manifest_lock():
        check_manifest_tail()


def check_manifest_tail():
    """Refuse to append to a manifest whose tail is not intact.

    Holds no lock of its own: the caller MUST already hold manifest_lock(),
    and must call this BEFORE migrate_manifest(), because migrating rewrites
    the whole file and would silently paper over -- or worse, consume -- the
    damage this function exists to find.

    Four failure modes, all fatal, none a warning:

    1. NO TRAILING NEWLINE. A csv writer opened in append mode starts exactly
       where the file ends. If the final byte is not "\\n", the next row welds
       onto the previous line. csv.DictReader then hands the surplus fields to
       `restkey` and the welded row ceases to exist AS A ROW: the capture
       writes its HTML and its JSON, logs nothing wrong, and exits 0 with no
       manifest row. Silent, total, and invisible from the exit code -- the
       single worst thing this script could do.

    2. A SHORT OR UNHASHED FINAL ROW. A process killed mid-append leaves a
       truncated last record. read_prev_hash() would then return None or "",
       the next row would hash over the string "None" while writing "" into
       the prev_hash cell, and that row would be permanently unverifiable.
       So: the last row must have exactly as many fields as the header, and a
       non-empty row_hash. It must also pass manifest_row_problems(), since it
       is the row the next row's hash is chained to.

    3. A ROW_HASH THAT DOES NOT REPRODUCE. Checking that row_hash is PRESENT
       was not enough, and the gap was not theoretical. Consider failure mode
       1 one run later: row N was welded onto row N-1 by the old script, and
       row N+1's own append terminated that line with a newline. The file now
       ends in "\\n", so check 1 passes. The welded line, split on commas, can
       carry exactly as many fields as the header -- the weld lands inside the
       first cell, so only capture_id is mangled and everything after it
       shifts back into place -- so check 2 passes. row_hash is present and
       well-formed, because it is row N's real hash. Every existing check
       passes, the run chains happily from a corrupted row and exits 0.
       So: RECOMPUTE the tail row's row_hash from its own cells and compare.
       That also closes the plain case of a hand-edited tail row, where some
       cell was changed and the stored hash left behind.

    4. A SEVERED CHAIN LINK. The tail row's prev_hash must equal the stored
       row_hash of the row before it (or the genesis constant when the tail IS
       the first row). Check 3 proves the tail row is internally consistent;
       this proves it is attached to the history. Without it, a row could be
       deleted from the middle of the file, or a whole forged row spliced in
       with a correctly recomputed self-hash, and the next append would extend
       the broken chain as if nothing had happened.

    Checks 3 and 4 are deliberately limited to the TAIL, not the whole file:
    this function guards an append, and an append can only ever be wrong about
    the row it chains from. Walking every row is `verify` (step 4), which
    reports all problems instead of dying on the first.

    THE DELIBERATE DECISION: on any of these, append NOTHING -- not even a
    FAIL row. Writing a FAIL row into a manifest whose tail is already
    malformed appends to the corruption just detected, and very possibly welds
    that FAIL row onto the broken line, destroying the evidence of what
    happened. An intact chain plus a loud log is worth more than a row. The
    run logs exactly what is wrong and on which line, exits 1, and leaves the
    raw artifacts on disk as proof the capture itself ran.
    """
    if not MANIFEST_PATH.exists():
        return                      # a fresh file gets a header, not a weld
    data = MANIFEST_PATH.read_bytes()
    if not data:
        return                      # ditto: zero bytes, nothing to weld onto

    if not data.endswith(b"\n"):
        line_num = data.count(b"\n") + 1
        tail = data.rsplit(b"\n", 1)[-1][-160:].decode("utf-8", "replace")
        raise ManifestIntegrityError(
            "%s line %d does not end in a newline. Appending now would weld "
            "the next row onto that line; csv.DictReader would dump the "
            "surplus into `restkey` and the welded row would vanish from the "
            "manifest entirely. Nothing was appended. Unterminated tail: %r"
            % (MANIFEST_PATH.name, line_num, tail))

    with MANIFEST_PATH.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        last, last_line = None, None
        # kept only to check the tail's prev_hash against its real predecessor
        second_last, second_last_line = None, None
        for record in reader:
            if not record:
                continue            # a wholly blank line carries no row
            second_last, second_last_line = last, last_line
            last, last_line = record, reader.line_num

    if header is None:
        raise ManifestIntegrityError(
            "%s has bytes but no header row; refusing to append to a file "
            "whose columns are unknown. Nothing was appended."
            % MANIFEST_PATH.name)
    if "row_hash" not in header:
        raise ManifestIntegrityError(
            "%s header has no row_hash column (header: %s); the chain cannot "
            "be extended. Nothing was appended."
            % (MANIFEST_PATH.name, ", ".join(header)))
    if last is None:
        return                      # header only: the chain starts at genesis

    if len(last) != len(header):
        raise ManifestIntegrityError(
            "%s line %d has %d field(s) but the header declares %d -- the "
            "last row is truncated or run together, most likely a capture "
            "killed mid-append. Chaining from it would produce a permanently "
            "unverifiable row. Nothing was appended. First cell: %r"
            % (MANIFEST_PATH.name, last_line, len(last), len(header),
               last[0] if last else ""))

    tail_row = dict(zip(header, last))
    if not str(tail_row.get("row_hash") or "").strip():
        raise ManifestIntegrityError(
            "%s has an empty row_hash on its last row (%s). A row that was "
            "never hashed cannot be chained from: the next row would record "
            "prev_hash=\"\" and nothing would ever prove the two belong "
            "together. Nothing was appended."
            % (MANIFEST_PATH.name, row_label(tail_row, last_line)))

    problems = manifest_row_problems(tail_row, last_line)
    if problems:
        raise ManifestIntegrityError(
            "%s cannot be chained from: %s Nothing was appended."
            % (MANIFEST_PATH.name, " ".join(problems)))

    # Check 3: the tail row must reproduce its own row_hash. manifest_row_
    # problems() above has already established that the schema_version is one
    # this script has a column list for, so row_hash() cannot raise here.
    stored = str(tail_row.get("row_hash")).strip()
    computed = row_hash(tail_row)
    if computed != stored:
        raise ManifestIntegrityError(
            "%s line %d (%s) does not reproduce its own row_hash: stored %s, "
            "recomputed %s over the schema-v%s columns. The row's cells and "
            "its hash disagree, so either a cell was edited after the row was "
            "written, or the line is two rows welded together by an append "
            "onto a file with no trailing newline (the weld hides inside "
            "capture_id and leaves the field COUNT correct, which is exactly "
            "why counting fields was not enough). Chaining from it would sign "
            "the corruption into the next row. Nothing was appended."
            % (MANIFEST_PATH.name, last_line, row_label(tail_row),
               stored or "(empty)", computed,
               tail_row.get("schema_version") or "1"))

    # Check 4: the tail row must be attached to the row before it.
    if second_last is None:
        expected_prev = GENESIS_PREV_HASH
        prev_desc = "the genesis constant (the tail is the first row)"
    else:
        prev_row = dict(zip(header, second_last))
        expected_prev = str(prev_row.get("row_hash") or "").strip()
        prev_desc = "the stored row_hash of %s" % row_label(prev_row,
                                                            second_last_line)
    found_prev = str(tail_row.get("prev_hash") or "").strip()
    if found_prev != expected_prev:
        raise ManifestIntegrityError(
            "%s line %d (%s) records prev_hash=%s but %s is %s -- the chain "
            "is severed at the tail. A row may have been deleted, reordered, "
            "or spliced in. Appending here would extend a broken chain and "
            "make the break look like history. Nothing was appended."
            % (MANIFEST_PATH.name, last_line, row_label(tail_row),
               found_prev or "(empty)", prev_desc,
               expected_prev or "(empty)"))


def read_prev_hash():
    """row_hash of the last manifest row, or the genesis constant.

    Callers MUST hold manifest_lock() across this and the matching append,
    otherwise two overlapping runs read the same prev_hash and fork the chain,
    and MUST have called check_manifest_tail() first. The blank check below is
    belt-and-braces: check_manifest_tail() has already made it unreachable,
    and it stays because the alternative -- hashing the string "None" -- is
    silent and permanent.
    """
    if not MANIFEST_PATH.exists():
        return GENESIS_PREV_HASH
    last = None
    with MANIFEST_PATH.open("r", newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            last = row
    if last is None:
        return GENESIS_PREV_HASH
    prev = str(last.get("row_hash") or "").strip()
    if not prev:
        raise ManifestIntegrityError(
            "last row of %s has no row_hash to chain from (%s)"
            % (MANIFEST_PATH.name, row_label(last)))
    return prev


def append_manifest_row(row):
    """Append one row. Caller holds the lock and has run check_manifest_tail().

    The row is validated before it is written, so the schema rules in
    manifest_row_problems() guard the way IN as well as the way out: no path
    through this script can put a row on disk that `verify` would then have to
    flag.
    """
    problems = manifest_row_problems(row)
    if problems:
        raise ManifestIntegrityError(
            "refusing to append an invalid manifest row: %s" % " ".join(problems))
    is_new = not MANIFEST_PATH.exists() or MANIFEST_PATH.stat().st_size == 0
    with MANIFEST_PATH.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=MANIFEST_COLUMNS,
                                lineterminator="\n")
        if is_new:
            writer.writeheader()
        writer.writerow(row)
        fh.flush()
        os.fsync(fh.fileno())
    if is_new:
        # On the very first run captures.csv itself is a brand-new directory
        # entry in fomo/. The fsync above committed its bytes and said nothing
        # about its NAME -- exactly the gap fsync_dir() exists to close for
        # raw/<date>/, and the manifest had been left out of it.
        fsync_dir(BASE_DIR)


def migrate_manifest():
    """Widen an older header to MANIFEST_COLUMNS. NEVER narrow one.

    Caller holds the lock, and has already run check_manifest_tail().

    Existing field VALUES are never touched -- new columns are added empty --
    and empty schema_version keeps those rows on the v1 hash rule, so their
    stored row_hash values still reproduce byte for byte.

    ADD-ONLY, deliberately. Rewriting the header to exactly MANIFEST_COLUMNS
    whenever it differs means an OLDER checkout of this script silently
    DELETES columns a newer one added, taking the data in them with it -- and
    the deletion is invisible, because the rows it rewrites still hash fine
    under their own schema version. So a header carrying a column this script
    does not know is a fatal error that changes NOTHING on disk. The fix is to
    run the newer script, not to drop the column.
    """
    if not MANIFEST_PATH.exists() or MANIFEST_PATH.stat().st_size == 0:
        return
    with MANIFEST_PATH.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        existing = reader.fieldnames
        if existing is None:
            return

        if len(set(existing)) != len(existing):
            dupes = sorted({c for c in existing if existing.count(c) > 1})
            raise ManifestIntegrityError(
                "%s header repeats column(s): %s. A duplicated column name "
                "makes every row ambiguous. Nothing was changed."
                % (MANIFEST_PATH.name, ", ".join(dupes)))

        unknown = [c for c in existing if c not in MANIFEST_COLUMNS]
        if unknown:
            raise ManifestIntegrityError(
                "%s header carries %d column(s) this script does not know: "
                "%s. Rewriting the header to this script's layout would "
                "DELETE them and every value in them -- almost certainly the "
                "work of a newer capture_leaderboard.py. Nothing was changed; "
                "run the newer script instead."
                % (MANIFEST_PATH.name, len(unknown), ", ".join(unknown)))

        if existing == MANIFEST_COLUMNS:
            return                  # already current: idempotent, no rewrite
        rows = list(reader)

    added = [c for c in MANIFEST_COLUMNS if c not in existing]
    tmp = MANIFEST_PATH.with_suffix(".csv.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=MANIFEST_COLUMNS,
                                lineterminator="\n", restval="")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") or "" for k in MANIFEST_COLUMNS})
        fh.flush()
        os.fsync(fh.fileno())
    tmp.replace(MANIFEST_PATH)      # atomic: readers see old or new, never half
    # The rename is a DIRECTORY operation: it changed which inode the name
    # `captures.csv` points at. Fsyncing the temp file committed the new
    # bytes; only fsyncing fomo/ commits the name now pointing at them. Without
    # this, a crash in this window can leave captures.csv resolving to the
    # pre-migration inode -- or to nothing at all -- with a widened, fully
    # durable file sitting unreachable beside it.
    fsync_dir(BASE_DIR)
    log.info("manifest header widened to schema v%s: added %s "
             "(%d existing rows padded, no column dropped)",
             SCHEMA_VERSION, ", ".join(added) or "(reordered only)", len(rows))


# ----------------------------------------------------------------- helpers --
def iso_z(moment):
    """The one timestamp format written to the manifest. UTC, second-resolution."""
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def capture_date(moment):
    """The UTC date a capture belongs to. THE ONLY PLACE this is derived.

    It is one strftime call, and it is a function anyway because two callers
    must agree on it exactly: new_row() puts it in the capture_date_utc
    column, and the idempotence guard in capture_one() compares against it to
    decide whether today already has this board. Two copies of the same
    expression would work until someone changed one of them, and the failure
    mode of that divergence is a guard that never fires (a duplicate capture
    every run) or one that always fires (a day silently lost).
    """
    return moment.strftime("%Y-%m-%d")


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_durably(path, data):
    """Write bytes and do not return until the kernel says they are on disk.

    The manifest append was already fsynced; the raw artifacts were not, and
    the parent directory never was. That is backwards. PLAN.md calls the raw
    bytes irreplaceable and the CSV derived -- so the derived thing had the
    stronger durability guarantee, and a crash in the window between the raw
    write and the manifest fsync would leave a durably-committed,
    chain-verifying row pinning a file that is short, empty or absent:
    invariant 1 broken while the chain reports all-clear, which is the worst
    shape a failure can take here because nothing complains.

    Pair every call with fsync_dir() on the containing directory -- see there
    for why the file's own fsync is not sufficient -- and do BOTH before the
    manifest row that names the file is appended.
    """
    with path.open("wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())


def fsync_dir(path):
    """fsync a DIRECTORY, so its new entries survive a crash too.

    fsync on a file commits that file's CONTENTS. It says nothing about the
    directory entry that gives the file its name: after a crash the bytes can
    be safely on disk and unreachable, because the directory that named them
    was never committed. raw/<date>/ is usually brand new on the first capture
    of a UTC day, so this is not an exotic case -- it is the first capture of
    every day.

    Opening a directory O_RDONLY and fsyncing the descriptor is the portable
    POSIX idiom and works on Darwin. It is best-effort: if a platform refuses,
    the raw file itself is still fsynced, so we log and carry on rather than
    fail a capture whose bytes are already safe.
    """
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError as exc:                            # noqa: BLE001
        log.warning("could not open %s to fsync its directory entry: %s",
                    path, exc)
        return
    try:
        os.fsync(fd)
    except OSError as exc:                            # noqa: BLE001
        log.warning("could not fsync directory %s: %s", path, exc)
    finally:
        os.close(fd)


def query_pairs(url):
    return parse_qsl(urlparse(url).query, keep_blank_values=True)


def pick_param(url, candidates):
    pairs = query_pairs(url)
    for wanted in candidates:
        for key, value in pairs:
            if key.lower() == wanted:
                return value
    return ""


def selection_fields(url):
    """sort/direction/min_trades/min_days as they actually landed on the wire."""
    if not url:
        return {name: "" for name in QUERY_KEY_CANDIDATES}
    return {name: pick_param(url, cands)
            for name, cands in QUERY_KEY_CANDIDATES.items()}


def is_continuation(url):
    """True iff this wire URL asks for a page AFTER the first.

    See CURSOR_KEY_CANDIDATES for why this matters: the endpoint paginates,
    and PLAN.md captures the first page as served.

    A cursor key that is PRESENT BUT EMPTY is not a continuation -- an empty
    cursor is one way a first page gets spelled -- so the test is on the
    value, never on the key alone.
    """
    for key, value in query_pairs(url):
        name = key.lower()
        if name in CURSOR_KEY_CANDIDATES and value.strip():
            return True
        if (name in FIRST_PAGE_VALUES
                and value.strip() not in FIRST_PAGE_VALUES[name]):
            return True
    return False


def board_url(page_slug):
    """The page URL for one platform, built from its PAGE SLUG.

    The slug, never the wire name: /leaderboard/pumpfun-app is a 404 while
    /leaderboard/pumpfun is the board. See PLATFORMS.

    The timeframe is not in this URL either -- see PERIOD_TAB_SELECTOR, all
    three timeframes and the canary share the one page URL per platform.
    """
    return BOARD_URL_TEMPLATE % page_slug


def url_matches_capture(url, platform, timeframe_days):
    """True iff this wire URL is for the platform and timeframe being captured.

    Both halves matter and both are now load-bearing. `days` is the guard
    against a 7D row carrying 30D bytes -- the response from before a tab
    click, or from a click that never took -- and `platform` is the guard
    against a slug's request landing in another slug's capture. Invariant 10
    is the days half; the platform half is this step's own addition.
    """
    return (pick_param(url, DAYS_KEY_CANDIDATES).strip() == str(timeframe_days)
            and pick_param(url, PLATFORM_KEY_CANDIDATES).strip() == platform)


def setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
    fmt.converter = lambda *a: datetime.now(timezone.utc).timetuple()
    log.setLevel(logging.INFO)
    for handler in (logging.FileHandler(LOG_DIR / "capture.log",
                                        encoding="utf-8"),
                    logging.StreamHandler(sys.stdout)):
        handler.setFormatter(fmt)
        log.addHandler(handler)


# ----------------------------------------------------------------- capture --
def launch_browser(pw):
    """One browser for the whole run, not one per capture.

    22 captures means 22 browser launches if this is done naively, which is
    minutes of pure startup and 22 chances for a launch to fail. The browser
    and one context are opened once in run_captures(); each capture gets a
    fresh PAGE (see fetch_page), which is what actually isolates one capture's
    observed responses from another's.

    HEADLESS stays False: measured twice, headless renders the shell and the
    leaderboard XHR never fires. `method` reports the flags actually in force,
    never a guess.
    """
    try:
        channel = "chrome"
        browser = pw.chromium.launch(channel=channel, headless=HEADLESS,
                                     args=BROWSER_ARGS)
    except Exception as exc:                          # noqa: BLE001
        log.warning("installed Chrome unavailable (%s); "
                    "falling back to bundled chromium", exc)
        channel = "chromium"
        browser = pw.chromium.launch(headless=HEADLESS, args=BROWSER_ARGS)
    method = "playwright-%s-%s" % (
        channel, "headless" if HEADLESS else "headed")
    log.info("browser: %s", method)
    return browser, method


def select_timeframe(page, timeframe_days):
    """Put the board on one timeframe tab. Returns "" or a failure reason.

    This only CLICKS. It deliberately does not decide that the click worked --
    that is wait_for_matching_response()'s job, and the separation is the
    point: a click that silently did nothing and a click that worked look
    identical from here. The only evidence that the page is now showing the
    timeframe we asked for is an XHR carrying that timeframe's `days`.

    Clicking a tab that is already selected is skipped rather than forced: on
    a cold load the page selects 30D itself, and re-clicking the active tab
    may fire no request at all, which would then look like a timeout.
    """
    label = TIMEFRAME_LABEL % timeframe_days
    try:
        page.wait_for_selector(PERIOD_TABLIST_SELECTOR, timeout=TAB_TIMEOUT_MS)
    except Exception as exc:                          # noqa: BLE001
        return ("the timeframe tablist (%s) never appeared, so %s could not "
                "be selected (%s: %s)"
                % (PERIOD_TABLIST_SELECTOR, label, type(exc).__name__, exc))

    # Exact text match, anchored: "1D" must not select "14D", and the label
    # must not be matched against the neighbouring Sort tablist.
    tab = page.locator(PERIOD_TAB_SELECTOR).filter(
        has_text=re.compile(r"^\s*%s\s*$" % re.escape(label)))
    try:
        found = tab.count()
    except Exception as exc:                          # noqa: BLE001
        return ("could not enumerate timeframe tabs for %s (%s: %s)"
                % (label, type(exc).__name__, exc))
    if found != 1:
        return ("expected exactly one %r timeframe tab under %s, found %d -- "
                "the period tablist markup has changed"
                % (label, PERIOD_TABLIST_SELECTOR, found))

    try:
        selected = (tab.first.get_attribute("aria-selected") or "").strip()
    except Exception:                                 # noqa: BLE001
        selected = ""
    if selected.lower() == "true":
        log.info("timeframe tab %s is already selected; not clicking it",
                 label)
        return ""
    try:
        tab.first.click(timeout=TAB_TIMEOUT_MS)
    except Exception as exc:                          # noqa: BLE001
        return ("clicking the %r timeframe tab failed (%s: %s)"
                % (label, type(exc).__name__, exc))
    log.info("clicked timeframe tab %s", label)
    return ""


def wait_for_matching_response(page, observed, platform, timeframe_days):
    """Block until THIS capture observes an XHR for exactly this board.

    The confirmation that a timeframe switch took effect. `observed` holds
    only responses seen by this capture's own page, so there is nothing stale
    in it to accept by accident: a record here matching `days` can only have
    been produced by this page after this capture's tab click (or by its own
    cold load, when the tab was already selected).

    page.wait_for_timeout() is what pumps the Playwright connection, so the
    response handlers actually run while this loop spins. Returns True/False;
    the caller turns False into a named failure reason.
    """
    deadline = time.monotonic() + MATCHING_XHR_TIMEOUT_MS / 1000.0
    while True:
        if any(url_matches_capture(rec["url"], platform, timeframe_days)
               for rec in observed):
            return True
        if time.monotonic() >= deadline:
            return False
        page.wait_for_timeout(100)


def fetch_page(context, page_slug, platform, timeframe_days):
    """Render one board at one timeframe. Returns a dict of evidence.

    Two platform names, and they are not interchangeable: `page_slug` builds
    the URL to navigate to, `platform` is the wire name the observed XHR must
    carry. They differ for pumpfun alone -- see PLATFORMS.

    Raises only if the browser itself is unusable: a board that will not load,
    a tab that will not click and an XHR that never arrives are all FAIL
    conditions with evidence, not exceptions -- the HTML still gets written
    and a FAIL row still gets appended.

    "observed" is one record per XHR matching API_URL_FRAGMENT:
      {"url": str, "body": bytes|None, "body_error": str, "received_at": dt}
    The body is read inside the handler because the response is gone by the
    time the page closes. The list is created HERE, per capture, and the
    listener that fills it belongs to a page that is closed before the next
    capture starts -- so one capture structurally cannot consume another's
    response.

    "harvested_at" is the instant page.content() was taken. It is the fallback
    capture timestamp, used only when no matching body ever arrived; see
    capture_one() for which instant wins.
    """
    observed = []
    url = board_url(page_slug)

    def on_response(response):
        if API_URL_FRAGMENT not in response.url:
            return
        # THE CAPTURE TIMESTAMP, for every capture that gets a body. Stamped
        # HERE, at the moment the matching response arrives, not after the
        # settle: the XHR body is the primary raw artifact, so the honest
        # answer to "when was this board observed" is when its bytes landed.
        # Stamping after page.content() put it a measured +3.01s late, a
        # constant lie -- immaterial at 30D, but the 1D board is a rolling
        # 24h window, so at 1D this number IS the measurement. Exact costs
        # nothing here, so it is exact.
        record = {"url": response.url, "body": None, "body_error": "",
                  "received_at": datetime.now(timezone.utc)}
        try:
            record["body"] = response.body()
        except Exception as exc:                      # noqa: BLE001
            record["body_error"] = "%s: %s" % (type(exc).__name__, exc)
        observed.append(record)

    page = context.new_page()
    try:
        # Listener registered BEFORE navigation. We only observe the page's
        # own requests; we never issue one ourselves (the endpoint 403s).
        page.on("response", on_response)
        page.set_default_timeout(NAV_TIMEOUT_MS)
        log.info("navigating to %s (timeframe %dD, wire platform %r)",
                 url, timeframe_days, platform)

        problems = []
        try:
            page.goto(url, wait_until="domcontentloaded",
                      timeout=NAV_TIMEOUT_MS)
        except Exception as exc:                      # noqa: BLE001
            problems.append("navigation to %s failed (%s: %s)"
                            % (url, type(exc).__name__, exc))
            log.warning("%s", problems[-1])

        tab_problem = select_timeframe(page, timeframe_days)
        if tab_problem:
            problems.append(tab_problem)
            log.warning("%s", tab_problem)

        # The switch is CONFIRMED here or not at all.
        if not wait_for_matching_response(page, observed, platform,
                                          timeframe_days):
            problems.append(
                "no XHR with days=%d and platform=%r arrived within %dms of "
                "selecting the %s tab (%d matching-endpoint response(s) seen: "
                "%s) -- the timeframe switch is unconfirmed, so these bytes "
                "cannot be trusted to be this timeframe's"
                % (timeframe_days, platform, MATCHING_XHR_TIMEOUT_MS,
                   TIMEFRAME_LABEL % timeframe_days, len(observed),
                   ", ".join(sorted({
                       pick_param(r["url"], DAYS_KEY_CANDIDATES) or "?"
                       for r in observed})) or "none"))
            log.warning("%s", problems[-1])

        # Validity gate: the board rendered. Not fatal -- a genuinely empty
        # board must still produce evidence and a FAIL row. This wait runs
        # AFTER the matching XHR has landed, so it covers render time only,
        # not fetch time, which is why it is much shorter than the nav
        # timeout.
        try:
            page.wait_for_selector(BOARD_READY_SELECTOR,
                                   timeout=READY_TIMEOUT_MS)
        except Exception as exc:                      # noqa: BLE001
            problems.append("board never rendered a %s anchor (%s: %s)"
                            % (BOARD_READY_SELECTOR, type(exc).__name__, exc))
            log.warning("%s", problems[-1])
        page.wait_for_timeout(API_SETTLE_MS)

        harvested_at = datetime.now(timezone.utc)
        html = page.content()

        try:
            anchors = len(page.query_selector_all(BOARD_READY_SELECTOR))
        except Exception:                             # noqa: BLE001
            anchors = len(re.findall(r'href="/wallet/', html))
        try:
            text = page.inner_text("body")
        except Exception:                             # noqa: BLE001
            text = html
        return {"html": html, "anchors": anchors, "observed": observed,
                "empty_text": EMPTY_BOARD_TEXT in text,
                "nav_error": "; ".join(problems),
                "harvested_at": harvested_at}
    finally:
        page.close()


def choose_source_url(observed, platform, timeframe_days):
    """The wire record for this capture, or None.

    A record is only a CANDIDATE if BOTH its days param equals this capture's
    timeframe_days and its platform param equals this capture's platform.
    There is no fallback to a non-matching URL: recording one would put a
    source_url on the row that does not describe the bytes captured
    (invariant 10), and at worst would pair 30D bytes with a 7D row.

    Among the candidates: THE FIRST ONE WITH A NON-EMPTY BODY AND NO CURSOR.
    Both halves of that rule are load-bearing and one of them is a correction.

    NO CURSOR, because this endpoint paginates. Every body captured so far
    carries "hasMore": true and a nextCursor, and PLAN.md's NOT IN SCOPE says
    "Load more -- capture the first page as served." A continuation is a
    different slice of the same board, not a better view of it. See
    is_continuation().

    THE FIRST, not the last -- this is the correction. Taking the last was
    justified by "the rendered DOM reflects the most recent response". That
    holds for a RE-FETCH of the same page and fails for pagination: after a
    "load more" the newest response is page 2 while the DOM shows page 1
    followed by page 2. The old rule would then write page 2 into the .json,
    pair it with an HTML artifact whose first row is rank 1, and mark the row
    OK -- a mismatch invisible in the data and, being OK, invisible in the
    exit code too. Demonstrated, not theorised: a page-1 response followed by
    a &cursor=... response for the same days and platform made the old rule
    take the cursor response.

    With continuations excluded, the surviving candidates are all first-page
    fetches of the same board, so "first" and "last" now agree on substance,
    and first is the one the rendered page was built from.

    NON-EMPTY BODY, unchanged and still load-bearing: a request that was
    aborted, or that came back empty, followed by a good one must not hand
    back the broken record and produce an "OK-shaped" row whose primary
    artifact is nothing. So the scan is for the first USABLE first-page
    candidate, not merely the first first-page candidate, and empty-then-good
    still takes the good one.

    When nothing qualifies, a record is still returned where one exists, so
    failure_reasons() can name what was wrong with it -- empty, unavailable,
    or continuation-only -- instead of reporting the vaguer "nothing matched".
    """
    matches = [rec for rec in observed
               if url_matches_capture(rec["url"], platform, timeframe_days)]
    if not matches:
        return None
    for index, rec in enumerate(matches, 1):
        log.info("candidate %d/%d: %s [%s]%s", index, len(matches), rec["url"],
                 "body unavailable: %s" % rec["body_error"]
                 if rec["body"] is None
                 else "%d bytes" % len(rec["body"]),
                 "" if not is_continuation(rec["url"])
                 else " -- CONTINUATION, not eligible")
    first_page = [rec for rec in matches if not is_continuation(rec["url"])]
    if not first_page:
        log.warning("%d candidate(s) matched days=%d platform=%r but every one "
                    "carried a pagination cursor; PLAN.md captures the first "
                    "page as served", len(matches), timeframe_days, platform)
        return matches[-1]
    # b"" is falsy and None is falsy: this is exactly "non-empty retrievable".
    usable = [rec for rec in first_page if rec["body"]]
    if not usable:
        log.warning("%d first-page candidate(s) matched days=%d platform=%r "
                    "but none had a non-empty body",
                    len(first_page), timeframe_days, platform)
        return first_page[-1]
    if len(usable) > 1:
        log.info("%d usable first-page candidates; taking the FIRST -- the "
                 "page as served", len(usable))
    return usable[0]


def one_line(text):
    """Collapse whitespace: the error cell stays one CSV line, always."""
    return " ".join(str(text).split())


def failure_reasons(observed, chosen, platform, timeframe_days, anchors,
                    empty_text, json_path):
    """Every invariant-10 / empty-board condition this capture tripped.

    A FAIL row's `error` must be non-empty AND specific (invariant 9 plus
    PLAN.md's "loud when a capture fails"), so every branch here names what
    was actually wrong rather than "capture failed".

    The nav/readiness trace is deliberately NOT in this list any more; it
    moved to nav_note(), which is where the reasoning for that lives.
    """
    reasons = []
    if not observed:
        reasons.append("no XHR matching %s was observed" % API_URL_FRAGMENT)
    elif chosen is None:
        rec = observed[-1]
        days = pick_param(rec["url"], DAYS_KEY_CANDIDATES)
        plat = pick_param(rec["url"], PLATFORM_KEY_CANDIDATES)
        if days.strip() != str(timeframe_days):
            reasons.append("observed days=%r != timeframe_days=%d"
                           % (days, timeframe_days))
        if plat.strip() != platform:
            reasons.append("observed platform=%r != platform=%r"
                           % (plat, platform))
        reasons.append("no matching XHR among %d observed (last: %s)"
                       % (len(observed), rec["url"]))
    elif is_continuation(chosen["url"]):
        reasons.append(
            "every XHR matching days=%d platform=%r carried a pagination "
            "cursor (last: %s). PLAN.md captures the first page as served, "
            "and a continuation is a later slice of the board, not this "
            "capture's artifact -- so no primary artifact was accepted"
            % (timeframe_days, platform, chosen["url"]))
    elif chosen["body"] is None:
        reasons.append("XHR response body unavailable for %s (%s)"
                       % (chosen["url"], chosen["body_error"] or "empty"))
    elif len(chosen["body"]) == 0:
        # Named separately and treated as FAIL on purpose. A zero-byte body
        # hashes to e3b0c442...b855 -- the sha256 of nothing -- and a row
        # carrying that hash is indistinguishable, at a glance, from a row
        # carrying real evidence. There is no capture here to record.
        reasons.append("XHR response body for %s was ZERO BYTES; an empty "
                       "body is not a capture and the sha256 of nothing is "
                       "not evidence" % chosen["url"])
    if anchors == 0:
        # Still a FAIL even when the JSON body landed, and the json_path is
        # named here rather than quietly downgrading the row. Invariant 3
        # counts wallet anchors IN THE RAW HTML against the parsed rows, so an
        # HTML artifact with no anchors is one step 2 cannot use. Saying so,
        # and saying where the wire bytes are, beats an OK row step 2 would
        # then choke on. READY_TIMEOUT_MS carries the other half of this: it
        # is now long enough that a slow render does not land here.
        reasons.append(
            "zero %s anchors on the rendered board%s"
            % (BOARD_READY_SELECTOR,
               "" if not json_path else
               " (the XHR body IS on disk at %s and pinned by this row, so "
               "the board is recoverable by hand; the HTML artifact is not "
               "usable by the anchor-counting parser of invariant 3)"
               % json_path))
    if empty_text:
        reasons.append("page text contains %r" % EMPTY_BOARD_TEXT)
    return [one_line(r) for r in reasons]


def add_note(row, text):
    """Append one tagged note to a row's `notes` cell, IMMEDIATELY.

    Written straight into the row rather than accumulated in a local list, and
    that is the whole point: a row taken over by failed_capture_row() partway
    through a capture must already carry the notes it has earned -- above all
    the pointer to a refused body already on disk, since that note is the only
    thing naming that file.
    """
    existing = row.get("notes") or ""
    row["notes"] = "; ".join(x for x in (existing, one_line(text)) if x)
    return row["notes"]


def nav_note(nav_error):
    """The nav/readiness trace for `notes`, or "" when the run was clean.

    THE DEFECT THIS CLOSES. The trace used to be appended to the failure list
    as `if reasons and nav_error` -- that is, recorded only when something ELSE
    had already gone wrong. The one case where the trace IS the whole story
    therefore recorded nothing at all. Concretely: the period tablist never
    appears, so no tab is ever clicked; the cold load is 30D and this capture
    wanted 30D, so the confirming XHR arrives anyway; the board renders. The
    row is status=OK with an empty `error`, and by saying nothing it asserts a
    confirmed 30D tab selection -- when in fact the tab machinery was dead and
    the timeframe matched by luck. The bytes are genuinely 30D, so this is an
    honesty gap rather than a wrong number, but the manifest could not tell
    "30D tab confirmed" from "tab machinery broken, cold load happened to
    match", and the only trace was logs/capture.log, which is neither
    hash-pinned nor committed.

    So it is now recorded on the row every time, in a hashed column,
    regardless of whether any other reason fired.

    IT IS NOT BY ITSELF A FAILURE. That is a judgement, so here is the
    reasoning. What makes a capture's bytes trustworthy is not that a click
    happened -- select_timeframe() never claimed to know that, and says so --
    it is that an XHR carrying this capture's own `days` AND `platform` was
    observed by this capture's own page (url_matches_capture,
    wait_for_matching_response). That evidence is independent of how the
    timeframe came to be selected. When the tablist is missing and the wanted
    timeframe is NOT the cold-load one, no matching XHR ever arrives and the
    capture already FAILs on exactly that. The only case left standing is the
    one above, where the wire itself confirms the bytes are what the row says
    they are. Failing that capture would discard a board that is demonstrably
    correct, and PLAN.md forbids retries, so the board would be gone for the
    day. Recording it loudly and keeping the bytes is the better trade -- and
    a reader who wants only captures whose tab machinery was healthy has an
    exact filter: an empty `notes`.
    """
    if not nav_error:
        return ""
    return "nav/readiness: %s" % nav_error


def canary_verdict(item, row, compare_sha):
    """What the canary saw, in words, for its `notes` cell.

    A hash comparison and nothing else -- see the CANARY block at the top of
    this file for why this and not a read of the payload.

    It never touches `status`. Whether the canary captured cleanly is one
    question, answered by its status, and it counts toward the exit code like
    any capture's. Whether the bytes changed is a different question, answered
    here, and it does not.
    """
    days = item["timeframe_days"]
    label = "%s %dD" % (item["compare_to"][0], item["compare_to"][1])
    head = "canary: NOT PART OF THE MEASUREMENT."
    mine = row["raw_json_sha256"]
    if not mine:
        return ("%s No %dD payload was accepted, so there was nothing to "
                "compare against %s -- see `error` for why" % (head, days, label))
    if not compare_sha:
        return ("%s %dD payload sha256=%s, but this run captured no %s payload "
                "to compare it against, so no comparison was made"
                % (head, days, mine, label))
    if mine == compare_sha:
        return ("%s %dD payload sha256=%s is byte-identical to this run's %s "
                "payload, so days=%d is still serving the %s board and stays "
                "correctly out of TIMEFRAMES" % (head, days, mine, label, days,
                                                 label))
    return ("%s CHANGED -- %dD payload sha256=%s DIFFERS from this run's %s "
            "payload sha256=%s. Either days=%d now returns a real %d-day "
            "window, or the %s board churned between the two captures; "
            "re-measure before trusting either reading. This does not affect "
            "the exit code" % (head, days, mine, label, compare_sha, days,
                               days, label))


def append_row(row):
    """Chain and append one manifest row. The only writer of captures.csv.

    read-prev-hash and append are ONE critical section: a launchd
    kickstart-on-wake landing on a manual run must queue, not fork. Order
    inside it matters. check_manifest_tail() runs FIRST, before
    migrate_manifest(), because migrating rewrites the whole file: run the
    other way round it would quietly repair -- or swallow -- exactly the
    damage the tail check exists to refuse to append onto.

    Taken per capture, not per run: 22 captures append 22 rows, and holding
    the lock across a six-minute browser session would be a good way to make
    a second run hang rather than queue.
    """
    with manifest_lock():
        check_manifest_tail()
        migrate_manifest()
        row["prev_hash"] = read_prev_hash()
        row["row_hash"] = row_hash(row)
        append_manifest_row(row)
    log.info("manifest row appended: %s status=%s prev_hash=%s row_hash=%s",
             row["capture_id"], row["status"], row["prev_hash"],
             row["row_hash"])
    return row


def new_row(method, item, moment):
    """Build one manifest row, every column populated. THE ONLY PLACE a row
    dict is constructed or a capture_id is minted.

    Both the success path and the failure path come through here, and they
    have to: they used to build the dict separately, mint the capture_id
    separately and call selection_fields() separately, so adding a column
    meant remembering two places -- while only one of them is exercised on a
    good day, and the one that is not is precisely the path that runs after
    something has already gone wrong.

    The row is born as a FAIL with no evidence and no error. capture_one()
    fills evidence in as each artifact lands and settles `status` last, so a
    row handed to failed_capture_row() half-finished still names every file
    already on disk. See failed_capture_row().

    `moment` is the capture instant -- the arrival of the matching XHR body
    where there is one. capture_id is minted from it here and nowhere else, so
    the id's embedded timestamp and the captured_at_utc column can never
    disagree, and raw/<date>/ is that instant's UTC date. The id also carries
    platform, timeframe and (for a canary) role, so one run's captures are
    distinguishable by filename alone and cannot collide.
    """
    platform = item["platform"]
    days = item["timeframe_days"]
    # "-canary-" in the id, and therefore in both raw filenames on disk. A
    # measured capture adds nothing, so existing ids keep their shape.
    role_tag = "" if item["role"] == ROLE_MEASUREMENT else "-%s" % item["role"]
    row = {
        "capture_id": "%s-%dd%s-%s-%s" % (platform, days, role_tag,
                                          moment.strftime("%Y%m%dT%H%M%SZ"),
                                          uuid.uuid4().hex[:8]),
        "captured_at_utc": iso_z(moment),
        "capture_date_utc": capture_date(moment),
        "platform": platform,          # the WIRE name -- see PLATFORMS
        "timeframe_days": str(days),
        "source_url": "",
        "method": method,
        "status": "FAIL",              # settled last; FAIL until earned
        "row_count": "",               # step 2 parses; nothing counted here
        "raw_path": "",
        "raw_sha256": "",
        "page_text_sha256": "",        # step 5
        "parser_version": PARSER_VERSION,
        "error": "",
        "schema_version": SCHEMA_VERSION,
        "raw_json_path": "",
        "raw_json_sha256": "",
        "capture_role": item["role"],
        "notes": "",
    }
    row.update(selection_fields(""))
    return row


# ------------------------------------------------------------ idempotence --
# PLAN.md step 5 and invariant 12: running `capture` again on the same UTC
# date adds no second capture for anything already OK that date. That is not a
# convenience -- launchd fires this job at 07:30 AND at load AND on wake
# (decision 6), so a laptop opened three times in a day runs this script three
# times, and without a guard that is three sets of raw files and 66 manifest
# rows for 22 boards.
#
# WHAT IS SKIPPED IS A CAPTURE, NOT A PLATFORM. The identity of a capture here
# is the four-tuple below, and each part of it earns its place:
#
#   platform        -- fomo's board is not axiom's.
#   timeframe_days  -- fomo 1D landing does not mean fomo 30D landed.
#   capture_role    -- the canary is (fomo, 14, canary). Leave the role out
#                      and it collides with nothing (no measured capture is
#                      14D), so it would never be skipped and would run a
#                      23rd time every day; worse, if TIMEFRAMES ever regained
#                      14 the canary and the measured 14D board would become
#                      the same key and one would suppress the other.
#   capture_date_utc-- the whole point. A board captured yesterday must be
#                      captured again today.
#
# A FAIL row is deliberately NOT a reason to skip. A board that failed this
# morning is a board this instrument still wants today, and PLAN.md's ban on
# retry sophistication is about not retrying WITHIN a run -- the next fire is
# not a retry framework, it is the next fire.
SKIP_MARK = "skipped"


def capture_key(platform, timeframe_days, role, date_utc):
    """The identity of one capture for idempotence. Used by BOTH sides.

    Both the manifest reader below and the guard in capture_one() build their
    keys here, so a row on disk and an item in the matrix cannot fail to match
    for a reason as silly as int 7 against str "7".

    A MISSING OR BLANK capture_role NORMALISES TO ROLE_MEASUREMENT, and that
    is not a guess: PLAN.md's data model says capture_role is blank for a
    measured capture and "canary" for one that is not, and the canary did not
    exist before the column did. So every pre-v3 row in captures.csv -- all 34
    OK ones, written on 2026-09-18 -- is a measured capture, and without this
    normalisation the guard would look straight past them.

    MISSING AND BLANK ARE TWO DIFFERENT INPUTS and the old code only handled
    one of them. A caller reading a row whose header HAS the column but whose
    cell is empty passes ""; a caller reading a row from a header that has no
    capture_role column at all -- an older checkout, or a clone of an older
    commit, both of which PLAN.md's "get fomo/raw/ onto the remote on day one"
    makes ordinary -- gets None out of dict.get() and passes None. str(None)
    is the four-character string "None", which is perfectly truthy, so
    `str(role).strip() or ROLE_MEASUREMENT` never fired and every key built
    from such a row carried the role "None". No key could then ever match, the
    idempotence guard silently failed OPEN, and every board was captured again
    -- with no line in the log to say why. So None is tested for, not coerced.
    """
    role_text = "" if role is None else str(role).strip()
    return (str(platform).strip(), str(timeframe_days).strip(),
            role_text or ROLE_MEASUREMENT, str(date_utc).strip())


def missing_evidence(row):
    """Why this OK row is not proof its board landed. [] means it is proof.

    A skip suppresses a capture forever -- the day cannot be re-collected --
    so the row it defers to has to be backed by bytes that are actually there.
    Delete an OK row's .html and re-run on the same UTC date and the old guard
    skipped anyway: nothing was recaptured, nothing was logged, exit 0, and
    that board became permanently un-recapturable for that date. An
    append-only record is not allowed to have a state like that in it.

    EXISTENCE AND SIZE ONLY -- stat(), never sha256. Comparing the file
    against raw_sha256 is `verify`'s job (step 4) and belongs there: this
    function runs once per OK row at the start of every run, and hashing 161
    files to decide whether to open a browser is the wrong trade. What it
    catches is the case that actually locks a board out: the file is gone, or
    it is a zero-byte stub, and the row still says OK.

    BLANK-BY-SCHEMA IS NOT DAMAGE, and the two must not be confused.
    raw_json_path arrived with schema v2, so on a v1 row (blank
    schema_version) it is empty BY DEFINITION -- manifest_row_problems()
    rule 2 makes a non-empty one invalid there. Measured on the live file:
    exactly 4 OK rows are v1 (all fomo 30D, 2026-09-18) and all 4 carry a
    blank raw_json_path legitimately; the other 30 pre-v3 OK rows are v2 and
    every one names a .json that is on disk and non-empty. So the test is
    "does this row's own schema cover this column" -- unhashed_columns() -- and
    not a hardcoded list of versions, which is the same rule that will extend
    itself when a v4 adds a column.

    A blank path in a column the row's schema DOES cover is a different thing:
    this script cannot write an OK row without a raw_path or a raw_json_path
    (failure_reasons() makes the absence of a primary artifact a FAIL), so
    such a row is damaged, and damaged is not proof.
    """
    problems = []
    version = str(row.get("schema_version") or "")
    if version not in SCHEMA_COLUMNS:
        return ["schema_version %r is not one this script knows, so nothing "
                "in the row can be read as evidence" % version]
    blank_by_schema = set(unhashed_columns(version))
    for column in ("raw_path", "raw_json_path"):
        named = str(row.get(column) or "").strip()
        if not named:
            if column not in blank_by_schema:
                problems.append(
                    "%s is blank on a schema-v%s row, which covers that column"
                    % (column, version or "1"))
            continue                # blank by schema: there is no file to find
        path = BASE_DIR / named
        try:
            size = path.stat().st_size
        except OSError as exc:      # missing, unreadable, or a broken symlink
            problems.append("%s=%s is not on disk (%s)"
                            % (column, named, exc.strerror or exc))
            continue
        if size == 0:
            problems.append("%s=%s is ZERO BYTES" % (column, named))
    return problems


def read_ok_captures():
    """{capture_key: capture_id} for every OK row in the manifest.

    Read ONCE per run, in run_captures(), BEFORE the browser launches. Not
    once per capture: the file only grows, re-reading it 22 times would give
    22 chances to take the lock and 22 chances to see a different answer, and
    the one row that appears mid-run which this map must know about is a row
    THIS run wrote -- which run_captures() adds to the map directly.

    Under manifest_lock() because another capture process may be part way
    through its own append. Without the lock this could read the file in the
    instant between the writer's write() and its fsync/newline, which is
    exactly the truncated-tail state check_manifest_tail() exists to refuse to
    build on. The lock is released before the browser starts; it is never held
    across a page load.

    The map carries the capture_id, not just the key, so the skip can name the
    row it deferred to. A reader who doubts a skip can go straight to that id
    in captures.csv and in raw/<date>/.

    AN OK ROW ONLY ENTERS THE MAP IF ITS EVIDENCE IS ON DISK. The map is the
    licence to NOT capture a board, so every entry in it has to be backed by
    files -- see missing_evidence() for what is checked and what is
    deliberately left to `verify`. A row that fails that check is not in the
    map, which means its board is captured again today, which is the only
    outcome that gets the bytes back.
    """
    ok = {}
    if not MANIFEST_PATH.exists():
        return ok
    with manifest_lock():
        with MANIFEST_PATH.open("r", newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                if str(row.get("status") or "").strip() != "OK":
                    continue
                gaps = missing_evidence(row)
                if gaps:
                    # Its own line, at WARNING, naming the row and the file:
                    # an OK row whose bytes are gone is a real anomaly, and
                    # silently recapturing would hide it. The recapture is the
                    # right response; being quiet about it is not.
                    log.warning(
                        "OK row NOT accepted as proof and its board will be "
                        "captured again today: %s -- %s",
                        row_label(row), "; ".join(gaps))
                    continue
                key = capture_key(row.get("platform"),
                                  row.get("timeframe_days"),
                                  row.get("capture_role"),
                                  row.get("capture_date_utc"))
                # Last OK row for a key wins. With the guard working there is
                # never more than one, and if history contains duplicates from
                # before it existed, the newest is the one worth naming.
                ok[key] = str(row.get("capture_id") or "")
    return ok


def seconds_to_utc_midnight(moment):
    """How long until the UTC date rolls over. Only preflight_skip() cares."""
    next_midnight = (moment + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    return (next_midnight - moment).total_seconds()


def preflight_skip(item, already_ok, moment):
    """(date_utc, capture_id) if this board can be skipped WITHOUT harvesting.

    Returns None when the capture must actually run. This is the CHEAP half of
    a two-stage check; the authoritative half is the guard in capture_one(),
    which runs after the harvest and uses the date the bytes actually arrived
    on. Both exist, and neither replaces the other:

      * Without this one, a run in which all 22 boards were already captured
        today still opened 22 pages in a visible browser -- about 332 seconds
        of a real Chrome window taking over the screen -- to write nothing at
        all. That is the common case, because launchd fires this job at 07:30,
        at load and on every wake.
      * Without the one in capture_one(), a run that starts before UTC
        midnight and harvests after it writes its rows under the wrong date,
        or skips a board whose capture belonged to the new day.

    THE TRAP IN COMBINING THEM, which is the whole reason this function has a
    clock in it. A pre-flight that only asked "is this board OK for today's
    date" would, at 23:59:58 on day N, see day N already done, skip, and
    RETURN BEFORE the authoritative check could ever run -- so the capture
    that belonged to day N+1 would vanish with no row, no file and no log
    line. That is precisely the loss the post-harvest check was written to
    prevent, handed straight back.

    So the pre-flight may only skip when it is CERTAIN the post-harvest date
    would be the same date. That certainty is exactly two conditions:

      1. the key is OK for the UTC date it is now, and
      2. UTC midnight is further away than the longest a single capture can
         take -- MAX_CAPTURE_SECONDS, composed from this script's own
         timeouts, about 348 seconds.

    Condition 2 is checked at the moment the capture would START, not once for
    the whole run, which is what makes a per-capture bound the right bound: if
    this capture began now it would be finished, one way or another, before
    the date could change.

    Inside that window -- the last ~6 minutes of each UTC day -- this function
    refuses to decide and returns None, the board is harvested in full, and
    the authoritative guard settles it on the real date. That costs one slow
    run a day at worst. It is the cheap direction to be wrong in.
    """
    date_utc = capture_date(moment)
    key = capture_key(item["platform"], item["timeframe_days"], item["role"],
                      date_utc)
    existing_capture_id = already_ok.get(key)
    if not existing_capture_id:
        return None
    if seconds_to_utc_midnight(moment) <= MAX_CAPTURE_SECONDS:
        return None
    return (date_utc, existing_capture_id)


def log_preflight_skip(item, decision):
    """One line for a board skipped before it was ever loaded.

    Says "pre-flight" where capture_one()'s line says "post-harvest", so a
    reader of capture.log can tell which of the two checks made the call
    without counting page loads.
    """
    date_utc, existing_capture_id = decision
    log.info("SKIP (pre-flight) %s %dD [%s]: capture_date_utc=%s already has "
             "an OK capture (%s) whose raw files are on disk. The page was "
             "never loaded; nothing written.",
             item["platform"], item["timeframe_days"], item["role"],
             date_utc, existing_capture_id)


def skipped_capture(item, date_utc, existing_capture_id):
    """The record of a capture that did not happen. NOT a manifest row.

    THE CONTRACT, because the whole run accounting leans on it: capture_one()
    returns either a manifest row dict or one of these, and the two are told
    apart by the SKIP_MARK key, which a manifest row can never carry -- it is
    not in MANIFEST_COLUMNS, and migrate_manifest() refuses a header column it
    does not know, so it cannot become one by accident either. run_captures()
    separates the two into different lists the instant it gets one, so nothing
    downstream has to keep making the distinction.

    A skip writes NOTHING: no raw HTML, no raw JSON, no manifest row, no hash
    chain link. It is not a status; there is no SKIP row in captures.csv, and
    there must not be. The record exists only so report() can show the skip
    and main() can account for it.
    """
    return {SKIP_MARK: True,
            "platform": item["platform"],
            "timeframe_days": str(item["timeframe_days"]),
            "capture_role": item["role"],
            "capture_date_utc": date_utc,
            "existing_capture_id": existing_capture_id}


def capture_one(context, method, item, compare_sha, pending, already_ok):
    """One board at one timeframe: evidence to disk, then exactly one row.

    All-or-nothing per PLAN.md step 3: this returns an OK row or a FAIL row
    with a non-empty `error`, and never a partial board. (There is no parser
    yet, so "zero rows" is automatic -- row_count stays blank either way.)

    OR it returns a skipped_capture() record and writes nothing at all,
    because this board already has an OK row for the UTC date this capture
    would have been stamped with. See the guard below and SKIP_MARK.

    `item` is one entry from build_matrix(): page slug, wire platform,
    timeframe, role, and for the canary the board its hash is compared to.
    `compare_sha` is that board's raw_json_sha256 from earlier in THIS run, or
    "" -- unused except by the canary.

    `already_ok` is run_captures()' live {capture_key: capture_id} map of
    captures that must not be repeated today. Read here, never written here.

    `pending` is how a half-finished row escapes an exception. The row is put
    there the instant it exists and mutated in place afterwards, so if
    anything below raises, run_captures() can hand the partial row to
    failed_capture_row() and the resulting FAIL row still names every file
    already written. Without it, an exception after the HTML was written left
    that file named by no manifest row at all: bytes nothing pins, invisible
    to `verify` because it does not know they exist, and indistinguishable in
    raw/<date>/ from a real capture.
    """
    page_slug = item["page_slug"]
    platform = item["platform"]
    timeframe_days = item["timeframe_days"]

    evidence = fetch_page(context, page_slug, platform, timeframe_days)
    observed = evidence["observed"]
    for rec in observed:
        log.info("observed api response: %s%s", rec["url"],
                 "" if rec["body"] is not None
                 else " [body unavailable: %s]" % rec["body_error"])
    chosen = choose_source_url(observed, platform, timeframe_days)

    # captured_at_utc is the instant the matching XHR body ARRIVED (stamped in
    # fetch_page's on_response). When no matching response arrived at all --
    # always a FAIL -- there is no such instant, so the DEFINED FALLBACK is
    # evidence["harvested_at"]: the moment page.content() was taken. That is
    # the only other instant at which this script provably observed the site,
    # and a row still needs a coherent timestamp for `status` (step 5) to
    # reason about which UTC dates have captures. The fallback is recorded in
    # `error` implicitly -- a row using it is by construction a FAIL row.
    now = chosen["received_at"] if chosen is not None else evidence["harvested_at"]

    # THE AUTHORITATIVE IDEMPOTENCE GUARD (PLAN.md step 5, invariant 12). It
    # sits HERE, on this line and not one line earlier, and that placement is
    # the decision.
    #
    # It tests capture_date(now) -- the capture_date_utc of the row this call
    # would actually write, derived from the instant the bytes arrived -- and
    # never a date read before the browser ran. The two differ for any run
    # straddling UTC midnight, and the pre-harvest date is the wrong one: a
    # run started at 23:59:58 would skip because "today" already had this
    # board, while the row it suppressed belonged to the NEXT day. That is a
    # day silently lost, which is the one loss this whole instrument exists to
    # prevent, and it would happen without a single line in the log to say so.
    #
    # THE COST is paid by this capture alone, and only when it is paid at all.
    # run_captures() runs preflight_skip() before the harvest, so a board that
    # can be skipped SAFELY -- meaning UTC midnight is far enough away that
    # this capture could not outlive the date -- never reaches this line and
    # never loads a page. What arrives here is either a board that genuinely
    # needs capturing or one whose date the pre-flight refused to guess at,
    # and for the second kind the harvest is what buys the right answer.
    # Reaching this line with the key already present is therefore not
    # redundant: it is the near-midnight case being settled correctly.
    #
    # The guard is also why `already_ok` is a live map rather than a snapshot:
    # run_captures() adds each OK row as it lands, so a matrix that ever
    # contained the same capture twice would still write it once.
    key = capture_key(platform, timeframe_days, item["role"], capture_date(now))
    if key in already_ok:
        log.info("SKIP (post-harvest) %s %dD [%s]: capture_date_utc=%s "
                 "already has an OK capture (%s). Nothing written: no raw "
                 "file, no manifest row.", platform, timeframe_days,
                 item["role"], key[3], already_ok[key])
        return skipped_capture(item, key[3], already_ok[key])

    row = new_row(method, item, now)
    pending["row"] = row            # from here on, an exception keeps the row
    capture_id = row["capture_id"]
    log.info("capture_id=%s captured_at_utc=%s role=%s (body arrival; %.2fs "
             "before the page harvest instant)", capture_id,
             row["captured_at_utc"], row["capture_role"],
             (evidence["harvested_at"] - now).total_seconds())

    row["source_url"] = chosen["url"] if chosen is not None else ""
    row.update(selection_fields(row["source_url"]))
    if row["source_url"]:
        log.info("source_url query params: %s", query_pairs(row["source_url"]))

    note = nav_note(evidence["nav_error"])
    if note:
        add_note(row, note)

    # Raw bytes land next, before anything is evaluated: a failed capture must
    # still leave its evidence on disk. They are fsynced, and so is every
    # directory that names them, BEFORE the manifest row that pins them is
    # appended -- see write_durably(). Bytes first, then the row naming them.
    day_dir = RAW_DIR / row["capture_date_utc"]
    day_dir.mkdir(parents=True, exist_ok=True)

    raw_path = day_dir / ("%s.html" % capture_id)
    if raw_path.exists():
        raise RuntimeError("refusing to rewrite raw file: %s" % raw_path)
    # THE ROW NAMES THE FILE BEFORE THE BYTES ARE WRITTEN, never after. The
    # window between "a file exists on disk" and "a manifest row names it" is
    # where an orphan is born, so that window has to be empty -- and an
    # exception can land anywhere in it, including inside write_durably()
    # after open() has already created the file, or inside sha256_file(). A
    # FAIL row naming a file that turned out short or absent is honest;
    # invariant 1 speaks only of OK rows. A file on disk that no row names is
    # not: `verify` cannot check what it does not know exists.
    row["raw_path"] = str(raw_path.relative_to(BASE_DIR))
    write_durably(raw_path, evidence["html"].encode("utf-8"))
    row["raw_sha256"] = sha256_file(raw_path)
    log.info("raw html: %s (%d bytes)", raw_path, raw_path.stat().st_size)

    # The XHR body is the PRIMARY raw artifact -- these are the wire bytes;
    # page.content() is a DOM re-serialisation of them. An empty body is not
    # written at all: there is nothing to write, and a zero-byte artifact
    # pinned by the sha256 of nothing is worse than no artifact. A continuation
    # is not written either -- it is a later slice of the board, and PLAN.md
    # captures the first page as served.
    if (chosen is not None and chosen["body"]
            and not is_continuation(chosen["url"])):
        path = day_dir / ("%s.json" % capture_id)
        if path.exists():
            raise RuntimeError("refusing to rewrite raw file: %s" % path)
        row["raw_json_path"] = str(path.relative_to(BASE_DIR))   # name first
        write_durably(path, chosen["body"])
        row["raw_json_sha256"] = sha256_file(path)
        log.info("raw json: %s (%d bytes)", path, path.stat().st_size)

    # REFUSED BODIES. This file's own contract says the raw bytes land before
    # anything is evaluated, so that a FAIL keeps its evidence -- but the .json
    # was only ever written when the body PASSED the gate, which made that
    # promise false in exactly the case it was written for. The four pumpfun
    # captures of 2026-09-18 each watched a good 100-wallet body go past and
    # then discarded it, because the wire said platform=pumpfun-app while the
    # capture was asking for platform=pumpfun. Those primary bytes are gone and
    # that day cannot be re-collected.
    #
    # So: when this capture accepted NO primary artifact, every non-empty body
    # it did observe is written out anyway.
    #
    # HOW A REFUSED BODY STAYS DISTINGUISHABLE FROM AN ACCEPTED ONE -- three
    # ways, none of which asks a future reader to be careful:
    #   1. It NEVER goes into raw_json_path / raw_json_sha256. Those two
    #      columns mean "this capture's primary artifact" and nothing else, so
    #      every existing reader of them is untouched by this.
    #   2. Its filename is <capture_id>.refused-<n>.json, not
    #      <capture_id>.json. The distinction is in the name on disk.
    #   3. It is named, hashed and described in `notes` on a row whose status
    #      is FAIL -- a refused body can only exist on a capture that accepted
    #      nothing, so no row ever carries both kinds.
    #
    # Bounded on purpose, to the no-accepted-artifact case. When a capture DID
    # accept a first page, the later pages of the same board are not refused
    # evidence of a failure; they are "Load more", which PLAN.md puts out of
    # scope, and writing them would add a file per capture per day for a slice
    # nothing will read.
    if not row["raw_json_path"]:
        for index, rec in enumerate([r for r in observed if r["body"]], 1):
            path = day_dir / ("%s.refused-%d.json" % (capture_id, index))
            if path.exists():
                raise RuntimeError("refusing to rewrite raw file: %s" % path)
            # Named first, for the same reason as the two artifacts above --
            # and here the note is the ONLY thing that will ever name this
            # file, so the ordering matters more, not less. The digest is
            # taken over the observed bytes rather than re-read from disk,
            # because it has to be in the note before the file exists;
            # write_durably() fsyncs, so on a clean return the file holds
            # exactly these bytes, and on a dirty one the row states what
            # should be there for a reader to diff against.
            add_note(row, "refused-body: %s sha256=%s (%d bytes) observed at "
                          "%s -- NOT this capture's artifact, kept as evidence"
                     % (path.relative_to(BASE_DIR),
                        hashlib.sha256(rec["body"]).hexdigest(),
                        len(rec["body"]), rec["url"]))
            write_durably(path, rec["body"])
            log.warning("refused body kept as evidence: %s", path)

    # Every file's CONTENTS are committed above; these three calls commit the
    # directory ENTRIES that name them -- raw/<date>/'s entries, raw/<date>/'s
    # own entry in raw/ (brand new on the first capture of every UTC day), and
    # raw/'s own entry in fomo/ (brand new on the very first run, and the one
    # link in the chain that used to be missing).
    fsync_dir(day_dir)
    fsync_dir(RAW_DIR)
    fsync_dir(BASE_DIR)

    reasons = failure_reasons(observed, chosen, platform, timeframe_days,
                              evidence["anchors"], evidence["empty_text"],
                              row["raw_json_path"])
    row["status"] = "FAIL" if reasons else "OK"
    row["error"] = "; ".join(reasons)

    if item["role"] == ROLE_CANARY:
        add_note(row, canary_verdict(item, row, compare_sha))

    log.info("wallet anchors on rendered board: %d", evidence["anchors"])
    for reason in reasons:
        log.error("FAIL %s %dD: %s", platform, timeframe_days, reason)
    if row["notes"]:
        # ERROR level on purpose: `notes` exists for things that must not be
        # scrolled past, and a changed canary is the loudest of them.
        log.error("NOTE %s %dD: %s", platform, timeframe_days, row["notes"])
    return append_row(row)


def failed_capture_row(method, item, reason, partial=None):
    """A FAIL row for a capture that raised. Carries whatever already landed.

    Reached only when capture_one() itself raised -- a browser that died
    mid-run, say. The alternative, letting the exception end the run, would
    lose every remaining capture, which is the behaviour step 3 exists to
    forbid.

    THE DEFECT THIS CLOSES: this used to build a row from scratch with every
    raw column empty. If capture_one() raised AFTER writing the HTML -- or
    after writing the JSON, or a refused body -- that file was left on disk
    named by no manifest row at all. Orphan bytes: `verify` cannot check them
    because it does not know they exist, and a person reading raw/<date>/
    cannot tell them from a real capture's artifacts. So the partially filled
    row capture_one() was already mutating is passed in here and completed,
    and every artifact it had recorded -- raw_path, raw_json_path and any
    refused-body note -- is carried onto the FAIL row.

    The raw columns come out empty only when there genuinely is no artifact,
    which is the case this function was originally written for. Invariant 1
    speaks only of OK rows, and invariant 9 asks only for exactly one row with
    a non-empty error.
    """
    row = partial if partial is not None else new_row(
        method, item, datetime.now(timezone.utc))
    row["status"] = "FAIL"
    carried = ", ".join("%s=%s" % (name, row[name])
                        for name in ("raw_path", "raw_json_path")
                        if row.get(name))
    if row.get("notes"):
        carried = "; ".join(x for x in (carried, "notes=%s" % row["notes"]) if x)
    detail = one_line(reason)
    if carried:
        detail += (" (artifacts already on disk, carried onto this row: %s)"
                   % carried)
    row["error"] = "; ".join(x for x in (row.get("error") or "", detail) if x)
    log.error("FAIL %s %dD: %s", item["platform"], item["timeframe_days"],
              row["error"])
    return append_row(row)


def build_matrix():
    """Every capture this run will attempt, in order. The one source of truth.

    Each item is one capture:

      page_slug       -- /leaderboard/<page_slug>; the page to navigate to.
      platform        -- the WIRE name. It is what this row's `platform`
                         column says, what its source_url will say, and what
                         url_matches_capture() compares. See PLATFORMS for
                         why the two names exist and why this is the one the
                         manifest records.
      timeframe_days  -- the tab to select, and the `days` the confirming XHR
                         must carry.
      role            -- ROLE_MEASUREMENT or ROLE_CANARY. Never blank.
      compare_to      -- canary only: the (platform, timeframe) whose
                         raw_json_sha256 this capture's is compared against.

    7 platforms x 3 timeframes = 21 measured captures, plus the single canary:
    22 rows per run.

    The canary is inserted directly after fomo's 7D capture rather than
    appended at the end, because its entire signal is a hash comparison
    against that capture and the two payloads should be observed as close
    together as the run allows. It is built here BY NAME, as its own item --
    never as a member of TIMEFRAMES -- so no product of platforms and
    timeframes anywhere in this file yields a 14, and nothing that loops the
    matrix can pick it up as a board.
    """
    items = []
    for page_slug, platform in PLATFORMS:
        for days in TIMEFRAMES:
            items.append({"page_slug": page_slug, "platform": platform,
                          "timeframe_days": days, "role": ROLE_MEASUREMENT,
                          "compare_to": None})
            if page_slug == CANARY_SLUG and days == CANARY_PAIRED_TIMEFRAME:
                items.append({"page_slug": page_slug, "platform": platform,
                              "timeframe_days": CANARY_TIMEFRAME,
                              "role": ROLE_CANARY,
                              "compare_to": (platform,
                                             CANARY_PAIRED_TIMEFRAME)})

    # Loud rather than silent if the constants stop agreeing: a CANARY_SLUG
    # that is not in PLATFORMS, or a CANARY_PAIRED_TIMEFRAME not in
    # TIMEFRAMES, would otherwise just drop the canary without a word.
    canaries = [i for i in items if i["role"] == ROLE_CANARY]
    if len(canaries) != 1:
        raise RuntimeError(
            "expected exactly 1 canary in the matrix, built %d: CANARY_SLUG=%r "
            "must name a page_slug in PLATFORMS and CANARY_PAIRED_TIMEFRAME=%r "
            "must be in TIMEFRAMES=%r"
            % (len(canaries), CANARY_SLUG, CANARY_PAIRED_TIMEFRAME, TIMEFRAMES))
    return items


def run_captures():
    """Every capture in build_matrix(). Returns (rows written, skips).

    ONE CAPTURE FAILING NEVER ABORTS THE RUN -- that is the whole point of
    step 3, and a missed board is a day of that board lost forever. Anything a
    capture raises becomes a FAIL row, carrying any artifact it had already
    written, and the loop carries on.

    The single exception is ManifestIntegrityError, which is re-raised and
    ends the run: it means captures.csv cannot be appended to at all, so
    continuing would just be 21 more failed appends. The raw artifacts already
    written stay on disk as evidence.

    TWO LISTS COME BACK, not one. A row was written to the manifest; a skip
    was not. Keeping them apart from the moment capture_one() returns is what
    stops a skip from ever being counted as a capture -- by report(), by
    main()'s exit code, or by anything added later. Their lengths add up to
    the matrix: see main().
    """
    started_at = datetime.now(timezone.utc)
    items = build_matrix()
    measured = sum(1 for i in items if i["role"] == ROLE_MEASUREMENT)
    log.info("capture run started at %s: %d captures (%d platforms x %d "
             "timeframes = %d measured, plus %d canary)", iso_z(started_at),
             len(items), len(PLATFORMS), len(TIMEFRAMES), measured,
             len(items) - measured)

    # The idempotence guard's source of truth, read once, before anything
    # else happens and well before a browser exists. If captures.csv cannot be
    # read, this raises and the run ends here -- correctly: a run that cannot
    # tell what it already captured today would capture everything twice.
    already_ok = read_ok_captures()
    log.info("idempotence guard: %d OK capture(s) on record in %s with their "
             "raw files present; any of them dated today will be skipped",
             len(already_ok), MANIFEST_PATH.name)

    rows = []
    skipped = []
    # raw_json_sha256 by (platform, timeframe), for the canary's comparison.
    # Filled only from THIS run's own measured captures, so the comparison can
    # never reach across days or pick up a canary's own hash.
    shas = {}

    # THE CHEAP PATH, and the only reason it is spelled out separately from
    # the identical check inside the loop: it is the one decision that has to
    # be made while no browser exists yet. If EVERY board in the matrix can be
    # skipped safely, this run has nothing to do, and the difference between
    # knowing that here and knowing it one line later is 332 seconds of a
    # Chrome window on someone's screen. launchd fires on every wake, so this
    # is the ordinary case, not the exotic one.
    #
    # It is sound for the same reason the in-loop check is sound, and it is
    # deliberately all-or-nothing. Every decision below is taken at one
    # instant, `now`, and if all of them say skip then nothing runs, so no
    # time passes and no capture can drift across UTC midnight. The moment
    # even ONE board must be captured, time starts passing, and the skips for
    # the boards after it are no longer decidable from this instant -- so they
    # are left to the loop, which re-asks at the moment it reaches each one.
    now = datetime.now(timezone.utc)
    preflight = [preflight_skip(item, already_ok, now) for item in items]
    if all(decision is not None for decision in preflight):
        for item, decision in zip(items, preflight):
            log_preflight_skip(item, decision)
            skipped.append(skipped_capture(item, decision[0], decision[1]))
        log.info("nothing to capture: all %d boards already have an OK "
                 "capture for %s with their raw files on disk. No browser "
                 "was launched.", len(skipped), capture_date(now))
        return rows, skipped

    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser, method = launch_browser(pw)
        try:
            # One context for the run; one PAGE per capture (fetch_page).
            # The page is what scopes the observed-response list, so sharing
            # the context costs nothing in isolation and saves 22 cold starts.
            context = browser.new_context(viewport=VIEWPORT, locale="en-US")
            try:
                for index, item in enumerate(items, 1):
                    if index > 1:
                        # Politeness only. Not a retry, not a backoff.
                        time.sleep(INTER_CAPTURE_PAUSE_S)
                    log.info("---- capture %d/%d: %s %dD [%s] ----", index,
                             len(items), item["platform"],
                             item["timeframe_days"], item["role"])
                    # Re-asked HERE, at the instant this capture would start,
                    # and not reused from the pass above: by now the earlier
                    # captures have spent real time, and the answer to "is
                    # midnight still far enough away" may have changed.
                    decision = preflight_skip(item, already_ok,
                                              datetime.now(timezone.utc))
                    if decision is not None:
                        log_preflight_skip(item, decision)
                        skipped.append(
                            skipped_capture(item, decision[0], decision[1]))
                        continue
                    compare_sha = (shas.get(item["compare_to"], "")
                                   if item["compare_to"] else "")
                    pending = {}        # capture_one parks its row here
                    try:
                        result = capture_one(context, method, item,
                                             compare_sha, pending, already_ok)
                    except ManifestIntegrityError:
                        raise
                    except Exception as exc:          # noqa: BLE001
                        result = failed_capture_row(
                            method, item,
                            "capture raised %s: %s"
                            % (type(exc).__name__, exc),
                            pending.get("row"))
                    if result.get(SKIP_MARK):
                        # Nothing was written, so there is nothing to chain,
                        # nothing to hash and nothing for the canary to
                        # compare against. If fomo 7D is skipped while the
                        # canary is not, `shas` simply has no entry and
                        # canary_verdict() says in so many words that no
                        # comparison was made -- it never invents one from
                        # another day's hash.
                        skipped.append(result)
                        continue
                    row = result
                    rows.append(row)
                    if (item["role"] == ROLE_MEASUREMENT
                            and row["raw_json_sha256"]):
                        shas[(item["platform"], item["timeframe_days"])] = \
                            row["raw_json_sha256"]
                    if row["status"] == "OK":
                        # This run's own OK rows join the guard's map as they
                        # land. Today that changes nothing, because no capture
                        # appears in build_matrix() twice -- it is defence
                        # against a future matrix that does, not a live
                        # condition, and it is one line here against a silent
                        # duplicate capture there.
                        already_ok[capture_key(
                            row["platform"], row["timeframe_days"],
                            row["capture_role"],
                            row["capture_date_utc"])] = row["capture_id"]
            finally:
                context.close()
        finally:
            browser.close()

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    log.info("capture run finished in %.1fs", elapsed)
    return rows, skipped


def report(rows, skipped):
    """One line per capture, then the totals. Reading the log should be enough
    to know which boards landed, which did not and why, which were skipped and
    what the canary saw -- without opening captures.csv.

    SKIP is printed in the status column beside OK and FAIL, and it is the one
    value in that column that is NOT a manifest status: no captures.csv row
    ever says SKIP. A skipped line names the capture_id it deferred to, so the
    claim "this board already landed today" is checkable on the spot.
    """
    log.info("---- run summary: %d captured, %d skipped, %d in the matrix ----",
             len(rows), len(skipped), len(rows) + len(skipped))
    for row in rows:
        log.info("%-12s %3sD %-11s %-4s %s", row["platform"],
                 row["timeframe_days"], row["capture_role"], row["status"],
                 row["error"] or row["notes"] or row["capture_id"])
    for rec in skipped:
        log.info("%-12s %3sD %-11s %-4s already OK for %s: %s",
                 rec["platform"], rec["timeframe_days"], rec["capture_role"],
                 "SKIP", rec["capture_date_utc"], rec["existing_capture_id"])
    failed = [r for r in rows if r["status"] != "OK"]
    log.info("OK %d / %d captured; FAIL %d; SKIP %d",
             len(rows) - len(failed), len(rows), len(failed), len(skipped))
    return failed


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("verb", nargs="?", default="capture", choices=["capture"])
    ap.parse_args()
    setup_logging()
    with run_lock() as acquired:
        if not acquired:
            # NOT AN ERROR, and the log must not let anyone think it was.
            # INFO, no traceback, no "FAIL", and the words "NOT A FAILURE" in
            # the line itself -- because the person reading capture.log a week
            # later is scanning for trouble, and a second process standing
            # down politely is the system working exactly as designed.
            log.info("ANOTHER CAPTURE RUN IS IN PROGRESS (it holds %s), so "
                     "this one is standing down. NOT A FAILURE: nothing was "
                     "written -- no raw file, no manifest row, no chain link "
                     "-- and this process exits 0. The run that holds the "
                     "lock is capturing exactly the boards this one would "
                     "have.", RUN_LOCK_PATH.name)
            return 0
        return capture_run()


def capture_run():
    """One whole capture run, start to exit code. Called holding run_lock()."""
    expected = len(build_matrix())
    try:
        # BEFORE THE BROWSER, not inside the first append. A torn manifest
        # tail is a reason not to start a run at all -- and, worse, a torn row
        # would otherwise be read as proof its board landed and suppress the
        # capture that should replace it. See check_manifest_tail_before_run().
        check_manifest_tail_before_run()
        rows, skipped = run_captures()
    except ManifestIntegrityError as exc:
        # Loud on purpose, and specific: the message names the file, the line
        # and what is wrong with it. Nothing was appended -- see
        # check_manifest_tail() for why appending a FAIL row here would make
        # things worse, not better.
        log.error("MANIFEST INTEGRITY FAILURE: %s", exc)
        log.error("captures.csv was NOT modified and NO row was appended. "
                  "Any raw artifacts this run wrote are still on disk as "
                  "evidence. Take a copy of captures.csv, repair the named "
                  "line by hand, then capture again.")
        return 1
    except Exception as exc:                          # noqa: BLE001
        log.error("capture run failed: %s: %s", type(exc).__name__, exc)
        return 1

    failed = report(rows, skipped)
    # Invariant 11, both directions. A run that produced fewer rows than
    # captures is also not a clean run, so it is counted as a failure rather
    # than allowed to exit 0 on a short list. The canary is counted here like
    # any other capture: whether it CAPTURED is an ordinary OK/FAIL question.
    # What the canary SAW -- matching bytes or changed bytes -- is in `notes`
    # and deliberately has no effect on this number. See the CANARY block.
    #
    # THE SUM IS NOW captured + skipped, and invariant 11 is not weakened by
    # that -- it is the same accounting with the same total. The check asks
    # one question, "is every capture in the matrix accounted for", and a skip
    # is an account: it names, in the log, the OK row that already covers that
    # board for that UTC date. Invariant 12 is what makes the skip legitimate,
    # and it is the stronger statement of the two -- the board is on disk, the
    # row is in the chain, they were simply written earlier today.
    #
    # What has NOT changed: a skip is not an OK. `failed` is computed from
    # manifest rows only, so the exit code still tracks exactly the captures
    # this run attempted. A run where all 22 are skipped attempts nothing,
    # fails nothing and exits 0 -- which is the required behaviour, because
    # launchd fires this job on every wake and an exit 1 for "there was
    # nothing to do" would be an alarm that means nothing and would train a
    # reader to ignore the one that does.
    accounted = len(rows) + len(skipped)
    if accounted != expected:
        log.error("expected %d captures this run, accounted for %d "
                  "(%d manifest rows written + %d skipped)",
                  expected, accounted, len(rows), len(skipped))
        return 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
