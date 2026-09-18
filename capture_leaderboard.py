#!/usr/bin/env python3
"""capture_leaderboard.py — FOMO leaderboard capture instrument.

BUILD STEP 1: capture one board (fomo / 30D), raw bytes only, no parsing.
Writes the XHR response body to raw/<date>/<capture_id>.json and the rendered
page source to raw/<date>/<capture_id>.html, then appends one hash-chained row
to captures.csv. Nothing is extracted from the page: the wallet-anchor count
and the "No data yet" check below are a validity gate, not parsing, and
row_count stays blank on every row (step 2 fills it).

The manifest is the only thing here that cannot be regenerated, so it is
defended harder than anything else in the file:

  * The tail of captures.csv is checked BEFORE every append. A file that does
    not end in a newline, or whose last row is short or unhashed, fails the
    run with exit 1 and NO append at all -- see check_manifest_tail().
  * A blank schema_version means the v2 columns are EMPTY. It is never a
    licence to carry unhashed evidence -- see manifest_row_problems().
  * The header may be widened, never narrowed -- see migrate_manifest().

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
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, parse_qsl

# ---------------------------------------------------------------- constants --
BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "raw"
LOG_DIR = BASE_DIR / "logs"
MANIFEST_PATH = BASE_DIR / "captures.csv"
LOCK_PATH = BASE_DIR / ".captures.lock"

PARSER_VERSION = "0.1.0"
SCHEMA_VERSION = "2"
PLATFORM = "fomo"
TIMEFRAME_DAYS = 30
BOARD_URL = "https://www.solanatracker.io/leaderboard/fomo"

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

NAV_TIMEOUT_MS = 60_000
READY_TIMEOUT_MS = 60_000
API_SETTLE_MS = 3_000
LOCK_TIMEOUT_S = 120

GENESIS_PREV_HASH = "0" * 64

# The original 19-hashed-field layout. Frozen forever: rows written before the
# schema_version column existed are hashed over exactly these columns.
MANIFEST_COLUMNS_V1 = [
    "capture_id", "captured_at_utc", "capture_date_utc", "platform",
    "timeframe_days", "sort", "direction", "min_trades", "min_days",
    "source_url", "method", "status", "row_count", "raw_path", "raw_sha256",
    "page_text_sha256", "parser_version", "error", "prev_hash", "row_hash",
]

# v2 inserts three columns between `error` and `prev_hash`. Derived from the
# frozen v1 list so the two layouts can never drift apart.
V2_ADDED_COLUMNS = ["schema_version", "raw_json_path", "raw_json_sha256"]
_CUT = MANIFEST_COLUMNS_V1.index("prev_hash")
MANIFEST_COLUMNS = (MANIFEST_COLUMNS_V1[:_CUT] + V2_ADDED_COLUMNS
                    + MANIFEST_COLUMNS_V1[_CUT:])

SCHEMA_COLUMNS = {"": MANIFEST_COLUMNS_V1, "2": MANIFEST_COLUMNS}

# The v2 columns that carry EVIDENCE. `schema_version` itself is the
# declaration, not evidence, so it is excluded. A row that declares itself v1
# (blank schema_version) must leave every column in this list empty; the v1
# hash does not cover them, so anything sitting here on such a row is data
# nobody signed. See manifest_row_problems().
V2_EVIDENCE_COLUMNS = [c for c in V2_ADDED_COLUMNS if c != "schema_version"]

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

    2. A BLANK `schema_version` means ONE thing, and only one: this is a
       schema-v1 row, and its v2 columns (V2_EVIDENCE_COLUMNS) are EMPTY.
       It is NOT a declaration that v2 columns may be ignored. The v1 hash
       does not cover those columns, so a blank-version row carrying a
       non-empty raw_json_path or raw_json_sha256 would be evidence that no
       hash signs -- and a row shaped exactly like that is how a forged
       capture would walk in through the front door and still verify clean.
       Such a row is invalid: either it is v2 and must say so, or the columns
       must be empty.

    The four rows this file was born with have those columns empty, so they
    remain valid v1 rows under this tightened reading and their stored
    row_hash values are untouched by it.
    """
    version = row.get("schema_version") or ""
    if version not in SCHEMA_COLUMNS:
        return [unknown_schema_message(row, version, line_num)]

    problems = []
    if version == "":
        label = row_label(row, line_num)
        for name in V2_EVIDENCE_COLUMNS:
            value = str(row.get(name) or "")
            if value.strip():
                problems.append(
                    "%s leaves schema_version blank -- which declares it a v1 "
                    "row whose hash does NOT cover %s -- yet carries %s=%r. "
                    "That is unhashed evidence. A v1 row must have every v2 "
                    "column (%s) empty; a row with data in them must declare "
                    "schema_version=%r."
                    % (label, name, name, value,
                       ", ".join(V2_EVIDENCE_COLUMNS), SCHEMA_VERSION))
    return problems


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
                       "the v2 columns are empty", never "ignore whatever is
                       in them" (manifest_row_problems(), rule 2).
           v2 ("2") -> MANIFEST_COLUMNS     (23 names; schema_version,
                       raw_json_path, raw_json_sha256 inserted between `error`
                       and `prev_hash`).
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


def check_manifest_tail():
    """Refuse to append to a manifest whose tail is not intact.

    Holds no lock of its own: the caller MUST already hold manifest_lock(),
    and must call this BEFORE migrate_manifest(), because migrating rewrites
    the whole file and would silently paper over -- or worse, consume -- the
    damage this function exists to find.

    Two failure modes, both fatal, neither a warning:

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
        for record in reader:
            if not record:
                continue            # a wholly blank line carries no row
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
    log.info("manifest header widened to schema v%s: added %s "
             "(%d existing rows padded, no column dropped)",
             SCHEMA_VERSION, ", ".join(added) or "(reordered only)", len(rows))


# ----------------------------------------------------------------- helpers --
def iso_z(moment):
    """The one timestamp format written to the manifest. UTC, second-resolution."""
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def url_matches_capture(url):
    """True iff this wire URL is for the platform and timeframe being captured."""
    return (pick_param(url, DAYS_KEY_CANDIDATES).strip() == str(TIMEFRAME_DAYS)
            and pick_param(url, PLATFORM_KEY_CANDIDATES).strip() == PLATFORM)


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
def fetch_page(observed):
    """Render the board. Returns a dict of evidence; raises only if unusable.

    `observed` is appended one record per matching XHR:
    {"url": str, "body": bytes|None, "body_error": str}. The body is read
    inside the handler because the response is gone by the time the page
    closes.

    The returned "captured_at" is THE capture timestamp: see the comment at
    the stamp site below. It is deliberately produced here, not by the caller.
    """
    from playwright.sync_api import sync_playwright

    def on_response(response):
        if API_URL_FRAGMENT not in response.url:
            return
        record = {"url": response.url, "body": None, "body_error": ""}
        try:
            record["body"] = response.body()
        except Exception as exc:                      # noqa: BLE001
            record["body_error"] = "%s: %s" % (type(exc).__name__, exc)
        observed.append(record)

    with sync_playwright() as pw:
        # `method` reports the flags actually in force, never a guess.
        try:
            channel = "chrome"
            browser = pw.chromium.launch(channel=channel, headless=HEADLESS,
                                         args=BROWSER_ARGS)
        except Exception as exc:                      # noqa: BLE001
            log.warning("installed Chrome unavailable (%s); "
                        "falling back to bundled chromium", exc)
            channel = "chromium"
            browser = pw.chromium.launch(headless=HEADLESS, args=BROWSER_ARGS)
        method = "playwright-%s-%s" % (
            channel, "headless" if HEADLESS else "headed")
        log.info("browser: %s", method)
        try:
            context = browser.new_context(viewport=VIEWPORT, locale="en-US")
            page = context.new_page()
            # Listener registered BEFORE navigation. We only observe the
            # page's own request; we never issue one ourselves.
            page.on("response", on_response)
            page.set_default_timeout(NAV_TIMEOUT_MS)
            log.info("navigating to %s", BOARD_URL)
            nav_error = ""
            try:
                page.goto(BOARD_URL, wait_until="domcontentloaded",
                          timeout=NAV_TIMEOUT_MS)
                # Validity gate: wait for the board to render. A timeout is
                # NOT fatal here -- we still want the evidence on disk and a
                # FAIL row, so it falls through to the zero-anchor check.
                page.wait_for_selector(BOARD_READY_SELECTOR,
                                       timeout=READY_TIMEOUT_MS)
            except Exception as exc:                  # noqa: BLE001
                nav_error = "%s: %s" % (type(exc).__name__, exc)
                log.warning("board did not become ready: %s", nav_error)
            page.wait_for_timeout(API_SETTLE_MS)

            # THE CAPTURE TIMESTAMP. The board is now either confirmed
            # rendered or provably not, and the next statement harvests its
            # content: this instant is when the site was actually observed.
            # Stamping it before the browser ran (as this did) made it 5-10s
            # early -- immaterial at 30D, but the 1D board is a rolling 24h
            # window, so at 1D this number IS the measurement: the analysis
            # discards pairs whose windows overlap too much and can only do
            # that from an honest timestamp. capture_id embeds this same
            # instant (see capture()), so the id and the column never disagree.
            captured_at = datetime.now(timezone.utc)
            html = page.content()

            try:
                anchors = len(page.query_selector_all(BOARD_READY_SELECTOR))
            except Exception:                         # noqa: BLE001
                anchors = len(re.findall(r'href="/wallet/', html))
            try:
                text = page.inner_text("body")
            except Exception:                         # noqa: BLE001
                text = html
            return {"html": html, "method": method, "anchors": anchors,
                    "empty_text": EMPTY_BOARD_TEXT in text,
                    "nav_error": nav_error, "captured_at": captured_at}
        finally:
            browser.close()


def choose_source_url(observed):
    """The wire record for this capture, or None.

    A URL is only usable if BOTH its days param equals TIMEFRAME_DAYS and its
    platform param equals PLATFORM. There is no fallback to a non-matching
    URL: recording one would put a source_url on the row that does not
    describe the bytes captured (invariant 10).
    """
    matches = [rec for rec in observed if url_matches_capture(rec["url"])]
    if len(matches) > 1:
        log.warning("%d observed URLs match; using the first", len(matches))
    return matches[0] if matches else None


def one_line(text):
    """Collapse whitespace: the error cell stays one CSV line, always."""
    return " ".join(str(text).split())


def failure_reasons(observed, chosen, anchors, empty_text, nav_error):
    """Every invariant-10 / empty-board condition this capture tripped."""
    reasons = []
    if not observed:
        reasons.append("no XHR matching %s was observed" % API_URL_FRAGMENT)
    elif chosen is None:
        rec = observed[0]
        days = pick_param(rec["url"], DAYS_KEY_CANDIDATES)
        plat = pick_param(rec["url"], PLATFORM_KEY_CANDIDATES)
        if days.strip() != str(TIMEFRAME_DAYS):
            reasons.append("observed days=%r != timeframe_days=%d"
                           % (days, TIMEFRAME_DAYS))
        if plat.strip() != PLATFORM:
            reasons.append("observed platform=%r != platform=%r"
                           % (plat, PLATFORM))
        reasons.append("no matching XHR among %d observed (first: %s)"
                       % (len(observed), rec["url"]))
    elif chosen["body"] is None:
        reasons.append("XHR response body unavailable for %s (%s)"
                       % (chosen["url"], chosen["body_error"] or "empty"))
    if anchors == 0:
        reasons.append("zero %s anchors on the rendered board"
                       % BOARD_READY_SELECTOR)
    if empty_text:
        reasons.append("page text contains %r" % EMPTY_BOARD_TEXT)
    if reasons and nav_error:
        reasons.append("nav/readiness error: %s" % nav_error)
    return [one_line(r) for r in reasons]


def capture():
    started_at = datetime.now(timezone.utc)
    log.info("capture run started at %s", iso_z(started_at))

    observed = []
    page_info = fetch_page(observed)

    # captured_at_utc comes from INSIDE fetch_page -- the moment the rendered
    # board was harvested -- not from before the browser launched. capture_id
    # is minted from that same instant, here and nowhere else, because the id
    # names the raw files and the raw files must be written before any gate
    # condition is evaluated (a failed capture still leaves its evidence).
    # So: harvest -> stamp -> mint id -> write raw -> evaluate -> append row.
    now = page_info["captured_at"]
    captured_at_utc = iso_z(now)
    capture_date = now.strftime("%Y-%m-%d")
    capture_id = "%s-%dd-%s-%s" % (PLATFORM, TIMEFRAME_DAYS,
                                   now.strftime("%Y%m%dT%H%M%SZ"),
                                   uuid.uuid4().hex[:8])
    log.info("capture_id=%s captured_at_utc=%s (%.1fs after run start; the "
             "browser work is inside that gap, not after it)",
             capture_id, captured_at_utc, (now - started_at).total_seconds())

    # Raw bytes land next, before anything is evaluated: a failed capture
    # must still leave its evidence on disk.
    day_dir = RAW_DIR / capture_date
    day_dir.mkdir(parents=True, exist_ok=True)
    raw_path = day_dir / ("%s.html" % capture_id)
    if raw_path.exists():
        raise RuntimeError("refusing to rewrite raw file: %s" % raw_path)
    raw_path.write_text(page_info["html"], encoding="utf-8")
    log.info("raw html: %s (%d bytes)", raw_path, raw_path.stat().st_size)

    for rec in observed:
        log.info("observed api response: %s%s", rec["url"],
                 "" if rec["body"] is not None
                 else " [body unavailable: %s]" % rec["body_error"])
    chosen = choose_source_url(observed)

    # The XHR body is the PRIMARY raw artifact -- these are the wire bytes;
    # page.content() is a DOM re-serialisation of them.
    json_path = ""
    json_sha = ""
    if chosen is not None and chosen["body"] is not None:
        path = day_dir / ("%s.json" % capture_id)
        if path.exists():
            raise RuntimeError("refusing to rewrite raw file: %s" % path)
        path.write_bytes(chosen["body"])
        json_path = str(path.relative_to(BASE_DIR))
        json_sha = sha256_file(path)
        log.info("raw json: %s (%d bytes)", path, path.stat().st_size)

    reasons = failure_reasons(observed, chosen, page_info["anchors"],
                              page_info["empty_text"], page_info["nav_error"])
    source_url = chosen["url"] if chosen is not None else ""
    if source_url:
        log.info("source_url query params: %s", query_pairs(source_url))
    log.info("wallet anchors on rendered board: %d", page_info["anchors"])

    row = {
        "capture_id": capture_id,
        "captured_at_utc": captured_at_utc,
        "capture_date_utc": capture_date,
        "platform": PLATFORM,
        "timeframe_days": str(TIMEFRAME_DAYS),
        "source_url": source_url,
        "method": page_info["method"],
        "status": "FAIL" if reasons else "OK",
        "row_count": "",          # step 2 parses; nothing counted here
        "raw_path": str(raw_path.relative_to(BASE_DIR)),
        "raw_sha256": sha256_file(raw_path),
        "page_text_sha256": "",   # step 5
        "parser_version": PARSER_VERSION,
        "error": "; ".join(reasons),
        "schema_version": SCHEMA_VERSION,
        "raw_json_path": json_path,
        "raw_json_sha256": json_sha,
    }
    row.update(selection_fields(source_url))

    # read-prev-hash and append are ONE critical section: a launchd
    # kickstart-on-wake landing on a manual run must queue, not fork.
    # Order inside it matters. check_manifest_tail() runs FIRST, before
    # migrate_manifest(), because migrating rewrites the whole file: run the
    # other way round it would quietly repair -- or swallow -- exactly the
    # damage the tail check exists to refuse to append onto.
    with manifest_lock():
        check_manifest_tail()
        migrate_manifest()
        row["prev_hash"] = read_prev_hash()
        row["row_hash"] = row_hash(row)
        append_manifest_row(row)
    log.info("manifest row appended: status=%s prev_hash=%s row_hash=%s",
             row["status"], row["prev_hash"], row["row_hash"])
    for reason in reasons:
        log.error("FAIL: %s", reason)
    return row


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("verb", nargs="?", default="capture", choices=["capture"])
    ap.parse_args()
    setup_logging()
    try:
        row = capture()
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
        log.error("capture failed: %s: %s", type(exc).__name__, exc)
        return 1
    return 0 if row["status"] == "OK" else 1


if __name__ == "__main__":
    sys.exit(main())
