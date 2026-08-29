"""Rebuild a historical blob from the snapshot archive.

Pick a past session and a snapshot time, get the blob that session would have
produced, and paste it into the indicator with TradingView's bar replay parked on
that day. Nothing is fetched and nothing is written -- this reads
`archive/<session-date>/<TICKER>_<quote-stamp>Z.csv.gz` and nothing else.

Two rules are what make this a replay rather than a reconstruction:

  * **The maths and the encoder are not forked.** `record.build_record` and
    `encode.encode_blob` are the same functions the live path calls, run over
    chains rebuilt out of the CSV. A second implementation would drift, and a
    drifting replay is the worst outcome available: it would look authoritative
    while disagreeing with what the chart actually showed on the day.
  * **No wall-clock filtering, anywhere.** The archived chain was already reduced
    when it was written -- `cboe.reduce_payload` dropped the settled expiries as of
    *that* session, which is the only moment that filter is meaningful. Re-applying
    an "expired versus now" test on read would drop every contract that has expired
    since, which on any past session is most of the chain and is always the whole
    0DTE book: precisely the thing a replay exists to show, deleted silently. So
    the session date comes from the CSV column, the expiry maths from the archived
    quote stamp, and today's date is not consulted at all.
"""

import csv
import gzip
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List

from .archive import DEFAULT_ARCHIVE_DIR
from .cboe import ASSUMED_FEED_DELAY, Chain, Contract
from .encode import encode_blob
from .nytime import NY, now_utc
from .pipeline import RunResult
from .record import build_record
from .symbols import DEFAULT_SYMBOLS, to_ticker

# `<TICKER>_<YYYYMMDDTHHMMSSZ>.csv.gz`, written by archive.snapshot_path.
_FILENAME_RE = re.compile(r"^([A-Z0-9]+)_(\d{8}T\d{6}Z)\.csv\.gz$")
_STAMP_FORMAT = "%Y%m%dT%H%M%SZ"

# Columns a snapshot must carry to be replayable at all. `spot_ts_utc` is
# deliberately absent from this list: it was appended to the archive later, and
# every file written before that has to keep working.
_REQUIRED_COLUMNS = ("ticker", "quote_ts_utc", "session_date", "spot", "occ",
                     "expiry", "right", "strike", "open_interest", "gamma", "iv",
                     "volume")

# One run writes one file per symbol, and each file carries CBOE's own quote stamp
# for that symbol -- so the files from a single run are seconds apart, not
# identical. The 2026-08-04 archive spreads 63 seconds across nine symbols
# (IWM 18:34:22 through TSLA 18:35:25). A "snapshot" is therefore a cluster of
# files rather than one stamp, and the cluster is closed by either test below:
#
#   * a gap wider than this to the next file, or
#   * the ticker turning up a second time.
#
# The second test is the one that does the work. Two runs on 2026-08-04 sit 102
# seconds apart (19:43:38 to 19:45:20), closer together than one run's own internal
# spread, so a gap threshold alone cannot separate them -- but DIA appearing twice
# can.
SNAPSHOT_GAP = timedelta(minutes=3)

_TICKER_ORDER = {ticker: i for i, ticker in enumerate(DEFAULT_SYMBOLS)}


class ReplayError(RuntimeError):
    """The archive cannot answer the question asked of it.

    Raised rather than returned, because every case is "the data you asked for is
    not here": a session with no directory, a snapshot stamp that does not exist, a
    file that will not parse. A half-built blob for any of those would be worse than
    an error -- the levels would look ordinary and be wrong.
    """


def _ordered(tickers):
    """Default-symbol order first, then anything else alphabetically.

    Only so a replayed blob lists its records in the same order a live one does,
    which makes the two directly comparable by eye. The indicator finds its own
    record by ticker and does not care.
    """
    return sorted(set(tickers),
                  key=lambda t: (_TICKER_ORDER.get(t, len(_TICKER_ORDER)), t))


def _parse_stamp(stamp):
    """'20260804T183422Z' -> aware UTC datetime."""
    try:
        return datetime.strptime(stamp, _STAMP_FORMAT).replace(tzinfo=timezone.utc)
    except ValueError:
        raise ReplayError("unreadable snapshot stamp %r "
                          "(expected YYYYMMDDTHHMMSSZ)" % (stamp,))


def _parse_archive_ts(raw, column, path):
    """'2026-08-04T18:34:22Z' -> aware UTC datetime.

    The archive writes an explicit Z; CBOE's own field does not, which is why this
    does not go through `nytime.parse_cboe_timestamp` unaltered.
    """
    text = (raw or "").strip()
    if text.endswith("Z"):
        text = text[:-1]
    text = text.replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ReplayError("%s: %s is not a timestamp: %r"
                      % (os.path.basename(path), column, raw))


@dataclass
class Snapshot:
    """One run's files: the same fetch, one symbol per file.

    `stamp` is the earliest file stamp in the cluster and is the snapshot's id --
    stable, sortable, and the same spelling the filenames use.
    """
    session_date: date
    stamp: str
    first_quote_ts: datetime
    last_quote_ts: datetime
    paths: Dict[str, str] = field(default_factory=dict)

    @property
    def tickers(self):
        return _ordered(self.paths)

    @property
    def time_ny(self):
        return self.first_quote_ts.astimezone(NY).strftime("%H:%M:%S")

    @property
    def quote_range_ny(self):
        """'14:34' or '14:34-14:35' -- the spread of CBOE stamps inside the run."""
        lo = self.first_quote_ts.astimezone(NY).strftime("%H:%M")
        hi = self.last_quote_ts.astimezone(NY).strftime("%H:%M")
        return lo if lo == hi else "%s-%s" % (lo, hi)

    @property
    def label(self):
        return "%s New York, %d symbol(s)" % (
            self.first_quote_ts.astimezone(NY).strftime("%H:%M"), len(self.paths))


@dataclass
class DayIndex:
    session_date: date
    snapshots: List[Snapshot] = field(default_factory=list)


def list_dates(archive_dir=DEFAULT_ARCHIVE_DIR):
    """Session dates the archive holds, oldest first.

    Anything in the archive root that is not a dated directory is ignored rather
    than reported: the archive is a directory a human may well have dropped a note
    or a stray export into, and that is not a data defect.
    """
    if not os.path.isdir(archive_dir):
        return []
    out = []
    for name in os.listdir(archive_dir):
        if not os.path.isdir(os.path.join(archive_dir, name)):
            continue
        try:
            out.append(date.fromisoformat(name))
        except ValueError:
            continue
    return sorted(out)


def list_snapshots(archive_dir, session_date):
    """Every snapshot of one session, earliest first.

    Filenames alone are enough here -- the stamp and the ticker are both in the
    name -- so listing the archive never opens a file.
    """
    session_date = _coerce_date(session_date)
    day_dir = os.path.join(archive_dir, session_date.isoformat())
    if not os.path.isdir(day_dir):
        return []

    files = []
    for name in os.listdir(day_dir):
        match = _FILENAME_RE.match(name)
        if not match:
            continue
        ticker, stamp = match.group(1), match.group(2)
        files.append((_parse_stamp(stamp), ticker, os.path.join(day_dir, name)))
    files.sort()

    snapshots = []
    current = None
    for quote_ts, ticker, path in files:
        starts_new = (
            current is None
            or ticker in current.paths                                  # a repeat
            or quote_ts - current.last_quote_ts > SNAPSHOT_GAP          # a gap
        )
        if starts_new:
            current = Snapshot(session_date=session_date,
                               stamp=quote_ts.strftime(_STAMP_FORMAT),
                               first_quote_ts=quote_ts,
                               last_quote_ts=quote_ts)
            snapshots.append(current)
        current.paths[ticker] = path
        current.last_quote_ts = quote_ts
    return snapshots


def archive_index(archive_dir=DEFAULT_ARCHIVE_DIR):
    """The whole archive: dates oldest first, each with its snapshots."""
    return [DayIndex(session_date=day, snapshots=list_snapshots(archive_dir, day))
            for day in list_dates(archive_dir)]


def _coerce_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError:
        raise ReplayError("%r is not a session date (expected YYYY-MM-DD)" % (value,))


def find_snapshot(archive_dir=DEFAULT_ARCHIVE_DIR, session_date=None, stamp=None):
    """Resolve a session date and snapshot stamp to a Snapshot.

    No date given resolves to the newest session in the archive. No stamp given
    resolves to the EARLIEST snapshot of that session, which is the interesting
    default: it is the book the morning actually opened on, and it is what the
    owner would have been looking at before the session moved.
    """
    if session_date is None:
        dates = list_dates(archive_dir)
        if not dates:
            raise ReplayError("no snapshots in %s -- the archive is empty, and it "
                              "cannot be backfilled: CBOE overwrites the file"
                              % archive_dir)
        session_date = dates[-1]
    session_date = _coerce_date(session_date)

    snapshots = list_snapshots(archive_dir, session_date)
    if not snapshots:
        held = list_dates(archive_dir)
        raise ReplayError("no snapshots for session %s in %s (held: %s)"
                          % (session_date, archive_dir,
                             ", ".join(d.isoformat() for d in held) or "nothing"))
    if stamp is None:
        return snapshots[0]

    wanted = stamp.strip()
    for snapshot in snapshots:
        if snapshot.stamp == wanted:
            return snapshot
    raise ReplayError("session %s has no snapshot %s (held: %s)"
                      % (session_date, wanted,
                         ", ".join(s.stamp for s in snapshots)))


def read_chain(path):
    """Read one archived CSV back into a `cboe.Chain`. Returns (chain, session_date).

    The session date is returned alongside rather than derived, because it is a
    column in the file: the archive recorded which session the contracts belonged
    to, and that is the value the 0DTE bucket has to be cut against. Deriving it
    from anything else on a replay -- today's date above all -- puts every same-day
    contract in the wrong bucket or drops it entirely.

    No expiry filtering happens here. The chain was reduced before it was written.
    """
    try:
        with gzip.open(path, "rt", newline="", encoding="ascii") as handle:
            reader = csv.DictReader(handle)
            columns = reader.fieldnames or []
            missing = [c for c in _REQUIRED_COLUMNS if c not in columns]
            if missing:
                raise ReplayError("%s: snapshot is missing column(s) %s"
                                  % (os.path.basename(path), ",".join(missing)))
            rows = list(reader)
    except ReplayError:
        raise
    except (OSError, csv.Error, UnicodeDecodeError) as exc:
        raise ReplayError("%s: cannot read snapshot: %s"
                          % (os.path.basename(path), exc))

    if not rows:
        raise ReplayError("%s: snapshot holds no contracts"
                          % os.path.basename(path))

    first = rows[0]
    ticker = to_ticker(first["ticker"])
    quote_ts = _parse_archive_ts(first["quote_ts_utc"], "quote_ts_utc", path)
    session_date = _coerce_date(first["session_date"])
    try:
        spot = float(first["spot"])
    except (TypeError, ValueError):
        raise ReplayError("%s: spot is not a number: %r"
                          % (os.path.basename(path), first["spot"]))
    if spot <= 0:
        raise ReplayError("%s: non-positive spot %r"
                          % (os.path.basename(path), first["spot"]))

    # Old snapshots carry no `spot_ts_utc`, so the instant spot was true has to be
    # inferred from the publish stamp less the known feed delay -- the same
    # fallback `cboe.spot_effective_time` uses, and reported the same way. It is not
    # cosmetic: the indicator anchors its futures basis on the header stamp, so an
    # inferred stamp lands the anchor on a bar a couple of minutes off the right
    # one. Never inferred silently.
    raw_spot_ts = (first.get("spot_ts_utc") or "").strip()
    if raw_spot_ts:
        spot_ts = _parse_archive_ts(raw_spot_ts, "spot_ts_utc", path)
        spot_ts_fallback = None
    else:
        spot_ts = quote_ts - ASSUMED_FEED_DELAY
        spot_ts_fallback = (
            "snapshot was written before the archive recorded spot_ts_utc, so the "
            "header stamp is inferred as the quote stamp less %d min"
            % (ASSUMED_FEED_DELAY.total_seconds() // 60))

    contracts = []
    for number, row in enumerate(rows, start=2):  # row 1 is the header
        try:
            contracts.append(Contract(
                occ=row["occ"].strip().upper(),
                expiry=date.fromisoformat(row["expiry"]),
                right=row["right"].strip().upper(),
                strike=float(row["strike"]),
                open_interest=float(row["open_interest"]),
                gamma=float(row["gamma"]),
                iv=float(row["iv"]),
                volume=float(row["volume"]),
            ))
        except (AttributeError, KeyError, TypeError, ValueError) as exc:
            raise ReplayError("%s line %d: unreadable contract row: %s"
                              % (os.path.basename(path), number, exc))

    # Row order is preserved deliberately. The strike ladder and the flip scan
    # accumulate in list order, so re-sorting would move the last digits of every
    # magnitude and a replay would stop matching the blob the session published.
    chain = Chain(ticker=ticker, quote_ts=quote_ts, spot=spot, contracts=contracts,
                  # The archive records what survived the filter, not what it
                  # dropped, so these two counts are unknowable on a replay. They
                  # are zero rather than invented, and the UI shows a dash.
                  dropped_expired=0, dropped_unparseable=0,
                  raw_count=len(contracts),
                  spot_ts=spot_ts, spot_ts_fallback=spot_ts_fallback)
    return chain, session_date


def replay(session_date=None, stamp=None, tickers=None,
           archive_dir=DEFAULT_ARCHIVE_DIR, now=None):
    """Rebuild the blob for one archived snapshot.

    Returns a `pipeline.RunResult`, the same shape the live path returns, so the UI
    and anything downstream can treat a replayed blob and a live one identically.
    `archived` stays empty for a reason that is not incidental: a replay must never
    write to the archive. Reading history is not making history, and a replay that
    wrote would key its file on the same quote stamp it just read -- a no-op at
    best, and at worst a snapshot of a subset of symbols overwriting nothing while
    looking like a fresh run.

    `tickers` of None means every symbol in the snapshot. That is the useful
    default: one blob carries every record and the indicator picks its own, so
    trimming buys nothing except a shorter line.
    """
    snapshot = find_snapshot(archive_dir, session_date=session_date, stamp=stamp)

    result = RunResult()
    # When the replay was assembled. Diagnostics only -- as on the live path, this
    # is emphatically not what the blob header carries.
    result.computed_at = now or now_utc()

    if tickers is None:
        wanted = snapshot.tickers
    else:
        wanted = _ordered(to_ticker(t) for t in tickers if str(t).strip())
        if not wanted:
            raise ReplayError("no symbols requested")

    chains = []
    csv_sessions = {}
    for ticker in wanted:
        path = snapshot.paths.get(ticker)
        if path is None:
            # Named, not skipped. A snapshot that happens to be missing a symbol
            # would otherwise emit a short blob that looks complete.
            result.failures.append((
                ticker,
                "%s: not in the %s snapshot of %s (it holds %s)"
                % (ticker, snapshot.stamp, snapshot.session_date,
                   ", ".join(snapshot.tickers))))
            continue
        try:
            chain, csv_session = read_chain(path)
        except ReplayError as exc:
            result.failures.append((ticker, str(exc)))
            continue
        chains.append(chain)
        csv_sessions[chain.ticker] = csv_session

    if not chains:
        return result

    # One session date for the whole blob, and it comes out of the files. Same rule
    # as the live path: a chain from another session is a stale file, reported
    # rather than blended in -- blending would put one symbol's 0DTE contracts in
    # another symbol's 0DTE bucket.
    result.session_date = max(csv_sessions.values())
    usable = []
    for chain in chains:
        if csv_sessions[chain.ticker] != result.session_date:
            result.failures.append((
                chain.ticker,
                "%s: snapshot row says session %s, blob session is %s -- stale file"
                % (chain.ticker, csv_sessions[chain.ticker], result.session_date)))
            continue
        usable.append(chain)

    if not usable:
        return result

    # The oldest spot time, exactly as the live path picks it, so a replayed header
    # can no more overstate freshness than a live one can.
    result.effective_at = min(c.spot_ts for c in usable)
    for chain in usable:
        if chain.spot_ts_fallback:
            result.warnings.append((chain.ticker, chain.spot_ts_fallback))

    result.chains = usable
    # The live functions, unforked. If these two lines ever grow a replay-specific
    # branch, the replay has stopped being a replay.
    result.records = [build_record(c, result.session_date) for c in usable]
    result.blob = encode_blob(result.records, result.effective_at, result.session_date)
    return result
