"""Fetch -> archive -> compute -> encode, for a list of symbols."""

import json
import os
from dataclasses import dataclass, field
from datetime import time as clock_time
from typing import Dict, List, Optional, Tuple

from . import yahoo
from .archive import DEFAULT_ARCHIVE_DIR, write_snapshot
from .cboe import (ASSUMED_FEED_DELAY, FetchError, MAX_QUOTE_AGE_HOURS,
                   fetch_raw, reduce_payload)
from .encode import encode_blob
from .nytime import NY, now_utc, session_date_of
from .record import build_record
from .symbols import DEFAULT_SYMBOLS, to_cboe, to_ticker

# How old CBOE's file may be before the second source is worth the requests.
# CBOE republishes about hourly, so two and a half hours is several missed
# refreshes -- long enough not to fire on a slow hour, short enough that a
# genuine stall is caught inside the same session it happens in.
CBOE_MAX_AGE_HOURS = 2.5

# The window the fallback is allowed to fire in, New York local, weekdays only.
# It opens before the cash session so a pre-market build still gets live data,
# and closes half an hour after the cash close. Outside it CBOE's file is
# *supposed* to sit still -- it is holding the last close, which is the correct
# data for that time of day -- and calling Yahoo would be several hundred
# requests to replace good numbers with the same numbers.
FALLBACK_OPEN_NY = clock_time(7, 0)
FALLBACK_CLOSE_NY = clock_time(16, 30)

SOURCE_CHOICES = ("cboe", "yahoo", "auto")


@dataclass
class RunResult:
    blob: Optional[str] = None
    records: list = field(default_factory=list)
    chains: list = field(default_factory=list)
    failures: List[Tuple[str, str]] = field(default_factory=list)
    # Not failures: the run produced a blob, but something about it is worth
    # saying out loud -- a spot timestamp that had to be inferred, or a symbol
    # that had to come from the second source.
    warnings: List[Tuple[str, str]] = field(default_factory=list)
    archived: List[str] = field(default_factory=list)
    session_date: object = None
    # When the pipeline ran. Diagnostics only -- it is NOT what the blob header
    # carries, and putting it there is the bug that misplaced every futures level.
    computed_at: object = None
    # The instant the records' spots were true. This is the blob header stamp.
    effective_at: object = None

    @property
    def ok(self):
        return self.blob is not None and not self.failures


def load_offline_payload(offline_dir, symbol):
    """Find a saved payload for one symbol. Accepts 'SPX.json' or '_SPX.json'."""
    for name in (to_cboe(symbol), to_ticker(symbol)):
        path = os.path.join(offline_dir, name + ".json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as handle:
                return json.load(handle)
    raise FetchError("%s: no saved payload in %s" % (to_ticker(symbol), offline_dir))


def save_raw(raw_dir, symbol, payload):
    os.makedirs(raw_dir, exist_ok=True)
    path = os.path.join(raw_dir, to_cboe(symbol) + ".json")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    os.replace(tmp, path)
    return path


def inside_fallback_hours(moment):
    """True when `moment` lands in the weekday window the fallback may fire in."""
    local = moment.astimezone(NY)
    if local.weekday() >= 5:
        return False
    return FALLBACK_OPEN_NY <= local.time() <= FALLBACK_CLOSE_NY


def wants_fallback(chain, error, now, max_age_hours=CBOE_MAX_AGE_HOURS):
    """Should the second source be tried for this symbol? Returns a reason or None.

    Two cases, and both are the same underlying fault -- CBOE's file has stopped
    being updated:

      * the file came back and its quote is older than `max_age_hours`;
      * the file did not come back at all, or was rejected outright, which is
        what a stall turns into once it passes `cboe.MAX_QUOTE_AGE_HOURS`.

    Both are gated on the clock. Outside the weekday window the file is meant to
    be still, and an overnight build reading last night's close is reading
    exactly the right thing.
    """
    if not inside_fallback_hours(now):
        return None
    if chain is None:
        return "CBOE gave nothing usable (%s)" % (error or "no chain")
    age_hours = (now - chain.quote_ts).total_seconds() / 3600.0
    if age_hours > max_age_hours:
        return ("CBOE's file is %.1fh old (limit %.1fh) -- the feed has stalled"
                % (age_hours, max_age_hours))
    return None


def prefer_fallback(second, chain, now):
    """Is the second source's chain actually better than what CBOE gave? Reason or None.

    Two conditions, and both have to hold:

      * the chain is today's New York session -- what the fallback exists to
        recover is *today's* data, and a previous session's chain is not that
        however fresh it looks against a stalled file;
      * it is newer than CBOE's -- a second source further behind the first is
        not a repair. Only asked when both are on the same session, because a
        chain from today beats one from yesterday whatever the two stamps read.

    Which session a Yahoo chain belongs to is decided by
    `yahoo.chain_session_date`, not by its quote stamp. Reading the stamp is what
    dropped SPX and RUT on 2026-09-24: pre-market, and in the opening minute
    before the cash indexes print, that stamp is still yesterday's close on a
    chain that is plainly today's.

    The session condition is what keeps a market holiday behaving as it always
    has. A holiday is a weekday inside the window, CBOE serves the last session's
    file, and Yahoo serves the same last close on a chain with nothing expiring
    today: the run drops every symbol as stale and the previous line stays up,
    which is the right answer. Without this check a holiday would republish the
    last session's levels with their touch odds gone, over the top of a good line.
    """
    today = session_date_of(now)
    if second.session_date != today:
        return None, ("its chain is from session %s, not today's %s"
                      % (second.session_date, today))
    if (chain is not None and chain.session_date == second.session_date
            and second.quote_ts <= chain.quote_ts):
        return None, ("its quote (%s) is no newer than CBOE's"
                      % second.quote_ts.strftime("%Y-%m-%d %H:%M:%SZ"))
    return True, None


def _fetch_cboe(symbol, offline_dir, raw_dir, max_age_hours, now, timeout):
    """One symbol from CBOE. Returns (chain, error_string); never both."""
    try:
        if offline_dir:
            payload = load_offline_payload(offline_dir, symbol)
        else:
            payload = fetch_raw(symbol, timeout=timeout)
            if raw_dir:
                save_raw(raw_dir, symbol, payload)
        chain = reduce_payload(symbol, payload,
                               max_age_hours=None if offline_dir else max_age_hours,
                               now=now)
    except FetchError as exc:
        return None, str(exc)
    except (OSError, ValueError) as exc:
        return None, "%s: %s" % (to_ticker(symbol), exc)
    return chain, None


def run(symbols=DEFAULT_SYMBOLS, offline_dir=None, archive_dir=DEFAULT_ARCHIVE_DIR,
        raw_dir=None, max_age_hours=MAX_QUOTE_AGE_HOURS, now=None, timeout=60,
        source="auto", cboe_max_age_hours=CBOE_MAX_AGE_HOURS,
        yahoo_workers=None):
    """Run the whole pipeline. Failures are collected, never swallowed.

    `source` picks the feed: 'cboe' never calls Yahoo, 'yahoo' never calls CBOE,
    and 'auto' -- the default -- uses CBOE and falls back per symbol when its
    file has stalled inside trading hours. See `wants_fallback`.

    An offline run never reaches the network, so it never falls back either: the
    saved payloads are the whole of the input by definition.

    `yahoo_workers` is how many of a symbol's expiries the second source fetches
    at once; None takes `yahoo.YAHOO_WORKERS`. It changes the wall clock and
    nothing else -- see `yahoo.fetch_payloads`.
    """
    result = RunResult()
    result.computed_at = now or now_utc()
    if source not in SOURCE_CHOICES:
        raise ValueError("unknown source %r (expected one of %s)"
                         % (source, ", ".join(SOURCE_CHOICES)))

    chains = []
    for symbol in symbols:
        ticker = to_ticker(symbol)

        chain = None
        error = None
        if source != "yahoo":
            chain, error = _fetch_cboe(symbol, offline_dir, raw_dir, max_age_hours,
                                       result.computed_at, timeout)

        if source == "yahoo":
            reason = "--source yahoo"
        elif source == "auto" and not offline_dir:
            reason = wants_fallback(chain, error, result.computed_at,
                                    max_age_hours=cboe_max_age_hours)
        else:
            # --source cboe, or an offline run: the second source is off the
            # table however stale the file turns out to be.
            reason = None

        if reason:
            try:
                second = yahoo.load_chain(symbol, timeout=min(timeout,
                                                              yahoo.DEFAULT_TIMEOUT),
                                          now=result.computed_at,
                                          workers=yahoo_workers)
            except FetchError as exc:
                if chain is None:
                    result.failures.append((ticker, str(exc)))
                    continue
                # Keeping the stale CBOE file is the right call -- the existing
                # age rules decide whether it survives -- but it is never silent.
                result.warnings.append((
                    ticker,
                    "%s: %s, and the second source failed too (%s) -- kept CBOE"
                    % (ticker, reason, exc)))
            else:
                use_it, why_not = prefer_fallback(second, chain, result.computed_at)
                if use_it:
                    chain = second
                    result.warnings.append((
                        ticker,
                        "%s: %s -- used Yahoo's chain instead (quote %s)"
                        % (ticker, reason,
                           second.quote_ts.strftime("%Y-%m-%d %H:%M:%SZ"))))
                elif chain is None:
                    result.failures.append((
                        ticker,
                        "%s: %s, and Yahoo is no use either -- %s"
                        % (ticker, reason, why_not)))
                    continue
                else:
                    result.warnings.append((
                        ticker,
                        "%s: %s, but %s -- kept CBOE" % (ticker, reason, why_not)))

        if chain is None:
            result.failures.append((ticker, error or "%s: no chain" % ticker))
            continue
        chains.append(chain)

    if not chains:
        return result

    # One session date for the whole blob: the newest session any chain belongs
    # to. A symbol from a different session day is a stale file, not a
    # contribution -- it is reported, not blended in.
    result.session_date = max(c.session_date for c in chains)
    usable = []
    for chain in chains:
        if chain.session_date != result.session_date:
            result.failures.append((
                chain.ticker,
                "%s: chain is from session %s, blob session is %s -- stale file"
                % (chain.ticker, chain.session_date, result.session_date)))
            continue
        usable.append(chain)

    if not usable:
        return result

    # Archive first. Levels are reproducible from the chain; the chain is not
    # reproducible from anywhere.
    if archive_dir:
        for chain in usable:
            try:
                path, written = write_snapshot(archive_dir, chain, result.session_date)
                if written:
                    result.archived.append(path)
            except OSError as exc:
                result.failures.append(
                    (chain.ticker, "%s: archive write failed: %s" % (chain.ticker, exc)))

    # One blob, one header stamp, but every symbol's spot was true at a slightly
    # different instant. Take the OLDEST: the header then never claims the data
    # is fresher than any part of it is, and the indicator's basis anchor lands
    # on a bar where every record's spot was already real. Taking the newest, or
    # a mean, would overstate freshness for at least one symbol -- and the whole
    # point of this field is that it is not allowed to do that. The spread across
    # symbols is about a minute; the error this replaced was fifteen.
    # A spot that is still a prior close -- an index Yahoo has not updated and
    # whose twin could not stand in -- does not get to drag the header stamp back
    # a session. That would move every futures basis anchor onto yesterday's
    # close and stretch every touch horizon by a day, for every symbol, to
    # describe one. The symbol keeps its own stale spot and is named under
    # WARNING instead.
    printed_today = [c for c in usable
                     if session_date_of(c.spot_ts) == result.session_date]
    result.effective_at = min(c.spot_ts for c in (printed_today or usable))
    for chain in usable:
        if chain.spot_note:
            result.warnings.append(
                (chain.ticker, "%s: %s" % (chain.ticker, chain.spot_note)))
        if chain.spot_ts_fallback:
            result.warnings.append((
                chain.ticker,
                "%s: spot timestamp fell back to the file stamp less %d min (%s)"
                % (chain.ticker, ASSUMED_FEED_DELAY.total_seconds() // 60,
                   chain.spot_ts_fallback)))

    result.chains = usable
    result.records = [build_record(c, result.session_date) for c in usable]
    result.blob = encode_blob(result.records, result.effective_at, result.session_date)
    return result
