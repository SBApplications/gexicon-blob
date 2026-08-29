"""Fetch -> archive -> compute -> encode, for a list of symbols."""

import json
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .archive import DEFAULT_ARCHIVE_DIR, write_snapshot
from .cboe import (ASSUMED_FEED_DELAY, FetchError, MAX_QUOTE_AGE_HOURS,
                   fetch_raw, reduce_payload)
from .encode import encode_blob
from .nytime import now_utc, session_date_of
from .record import build_record
from .symbols import DEFAULT_SYMBOLS, to_cboe, to_ticker


@dataclass
class RunResult:
    blob: Optional[str] = None
    records: list = field(default_factory=list)
    chains: list = field(default_factory=list)
    failures: List[Tuple[str, str]] = field(default_factory=list)
    # Not failures: the run produced a blob, but something about it is worth
    # saying out loud -- a spot timestamp that had to be inferred, mostly.
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


def run(symbols=DEFAULT_SYMBOLS, offline_dir=None, archive_dir=DEFAULT_ARCHIVE_DIR,
        raw_dir=None, max_age_hours=MAX_QUOTE_AGE_HOURS, now=None, timeout=60):
    """Run the whole pipeline. Failures are collected, never swallowed."""
    result = RunResult()
    result.computed_at = now or now_utc()

    chains = []
    for symbol in symbols:
        ticker = to_ticker(symbol)
        try:
            if offline_dir:
                payload = load_offline_payload(offline_dir, symbol)
            else:
                payload = fetch_raw(symbol, timeout=timeout)
                if raw_dir:
                    save_raw(raw_dir, symbol, payload)
            chain = reduce_payload(symbol, payload,
                                   max_age_hours=None if offline_dir else max_age_hours,
                                   now=result.computed_at)
        except FetchError as exc:
            result.failures.append((ticker, str(exc)))
            continue
        except (OSError, ValueError) as exc:
            result.failures.append((ticker, "%s: %s" % (ticker, exc)))
            continue
        chains.append(chain)

    if not chains:
        return result

    # One session date for the whole blob: the New York date of the newest quote.
    # A symbol whose quote lands on a different session day is a stale file, not a
    # contribution -- it is reported, not blended in.
    result.session_date = max(session_date_of(c.quote_ts) for c in chains)
    usable = []
    for chain in chains:
        if session_date_of(chain.quote_ts) != result.session_date:
            result.failures.append((
                chain.ticker,
                "%s: quote is from session %s, blob session is %s -- stale file"
                % (chain.ticker, session_date_of(chain.quote_ts),
                   result.session_date)))
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
    result.effective_at = min(c.spot_ts for c in usable)
    for chain in usable:
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
