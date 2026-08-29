"""Daily snapshot archive.

The reduced chain is written per symbol per run *before* anything downstream can
fail. This is not recoverable later: CBOE overwrites the file and nobody sells the
history, so a run that computes levels but skips the archive has thrown away the
only copy of that day's chain.

The filename carries CBOE's quote timestamp, so a re-run against unchanged data is
a no-op rather than duplicate history.
"""

import csv
import gzip
import os

from .gex import contract_gex

# `spot_ts_utc` was appended, never inserted. Snapshots written before it existed
# have to keep parsing, and a reader that keys on column position rather than name
# would break on every one of them -- so the column goes on the end and
# `replay.read_chain` treats it as optional. Without it a replayed blob has to
# infer its header stamp from the quote stamp, which puts the indicator's futures
# basis anchor a couple of minutes out; with it the replay carries the exact
# instant spot was true, the same value the live blob carried that day.
HEADER = ("ticker", "quote_ts_utc", "session_date", "spot", "occ", "expiry",
          "right", "strike", "open_interest", "gamma", "iv", "volume", "gex",
          "spot_ts_utc")

DEFAULT_ARCHIVE_DIR = "archive"


def snapshot_path(archive_dir, chain, session_date):
    day_dir = os.path.join(archive_dir, session_date.isoformat())
    stamp = chain.quote_ts.strftime("%Y%m%dT%H%M%SZ")
    return os.path.join(day_dir, "%s_%s.csv.gz" % (chain.ticker, stamp))


def write_snapshot(archive_dir, chain, session_date):
    """Write one symbol's reduced chain. Returns (path, written_bool)."""
    path = snapshot_path(archive_dir, chain, session_date)
    if os.path.exists(path):
        return path, False

    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    quote_ts = chain.quote_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    # The other instant, and the reason both are written: `quote_ts` is when CBOE
    # published the file, `spot_ts` is when `spot` was actually true. A replay that
    # only had the publish stamp would have to guess the second one.
    spot_ts = chain.spot_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    session = session_date.isoformat()

    with gzip.open(tmp, "wt", newline="", encoding="ascii") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADER)
        for contract in chain.contracts:
            writer.writerow((
                chain.ticker,
                quote_ts,
                session,
                "%.4f" % chain.spot,
                contract.occ,
                contract.expiry.isoformat(),
                contract.right,
                "%.3f" % contract.strike,
                "%.0f" % contract.open_interest,
                "%.10g" % contract.gamma,
                "%.10g" % contract.iv,
                "%.0f" % contract.volume,
                "%.4f" % contract_gex(contract, chain.spot),
                spot_ts,
            ))
    os.replace(tmp, path)
    return path, True
