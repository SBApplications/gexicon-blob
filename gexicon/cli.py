"""Command line entry point.

    python3 -m gexicon                       fetch the default symbols, print the blob
    python3 -m gexicon --offline samples     run against saved payloads
    python3 -m gexicon --save-raw samples    fetch and keep the payloads for offline use
    python3 -m gexicon --list-archive        what the snapshot archive holds
    python3 -m gexicon --replay 2026-08-04   rebuild that session's blob from the archive

The blob goes to stdout on its own line. Everything else goes to stderr, so
`python3 -m gexicon > blob.txt` yields a clean file. A replayed blob obeys the same
contract, and says on stderr that it is a replay -- an archived blob on stdout must
never be mistakable for a live one.

`--replay` on a session the archive does not hold is an error, never a silent fetch:
a replay reads the archive and nothing else, so a typo in a date is a complaint on
stderr rather than a live download dressed up as history.

Exit codes: 0 every symbol succeeded, 2 partial (blob printed, failures named on
stderr), 1 nothing usable.
"""

import argparse
import sys

from .archive import DEFAULT_ARCHIVE_DIR
from .cboe import MAX_QUOTE_AGE_HOURS
from .nytime import NY
from .pipeline import run
from .replay import ReplayError, archive_index, find_snapshot, replay
from .symbols import DEFAULT_SYMBOLS


def build_parser():
    parser = argparse.ArgumentParser(
        prog="gexicon",
        description="Build the Gexicon GEX data blob from CBOE delayed option chains.")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS),
                        help="comma-separated symbols (default: %(default)s)")
    parser.add_argument("--offline", metavar="DIR", default=None,
                        help="read saved JSON payloads from DIR instead of fetching")
    parser.add_argument("--save-raw", metavar="DIR", default=None,
                        help="write each fetched payload to DIR for later offline runs")
    parser.add_argument("--archive-dir", default=DEFAULT_ARCHIVE_DIR,
                        help="daily snapshot archive root (default: %(default)s)")
    parser.add_argument("--no-archive", action="store_true",
                        help="skip the snapshot archive (testing only)")
    parser.add_argument("--max-age-hours", type=float, default=MAX_QUOTE_AGE_HOURS,
                        help="reject a quote older than this (default: %(default)s)")
    parser.add_argument("--timeout", type=float, default=60.0,
                        help="per-request timeout in seconds (default: %(default)s)")
    parser.add_argument("--summary", action="store_true",
                        help="also print a human-readable per-symbol summary to stderr")
    parser.add_argument("--list-archive", action="store_true",
                        help="list the sessions and snapshots the archive holds")
    parser.add_argument("--replay", metavar="YYYY-MM-DD", default=None,
                        help="rebuild the blob for an archived session instead of "
                             "fetching (nothing is written to the archive)")
    parser.add_argument("--replay-stamp", metavar="STAMP", default=None,
                        help="which snapshot of the replayed session, as printed by "
                             "--list-archive (default: the earliest of that session)")
    parser.add_argument("--serve", action="store_true",
                        help="open a local browser page with the blob and a copy button")
    parser.add_argument("--port", type=int, default=8765,
                        help="port for --serve (default: %(default)s)")
    parser.add_argument("--no-open", action="store_true",
                        help="with --serve, do not open a browser window")
    return parser


def _summarise(result, stream):
    if result.effective_at is not None:
        print("header stamp %s -- the instant spot was true, oldest of %d symbol(s); "
              "the run itself was at %s"
              % (result.effective_at.strftime("%Y-%m-%d %H:%M:%SZ"),
                 len(result.chains),
                 result.computed_at.strftime("%Y-%m-%d %H:%M:%SZ")),
              file=stream)
    for record, chain in zip(result.records, result.chains):
        total = record.total
        tags = ",".join(s.tag for s in record.buckets) or "none"
        print("%-5s spot=%-10.2f flip=%-10s net=%+.2fB contracts=%-6d "
              "expired_dropped=%-5d buckets=%s"
              % (record.ticker, total.spot,
                 ("%.2f" % total.flip) if total.flip else "none",
                 total.net, len(chain.contracts), chain.dropped_expired, tags),
              file=stream)


def _report(result, args):
    """The stdout/stderr contract: blob on stdout, everything else on stderr."""
    if result.blob:
        print(result.blob)

    if args.summary and result.records:
        _summarise(result, sys.stderr)
        if result.archived:
            print("archived %d snapshot(s), newest %s"
                  % (len(result.archived), result.archived[-1]), file=sys.stderr)

    # A fallback stamp is not a failure, but it must never be silent: the header
    # is then inferred rather than read, and the futures basis rides on it.
    if result.warnings:
        print("", file=sys.stderr)
        print("WARNING (%d): the header timestamp was inferred, not read"
              % len(result.warnings), file=sys.stderr)
        for ticker, reason in result.warnings:
            print("  %-5s %s" % (ticker, reason), file=sys.stderr)

    if result.failures:
        print("", file=sys.stderr)
        print("FAILED (%d): these symbols are missing from the blob above"
              % len(result.failures), file=sys.stderr)
        for ticker, reason in result.failures:
            print("  %-5s %s" % (ticker, reason), file=sys.stderr)

    if not result.blob:
        return 1
    return 2 if result.failures else 0


def _list_archive(archive_dir, stream=sys.stdout):
    """Print what the archive holds. The listing is the output, so it is stdout."""
    days = archive_index(archive_dir)
    if not days:
        print("no snapshots in %s -- and the archive cannot be backfilled, CBOE "
              "overwrites the file" % archive_dir, file=sys.stderr)
        return 1
    print("%s holds %d session(s)" % (archive_dir, len(days)), file=stream)
    for day in days:
        print("%s  %d snapshot(s)" % (day.session_date, len(day.snapshots)),
              file=stream)
        for snapshot in day.snapshots:
            print("  %s  %s New York  quotes %s  %d symbol(s)  %s"
                  % (snapshot.stamp, snapshot.time_ny, snapshot.quote_range_ny,
                     len(snapshot.tickers), ",".join(snapshot.tickers)),
                  file=stream)
    return 0


def _run_replay(args, symbols):
    """Rebuild an archived session's blob. `symbols` is None for every ticker."""
    try:
        snapshot = find_snapshot(args.archive_dir, session_date=args.replay,
                                 stamp=args.replay_stamp)
        result = replay(session_date=snapshot.session_date, stamp=snapshot.stamp,
                        tickers=symbols, archive_dir=args.archive_dir)
    except ReplayError as exc:
        print("replay failed: %s" % exc, file=sys.stderr)
        return 1

    # Always said, not only with --summary. A blob on stdout carries no sign of
    # where it came from, and an archived one pasted into a live chart is exactly
    # the mistake worth spending two lines of stderr to prevent.
    print("REPLAY: session %s, snapshot %s (%s New York, quotes %s), from %s"
          % (snapshot.session_date, snapshot.stamp, snapshot.time_ny,
             snapshot.quote_range_ny, args.archive_dir), file=sys.stderr)
    if result.effective_at is not None:
        print("        header stamp %s (%s New York)%s"
              % (result.effective_at.strftime("%Y-%m-%d %H:%M:%SZ"),
                 result.effective_at.astimezone(NY).strftime("%H:%M:%S"),
                 " -- INFERRED, see the warning below" if result.warnings else ""),
              file=sys.stderr)
    return _report(result, args)


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        print("no symbols requested", file=sys.stderr)
        return 1

    if args.list_archive:
        return _list_archive(args.archive_dir)

    if args.serve:
        if args.replay:
            print("--replay is ignored with --serve: the page has a Replay mode",
                  file=sys.stderr)
        from .server import serve
        return serve(port=args.port, open_browser=not args.no_open,
                     symbols=symbols, offline_dir=args.offline,
                     archive_dir=None if args.no_archive else args.archive_dir,
                     max_age_hours=args.max_age_hours, timeout=args.timeout,
                     # --no-archive stops the live path writing; the days already
                     # on disk stay replayable.
                     replay_dir=args.archive_dir)

    if args.replay:
        # A replay carries every ticker in the snapshot unless --symbols was
        # actually passed. The default is compared rather than a None sentinel
        # because --symbols advertises its default in --help, and losing that to
        # gain a sentinel would be a worse trade.
        given = args.symbols != parser.get_default("symbols")
        return _run_replay(args, symbols if given else None)

    result = run(
        symbols=symbols,
        offline_dir=args.offline,
        archive_dir=None if args.no_archive else args.archive_dir,
        raw_dir=args.save_raw,
        max_age_hours=args.max_age_hours,
        timeout=args.timeout,
    )
    return _report(result, args)
