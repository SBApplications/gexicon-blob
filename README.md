# gexicon-blob

Builds the data line the **Gexicon** TradingView indicator reads, five times a
session, and commits it here.

Pine Script cannot make network requests, so the indicator fetches nothing. It
draws from one line of text pasted into its settings. This repo produces that
line.

## Get the current blob

    https://raw.githubusercontent.com/SBApplications/gexicon-blob/main/latest.blob

Copy it, open the indicator's settings, paste it into **GEX data blob**. That is
the whole workflow. Once a morning is enough — open interest is published
overnight and does not change during the session.

## What is in it

One line carrying every symbol: the gamma flip, net gamma, the ten heaviest
strikes, the call wall and the put wall, and the market-implied odds of price
touching each wall before the close. Symbols are SPY, SPX, QQQ, NDX, DIA, RUT,
IWM, TSLA, NVDA, MU, SNDK, AMD, GOOGL, PLTR and AAPL.

Records carry expiry buckets — today's expiry apart from everything after it —
because the two frequently disagree, and blended into one number that
disagreement is invisible.

## Run it yourself

Standard library only. No install, no virtualenv, no API key.

    python3 -m gexicon                      # the blob, to stdout
    python3 -m gexicon --symbols SPY,SPX    # a subset
    python3 -m gexicon --serve              # a page with a copy button

## Where the data comes from

CBOE's public delayed option chains. Free, no key, 15 minutes behind.

Two things about it that matter and are easy to get wrong:

**Open interest is a prior-night snapshot** and does not move during the
session. This is true of paid feeds too. It matters most for same-day expiry,
where most positions are opened that morning and are therefore invisible here —
treat the 0DTE view as an approximation, and never present any of it as live
positioning.

**The touch odds are risk-neutral.** They are what the options market charges
for that outcome, not a forecast, and the downside is systematically overpriced
because people pay up for protection. Not a win rate.

## The second source

CBOE's file has a failure mode that looks like nothing at all: it keeps serving,
with a well-formed timestamp on it, and stops being updated. On 23 September 2026
it froze at 03:56 UTC and every build that morning republished the previous
session's numbers, with no same-day expiry in them, while reporting success.

So a stalled file now falls back to Yahoo Finance's option chain, per symbol:

> If CBOE's quote is more than **2.5 hours** old (or the file did not come back
> at all) **and** the New York clock reads between **08:00 and 16:30 on a
> weekday**, fetch that symbol from Yahoo and use it when its quote is both newer
> than CBOE's and from today's session.

Outside those hours nothing is fetched. CBOE's file is meant to sit still
overnight — it is holding the last close, which is the right data for that time
of day. If Yahoo fails too, the CBOE file is kept and the run says so under
WARNING; the existing stale-drop rules then decide whether it survives.

Two things about the second source worth knowing. Yahoo needs one request per
expiry where CBOE publishes one file per symbol, so a fallback run takes minutes
rather than seconds. And Yahoo publishes no gamma, so it is computed here from
the contract's implied vol with the same Black-Scholes form, risk-free rate and
time-to-expiry the gamma flip already uses — the levels come out of one formula
whichever feed they came from.

`--source cboe` never calls Yahoo, `--source yahoo` never calls CBOE, and
`--source auto` (the default) is the rule above. `--cboe-max-age` moves the 2.5
hours. A run that fell back names the symbol under WARNING and marks it
`source yahoo` in the `--summary` listing; a line saying nothing about its source
came from CBOE.

## Scheduling

Ten runs a session, on the half hour, 08:30 to 17:30 New York, Monday to
Friday. GitHub's cron has no timezone, so the times are written in UTC and
shift by an hour relative to New York when the clocks change — in winter time
the same UTC window starts at 07:30 New York instead, one extra pre-market run.

A symbol whose quote is stale, or that otherwise fails to fetch, is dropped
from that run and named on stderr under WARNING — it does not stop the rest of
the symbols from publishing. `latest.blob` only goes unpublished when nothing
usable came back at all.

**A red run on a market holiday is expected.** CBOE keeps serving the previous
session's file, the pipeline drops every symbol whose quote is older than twelve
hours, and with every symbol stale there is nothing left to publish. Friday's
line stays in place. The second source does not change this: a holiday is still
a weekday inside the window, but Yahoo serves the same last close, and a quote
from a previous session is refused whatever its age.

## The archive

The commit history of `latest.blob` is the archive: every run leaves the levels
as they stood, timestamped, at a few kilobytes each. The raw option chains are
not stored here; they are a gigabyte a year and are archived elsewhere.

## Format and tests

The wire format and the maths live in the source under `gexicon/`. The test
suite is `tests/`, standard library unittest, no plugins:

    python3 -m pytest -q tests
    python3 -m unittest discover -s tests

A few tests want saved CBOE payloads in `samples/` and a snapshot archive in
`archive/`; neither is kept in this repo, so those tests skip here and run in the
working copy alongside the indicator.

## On-time runs

GitHub queues scheduled workflows and fires them hours late. `ops/supabase_dispatch.sql`
sets up a Supabase pg_cron job that calls this workflow's manual trigger on the same
hourly schedule, on the minute. Run it once in the SQL editor of an active Supabase
project and follow the credential note at the bottom of the file. The GitHub cron stays
on as a fallback; the concurrency group and the commit-if-changed step keep the two
from colliding.
