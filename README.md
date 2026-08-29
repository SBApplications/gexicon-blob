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
IWM, TSLA and NVDA.

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

## Scheduling

Five runs a session, on odd minutes because scheduled jobs bunch on the hour and
get queued. GitHub's cron has no timezone, so the times are UTC and shift by an
hour relative to New York when the clocks change. Nothing downstream depends on
the exact minute.

A run that produces a short blob — a symbol failed to fetch — fails instead of
replacing a complete one, so `latest.blob` keeps the last complete set rather
than being quietly half-replaced.

**A red run on a market holiday is expected.** CBOE keeps serving the previous
session's file, the pipeline drops every symbol whose quote is older than twelve
hours, and the blob comes back short and is refused. Nothing to publish on a day
the market is shut, and Friday's line stays in place.

## The archive

The commit history of `latest.blob` is the archive: every run leaves the levels
as they stood, timestamped, at a few kilobytes each. The raw option chains are
not stored here; they are a gigabyte a year and are archived elsewhere.

## Format and tests

The wire format, the maths and the full test suite live with the source, not
here. This repo is the runner.
