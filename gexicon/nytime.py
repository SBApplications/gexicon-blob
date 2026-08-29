"""New York session time helpers.

Two clocks matter and they are easy to mix up:

  * CBOE's top-level `timestamp` is UTC (verified: 18:26:19 in the file while the
    New York wall clock read 14:26). `data.last_trade_time` is New York local.
  * The session date in the blob is a *New York* date, because that is the day the
    0DTE bucket belongs to.
"""

from datetime import datetime, time, timedelta, timezone, tzinfo

_HOUR = timedelta(hours=1)
_EST = timedelta(hours=-5)
_EDT = timedelta(hours=-4)


def _first_sunday_on_or_after(year, month, day):
    d = datetime(year, month, day)
    return d + timedelta(days=(6 - d.weekday()) % 7)


class _USEastern(tzinfo):
    """US Eastern without the IANA database.

    Windows ships no system tz database, so ZoneInfo("America/New_York") raises
    unless the `tzdata` PyPI package is installed -- which would break the
    standard-library-only promise on exactly the platform least able to fix it.
    Every session date, settlement time and 0DTE bucket here depends on this zone,
    so it gets a built-in fallback rather than a dependency.

    US DST has been fixed by statute since 2007: forward on the second Sunday in
    March, back on the first Sunday in November. ZoneInfo is still preferred when
    it is available, so this only runs where there is no alternative.
    """

    def _is_dst(self, dt):
        if dt is None:
            return False
        start = _first_sunday_on_or_after(dt.year, 3, 8).replace(hour=2)
        end = _first_sunday_on_or_after(dt.year, 11, 1).replace(hour=2)
        return start <= dt.replace(tzinfo=None) < end

    def utcoffset(self, dt):
        return _EDT if self._is_dst(dt) else _EST

    def dst(self, dt):
        return _HOUR if self._is_dst(dt) else timedelta(0)

    def tzname(self, dt):
        return "EDT" if self._is_dst(dt) else "EST"

    def __repr__(self):
        return "<US/Eastern fallback>"


def _new_york():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("America/New_York")
    except Exception:
        return _USEastern()


NY = _new_york()

# Equity/ETF options stop trading at 16:00 New York. Past that on expiry day a
# contract has settled and must leave the record entirely.
SETTLEMENT_TIME = time(16, 0)

SECONDS_PER_YEAR = 365.0 * 24.0 * 3600.0
MIN_TTE_YEARS = 1.0 / (365.0 * 24.0)  # one hour, so 0DTE cannot divide by zero

# The touch probability's horizon is pinned to a 365-day year on both sides of
# this project -- here and in the market-terminal reference pipeline -- so the
# same payload rounds to the same integer in both. Do not silently swap it for a
# 365.25-day year: the difference is 0.03% on sigma*sqrt(T), which is invisible
# except at a rounding boundary, and a rounding boundary is exactly where two
# implementations start disagreeing.
TOUCH_SECONDS_PER_YEAR = SECONDS_PER_YEAR


def _parse_naive(raw):
    """'YYYY-MM-DD HH:MM[:SS]' or the same with a 'T', as a naive datetime."""
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("missing timestamp")
    text = raw.strip().replace("T", " ")
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(text, "%Y-%m-%d %H:%M")
        except ValueError:
            raise ValueError("unparseable timestamp: %r" % (raw,))


def parse_cboe_timestamp(raw):
    """Parse CBOE's top-level 'YYYY-MM-DD HH:MM:SS' stamp as an aware UTC datetime.

    This field is the moment CBOE *published the file*, and it is UTC (verified:
    18:26:19 in the file while the New York wall clock read 14:26).
    """
    try:
        return _parse_naive(raw).replace(tzinfo=timezone.utc)
    except ValueError:
        raise ValueError("unparseable CBOE timestamp: %r" % (raw,))


def parse_ny_timestamp(raw):
    """Parse a New York local stamp -- `data.last_trade_time` -- as aware UTC.

    CBOE writes this one in New York wall-clock time with no zone suffix, which
    is why it cannot go through `parse_cboe_timestamp`. Mixing the two up is a
    four- or five-hour error that looks entirely plausible on the wire.
    """
    try:
        return _parse_naive(raw).replace(tzinfo=NY).astimezone(timezone.utc)
    except ValueError:
        raise ValueError("unparseable last_trade_time: %r" % (raw,))


def session_date_of(moment):
    """The New York calendar date an aware instant falls on."""
    return moment.astimezone(NY).date()


def settlement_utc(expiry_date):
    """16:00 New York on an expiry date, as UTC."""
    return datetime.combine(expiry_date, SETTLEMENT_TIME, tzinfo=NY).astimezone(
        timezone.utc
    )


def years_to_expiry(expiry_date, now_utc):
    """Years from `now_utc` to settlement, floored at one hour.

    The floor exists so 0DTE contracts do not divide by zero in Black-Scholes.
    Contracts that have genuinely settled are dropped upstream, not floored.
    """
    seconds = (settlement_utc(expiry_date) - now_utc).total_seconds()
    return max(seconds / SECONDS_PER_YEAR, MIN_TTE_YEARS)


def touch_horizon_years(anchor_utc, session_date):
    """Years from `anchor_utc` to 16:00 New York on `session_date`. None once gone.

    `anchor_utc` is the instant the record's spot was true -- the same instant the
    blob header carries -- not the file's publish stamp and not the run clock.
    Pairing a spot with a horizon measured from fifteen minutes later would
    silently shorten every horizon by that gap.

    **One horizon for every chunk, the total book included.** A touch probability
    is only meaningful against a stated horizon, and a mixed-expiry book has no
    natural one, so every chunk is asked the same question: can price get there
    before today's close. That is also what makes the number comparable across
    chunks and across symbols, which is the point of transmitting it. Do not
    "fix" this later by giving the `R` bucket its own expiry-weighted horizon --
    two chunks would then carry numbers that look alike and mean different things.

    None once 16:00 has passed, and the field is then omitted rather than emitted
    as a zero.
    """
    seconds = (settlement_utc(session_date) - anchor_utc).total_seconds()
    if seconds <= 0.0:
        return None
    return seconds / TOUCH_SECONDS_PER_YEAR


def is_expired(expiry_date, now_utc):
    """True once settlement has passed.

    CBOE leaves settled expiries in the payload with open interest intact for a
    day or more. Summing them invents walls that do not exist.
    """
    return settlement_utc(expiry_date) <= now_utc


def utc_stamp(moment):
    """'YYYYMMDDHHMM' + literal Z, the blob's header time field.

    That field is the instant the record's `spot` was true -- NOT the moment the
    pipeline ran. See `cboe.spot_effective_time` for why they are different and
    what goes wrong when they are confused.
    """
    return moment.astimezone(timezone.utc).strftime("%Y%m%d%H%M") + "Z"


def date_stamp(day):
    """'YYYYMMDD'."""
    return day.strftime("%Y%m%d")


def now_utc():
    return datetime.now(timezone.utc)


__all__ = [
    "NY",
    "MIN_TTE_YEARS",
    "parse_cboe_timestamp",
    "parse_ny_timestamp",
    "session_date_of",
    "settlement_utc",
    "touch_horizon_years",
    "years_to_expiry",
    "is_expired",
    "utc_stamp",
    "date_stamp",
    "now_utc",
    "timedelta",
]
