"""Fetch and reduce CBOE delayed option chains.

The endpoint is public, unofficial and undocumented. Every assumption about the
payload is checked here and failure is loud: a short blob that looks fine is worse
than no blob at all.
"""

import gzip
import json
import os
import ssl
import urllib.error
import urllib.request

from .nytime import (is_expired, now_utc, parse_cboe_timestamp,
                     parse_ny_timestamp, session_date_of, timedelta)
from .occ import OCCParseError, parse_occ
from .symbols import to_cboe, to_ticker

URL_TEMPLATE = "https://cdn.cboe.com/api/global/delayed_quotes/options/{symbol}.json"

USER_AGENT = "gexicon/2.0 (+personal GEX levels; stdlib urllib)"
DEFAULT_TIMEOUT = 60
# A quote older than this means the feed has stalled. Never emit it as today's data.
MAX_QUOTE_AGE_HOURS = 12

# The feed is delayed by about fifteen minutes, so this is what the gap between
# the file's publish stamp and the effective time of its spot should look like.
# Used only as a fallback when `last_trade_time` cannot be trusted.
ASSUMED_FEED_DELAY = timedelta(minutes=15)

# How far `last_trade_time` may sit from the file's own publish stamp before it
# is treated as stale or malformed rather than as the normal delay. Two hours is
# far wider than any delay the feed has ever shown and far narrower than the
# overnight/weekend gap a genuinely stale field produces.
MAX_SPOT_TIME_SKEW = timedelta(hours=2)


class FetchError(RuntimeError):
    """Any failure to obtain a usable chain for one symbol."""


# A python.org macOS build that has never had "Install Certificates.command" run
# has no trust roots at all, and every fetch dies with CERTIFICATE_VERIFY_FAILED.
# Find a bundle rather than depend on the installer having been run. Verification
# is never disabled.
_CA_BUNDLE_FALLBACKS = (
    "/etc/ssl/cert.pem",                      # macOS system bundle
    "/etc/pki/tls/certs/ca-bundle.crt",       # RHEL/Fedora
    "/etc/ssl/certs/ca-certificates.crt",     # Debian/Ubuntu
)

_ssl_context_cache = []


def ssl_context():
    """A verifying SSL context, with trust roots located the hard way if needed."""
    if _ssl_context_cache:
        return _ssl_context_cache[0]

    context = ssl.create_default_context()
    if not context.get_ca_certs():
        loaded = False
        for path in _CA_BUNDLE_FALLBACKS:
            if os.path.exists(path):
                try:
                    context.load_verify_locations(cafile=path)
                    loaded = True
                    break
                except (ssl.SSLError, OSError):
                    continue
        if not loaded:
            try:
                import certifi  # optional; not a dependency
                context.load_verify_locations(cafile=certifi.where())
            except Exception:
                pass
    _ssl_context_cache.append(context)
    return context


class Contract(object):
    __slots__ = ("occ", "expiry", "right", "strike", "open_interest", "gamma",
                 "iv", "volume")

    def __init__(self, occ, expiry, right, strike, open_interest, gamma, iv, volume):
        self.occ = occ
        self.expiry = expiry
        self.right = right
        self.strike = strike
        self.open_interest = open_interest
        self.gamma = gamma
        self.iv = iv
        self.volume = volume

    @property
    def sign(self):
        """Dealers are assumed short calls and long puts."""
        return 1.0 if self.right == "C" else -1.0

    def __repr__(self):
        return "<Contract %s oi=%g gamma=%g iv=%g>" % (
            self.occ, self.open_interest, self.gamma, self.iv)


class Chain(object):
    """One symbol's reduced chain: spot, quote time, live contracts only.

    Two instants, and confusing them puts every futures level at the wrong price:

      * `quote_ts` -- when CBOE published the file. Used for expiry maths and for
        the archive filename.
      * `spot_ts` -- when `spot` was actually true, about fifteen minutes earlier.
        This is what the blob header carries, because the indicator anchors its
        futures basis on it. See `spot_effective_time`.
    """

    def __init__(self, ticker, quote_ts, spot, contracts, dropped_expired,
                 dropped_unparseable, raw_count, spot_ts=None,
                 spot_ts_fallback=None):
        self.ticker = ticker
        self.quote_ts = quote_ts
        self.spot = spot
        self.contracts = contracts
        self.dropped_expired = dropped_expired
        self.dropped_unparseable = dropped_unparseable
        self.raw_count = raw_count
        # Never None in practice: spot_effective_time always returns something,
        # and says so when it had to fall back.
        self.spot_ts = spot_ts if spot_ts is not None else quote_ts
        # None when last_trade_time was used as-is; otherwise the reason the
        # fallback fired, for the run log.
        self.spot_ts_fallback = spot_ts_fallback

    @property
    def session_date(self):
        return session_date_of(self.quote_ts)


def spot_effective_time(data, quote_ts):
    """The instant `data.current_price` was true. Returns (moment, fallback_reason).

    `current_price` is fifteen-minute delayed data. CBOE's top-level `timestamp`
    is when the *file* was published, so stamping that on the record claims the
    spot is a quarter of an hour fresher than it is. The indicator anchors a
    futures chart with

        basis = chart close at the header stamp - record spot * ratio

    so a fifteen-minute error there absorbs the index's move over that gap into
    "basis" and slides every level by it. Measured live 2026-08-05: a reported
    basis of 16.87 against a true futures-cash spread of 31.70.

    `data.last_trade_time` is the effective time of `current_price`, written in
    New York local time. It is trusted only when it lands within MAX_SPOT_TIME_SKEW
    of the file's own stamp; a missing, unparseable or wildly-off value means a
    stale or malformed field, and the fallback is the publish stamp less the
    known feed delay. The fallback is always reported -- never emit a timestamp
    you do not trust without saying so.
    """
    raw = data.get("last_trade_time") if isinstance(data, dict) else None
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return quote_ts - ASSUMED_FEED_DELAY, "last_trade_time is missing"
    try:
        moment = parse_ny_timestamp(raw)
    except ValueError:
        return (quote_ts - ASSUMED_FEED_DELAY,
                "last_trade_time %r will not parse" % (raw,))

    skew = moment - quote_ts
    if abs(skew) > MAX_SPOT_TIME_SKEW:
        return (quote_ts - ASSUMED_FEED_DELAY,
                "last_trade_time %s sits %+.1fh from the file stamp %s -- stale field"
                % (raw, skew.total_seconds() / 3600.0,
                   quote_ts.strftime("%Y-%m-%d %H:%M:%SZ")))
    return moment, None


def _as_float(value, default=0.0):
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def fetch_raw(symbol, timeout=DEFAULT_TIMEOUT):
    """Download one symbol's payload and return the decoded JSON dict."""
    url = URL_TEMPLATE.format(symbol=to_cboe(symbol))
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip",
            # Ask every intermediary for the current file. Without this a proxy or
            # CDN edge can hand back a copy from earlier in the session, and the
            # blob would carry a stale spot with a perfectly fresh-looking
            # computation stamp on it.
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout,
                                    context=ssl_context()) as response:
            if response.status != 200:
                raise FetchError("%s: HTTP %s" % (symbol, response.status))
            body = response.read()
            if response.headers.get("Content-Encoding") == "gzip":
                body = gzip.decompress(body)
    except urllib.error.HTTPError as exc:
        raise FetchError("%s: HTTP %s from %s" % (symbol, exc.code, url))
    except urllib.error.URLError as exc:
        raise FetchError("%s: network error: %s" % (symbol, exc.reason))
    except OSError as exc:
        raise FetchError("%s: transport error: %s" % (symbol, exc))

    if not body:
        raise FetchError("%s: empty response" % symbol)
    try:
        return json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise FetchError("%s: payload is not JSON: %s" % (symbol, exc))


def reduce_payload(symbol, payload, max_age_hours=MAX_QUOTE_AGE_HOURS, now=None):
    """Validate a payload and reduce it to a Chain of live contracts.

    Contracts whose expiry has already settled are dropped here and nowhere else.
    """
    ticker = to_ticker(symbol)

    if not isinstance(payload, dict):
        raise FetchError("%s: payload is not an object" % ticker)
    data = payload.get("data")
    if not isinstance(data, dict):
        raise FetchError("%s: payload has no data object" % ticker)

    try:
        quote_ts = parse_cboe_timestamp(payload.get("timestamp"))
    except ValueError as exc:
        raise FetchError("%s: %s" % (ticker, exc))

    if max_age_hours is not None:
        reference = now if now is not None else now_utc()
        age_hours = (reference - quote_ts).total_seconds() / 3600.0
        if age_hours > max_age_hours:
            raise FetchError(
                "%s: quote is %.1fh old (limit %.1fh) -- feed looks stalled"
                % (ticker, age_hours, max_age_hours))

    spot = _as_float(data.get("current_price"), 0.0)
    if spot <= 0:
        raise FetchError("%s: missing or non-positive current_price" % ticker)

    spot_ts, spot_ts_fallback = spot_effective_time(data, quote_ts)

    options = data.get("options")
    if not isinstance(options, list) or not options:
        raise FetchError("%s: payload carries no options array" % ticker)

    contracts = []
    dropped_expired = 0
    dropped_unparseable = 0

    for row in options:
        if not isinstance(row, dict):
            dropped_unparseable += 1
            continue
        try:
            _root, expiry, right, strike = parse_occ(row.get("option"))
        except OCCParseError:
            dropped_unparseable += 1
            continue

        # The single easiest way to get this wrong: CBOE keeps settled expiries in
        # the payload with open interest intact.
        if is_expired(expiry, quote_ts):
            dropped_expired += 1
            continue

        open_interest = _as_float(row.get("open_interest"))
        if open_interest <= 0:
            continue

        contracts.append(Contract(
            occ=row.get("option").strip().upper(),
            expiry=expiry,
            right=right,
            strike=strike,
            open_interest=open_interest,
            gamma=_as_float(row.get("gamma")),
            iv=_as_float(row.get("iv")),
            volume=_as_float(row.get("volume")),
        ))

    if not contracts:
        raise FetchError("%s: no live contracts left after filtering" % ticker)
    if dropped_unparseable and dropped_unparseable > len(options) // 2:
        raise FetchError(
            "%s: %d of %d contract symbols failed to parse -- payload shape changed"
            % (ticker, dropped_unparseable, len(options)))

    return Chain(ticker=ticker, quote_ts=quote_ts, spot=spot, contracts=contracts,
                 dropped_expired=dropped_expired,
                 dropped_unparseable=dropped_unparseable,
                 raw_count=len(options),
                 spot_ts=spot_ts, spot_ts_fallback=spot_ts_fallback)


def load_chain(symbol, payload=None, max_age_hours=MAX_QUOTE_AGE_HOURS, now=None,
               timeout=DEFAULT_TIMEOUT):
    """Fetch (or accept an offline payload) and reduce it."""
    if payload is None:
        payload = fetch_raw(symbol, timeout=timeout)
    return reduce_payload(symbol, payload, max_age_hours=max_age_hours, now=now)
