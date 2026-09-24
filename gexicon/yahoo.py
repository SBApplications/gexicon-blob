"""Second source: Yahoo Finance's option chain.

CBOE's delayed-quotes CDN file is the primary source and stays that way. It has
one failure mode that is invisible from the outside: the file keeps serving, with
a well-formed timestamp on it, and simply stops being updated. On 2026-09-23 it
froze at 03:56 UTC and every build for the rest of the morning published the
previous session's numbers with no 0DTE bucket at all.

This module is what the pipeline reaches for when that happens. It is never the
preferred source -- see `pipeline.wants_fallback` for the rule -- because CBOE
publishes one file per symbol while Yahoo needs one request per expiry, and
because CBOE's own gamma is used as-is while Yahoo's has to be computed here.

Three requests to get started, then one per expiry:

  1. GET https://fc.yahoo.com -- any response will do; it is fetched only for the
     Set-Cookie it hands back. A 404 is normal and is not an error.
  2. GET .../v1/test/getcrumb with that cookie -- plain text, and the chain
     endpoint rejects the request without it.
  3. GET .../v7/finance/options/{SYM}?crumb=... -- quote, the list of expiry
     epochs, and the FIRST expiry's contracts. Every further expiry needs its own
     request with &date={epoch}.

The per-expiry requests for one symbol go out a few at a time -- see
`fetch_payloads`. The handshake above happens once, on one thread, before any of
them start.

Index symbols carry a caret (^SPX) and have to be URL-encoded; ETFs and single
stocks are passed through bare.

Two things about Yahoo's quote block drove the 2026-09-24 repairs and are worth
carrying in your head while reading this file:

  * `regularMarketTime` is the last *print*, not the moment of the request. Before
    09:30 it is yesterday's close for every symbol on the board, so it cannot be
    used to decide which session a chain belongs to. `chain_session_date` asks the
    chain instead.
  * cash indexes carry no pre- or post-market print and their quotes can sit
    still for hours during the session (^SPX and ^RUT still held the previous
    close at 09:50 on 2026-09-24). `proxy_spot` carries the ETF twin's live price
    onto the index's scale rather than publishing a day-old spot.
"""

import concurrent.futures
import http.cookiejar
import json
import math
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, time as clock_time, timedelta, timezone

from .cboe import Chain, Contract, FetchError, ssl_context
from .gex import MAX_IV, MIN_IV, RISK_FREE_RATE, SQRT_2PI
from .nytime import NY, is_expired, now_utc, session_date_of, years_to_expiry
from .occ import OCCParseError, parse_occ
from .symbols import INDEX_TICKERS, INDEX_TWINS, to_ticker

COOKIE_URL = "https://fc.yahoo.com"
CRUMB_URL = "https://query2.finance.yahoo.com/v1/test/getcrumb"
CHAIN_URL = "https://query2.finance.yahoo.com/v7/finance/options/{symbol}"

# Yahoo serves the chain endpoint only to something that looks like a browser.
# The gexicon user agent CBOE is happy with gets a 401 here.
USER_AGENT = "Mozilla/5.0"
DEFAULT_TIMEOUT = 20

# One request per expiry, and SPX alone carries about sixty of them. A short
# pause between requests keeps a fifteen-symbol run from looking like a scrape.
REQUEST_PAUSE = 0.15

# One retry, on the two statuses that mean "ask again", and nothing else. A 404
# or a 401 will not get better by being repeated.
RETRY_STATUSES = (429, 500, 502, 503, 504)
RETRY_PAUSE = 1.0

# How many of one symbol's expiries are in flight at once. Fifteen symbols at one
# request per expiry is about 450 requests, and one at a time took roughly three
# minutes of wall clock. Four at a time is the same requests, the same pauses per
# request, and a quarter of the waiting.
YAHOO_WORKERS = 4

# The gap left between handing one expiry to the pool and handing over the next,
# so the first four do not leave in the same instant. This is not the throttle --
# RETRY_PAUSE and the retry rule above still do that work -- just a stagger.
SUBMIT_STAGGER = 0.05

SOURCE_NAME = "yahoo"

# The weekday window, New York local, in which a quote is expected to be moving
# and the session rules below are allowed to fire. It opens at 04:00 because that
# is when pre-market trading starts and closes with the fallback window itself.
BUILD_WINDOW_OPEN_NY = clock_time(4, 0)
BUILD_WINDOW_CLOSE_NY = clock_time(16, 30)

# How far behind the clock a quote may sit inside that window and still count as
# this session's live print. Half an hour is longer than any gap a traded symbol
# shows and far shorter than the overnight gap a frozen one shows.
STALE_QUOTE = timedelta(minutes=30)


def inside_build_window(moment):
    """True when `moment` is a weekday inside the pre-market-to-close window."""
    local = moment.astimezone(NY)
    if local.weekday() >= 5:
        return False
    return BUILD_WINDOW_OPEN_NY <= local.time() <= BUILD_WINDOW_CLOSE_NY


def previous_weekday(day):
    """The trading day before `day`, weekends aside. Monday looks back to Friday.

    Holidays are not in it -- there is no holiday calendar here and this is only
    ever used as an outer bound, never as a claim that the market was open.
    """
    step = {0: 3, 5: 1, 6: 2}.get(day.weekday(), 1)
    return day - timedelta(days=step)


def quote_is_stale(quote_ts, now):
    """True when a quote is too old to be the session's live print.

    Outside the window every quote is legitimately still -- the market is shut --
    so nothing is stale there.
    """
    if now is None or not inside_build_window(now):
        return False
    return (now - quote_ts) > STALE_QUOTE


def to_yahoo(symbol):
    """The endpoint name: 'SPX' -> '^SPX', 'SPY' -> 'SPY'.

    Cash indices carry a caret on Yahoo where CBOE prefixes them with an
    underscore. The caret still has to be percent-encoded into the URL; that
    happens at the call site, not here, so this returns the symbol as Yahoo
    names it rather than as a URL spells it.
    """
    ticker = to_ticker(symbol)
    return "^" + ticker if ticker in INDEX_TICKERS else ticker


def expiry_from_epoch(epoch):
    """The calendar date an expiry epoch names, read in UTC.

    Yahoo writes each expiry as midnight UTC on the day it expires. Reading it
    in local time moves it a day for anyone west of Greenwich, which would put
    today's expiry in yesterday's bucket and empty the 0DTE view -- the exact
    thing this whole source exists to repair.
    """
    return datetime.fromtimestamp(int(epoch), timezone.utc).date()


def _moment(epoch):
    return datetime.fromtimestamp(int(epoch), timezone.utc)


def chain_url(symbol, crumb, date_epoch=None):
    """The chain endpoint URL for one symbol, optionally for one expiry."""
    url = CHAIN_URL.format(symbol=urllib.parse.quote(to_yahoo(symbol)))
    query = {"crumb": crumb}
    if date_epoch is not None:
        query["date"] = str(int(date_epoch))
    return url + "?" + urllib.parse.urlencode(query)


def _open_static(url, headers, timeout=DEFAULT_TIMEOUT):
    """One request built from plain header strings, on its own opener.

    This is the call the worker threads make. It deliberately does NOT go through
    `_Session`: an `http.cookiejar.CookieJar` is written to on every response, and
    the opener that wraps it is shared state that several threads would be
    mutating at once. The cookie is a fixed string by the time this runs -- see
    `_Session.cookie_header` -- so a worker only ever reads it.

    `ssl_context()` is cached after its first call, and that call happens during
    the handshake, before any thread starts. The opener itself is built per
    request, which costs nothing next to a network round trip.
    """
    request = urllib.request.Request(url, headers=dict(headers))
    opener = urllib.request.build_opener(
        urllib.request.HTTPSHandler(context=ssl_context()))
    for attempt in (0, 1):
        try:
            with opener.open(request, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            if exc.code in RETRY_STATUSES and attempt == 0:
                # A second ago is soon enough on a 429, and this sleep is the
                # worker's own -- the other threads carry on.
                time.sleep(RETRY_PAUSE)
                continue
            raise FetchError("yahoo: HTTP %s from %s" % (exc.code, url))
        except urllib.error.URLError as exc:
            if attempt == 0:
                time.sleep(RETRY_PAUSE)
                continue
            raise FetchError("yahoo: network error on %s: %s" % (url, exc.reason))
        except OSError as exc:
            raise FetchError("yahoo: transport error on %s: %s" % (url, exc))
    raise FetchError("yahoo: gave up on %s" % url)


def _decode_chain(body, ticker):
    if not body:
        raise FetchError("yahoo: empty response for %s" % ticker)
    try:
        return json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise FetchError("yahoo: payload is not JSON for %s: %s" % (ticker, exc))


def fetch_expiry(symbol, crumb, headers, date_epoch, timeout=DEFAULT_TIMEOUT):
    """One expiry's chain response. Safe to call from a worker thread."""
    body = _open_static(chain_url(symbol, crumb, date_epoch=date_epoch),
                        headers, timeout=timeout)
    return _decode_chain(body, to_ticker(symbol))


class _Session(object):
    """A cookie jar plus the crumb that goes with it. Fetched once, reused."""

    def __init__(self, timeout=DEFAULT_TIMEOUT):
        self.timeout = timeout
        self.jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.jar),
            urllib.request.HTTPSHandler(context=ssl_context()))
        self.crumb = None

    def _open(self, url, tolerate_http_error=False):
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        for attempt in (0, 1):
            try:
                with self.opener.open(request, timeout=self.timeout) as response:
                    return response.read()
            except urllib.error.HTTPError as exc:
                if tolerate_http_error:
                    # The cookie step answers 404 and still sets the cookie.
                    return b""
                if exc.code in RETRY_STATUSES and attempt == 0:
                    time.sleep(RETRY_PAUSE)
                    continue
                raise FetchError("yahoo: HTTP %s from %s" % (exc.code, url))
            except urllib.error.URLError as exc:
                if attempt == 0:
                    time.sleep(RETRY_PAUSE)
                    continue
                raise FetchError("yahoo: network error on %s: %s" % (url, exc.reason))
            except OSError as exc:
                raise FetchError("yahoo: transport error on %s: %s" % (url, exc))
        raise FetchError("yahoo: gave up on %s" % url)

    def start(self):
        if self.crumb:
            return self.crumb
        self._open(COOKIE_URL, tolerate_http_error=True)
        crumb = self._open(CRUMB_URL).decode("utf-8", "replace").strip()
        # An expired or missing cookie gets an HTML login page here rather than a
        # crumb. Anything long or with a tag in it is not a crumb.
        if not crumb or len(crumb) > 32 or "<" in crumb:
            raise FetchError("yahoo: no usable crumb (got %r)" % crumb[:40])
        self.crumb = crumb
        return crumb

    def chain(self, symbol, date_epoch=None):
        """One chain response: the whole payload, decoded."""
        crumb = self.start()
        body = self._open(chain_url(symbol, crumb, date_epoch=date_epoch))
        return _decode_chain(body, to_ticker(symbol))

    def cookie_header(self):
        """What the jar would put in a Cookie header, as a fixed string.

        Taken once, after the handshake, and handed to the workers so none of
        them has to reach into the jar. The jar keeps being written to by the
        main thread's own requests; this string does not change under anyone.
        """
        probe = urllib.request.Request(chain_url("SPY", "probe"),
                                       headers={"User-Agent": USER_AGENT})
        self.jar.add_cookie_header(probe)
        return probe.get_header("Cookie") or ""

    def worker_headers(self):
        """The headers a worker thread sends: user agent plus the cookie."""
        headers = {"User-Agent": USER_AGENT}
        cookie = self.cookie_header()
        if cookie:
            headers["Cookie"] = cookie
        return headers


_session_cache = []


def session(timeout=DEFAULT_TIMEOUT):
    """The process-wide Yahoo session. The crumb is good for the whole run."""
    if not _session_cache:
        _session_cache.append(_Session(timeout=timeout))
    return _session_cache[0]


def reset_session():
    """Forget the cookie and crumb. Tests use this; the pipeline does not."""
    del _session_cache[:]


def _result(payload, ticker):
    if not isinstance(payload, dict):
        raise FetchError("%s: yahoo payload is not an object" % ticker)
    chain = payload.get("optionChain")
    if not isinstance(chain, dict):
        raise FetchError("%s: yahoo payload has no optionChain" % ticker)
    if chain.get("error"):
        raise FetchError("%s: yahoo returned an error: %s" % (ticker, chain["error"]))
    results = chain.get("result")
    if not isinstance(results, list) or not results:
        raise FetchError("%s: yahoo returned no result" % ticker)
    first = results[0]
    if not isinstance(first, dict):
        raise FetchError("%s: yahoo result is not an object" % ticker)
    return first


def bs_gamma(spot, strike, sigma, years, rate=RISK_FREE_RATE):
    """Black-Scholes gamma, in the same units CBOE publishes it.

    CBOE hands its gamma over ready-made and `gex.contract_gex` multiplies it
    straight through, so a Yahoo contract has to arrive carrying the same
    quantity or every level, net and wall would come out of a different formula
    than the CBOE path uses.

        gamma = exp(-0.5 * d1^2) / (S * sigma * sqrt(T) * sqrt(2*pi))

    This is the same closed form `gex._flip_terms` re-prices the flip with, at
    the same risk-free rate and off the same `years_to_expiry`, so the two agree
    with each other whichever source the chain came from. No dividend yield, for
    the same reason: the flip does not use one either.

    Zero when the inputs cannot support a number -- a junk vol, a settled expiry
    -- rather than a fabricated figure. A zero-gamma contract contributes
    nothing and is skipped downstream, which is the honest answer.
    """
    if spot <= 0.0 or strike <= 0.0 or years <= 0.0:
        return 0.0
    if not (MIN_IV <= sigma <= MAX_IV):
        return 0.0
    vol_sqrt_t = sigma * math.sqrt(years)
    if vol_sqrt_t <= 0.0:
        return 0.0
    d1 = (math.log(spot / strike) + (rate + 0.5 * sigma * sigma) * years) / vol_sqrt_t
    return math.exp(-0.5 * d1 * d1) / (spot * vol_sqrt_t * SQRT_2PI)


def _as_float(value, default=0.0):
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# The two prints besides the regular one a Yahoo quote can carry. ETFs and single
# stocks have them; a cash index has neither, which is why its quote reads as
# yesterday's close every pre-market.
_EXTRA_PRINTS = (("preMarketPrice", "preMarketTime", "the pre-market print"),
                 ("postMarketPrice", "postMarketTime", "the post-market print"))


def choose_spot(quote, ticker):
    """The freshest print in a quote block: (price, instant, which).

    `regularMarketPrice` is the only field guaranteed to be there, and pre-market
    it is yesterday's 16:00 close for every symbol on the board. Where Yahoo also
    carries a pre- or post-market print, the newer of them is the live one and is
    what the record should be built on.
    """
    price = _as_float(quote.get("regularMarketPrice"), 0.0)
    if price <= 0:
        raise FetchError("%s: missing or non-positive regularMarketPrice" % ticker)
    market_time = quote.get("regularMarketTime")
    if market_time is None:
        raise FetchError("%s: yahoo quote carries no regularMarketTime" % ticker)
    try:
        moment = _moment(market_time)
    except (TypeError, ValueError, OSError, OverflowError):
        raise FetchError("%s: unparseable regularMarketTime %r"
                         % (ticker, market_time))
    which = "the regular session close"

    for price_key, time_key, label in _EXTRA_PRINTS:
        other = _as_float(quote.get(price_key), 0.0)
        stamp = quote.get(time_key)
        if other <= 0 or stamp is None:
            continue
        try:
            when = _moment(stamp)
        except (TypeError, ValueError, OSError, OverflowError):
            continue
        if when > moment:
            price, moment, which = other, when, label
    return price, moment, which


def proxy_spot(index_quote, twin_quote, twin_ticker):
    """An index spot carried across from its ETF twin. (price, instant) or None.

    Yahoo's cash index quotes stop moving. Pre-market that is expected -- no
    index prints before 09:30 -- but on 2026-09-24 ^SPX and ^RUT still held the
    previous close at 09:50 while every ETF quote was live. The index has no
    second print to fall back on, so the twin supplies one:

        SPX = SPY_live * (SPX_prev_close / SPY_prev_close)

    Both previous closes are the same session's, so the ratio is that day's own
    tracking ratio rather than a constant. What is left over is the ETF's
    tracking drift across one session -- basis points -- against a spot that is
    otherwise a whole session out of date.
    """
    index_prev = _as_float(index_quote.get("regularMarketPreviousClose"), 0.0)
    twin_prev = _as_float(twin_quote.get("regularMarketPreviousClose"), 0.0)
    if index_prev <= 0 or twin_prev <= 0:
        return None
    try:
        twin_price, twin_ts, _which = choose_spot(twin_quote, twin_ticker)
    except FetchError:
        return None
    if twin_price <= 0:
        return None
    return twin_price * (index_prev / twin_prev), twin_ts


def chain_session_date(quote_ts, expiries, now):
    """Which New York session a chain belongs to.

    The obvious answer -- the session the quote stamp falls on -- is the one that
    broke the 2026-09-24 builds. Pre-market that stamp is yesterday's 16:00 close
    for every symbol, and the cash indexes keep it well past the open, so reading
    the stamp alone calls a live chain yesterday's and drops it.

    The chain itself knows. Open interest rolls overnight and settled expiries
    leave the file, so a chain whose earliest live expiry is today is today's
    chain, whatever its quote stamp says.

    That is also what still refuses a market holiday. Nothing expires on a
    holiday, so the earliest live expiry is the next trading day, the stamp rule
    stands, the symbol is dropped as stale and the last good line stays up. Note
    that `earliest >= today` would NOT refuse it -- tomorrow's expiry satisfies
    that -- which is why the test is equality.
    """
    quoted = session_date_of(quote_ts)
    if now is None:
        return quoted
    today = session_date_of(now)
    if quoted == today or not inside_build_window(now):
        return quoted
    if not expiries or min(expiries) != today:
        return quoted
    # Belt and braces on a chain that has stopped being updated altogether: a
    # quote older than the previous trading day is not a session behind, it is
    # abandoned, whatever expiries it still lists.
    if quoted < previous_weekday(today):
        return quoted
    return today


def map_contracts(rows, expiry_epoch, spot, as_of, rate=RISK_FREE_RATE):
    """One expiry's calls and puts -> Contracts. Returns (contracts, dropped).

    `dropped` is a dict of counts, so a shape change shows up as a number in the
    run log instead of as a quietly shorter chain.

    `as_of` is the instant the expiry maths is asked at -- the run clock when
    there is one, not the quote stamp. They are the same thing during a normal
    session, and a session apart pre-market, where pricing today's expiry off
    yesterday's stamp would hand it an extra day of life and halve its gamma.

    Every filter here matches the CBOE path exactly: a contract symbol that will
    not parse is dropped, an expiry that has already settled is dropped, and open
    interest of zero is dropped. The one rule Yahoo needs and CBOE does not is
    the missing implied vol, because gamma is computed from it here rather than
    read off the feed -- with no vol there is no gamma, and a contract carrying
    a zero gamma is weight in the chain that moves nothing.
    """
    expiry_date = expiry_from_epoch(expiry_epoch)
    contracts = []
    dropped = {"unparseable": 0, "expired": 0, "no_oi": 0, "no_iv": 0,
               "expiry_mismatch": 0}

    for right_key in ("calls", "puts"):
        for row in rows.get(right_key) or ():
            if not isinstance(row, dict):
                dropped["unparseable"] += 1
                continue
            occ = row.get("contractSymbol")
            try:
                _root, occ_expiry, right, strike = parse_occ(occ)
            except OCCParseError:
                dropped["unparseable"] += 1
                continue

            # Two independent statements of the same date: the epoch the expiry
            # was requested under, and the six digits inside the contract symbol.
            # They have never disagreed in practice. If they ever do, the expiry
            # is unknown, and guessing one is how a contract lands in the wrong
            # bucket -- the 0DTE view is the whole reason this source exists.
            if occ_expiry != expiry_date:
                dropped["expiry_mismatch"] += 1
                continue

            if is_expired(expiry_date, as_of):
                dropped["expired"] += 1
                continue

            open_interest = _as_float(row.get("openInterest"))
            if open_interest <= 0:
                dropped["no_oi"] += 1
                continue

            iv = row.get("impliedVolatility")
            if iv is None:
                dropped["no_iv"] += 1
                continue
            iv = _as_float(iv)

            years = years_to_expiry(expiry_date, as_of)
            contracts.append(Contract(
                occ=occ.strip().upper(),
                expiry=expiry_date,
                right=right,
                strike=strike,
                open_interest=open_interest,
                gamma=bs_gamma(spot, strike, iv, years, rate=rate),
                iv=iv,
                volume=_as_float(row.get("volume")),
            ))

    return contracts, dropped


def reduce_payloads(symbol, payloads, now=None, twin_quote=None):
    """Validate the responses for one symbol and reduce them to a Chain.

    `payloads` is the first response followed by one per extra expiry. The quote
    is read out of the first; the rest contribute contracts only.

    `twin_quote` is the ETF twin's quote block, supplied only for a cash index
    whose own quote has gone stale. See `proxy_spot`.
    """
    ticker = to_ticker(symbol)
    if not payloads:
        raise FetchError("%s: no yahoo payloads" % ticker)

    head = _result(payloads[0], ticker)
    quote = head.get("quote")
    if not isinstance(quote, dict):
        raise FetchError("%s: yahoo result carries no quote" % ticker)

    spot, quote_ts, _which = choose_spot(quote, ticker)

    # A cash index with no live print of its own. The twin stands in where it
    # can; where it cannot the prior close is kept -- the flip and the walls are
    # built from open interest and strikes and do not need a live spot -- and
    # either way the run log says which happened.
    spot_note = None
    twin_ticker = INDEX_TWINS.get(ticker)
    if twin_ticker and quote_is_stale(quote_ts, now):
        age_hours = (now - quote_ts).total_seconds() / 3600.0
        proxied = None
        if isinstance(twin_quote, dict):
            proxied = proxy_spot(quote, twin_quote, twin_ticker)
        if proxied is not None and not quote_is_stale(proxied[1], now):
            spot, quote_ts = proxied
            spot_note = ("spot proxied from %s (index quote %.1fh old)"
                         % (twin_ticker, age_hours))
        else:
            spot_note = ("spot is the prior close until the index prints "
                         "(index quote %.1fh old)" % age_hours)

    # Expiry maths is asked as of the run clock wherever there is one. See
    # `map_contracts`.
    as_of = now if (now is not None and now > quote_ts) else quote_ts

    contracts = []
    dropped_expired = 0
    dropped_unparseable = 0
    raw_count = 0

    for payload in payloads:
        result = _result(payload, ticker)
        for group in result.get("options") or ():
            if not isinstance(group, dict):
                continue
            epoch = group.get("expirationDate")
            if epoch is None:
                continue
            raw_count += len(group.get("calls") or ()) + len(group.get("puts") or ())
            mapped, dropped = map_contracts(group, epoch, spot, as_of)
            contracts.extend(mapped)
            dropped_expired += dropped["expired"]
            dropped_unparseable += dropped["unparseable"] + dropped["expiry_mismatch"]

    if not contracts:
        raise FetchError("%s: no live contracts left after filtering" % ticker)

    # Yahoo's quote is the live one, not a delayed snapshot, so the instant the
    # spot was true IS the quote time. No fifteen-minute correction and no
    # fallback -- see `cboe.spot_effective_time` for why the CBOE path needs one.
    # Where the spot was proxied, both are the twin's, because the twin's print
    # is the instant that number was true.
    session = chain_session_date(quote_ts, {c.expiry for c in contracts}, now)
    return Chain(ticker=ticker, quote_ts=quote_ts, spot=spot, contracts=contracts,
                 dropped_expired=dropped_expired,
                 dropped_unparseable=dropped_unparseable,
                 raw_count=raw_count,
                 spot_ts=quote_ts, spot_ts_fallback=None,
                 source=SOURCE_NAME, session_date=session, spot_note=spot_note)


def fetch_payloads(symbol, timeout=DEFAULT_TIMEOUT, now=None, pause=REQUEST_PAUSE,
                   max_expiries=None, workers=None):
    """Every expiry for one symbol, as a list of responses.

    The first response carries the expiry list and the front expiry's contracts;
    each remaining expiry costs one more request. Expiries that have already
    settled are skipped rather than fetched -- they would be dropped on the way
    in anyway.

    Those remaining requests go out `workers` at a time. Only the expiries of one
    symbol overlap; symbols still run one after another, because the fallback is
    decided per symbol and a whole board in flight at once is a scrape.

    The returned list is always the first response followed by the rest in
    ascending expiry order, whichever thread happened to finish first.
    `reduce_payloads` walks this list in order and never sorts, so the order of
    this list IS the order of the contracts in the blob -- a parallel run and a
    sequential one have to hand back the same list or they publish different
    bytes off the same data.

    An expiry that fails is dropped on its own; the symbol keeps every other
    expiry it did get. Only the first response cannot be missed, and by this
    point it has already succeeded.
    """
    ticker = to_ticker(symbol)
    reference = now if now is not None else now_utc()
    connection = session(timeout=timeout)

    first = connection.chain(symbol)
    result = _result(first, ticker)
    payloads = [first]

    served = set()
    for group in result.get("options") or ():
        if isinstance(group, dict) and group.get("expirationDate") is not None:
            served.add(int(group["expirationDate"]))

    epochs = []
    for raw in result.get("expirationDates") or ():
        try:
            epoch = int(raw)
        except (TypeError, ValueError):
            continue
        if epoch in served:
            continue
        if is_expired(expiry_from_epoch(epoch), reference):
            continue
        epochs.append(epoch)
    epochs.sort()
    if max_expiries is not None:
        epochs = epochs[:max_expiries]

    if not epochs:
        return payloads

    count = YAHOO_WORKERS if workers is None else max(1, int(workers))
    crumb = connection.start()
    headers = connection.worker_headers()

    if count == 1:
        for epoch in epochs:
            if pause:
                time.sleep(pause)
            try:
                payloads.append(fetch_expiry(symbol, crumb, headers, epoch,
                                             timeout=timeout))
            except FetchError:
                continue
        return payloads

    done = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
        pending = {}
        for epoch in epochs:
            pending[pool.submit(fetch_expiry, symbol, crumb, headers, epoch,
                                timeout=timeout)] = epoch
            if SUBMIT_STAGGER:
                time.sleep(SUBMIT_STAGGER)
        for future, epoch in pending.items():
            try:
                done[epoch] = future.result()
            except FetchError:
                continue

    # Expiry order, not the order the threads came back in. See the docstring.
    for epoch in epochs:
        if epoch in done:
            payloads.append(done[epoch])
    return payloads


def fetch_quote(symbol, timeout=DEFAULT_TIMEOUT):
    """One symbol's quote block. One request, no expiries fetched."""
    ticker = to_ticker(symbol)
    quote = _result(session(timeout=timeout).chain(symbol), ticker).get("quote")
    if not isinstance(quote, dict):
        raise FetchError("%s: yahoo result carries no quote" % ticker)
    return quote


def twin_quote_for(symbol, payloads, now, timeout=DEFAULT_TIMEOUT):
    """The ETF twin's quote, fetched only when the index's own has gone stale.

    One extra request, and only for an index, and only on a run where its quote
    has already stopped moving. A twin that cannot be fetched is not a lost
    symbol: the prior close is kept and the run log says so.
    """
    ticker = to_ticker(symbol)
    twin = INDEX_TWINS.get(ticker)
    if not twin or not payloads:
        return None
    try:
        quote = _result(payloads[0], ticker).get("quote")
        if not isinstance(quote, dict):
            return None
        _spot, quote_ts, _which = choose_spot(quote, ticker)
        if not quote_is_stale(quote_ts, now):
            return None
        return fetch_quote(twin, timeout=timeout)
    except FetchError:
        return None


def load_chain(symbol, payloads=None, timeout=DEFAULT_TIMEOUT, now=None,
               pause=REQUEST_PAUSE, max_expiries=None, twin_quote=None,
               workers=None):
    """Fetch (or accept saved responses) and reduce them to a Chain."""
    if payloads is None:
        payloads = fetch_payloads(symbol, timeout=timeout, now=now, pause=pause,
                                  max_expiries=max_expiries, workers=workers)
    if twin_quote is None:
        twin_quote = twin_quote_for(symbol, payloads, now, timeout=timeout)
    return reduce_payloads(symbol, payloads, now=now, twin_quote=twin_quote)
