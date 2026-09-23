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

Index symbols carry a caret (^SPX) and have to be URL-encoded; ETFs and single
stocks are passed through bare.
"""

import http.cookiejar
import json
import math
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from .cboe import Chain, Contract, FetchError, ssl_context
from .gex import MAX_IV, MIN_IV, RISK_FREE_RATE, SQRT_2PI
from .nytime import is_expired, now_utc, years_to_expiry
from .occ import OCCParseError, parse_occ
from .symbols import INDEX_TICKERS, to_ticker

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

SOURCE_NAME = "yahoo"


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
        url = CHAIN_URL.format(symbol=urllib.parse.quote(to_yahoo(symbol)))
        query = {"crumb": crumb}
        if date_epoch is not None:
            query["date"] = str(int(date_epoch))
        url = url + "?" + urllib.parse.urlencode(query)
        body = self._open(url)
        if not body:
            raise FetchError("yahoo: empty response for %s" % to_ticker(symbol))
        try:
            return json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise FetchError("yahoo: payload is not JSON for %s: %s"
                             % (to_ticker(symbol), exc))


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


def map_contracts(rows, expiry_epoch, spot, quote_ts, rate=RISK_FREE_RATE):
    """One expiry's calls and puts -> Contracts. Returns (contracts, dropped).

    `dropped` is a dict of counts, so a shape change shows up as a number in the
    run log instead of as a quietly shorter chain.

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

            if is_expired(expiry_date, quote_ts):
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

            years = years_to_expiry(expiry_date, quote_ts)
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


def reduce_payloads(symbol, payloads, now=None):
    """Validate the responses for one symbol and reduce them to a Chain.

    `payloads` is the first response followed by one per extra expiry. The quote
    is read out of the first; the rest contribute contracts only.
    """
    ticker = to_ticker(symbol)
    if not payloads:
        raise FetchError("%s: no yahoo payloads" % ticker)

    head = _result(payloads[0], ticker)
    quote = head.get("quote")
    if not isinstance(quote, dict):
        raise FetchError("%s: yahoo result carries no quote" % ticker)

    spot = _as_float(quote.get("regularMarketPrice"), 0.0)
    if spot <= 0:
        raise FetchError("%s: missing or non-positive regularMarketPrice" % ticker)

    market_time = quote.get("regularMarketTime")
    if market_time is None:
        raise FetchError("%s: yahoo quote carries no regularMarketTime" % ticker)
    try:
        quote_ts = _moment(market_time)
    except (TypeError, ValueError, OSError, OverflowError):
        raise FetchError("%s: unparseable regularMarketTime %r"
                         % (ticker, market_time))

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
            mapped, dropped = map_contracts(group, epoch, spot, quote_ts)
            contracts.extend(mapped)
            dropped_expired += dropped["expired"]
            dropped_unparseable += dropped["unparseable"] + dropped["expiry_mismatch"]

    if not contracts:
        raise FetchError("%s: no live contracts left after filtering" % ticker)

    # Yahoo's quote is the live one, not a delayed snapshot, so the instant the
    # spot was true IS the quote time. No fifteen-minute correction and no
    # fallback -- see `cboe.spot_effective_time` for why the CBOE path needs one.
    return Chain(ticker=ticker, quote_ts=quote_ts, spot=spot, contracts=contracts,
                 dropped_expired=dropped_expired,
                 dropped_unparseable=dropped_unparseable,
                 raw_count=raw_count,
                 spot_ts=quote_ts, spot_ts_fallback=None,
                 source=SOURCE_NAME)


def fetch_payloads(symbol, timeout=DEFAULT_TIMEOUT, now=None, pause=REQUEST_PAUSE,
                   max_expiries=None):
    """Every expiry for one symbol, as a list of responses.

    The first response carries the expiry list and the front expiry's contracts;
    each remaining expiry costs one more request. Expiries that have already
    settled are skipped rather than fetched -- they would be dropped on the way
    in anyway.
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

    for epoch in epochs:
        if pause:
            time.sleep(pause)
        try:
            payloads.append(connection.chain(symbol, date_epoch=epoch))
        except FetchError:
            # One expiry out of fifty is not worth losing the symbol over. The
            # first response is the one that cannot be missed, and it already
            # succeeded.
            continue
    return payloads


def load_chain(symbol, payloads=None, timeout=DEFAULT_TIMEOUT, now=None,
               pause=REQUEST_PAUSE, max_expiries=None):
    """Fetch (or accept saved responses) and reduce them to a Chain."""
    if payloads is None:
        payloads = fetch_payloads(symbol, timeout=timeout, now=now, pause=pause,
                                  max_expiries=max_expiries)
    return reduce_payloads(symbol, payloads, now=now)
