"""Symbol mapping between TradingView tickers and CBOE's endpoint names."""

# CBOE prefixes cash indices with an underscore. Everything else is bare.
INDEX_TICKERS = frozenset({"SPX", "NDX", "RUT", "DJX"})

# DIA carries the Dow. It is the ETF, not the _DJX index, and that is deliberate:
# measured on the 2026-08-04 chain DIA holds 548,810 contracts of open interest
# against DJX's 48,075, and about nine times the gamma. DJX is too thin to produce
# usable levels. Both price at roughly a hundredth of the index, so either would
# translate onto a Dow futures chart the same way -- DIA is simply the one with
# positions in it.
DEFAULT_SYMBOLS = ("SPY", "SPX", "QQQ", "NDX", "IWM", "RUT", "DIA", "TSLA", "NVDA",
                   "MU", "SNDK", "AMD", "GOOGL", "PLTR", "AAPL")


# The ETF that tracks each cash index, used when Yahoo's index quote has stopped
# updating. Yahoo publishes no pre-market print for an index, and on 2026-09-24 it
# held yesterday's close on ^SPX and ^RUT well past the open while every ETF quote
# was live. The ETF is the same market in a tradeable wrapper, so it can stand in
# for the index's spot. See `yahoo.proxy_spot`.
INDEX_TWINS = {"SPX": "SPY", "NDX": "QQQ", "RUT": "IWM"}


def to_ticker(symbol):
    """Normalise any spelling to the TradingView ticker: '_SPX' -> 'SPX'."""
    return symbol.strip().upper().lstrip("_")


def to_cboe(symbol):
    """Return the CBOE endpoint name: 'SPX' -> '_SPX', 'SPY' -> 'SPY'."""
    ticker = to_ticker(symbol)
    return "_" + ticker if ticker in INDEX_TICKERS else ticker
