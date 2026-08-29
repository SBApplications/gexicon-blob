"""OCC option symbol parsing.

An OCC symbol looks like SPY260803C00500000:

    SPY      root, variable length (1-6 chars, and index roots can carry digits)
    260803   expiry YYMMDD
    C        C or P
    00500000 strike in thousandths of a dollar

The root is variable length, so everything is parsed from the right.
"""

from datetime import date

STRIKE_DIGITS = 8
EXPIRY_DIGITS = 6
# strike + right + expiry
TAIL_LEN = STRIKE_DIGITS + 1 + EXPIRY_DIGITS  # 15


class OCCParseError(ValueError):
    """Raised when a contract symbol does not parse. Never swallowed silently."""


def parse_occ(symbol):
    """Return (root, expiry_date, right, strike_float).

    `right` is 'C' or 'P'. Raises OCCParseError on anything malformed.
    """
    if not isinstance(symbol, str):
        raise OCCParseError("symbol is not a string: %r" % (symbol,))
    sym = symbol.strip().upper()
    if len(sym) <= TAIL_LEN:
        raise OCCParseError("symbol too short to carry a root: %r" % (symbol,))

    strike_part = sym[-STRIKE_DIGITS:]
    right = sym[-(STRIKE_DIGITS + 1)]
    expiry_part = sym[-TAIL_LEN:-(STRIKE_DIGITS + 1)]
    root = sym[:-TAIL_LEN]

    if not root:
        raise OCCParseError("empty root: %r" % (symbol,))
    if right not in ("C", "P"):
        raise OCCParseError("bad right %r in %r" % (right, symbol))
    if not strike_part.isdigit():
        raise OCCParseError("bad strike %r in %r" % (strike_part, symbol))
    if not expiry_part.isdigit():
        raise OCCParseError("bad expiry %r in %r" % (expiry_part, symbol))

    yy = int(expiry_part[0:2])
    mm = int(expiry_part[2:4])
    dd = int(expiry_part[4:6])
    try:
        expiry = date(2000 + yy, mm, dd)
    except ValueError as exc:
        raise OCCParseError("impossible expiry date in %r: %s" % (symbol, exc))

    strike = int(strike_part) / 1000.0
    if strike <= 0:
        raise OCCParseError("non-positive strike in %r" % (symbol,))

    return root, expiry, right, strike
