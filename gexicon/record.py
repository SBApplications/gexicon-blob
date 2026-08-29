"""Build one symbol's record: whole-market chunk plus optional expiry buckets.

Three different things are computed per chunk and they are easy to confuse.

  * **Levels** are net GEX per strike at the current spot, ranked by size.
  * **The flip** is the spot at which total dealer gamma would be zero. It has to
    be re-priced, not read off the levels. See `gex.gamma_flip`.
  * **The two walls** are a third thing again, and they are *not* read off the
    signs in the level list. Call gamma and put gamma are aggregated apart per
    strike, and the call wall is the heaviest call-side strike while the put wall
    is the heaviest put-side one. Both therefore exist on any chunk holding both
    kinds of contract. See `derive_walls`.
"""

from dataclasses import dataclass, field
from typing import List, Optional

from .gex import (gamma_flip, net_gex, sided_ladders, strike_ladder,
                  strike_sigma, touch_probability)
from .nytime import touch_horizon_years

BILLION = 1.0e9

TOTAL_LEVEL_LIMIT = 10
BUCKET_LEVEL_LIMIT = 6

TAG_ZERO_DTE = "0"
TAG_REST = "R"


@dataclass
class Level:
    right: str      # 'C' where net GEX at the strike is positive, else 'P'
    price: float
    magnitude: float  # billions, signed


@dataclass
class Wall:
    """One wall. `side` is the kind of contract the gamma came from, not the sign
    of a net, so a call wall is always 'C' and a put wall always 'P' however the
    book as a whole leans. `magnitude` is that side's gamma alone -- never the net
    at the strike, and never something to add to anything else. It keeps the file's
    sign convention, so a put wall always reads with a minus.
    """
    side: str       # 'C' or 'P'
    price: float
    magnitude: float  # billions, signed, ONE-SIDED
    # Probability, 0-100, that price touches this strike before today's close.
    # None when it could not be computed -- no usable implied vol at the strike,
    # or the close has already gone by -- and the token is then encoded with two
    # fields exactly as it was before this existed. Never faked with a zero.
    touch: Optional[int] = None


@dataclass
class Section:
    flip: float
    net: float                      # billions, signed
    levels: List[Level] = field(default_factory=list)
    tag: Optional[str] = None       # None for the whole-market chunk
    spot: Optional[float] = None    # whole-market chunk only
    # One-sided walls, computed per chunk: a 0DTE put wall is the heaviest put
    # strike in the 0DTE book. None only when the chunk holds no gamma on that side.
    call_wall: Optional[Wall] = None
    put_wall: Optional[Wall] = None


@dataclass
class Record:
    ticker: str
    sections: List[Section]

    @property
    def total(self):
        return self.sections[0]

    @property
    def buckets(self):
        return self.sections[1:]


# A level that rounds to +0.00B would still draw a line on the chart while saying
# nothing. Below this it is noise, not a level.
MIN_LEVEL_BILLIONS = 0.005


def _levels_from(contracts, spot, limit):
    ladder = strike_ladder(contracts, spot)
    ordered = sorted(ladder.items(), key=lambda kv: (-abs(kv[1]), kv[0]))
    out = []
    for strike, value in ordered[:limit]:
        magnitude = value / BILLION
        if abs(magnitude) < MIN_LEVEL_BILLIONS:
            break  # the list is sorted, so everything after is smaller still
        out.append(Level(right="C" if value > 0 else "P",
                         price=strike,
                         magnitude=magnitude))
    return out


def _wall_touch(strike, spot, iv_source, horizon_years):
    """Touch probability for one wall, or None when any input is unavailable."""
    if iv_source is None or horizon_years is None:
        return None
    sigma = strike_sigma(iv_source, strike)
    if sigma is None:
        return None
    return touch_probability(spot, strike, sigma, horizon_years)


def _side_wall(levels, sided, spot, sign, iv_source=None, horizon_years=None):
    """Heaviest one-sided strike among `levels`, preferring `sign`'s side of spot.

    `sign` is +1 for the call wall (strikes at or above spot preferred) and -1 for
    the put wall (at or below). The preference is dropped only when the preferred
    side holds no gamma of that kind at all -- losing the wall is worse than
    quoting it on the wrong side of price.

    `iv_source` and `horizon_years` feed the touch probability. Both default to
    absent, and the wall then carries `touch=None` and encodes exactly as it did
    before that field existed.
    """
    strikes = [lv.price for lv in levels]
    if not strikes:
        return None
    preferred = [k for k in strikes if (k >= spot if sign > 0 else k <= spot)]
    for pool in (preferred, strikes):
        # Only strikes actually carrying gamma on this side are candidates; a zero
        # would otherwise be reported as a wall.
        candidates = [(k, sided.get(k, 0.0)) for k in pool
                      if sign * sided.get(k, 0.0) > 0.0]
        if candidates:
            # Largest one-sided magnitude, ties to the lower strike -- the same
            # ordering the level list is ranked with.
            strike, value = max(candidates, key=lambda kv: (sign * kv[1], -kv[0]))
            magnitude = value / BILLION
            if abs(magnitude) < MIN_LEVEL_BILLIONS:
                # Rounds to 0.00B at 2dp. Same rule as a level: it would draw a
                # floor or a ceiling that means nothing. It happens on a 0DTE book
                # late in the day, where the drawn levels are all calls and what
                # put gamma is left sits at strikes too small to be transmitted.
                # No token is better than a line at zero -- the reader skips what
                # is absent.
                return None
            return Wall(side="C" if sign > 0 else "P",
                        price=strike,
                        magnitude=magnitude,
                        touch=_wall_touch(strike, spot, iv_source, horizon_years))
    return None


def derive_walls(levels, contracts, spot, iv_source=None, horizon_years=None):
    """The call wall and the put wall for one chunk, from one-sided gamma.

    The search runs over the strikes the chunk actually transmits -- its own ranked
    level list -- and not over the whole chain. Two reasons, both measured on the
    2026-08-04 chains:

    * A wall has to be a line the trader can see. A ceiling quoted at a price with
      no level drawn there is worse than no ceiling.
    * Over the whole chain the one-sided maxima land on far-dated round strikes
      rather than anything price is trading into. SPX 8000 carries huge open
      interest on both sides, so it holds both the largest call gamma and the
      largest put gamma in the book -- taking global maxima puts the ceiling and
      the floor on the SAME strike, 4% above spot. Those two sides very nearly
      cancel, which is exactly why the net ranking leaves the strike out.

    Deriving the walls from the sign of net instead -- biggest positive is the
    ceiling, biggest negative the floor -- is the other wrong answer. On a
    call-dominated book there is no negative strike anywhere: SPX on 2026-08-04
    returned all ten levels tagged C and drew a ceiling with no floor, on a chain
    holding 177B of put gamma.

    `iv_source` is the contract set the touch probability reads implied vol out of
    -- the record's whole live chain, not the chunk's own contracts. A 0DTE wall
    and a monthly wall at the same strike are the same *price*, and they share the
    same horizon, so they have to be quoted the same odds.
    """
    calls, puts = sided_ladders(contracts, spot)
    return (_side_wall(levels, calls, spot, +1, iv_source, horizon_years),
            _side_wall(levels, puts, spot, -1, iv_source, horizon_years))


def _section(contracts, spot, quote_ts, limit, tag=None, include_spot=False,
             iv_source=None, horizon_years=None):
    levels = _levels_from(contracts, spot, limit)
    call_wall, put_wall = derive_walls(levels, contracts, spot, iv_source,
                                       horizon_years)
    return Section(
        tag=tag,
        spot=spot if include_spot else None,
        flip=gamma_flip(contracts, spot, quote_ts),
        net=net_gex(contracts, spot) / BILLION,
        levels=levels,
        call_wall=call_wall,
        put_wall=put_wall,
    )


def build_record(chain, session_date):
    """Turn a reduced Chain into a Record.

    Buckets are emitted only when the symbol actually has a same-day expiry. Most
    equities most days have none, and they get no bucket sections at all -- not
    empty ones. Past 16:00 New York the same-day contracts have already been
    dropped as settled, so the 0DTE bucket disappears for the rest of the day.
    """
    spot = chain.spot
    quote_ts = chain.quote_ts
    contracts = chain.contracts

    # The touch horizon is anchored on the instant `spot` was true -- the same
    # instant the blob header carries -- and runs to 16:00 New York on the session
    # date. ONE horizon for every chunk, the total book included; see
    # `nytime.touch_horizon_years` for why a per-bucket horizon is wrong.
    horizon = touch_horizon_years(chain.spot_ts, session_date)

    sections = [_section(contracts, spot, quote_ts, TOTAL_LEVEL_LIMIT,
                         tag=None, include_spot=True,
                         iv_source=contracts, horizon_years=horizon)]

    zero_dte = [c for c in contracts if c.expiry == session_date]
    if zero_dte:
        rest = [c for c in contracts if c.expiry > session_date]
        sections.append(_section(zero_dte, spot, quote_ts, BUCKET_LEVEL_LIMIT,
                                 tag=TAG_ZERO_DTE, iv_source=contracts,
                                 horizon_years=horizon))
        sections.append(_section(rest, spot, quote_ts, BUCKET_LEVEL_LIMIT,
                                 tag=TAG_REST, iv_source=contracts,
                                 horizon_years=horizon))

    return Record(ticker=chain.ticker, sections=sections)
