"""Gamma exposure maths.

Two different calculations live here and they are deliberately not the same thing:

  * Per-strike levels use CBOE's pre-computed gamma at the current spot. No
    Black-Scholes needed.
  * The gamma flip is the spot at which total dealer gamma would be zero. Gamma
    itself moves with spot, so the flip cannot be read off the per-strike numbers
    -- every contract is re-priced at each candidate spot.

A third, separate calculation lives at the bottom: the probability that price
TOUCHES a wall's strike before today's close, read out of the option-implied vol
at that strike. See `touch_probability`.

The common shortcut -- walking cumulative net GEX up the strike ladder looking for
a zero crossing -- is not implemented, on purpose. That curve only crosses when
total net GEX ends positive, so the flip vanishes on exactly the symbols sitting in
negative gamma, which is the regime where it matters most.
"""

import math

from .nytime import years_to_expiry

RISK_FREE_RATE = 0.04  # constant, not an input
FLIP_SPAN = 0.2        # +/- 20% of spot
FLIP_STEPS = 200

# Gamma scales as 1/sigma, so a single contract with a broken IV can dominate the
# whole re-pricing. Bound it hard.
MIN_IV = 0.01
MAX_IV = 5.0

SQRT_2PI = math.sqrt(2.0 * math.pi)


def contract_gex(contract, spot):
    """Dollar gamma per 1% move for one contract. Calls positive, puts negative."""
    if contract.open_interest <= 0 or contract.gamma == 0.0:
        return 0.0
    return (contract.sign * contract.gamma * contract.open_interest * 100.0
            * spot * spot * 0.01)


def net_gex(contracts, spot):
    """Total dollar gamma per 1% move across a set of contracts."""
    return math.fsum(contract_gex(c, spot) for c in contracts)


def strike_ladder(contracts, spot):
    """{strike: net dollar gamma} summed across expiries at that strike."""
    ladder = {}
    for contract in contracts:
        value = contract_gex(contract, spot)
        if value == 0.0:
            continue
        ladder[contract.strike] = ladder.get(contract.strike, 0.0) + value
    return ladder


def sided_ladders(contracts, spot):
    """Call-side and put-side dollar gamma per strike, kept apart.

    Returns ``(calls, puts)``. Both carry the same sign convention the net does --
    calls positive, puts negative -- so ``calls[k] + puts[k]`` is exactly
    ``strike_ladder(...)[k]`` and no second sign rule is needed anywhere.

    This exists because the net at a strike answers "how much do dealers have to
    hedge here", which is the right input for the level list and the wrong one for
    naming a floor. A book whose calls outweigh its puts at every strike has no
    negative strike at all, and a wall derived from the sign of net then reports no
    support -- an artefact of the labelling, not a market fact.
    """
    calls = {}
    puts = {}
    for contract in contracts:
        value = contract_gex(contract, spot)
        if value == 0.0:
            continue
        side = puts if contract.right == "P" else calls
        side[contract.strike] = side.get(contract.strike, 0.0) + value
    return calls, puts


def _flip_terms(contracts, quote_ts, rate=RISK_FREE_RATE):
    """Precompute the per-contract constants the flip scan needs.

    total(S) = S * sum_i w_i * exp(-0.5 * d1_i^2)

    which falls out of gamma * OI * 100 * S^2 * 0.01 with the closed-form gamma
    substituted in -- the 100 and the 0.01 cancel, and one factor of S cancels
    against the S in the gamma denominator.
    """
    terms = []
    for contract in contracts:
        if contract.open_interest <= 0:
            continue
        sigma = contract.iv
        if not (MIN_IV <= sigma <= MAX_IV):
            continue
        tte = years_to_expiry(contract.expiry, quote_ts)
        vol_sqrt_t = sigma * math.sqrt(tte)
        if vol_sqrt_t <= 0.0:
            continue
        shift = math.log(contract.strike) - (rate + 0.5 * sigma * sigma) * tte
        weight = contract.sign * contract.open_interest / (SQRT_2PI * vol_sqrt_t)
        terms.append((shift, 1.0 / vol_sqrt_t, weight))
    return terms


def total_gamma_at(terms, candidate_spot):
    """Re-priced total dealer gamma (dollars per 1% move) at a candidate spot."""
    if candidate_spot <= 0.0 or not terms:
        return 0.0
    log_spot = math.log(candidate_spot)
    exp_ = math.exp
    acc = 0.0
    for shift, inv_vs, weight in terms:
        d1 = (log_spot - shift) * inv_vs
        acc += weight * exp_(-0.5 * d1 * d1)
    return acc * candidate_spot


def gamma_flip(contracts, spot, quote_ts, rate=RISK_FREE_RATE, steps=FLIP_STEPS,
               span=FLIP_SPAN):
    """The spot at which re-priced total dealer gamma crosses zero.

    Scans 0.8x to 1.2x current spot in `steps` steps, interpolating between the
    bracketing steps. More than one crossing: the one nearest spot. None: 0.0.
    """
    terms = _flip_terms(contracts, quote_ts, rate=rate)
    if not terms or spot <= 0:
        return 0.0

    low = spot * (1.0 - span)
    high = spot * (1.0 + span)
    width = (high - low) / steps

    grid = [low + width * i for i in range(steps + 1)]
    totals = [total_gamma_at(terms, s) for s in grid]

    crossings = []
    for i in range(steps):
        left, right = totals[i], totals[i + 1]
        if left == 0.0:
            crossings.append(grid[i])
            continue
        if (left < 0.0 < right) or (left > 0.0 > right):
            crossings.append(grid[i] + width * (-left) / (right - left))
    if totals[-1] == 0.0:
        crossings.append(grid[-1])

    if not crossings:
        return 0.0
    return min(crossings, key=lambda x: abs(x - spot))


# --- touch probability --------------------------------------------------------
#
# The indicator names a target -- the wall on the side the lean points to -- and
# the trader wants to know whether that target is reachable today. Option prices
# already contain the answer: they price a distribution, so the probability of
# price TOUCHING a level before a horizon can be extracted rather than invented.
#
# **This is a risk-neutral probability, not a forecast.** It is what the market
# charges for the outcome. The downside is systematically overpriced, because
# people pay for protection they do not expect to need. Never present it as a
# prediction.


def norm_cdf(x):
    """Standard normal CDF. `math.erf` is stdlib, so no table and no dependency."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def touch_probability(spot, strike, sigma, years):
    """P(price touches `strike` before the horizon), as an integer 0-100.

    Driftless geometric Brownian motion, reflection principle:

        P(touch K before T) = 2 * N( -|ln(K/S)| / (sigma * sqrt(T)) )

    This is the probability of *touching*, which is what matters for a target,
    and roughly double the probability of finishing beyond it. A wall sitting on
    spot reads about 100; one a long way off with little time left reads about 0.

    None when the inputs cannot support a number -- non-positive spot, strike,
    vol or time. Never a fabricated zero.
    """
    if spot <= 0.0 or strike <= 0.0 or sigma <= 0.0 or years <= 0.0:
        return None
    distance = abs(math.log(strike / spot))
    probability = 2.0 * norm_cdf(-distance / (sigma * math.sqrt(years)))
    percent = 100.0 * probability
    if percent <= 0.0:
        return 0
    if percent >= 100.0:
        return 100
    return min(100, int(percent + 0.5))


def strike_sigma(contracts, strike):
    """Implied vol to price a move to `strike` with, or None if there is none.

    The nearest-expiry option **at** that strike, because the horizon is today's
    close and the front expiry is the market's read on today. Where the strike
    itself carries no usable vol at any expiry -- it happens on a thin name --
    the nearest strike that does is used instead. Calls and puts at the chosen
    strike and expiry are averaged: put-call parity says they should agree, the
    feed's two numbers differ slightly, and averaging is both the standard fix
    and the only side-neutral one.

    Vols outside MIN_IV..MAX_IV are junk from the feed and are not candidates --
    the same bound the flip re-pricing uses -- and neither is a contract nobody
    holds, since a zero-open-interest strike can carry a quote that has not moved
    in days. If nothing survives, the answer is None and the wall's touch field
    is omitted rather than guessed.
    """
    usable = [c for c in contracts
              if c.strike > 0.0 and c.open_interest > 0
              and MIN_IV <= c.iv <= MAX_IV]
    if not usable:
        return None
    # Nearest strike, ties to the lower one -- the same tie-break the wall search
    # and the level ranking use.
    best = min(set(c.strike for c in usable), key=lambda k: (abs(k - strike), k))
    at_strike = [c for c in usable if c.strike == best]
    front = min(c.expiry for c in at_strike)
    vols = [c.iv for c in at_strike if c.expiry == front]
    return sum(vols) / len(vols)
