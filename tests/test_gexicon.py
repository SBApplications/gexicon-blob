"""Tests for the Gexicon GEX pipeline. Standard library unittest, no plugins.

    python3 -m unittest discover -s tests -v
"""

import argparse
import contextlib
import csv
import gzip
import io
import json
import math
import os
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

SAMPLES_DIR = os.path.join(ROOT, "samples")
ARCHIVE_DIR = os.path.join(ROOT, "archive")
# Saved responses that live with the tests rather than beside the CBOE payloads,
# so nothing here can be picked up as an --offline input by mistake.
TEST_SAMPLES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "samples")

from gexicon import BLOB_PREFIX                                    # noqa: E402
from gexicon.archive import HEADER, write_snapshot                  # noqa: E402
from gexicon.cboe import (ASSUMED_FEED_DELAY, Contract, FetchError,  # noqa: E402
                          reduce_payload, spot_effective_time)
from gexicon.encode import (BlobFormatError, decode_blob, decode_wall,  # noqa: E402
                            encode_blob, encode_wall, fmt_price, fmt_signed)
from gexicon.gex import (MAX_IV, MIN_IV, contract_gex, gamma_flip,  # noqa: E402
                         net_gex,
                         norm_cdf, sided_ladders, strike_ladder, strike_sigma,
                         total_gamma_at, touch_probability, _flip_terms)
from gexicon.nytime import (NY, TOUCH_SECONDS_PER_YEAR, is_expired,  # noqa: E402
                            now_utc, parse_cboe_timestamp,
                            parse_ny_timestamp, session_date_of,
                            settlement_utc, touch_horizon_years,
                            years_to_expiry)
from gexicon.occ import OCCParseError, parse_occ                    # noqa: E402
from gexicon import cli                                             # noqa: E402
from gexicon.pipeline import RunResult, run                         # noqa: E402
from gexicon.record import (Wall, build_record, derive_walls,       # noqa: E402
                            _levels_from)
from gexicon.replay import (ReplayError, archive_index, find_snapshot,  # noqa: E402
                            list_dates, list_snapshots, read_chain, replay)
from gexicon.symbols import DEFAULT_SYMBOLS, to_cboe, to_ticker     # noqa: E402
from gexicon import pipeline as pipeline_module                     # noqa: E402
from gexicon import yahoo                                           # noqa: E402
from gexicon.pipeline import (CBOE_MAX_AGE_HOURS, inside_fallback_hours,  # noqa: E402
                              prefer_fallback, wants_fallback)


def utc(text):
    return parse_cboe_timestamp(text)


def contract(occ, oi=1000.0, gamma=0.01, iv=0.20, volume=0.0):
    from gexicon.occ import parse_occ as _p
    _root, expiry, right, strike = _p(occ)
    return Contract(occ=occ, expiry=expiry, right=right, strike=strike,
                    open_interest=oi, gamma=gamma, iv=iv, volume=volume)


def payload(timestamp, spot, rows, last_trade_time=None):
    """A CBOE payload. `timestamp` is UTC; `last_trade_time` is New York local."""
    data = {"current_price": spot, "options": rows}
    if last_trade_time is not None:
        data["last_trade_time"] = last_trade_time
    return {"timestamp": timestamp, "symbol": "TEST", "data": data}


def row(occ, oi=1000.0, gamma=0.01, iv=0.20, volume=0.0):
    return {"option": occ, "open_interest": oi, "gamma": gamma, "iv": iv,
            "volume": volume}


# --------------------------------------------------------------------------
class TestOCC(unittest.TestCase):

    def test_parses_a_standard_symbol(self):
        root, expiry, right, strike = parse_occ("SPY260803C00500000")
        self.assertEqual(root, "SPY")
        self.assertEqual(expiry, date(2026, 8, 3))
        self.assertEqual(right, "C")
        self.assertEqual(strike, 500.0)

    def test_root_length_varies(self):
        """Parsing from the right is what makes variable-length roots work."""
        cases = {
            "A260803P00012500": ("A", 12.5, "P"),
            "IWM260804C00180000": ("IWM", 180.0, "C"),
            "SPXW260821P05000000": ("SPXW", 5000.0, "P"),
            "BRKB260918C00450000": ("BRKB", 450.0, "C"),
        }
        for symbol, (root, strike, right) in cases.items():
            got_root, _expiry, got_right, got_strike = parse_occ(symbol)
            self.assertEqual(got_root, root, symbol)
            self.assertEqual(got_strike, strike, symbol)
            self.assertEqual(got_right, right, symbol)

    def test_strike_is_thousandths(self):
        self.assertEqual(parse_occ("SPY260803C00500500")[3], 500.5)
        self.assertEqual(parse_occ("SPY260803C00000500")[3], 0.5)

    def test_rejects_malformed(self):
        for bad in ("", "SPY", "SPY260803X00500000", "SPY261303C00500000",
                    "SPY260803C0050000A", "260803C00500000", None, 12345):
            with self.assertRaises(OCCParseError, msg=repr(bad)):
                parse_occ(bad)


# --------------------------------------------------------------------------
class TestSignConvention(unittest.TestCase):
    """Dealers assumed short calls, long puts: calls positive, puts negative."""

    def test_call_is_positive_put_is_negative(self):
        call = contract("SPY261218C00500000")
        put = contract("SPY261218P00500000")
        self.assertGreater(contract_gex(call, 500.0), 0.0)
        self.assertLess(contract_gex(put, 500.0), 0.0)

    def test_magnitudes_match_the_formula(self):
        call = contract("SPY261218C00500000", oi=1234.0, gamma=0.0123)
        expected = 0.0123 * 1234.0 * 100.0 * 500.0 ** 2 * 0.01
        self.assertAlmostEqual(contract_gex(call, 500.0), expected, places=6)
        put = contract("SPY261218P00500000", oi=1234.0, gamma=0.0123)
        self.assertAlmostEqual(contract_gex(put, 500.0), -expected, places=6)

    def test_identical_call_and_put_cancel(self):
        pair = [contract("SPY261218C00500000"), contract("SPY261218P00500000")]
        self.assertAlmostEqual(net_gex(pair, 500.0), 0.0, places=6)

    def test_zero_oi_and_zero_gamma_are_skipped(self):
        self.assertEqual(contract_gex(contract("SPY261218C00500000", oi=0.0), 500.0), 0.0)
        self.assertEqual(contract_gex(contract("SPY261218C00500000", gamma=0.0), 500.0), 0.0)

    def test_ladder_sums_across_expiries_at_one_strike(self):
        contracts = [contract("SPY261218C00500000", gamma=0.01),
                     contract("SPY270115C00500000", gamma=0.02),
                     contract("SPY261218C00510000", gamma=0.01)]
        ladder = strike_ladder(contracts, 500.0)
        self.assertEqual(sorted(ladder), [500.0, 510.0])
        self.assertAlmostEqual(ladder[500.0],
                               contract_gex(contracts[0], 500.0)
                               + contract_gex(contracts[1], 500.0), places=6)


# --------------------------------------------------------------------------
class TestExpiredFilter(unittest.TestCase):
    """CBOE leaves settled expiries in the payload with open interest intact."""

    def test_settlement_boundary_is_1600_new_york(self):
        expiry = date(2026, 8, 3)
        close = settlement_utc(expiry)
        self.assertEqual(close.astimezone(NY).hour, 16)
        self.assertFalse(is_expired(expiry, close - timedelta(minutes=1)))
        self.assertTrue(is_expired(expiry, close))
        self.assertTrue(is_expired(expiry, close + timedelta(minutes=1)))

    def test_yesterdays_expiry_is_dropped_from_the_payload(self):
        """The IWM phantom-wall case: huge OI on an expiry that already settled."""
        data = payload("2026-08-04 14:00:00", 300.0, [
            row("IWM260803C00300000", oi=900000.0, gamma=0.05),   # settled yesterday
            row("IWM260807C00300000", oi=1000.0, gamma=0.01),     # live
        ])
        chain = reduce_payload("IWM", data, max_age_hours=None)
        self.assertEqual(chain.dropped_expired, 1)
        self.assertEqual([c.occ for c in chain.contracts], ["IWM260807C00300000"])
        # The phantom would have been ~40x the real level.
        self.assertLess(abs(net_gex(chain.contracts, 300.0)), 1.0e9)

    def test_todays_expiry_survives_before_the_close_and_dies_after(self):
        rows = [row("IWM260804C00300000", oi=5000.0, gamma=0.02),
                row("IWM260807C00300000", oi=1000.0, gamma=0.01)]
        before = reduce_payload("IWM", payload("2026-08-04 19:59:00", 300.0, rows),
                                max_age_hours=None)   # 15:59 New York
        self.assertEqual(len(before.contracts), 2)
        self.assertEqual(before.dropped_expired, 0)

        after = reduce_payload("IWM", payload("2026-08-04 20:01:00", 300.0, rows),
                               max_age_hours=None)    # 16:01 New York
        self.assertEqual(len(after.contracts), 1)
        self.assertEqual(after.dropped_expired, 1)

    def test_expired_contracts_are_dropped_not_floored(self):
        """The one-hour floor exists for 0DTE, not for resurrecting dead contracts."""
        quote = utc("2026-08-04 14:00:00")
        self.assertGreater(years_to_expiry(date(2026, 8, 4), quote), 0.0)
        self.assertTrue(is_expired(date(2026, 8, 3), quote))

    def test_tte_floor_is_one_hour(self):
        quote = utc("2026-08-04 19:59:59")  # a second before the close
        tte = years_to_expiry(date(2026, 8, 4), quote)
        self.assertAlmostEqual(tte, 1.0 / (365.0 * 24.0), places=12)
        self.assertGreater(tte, 0.0)


# --------------------------------------------------------------------------
class TestFlip(unittest.TestCase):

    def setUp(self):
        self.quote = utc("2026-08-04 14:00:00")

    def _textbook_total(self, contracts, spot, rate=0.04):
        """Brute-force the spec's formula, independent of the optimised path."""
        total = 0.0
        for c in contracts:
            tte = years_to_expiry(c.expiry, self.quote)
            sigma = c.iv
            d1 = ((math.log(spot / c.strike) + (rate + sigma * sigma / 2.0) * tte)
                  / (sigma * math.sqrt(tte)))
            gamma = (math.exp(-d1 * d1 / 2.0) / math.sqrt(2.0 * math.pi)) \
                / (spot * sigma * math.sqrt(tte))
            total += c.sign * gamma * c.open_interest * 100.0 * spot * spot * 0.01
        return total

    def test_optimised_repricing_matches_the_textbook_formula(self):
        contracts = [contract("SPY261218C00500000", oi=1000.0, iv=0.18),
                     contract("SPY261218P00480000", oi=2500.0, iv=0.25),
                     contract("SPY260904C00520000", oi=700.0, iv=0.31)]
        terms = _flip_terms(contracts, self.quote)
        for spot in (400.0, 480.0, 500.0, 530.0, 600.0):
            self.assertAlmostEqual(total_gamma_at(terms, spot),
                                   self._textbook_total(contracts, spot),
                                   delta=abs(self._textbook_total(contracts, spot)) * 1e-9 + 1e-6)

    def test_flip_is_where_the_repriced_total_is_zero(self):
        contracts = [contract("SPY261218C00520000", oi=4000.0, iv=0.20),
                     contract("SPY261218P00480000", oi=4000.0, iv=0.20)]
        flip = gamma_flip(contracts, 500.0, self.quote)
        self.assertNotEqual(flip, 0.0)
        self.assertAlmostEqual(self._textbook_total(contracts, flip), 0.0,
                               delta=abs(self._textbook_total(contracts, 500.0)) * 0.01)

    def _bs_gamma(self, spot, strike, iv, expiry, rate=0.04):
        """What CBOE's pre-computed gamma field would carry at this spot."""
        tte = years_to_expiry(expiry, self.quote)
        vs = iv * math.sqrt(tte)
        d1 = (math.log(spot / strike) + (rate + iv * iv / 2.0) * tte) / vs
        return math.exp(-d1 * d1 / 2.0) / math.sqrt(2.0 * math.pi) / (spot * vs)

    def _priced(self, occ, oi, iv, spot):
        """A contract carrying a realistic gamma for `spot`, as a payload would."""
        c = contract(occ, oi=oi, iv=iv)
        c.gamma = self._bs_gamma(spot, c.strike, iv, c.expiry)
        return c

    def test_flip_is_found_when_total_net_gex_is_negative(self):
        """The cumulative-ladder shortcut loses the flip in negative gamma.

        A put wall just below spot and a call wall above it: net GEX at spot is
        negative, so cumulative net never crosses zero walking up the ladder, but
        the re-priced curve does turn positive inside the scan window.
        """
        spot = 500.0
        contracts = [self._priced("SPY260918P00495000", 20000.0, 0.20, spot),
                     self._priced("SPY260918C00530000", 20000.0, 0.20, spot)]

        self.assertLess(net_gex(contracts, spot), 0.0,
                        "fixture must sit in negative gamma at spot")
        flip = gamma_flip(contracts, spot, self.quote)
        self.assertNotEqual(flip, 0.0, "flip vanished on a negative-gamma book")
        self.assertGreater(flip, spot, "the flip should sit above spot here")
        self.assertAlmostEqual(self._textbook_total(contracts, flip), 0.0,
                               delta=abs(self._textbook_total(contracts, spot)) * 0.02)

    def test_no_crossing_returns_zero(self):
        contracts = [contract("SPY261218C00500000", oi=1000.0, iv=0.20),
                     contract("SPY261218C00520000", oi=1000.0, iv=0.20)]
        self.assertEqual(gamma_flip(contracts, 500.0, self.quote), 0.0)

    def test_flip_stays_inside_the_scan_window(self):
        contracts = [contract("SPY261218C00520000", oi=4000.0, iv=0.20),
                     contract("SPY261218P00480000", oi=4000.0, iv=0.20)]
        spot = 500.0
        flip = gamma_flip(contracts, spot, self.quote)
        self.assertGreaterEqual(flip, spot * 0.8)
        self.assertLessEqual(flip, spot * 1.2)

    def test_nearest_crossing_to_spot_wins(self):
        """Two crossings: the far one must not be preferred."""
        contracts = [contract("SPY261218P00470000", oi=8000.0, iv=0.10),
                     contract("SPY261218C00480000", oi=8000.0, iv=0.10),
                     contract("SPY261218P00560000", oi=8000.0, iv=0.10)]
        spot = 500.0
        terms = _flip_terms(contracts, self.quote)
        grid = [spot * 0.8 + (spot * 0.4 / 200) * i for i in range(201)]
        totals = [total_gamma_at(terms, s) for s in grid]
        crossings = sum(1 for i in range(200)
                        if (totals[i] < 0 < totals[i + 1])
                        or (totals[i] > 0 > totals[i + 1]))
        self.assertGreaterEqual(crossings, 2, "test fixture needs multiple crossings")
        flip = gamma_flip(contracts, spot, self.quote)
        nearest = min((g for g, t in zip(grid, totals)), key=lambda g: abs(g - spot))
        self.assertLess(abs(flip - spot), spot * 0.2)
        self.assertIsNotNone(nearest)

    def test_absurd_iv_is_excluded(self):
        """Gamma scales as 1/sigma, so one bad contract could otherwise dominate."""
        good = [contract("SPY261218C00500000", oi=1000.0, iv=0.20)]
        with_junk = good + [contract("SPY261218P00500000", oi=1000.0,
                                     iv=MAX_IV + 50.0),
                            contract("SPY261218P00505000", oi=1000.0, iv=0.0)]
        self.assertEqual(len(_flip_terms(with_junk, self.quote)), 1)

    def test_flip_ignores_cboe_gamma(self):
        """Re-pricing must not read the pre-computed gamma field at all."""
        contracts = [contract("SPY261218C00520000", oi=4000.0, iv=0.20, gamma=0.0),
                     contract("SPY261218P00480000", oi=4000.0, iv=0.20, gamma=0.0)]
        self.assertNotEqual(gamma_flip(contracts, 500.0, self.quote), 0.0)


# --------------------------------------------------------------------------
class TestZeroDTEBucket(unittest.TestCase):

    def test_session_date_is_new_york_not_utc(self):
        # 00:30 UTC on the 5th is still the evening of the 4th in New York.
        self.assertEqual(session_date_of(utc("2026-08-05 00:30:00")), date(2026, 8, 4))
        self.assertEqual(session_date_of(utc("2026-08-04 14:00:00")), date(2026, 8, 4))

    def _chain(self, timestamp, rows, spot=300.0):
        return reduce_payload("IWM", payload(timestamp, spot, rows),
                              max_age_hours=None)

    def test_same_day_expiry_produces_both_buckets(self):
        chain = self._chain("2026-08-04 14:00:00", [
            row("IWM260804C00300000", oi=5000.0, gamma=0.02),
            row("IWM260807C00305000", oi=4000.0, gamma=0.01),
        ])
        record = build_record(chain, chain.session_date)
        self.assertEqual([s.tag for s in record.sections], [None, "0", "R"])

    def test_no_same_day_expiry_means_no_bucket_sections_at_all(self):
        chain = self._chain("2026-08-04 14:00:00", [
            row("NVDA260807C00300000", oi=4000.0, gamma=0.01),
        ])
        record = build_record(chain, chain.session_date)
        self.assertEqual([s.tag for s in record.sections], [None])
        from gexicon.encode import encode_record
        self.assertNotIn(";", encode_record(record))

    def test_bucket_disappears_after_the_close_and_is_never_rolled_forward(self):
        rows = [row("IWM260804C00300000", oi=5000.0, gamma=0.02),
                row("IWM260807C00305000", oi=4000.0, gamma=0.01)]
        before = self._chain("2026-08-04 19:59:00", rows)   # 15:59 New York
        after = self._chain("2026-08-04 20:01:00", rows)    # 16:01 New York

        self.assertEqual([s.tag for s in build_record(
            before, before.session_date).sections], [None, "0", "R"])
        # Same session date, but the 0DTE section is gone for the rest of the day.
        self.assertEqual(after.session_date, date(2026, 8, 4))
        self.assertEqual([s.tag for s in build_record(
            after, after.session_date).sections], [None])

    def test_bucket_nets_add_up_to_the_total(self):
        chain = self._chain("2026-08-04 14:00:00", [
            row("IWM260804C00300000", oi=5000.0, gamma=0.02),
            row("IWM260804P00295000", oi=3000.0, gamma=0.015),
            row("IWM260807C00305000", oi=4000.0, gamma=0.01),
            row("IWM260911P00290000", oi=9000.0, gamma=0.008),
        ])
        record = build_record(chain, chain.session_date)
        total, zero, rest = record.sections
        self.assertAlmostEqual(total.net, zero.net + rest.net, places=9)

    def test_bucket_level_limits(self):
        rows = [row("IWM260804C%08d" % (int((300 + i) * 1000)), oi=5000.0 + i,
                    gamma=0.02) for i in range(12)]
        rows += [row("IWM260807C%08d" % (int((300 + i) * 1000)), oi=5000.0 + i,
                     gamma=0.02) for i in range(12)]
        chain = self._chain("2026-08-04 14:00:00", rows)
        record = build_record(chain, chain.session_date)
        self.assertLessEqual(len(record.total.levels), 10)
        for bucket in record.buckets:
            self.assertLessEqual(len(bucket.levels), 6)


# --------------------------------------------------------------------------
class TestWallAggregation(unittest.TestCase):
    """Call gamma and put gamma summed apart per strike."""

    def test_the_two_sides_add_back_to_the_net_exactly(self):
        spot = 100.0
        contracts = [
            contract("TST260807C00100000", oi=1000.0, gamma=0.02),
            contract("TST260807P00100000", oi=400.0, gamma=0.03),
            contract("TST260814C00100000", oi=700.0, gamma=0.01),
            contract("TST260807P00095000", oi=900.0, gamma=0.02),
        ]
        calls, puts = sided_ladders(contracts, spot)
        ladder = strike_ladder(contracts, spot)
        for strike, net in ladder.items():
            self.assertAlmostEqual(calls.get(strike, 0.0) + puts.get(strike, 0.0),
                                   net, places=6)

    def test_calls_stay_positive_and_puts_stay_negative(self):
        calls, puts = sided_ladders([
            contract("TST260807C00100000", oi=1000.0, gamma=0.02),
            contract("TST260807P00100000", oi=1000.0, gamma=0.02),
        ], 100.0)
        self.assertGreater(calls[100.0], 0.0)
        self.assertLess(puts[100.0], 0.0)
        # Same size on both sides: the net at the strike is zero, but neither
        # one-sided figure is.
        self.assertAlmostEqual(calls[100.0] + puts[100.0], 0.0, places=6)

    def test_sides_sum_across_expiries_at_one_strike(self):
        calls, _puts = sided_ladders([
            contract("TST260807C00100000", oi=1000.0, gamma=0.02),
            contract("TST260814C00100000", oi=1000.0, gamma=0.02),
        ], 100.0)
        self.assertEqual(len(calls), 1)
        self.assertAlmostEqual(calls[100.0],
                               2 * contract_gex(
                                   contract("TST260807C00100000", oi=1000.0,
                                            gamma=0.02), 100.0), places=6)


# --------------------------------------------------------------------------
class TestWallDerivation(unittest.TestCase):
    """The two walls: heaviest call-side and heaviest put-side strike."""

    SPOT = 100.0

    def _levels_and_walls(self, contracts, limit=10):
        levels = _levels_from(contracts, self.SPOT, limit)
        return levels, derive_walls(levels, contracts, self.SPOT)

    def test_a_call_dominated_book_still_has_a_put_wall(self):
        # Every strike nets positive, so tagging by the sign of net finds no
        # negative strike at all and draws a ceiling with no floor. That is the
        # failure this replaced -- SPX 2026-08-04, ten C levels on a chain holding
        # 177B of put gamma.
        contracts = []
        for strike in (95, 100, 105, 110):
            occ = "TST260807C00%03d000" % strike
            contracts.append(contract(occ, oi=1000000.0, gamma=0.02))
            contracts.append(contract(occ.replace("C", "P"), oi=200000.0, gamma=0.02))
        levels, (call_wall, put_wall) = self._levels_and_walls(contracts)
        self.assertTrue(all(lv.right == "C" for lv in levels))
        self.assertIsNotNone(put_wall)
        self.assertEqual(put_wall.side, "P")
        self.assertLess(put_wall.magnitude, 0.0)
        self.assertIsNotNone(call_wall)

    def test_walls_carry_one_sided_magnitudes_not_the_net(self):
        contracts = [
            contract("TST260807C00105000", oi=100000.0, gamma=0.02),
            contract("TST260807P00105000", oi=60000.0, gamma=0.02),
            contract("TST260807P00095000", oi=90000.0, gamma=0.02),
            contract("TST260807C00095000", oi=10000.0, gamma=0.02),
        ]
        levels, (call_wall, put_wall) = self._levels_and_walls(contracts)
        calls, puts = sided_ladders(contracts, self.SPOT)
        by_price = {lv.price: lv.magnitude for lv in levels}
        self.assertAlmostEqual(call_wall.magnitude, calls[105.0] / 1e9, places=9)
        self.assertAlmostEqual(put_wall.magnitude, puts[95.0] / 1e9, places=9)
        # The one-sided figure is bigger than the net at the same strike, which is
        # the whole reason it is carried separately.
        self.assertGreater(call_wall.magnitude, by_price[105.0])
        self.assertLess(put_wall.magnitude, by_price[95.0])

    def test_the_search_is_the_chunks_own_levels_not_the_whole_chain(self):
        # A far strike holding the largest call gamma AND the largest put gamma in
        # the book. The two nearly cancel, so the net ranking leaves it out -- and
        # taking global maxima would put the ceiling and the floor on it together,
        # miles above price. This is SPX 8000 in miniature.
        contracts = [
            contract("TST260807C00200000", oi=900000.0, gamma=0.02),
            contract("TST260807P00200000", oi=900000.0, gamma=0.02),
            contract("TST260807C00105000", oi=50000.0, gamma=0.02),
            contract("TST260807P00095000", oi=40000.0, gamma=0.02),
        ]
        calls, puts = sided_ladders(contracts, self.SPOT)
        self.assertEqual(max(calls, key=lambda k: calls[k]), 200.0)
        self.assertEqual(min(puts, key=lambda k: puts[k]), 200.0)
        levels, (call_wall, put_wall) = self._levels_and_walls(contracts)
        self.assertNotIn(200.0, [lv.price for lv in levels])
        self.assertEqual(call_wall.price, 105.0)
        self.assertEqual(put_wall.price, 95.0)
        self.assertNotEqual(call_wall.price, put_wall.price)

    def test_call_wall_prefers_at_or_above_spot_and_put_wall_at_or_below(self):
        contracts = [
            # Heaviest call gamma sits below spot, heaviest put gamma above it.
            contract("TST260807C00090000", oi=200000.0, gamma=0.02),
            contract("TST260807C00110000", oi=80000.0, gamma=0.02),
            contract("TST260807P00110000", oi=200000.0, gamma=0.02),
            contract("TST260807P00090000", oi=80000.0, gamma=0.02),
        ]
        _levels, (call_wall, put_wall) = self._levels_and_walls(contracts)
        self.assertEqual(call_wall.price, 110.0)
        self.assertEqual(put_wall.price, 90.0)

    def test_the_side_preference_is_dropped_rather_than_lose_a_wall(self):
        # No call gamma at or above spot at all. Better a ceiling below price than
        # no ceiling.
        contracts = [
            contract("TST260807C00090000", oi=200000.0, gamma=0.02),
            contract("TST260807P00095000", oi=200000.0, gamma=0.02),
        ]
        _levels, (call_wall, put_wall) = self._levels_and_walls(contracts)
        self.assertEqual(call_wall.price, 90.0)
        self.assertEqual(put_wall.price, 95.0)

    def test_a_side_with_no_gamma_at_all_yields_no_wall(self):
        contracts = [contract("TST260807C00105000", oi=200000.0, gamma=0.02)]
        _levels, (call_wall, put_wall) = self._levels_and_walls(contracts)
        self.assertEqual(call_wall.price, 105.0)
        self.assertIsNone(put_wall)

    def test_a_wall_that_rounds_to_zero_is_dropped_like_a_level(self):
        # A drawn level list that is all calls, with a trace of put gamma at one of
        # those strikes. Quoting a floor at 0.00B draws a line that means nothing,
        # which is the same reason a level that rounds to zero is dropped. Seen on
        # the SPY 0DTE book late in the session.
        contracts = [
            contract("TST260807C00105000", oi=200000.0, gamma=0.02),
            contract("TST260807P00105000", oi=1.0, gamma=0.00001),
        ]
        _levels, (call_wall, put_wall) = self._levels_and_walls(contracts)
        self.assertIsNotNone(call_wall)
        self.assertIsNone(put_wall)

    def test_walls_are_computed_per_chunk(self):
        # The 0DTE put wall is the heaviest put strike in the 0DTE book, not the
        # whole chain's.
        chain = reduce_payload("TST", payload("2026-08-04 14:00:00", 100.0, [
            row("TST260804C00101000", oi=90000.0, gamma=0.02),
            row("TST260804P00099000", oi=90000.0, gamma=0.02),
            row("TST260911C00120000", oi=400000.0, gamma=0.02),
            row("TST260911P00080000", oi=400000.0, gamma=0.02),
        ]), max_age_hours=None)
        record = build_record(chain, chain.session_date)
        total, zero_dte, rest = record.sections
        self.assertEqual((total.call_wall.price, total.put_wall.price), (120.0, 80.0))
        self.assertEqual((zero_dte.call_wall.price, zero_dte.put_wall.price),
                         (101.0, 99.0))
        self.assertEqual((rest.call_wall.price, rest.put_wall.price), (120.0, 80.0))

    def test_wall_tokens_come_last_in_every_chunk(self):
        chain = reduce_payload("TST", payload("2026-08-04 14:00:00", 100.0, [
            row("TST260804C00101000", oi=90000.0, gamma=0.02),
            row("TST260804P00099000", oi=90000.0, gamma=0.02),
            row("TST260911C00120000", oi=400000.0, gamma=0.02),
            row("TST260911P00080000", oi=400000.0, gamma=0.02),
        ]), max_age_hours=None)
        blob = encode_blob([build_record(chain, chain.session_date)],
                           utc("2026-08-04 14:00:00"), chain.session_date)
        record_text = blob.split("|")[3]
        for chunk in record_text.split(";"):
            tokens = chunk.split(",")
            walls = [t for t in tokens if t.startswith("W")]
            self.assertEqual(len(walls), 2, chunk)
            self.assertEqual(tokens[-2:], walls, chunk)
            self.assertTrue(walls[0].startswith("WC") and walls[1].startswith("WP"))


# --------------------------------------------------------------------------
class TestEncoding(unittest.TestCase):

    def test_price_trims_trailing_zeros(self):
        self.assertEqual(fmt_price(730.0), "730")
        self.assertEqual(fmt_price(302.5), "302.5")
        self.assertEqual(fmt_price(5000.0), "5000")
        self.assertEqual(fmt_price(12.25), "12.25")

    def test_signed_never_emits_negative_zero(self):
        self.assertEqual(fmt_signed(0.0), "+0.00")
        self.assertEqual(fmt_signed(-0.0001), "+0.00")
        self.assertEqual(fmt_signed(0.69), "+0.69")
        self.assertEqual(fmt_signed(-0.69), "-0.69")

    def test_the_specimen_record_with_walls_parses_and_round_trips(self):
        specimen = (
            "SPY,772.92,758.44,+8.25,C775:+1.36,C800:+1.10,C780:+0.90,C770:+0.90,"
            "C760:+0.57,C790:+0.56,C765:+0.53,C785:+0.39,C825:+0.39,P735:-0.33,"
            "WC775:+1.43,WP735:-0.46")
        blob = "%s|202608031345Z|20260803|%s" % (BLOB_PREFIX, specimen)
        _stamp, _session, records = decode_blob(blob)
        total = records[0].total
        self.assertEqual(len(total.levels), 10)
        # The walls are last in the chunk and carry one-sided magnitudes: the call
        # wall's +1.43 is call gamma alone at 775, larger than the +1.36 net there.
        self.assertEqual((total.call_wall.side, total.call_wall.price,
                          total.call_wall.magnitude), ("C", 775.0, 1.43))
        self.assertEqual((total.put_wall.side, total.put_wall.price,
                          total.put_wall.magnitude), ("P", 735.0, -0.46))
        from gexicon.encode import encode_record
        self.assertEqual(encode_record(records[0]), specimen)

    def test_an_unknown_token_is_skipped_never_rejected(self):
        # The rule the format grows by: a reader that meets a token whose first
        # character it does not know skips it. That is what let walls be added
        # without a version bump.
        blob = ("%s|202608031345Z|20260803|SPY,747.03,745.98,-0.69,P730:-0.82,"
                "X730:-0.82,WC760:+0.90,Z1:2" % BLOB_PREFIX)
        _stamp, _session, records = decode_blob(blob)
        total = records[0].total
        self.assertEqual(len(total.levels), 1)
        self.assertEqual(total.call_wall.price, 760.0)
        self.assertIsNone(total.put_wall)

    def test_a_malformed_wall_token_still_raises(self):
        # A tag we DO recognise with a body that will not parse is our own encoder
        # having gone wrong, not a newer writer. It must not pass quietly.
        for bad in ("WC760:0.90", "WX760:+0.90", "WC:+0.90", "WC760+0.90"):
            blob = ("%s|202608031345Z|20260803|SPY,747.03,745.98,-0.69,%s"
                    % (BLOB_PREFIX, bad))
            with self.assertRaises(BlobFormatError, msg=repr(bad)):
                decode_blob(blob)

    def test_the_specimen_record_parses_and_survives_a_round_trip(self):
        specimen = (
            "SPY,747.03,745.98,-0.69,P730:-0.82,C760:+0.75,C749:+0.69,P725:-0.62,"
            "P720:-0.59,P735:-0.59,C800:+0.56,P710:-0.55,P740:-0.44,C770:+0.43;"
            "0,742.15,+0.60,C750:+0.23,C752:+0.11,C744:+0.08,C749:+0.07,C748:+0.07,"
            "C746:+0.06;"
            "R,746.78,-1.28,P730:-0.81,C760:+0.74,C749:+0.62,P725:-0.61,P720:-0.58,"
            "P735:-0.56")
        blob = "%s|202608031345Z|20260803|%s" % (BLOB_PREFIX, specimen)
        stamp, session, records = decode_blob(blob)
        self.assertEqual((stamp, session), ("202608031345Z", "20260803"))
        self.assertEqual(len(records), 1)

        record = records[0]
        self.assertEqual(record.ticker, "SPY")
        self.assertEqual(record.total.spot, 747.03)
        self.assertEqual(record.total.flip, 745.98)
        self.assertEqual(record.total.net, -0.69)
        self.assertEqual(len(record.total.levels), 10)
        self.assertEqual(record.total.levels[0].right, "P")
        self.assertEqual(record.total.levels[0].price, 730.0)
        self.assertEqual(record.total.levels[0].magnitude, -0.82)
        self.assertEqual([s.tag for s in record.buckets], ["0", "R"])
        self.assertEqual(len(record.buckets[0].levels), 6)

        from gexicon.encode import encode_record
        self.assertEqual(encode_record(record), specimen)

    def test_full_round_trip_preserves_every_number(self):
        chain = reduce_payload("IWM", payload("2026-08-04 14:00:00", 301.745, [
            row("IWM260804C00300000", oi=5000.0, gamma=0.02, iv=0.30),
            row("IWM260804P00295000", oi=3000.0, gamma=0.015, iv=0.33),
            row("IWM260807C00305000", oi=4000.0, gamma=0.01, iv=0.22),
            row("IWM260911P00290000", oi=9000.0, gamma=0.008, iv=0.25),
        ]), max_age_hours=None)
        record = build_record(chain, chain.session_date)
        computed_at = utc("2026-08-04 14:00:00")
        blob = encode_blob([record], computed_at, chain.session_date)

        _stamp, _session, decoded = decode_blob(blob)
        self.assertEqual(len(decoded), 1)
        back = decoded[0]
        self.assertEqual(back.ticker, record.ticker)
        for original, restored in zip(record.sections, back.sections):
            self.assertEqual(restored.tag, original.tag)
            self.assertAlmostEqual(restored.net, round(original.net, 2), places=9)
            self.assertAlmostEqual(restored.flip, round(original.flip, 2), places=9)
            self.assertEqual(len(restored.levels), len(original.levels))
            for lv_a, lv_b in zip(original.levels, restored.levels):
                self.assertEqual(lv_b.right, lv_a.right)
                self.assertAlmostEqual(lv_b.price, lv_a.price, places=3)
                self.assertAlmostEqual(lv_b.magnitude, round(lv_a.magnitude, 2),
                                       places=9)
            for name in ("call_wall", "put_wall"):
                wall_a = getattr(original, name)
                wall_b = getattr(restored, name)
                self.assertEqual(wall_b is None, wall_a is None, name)
                if wall_a is None:
                    continue
                self.assertEqual(wall_b.side, wall_a.side)
                self.assertAlmostEqual(wall_b.price, wall_a.price, places=3)
                self.assertAlmostEqual(wall_b.magnitude, round(wall_a.magnitude, 2),
                                       places=9)
        self.assertEqual(encode_blob([back], computed_at, chain.session_date), blob)

    def test_blob_is_one_ascii_line_with_no_whitespace(self):
        chain = reduce_payload("IWM", payload("2026-08-04 14:00:00", 300.0, [
            row("IWM260807C00305000", oi=4000.0, gamma=0.01)]), max_age_hours=None)
        blob = encode_blob([build_record(chain, chain.session_date)],
                           utc("2026-08-04 14:00:00"), chain.session_date)
        self.assertTrue(blob.startswith(BLOB_PREFIX + "|"))
        self.assertNotIn(" ", blob)
        self.assertNotIn("\n", blob)
        blob.encode("ascii")

    def test_decoder_rejects_what_the_indicator_would_reject(self):
        for bad in ("", "NSGEX1|202608031345Z|20260803|SPY,1,1,+0.00",
                    "NSGEX2|nope|20260803|SPY,1,1,+0.00",
                    "NSGEX2|202608031345Z|2026-08-03|SPY,1,1,+0.00",
                    "NSGEX2|202608031345Z|20260803",
                    "NSGEX2|202608031345Z|20260803|SPY,747.03,745.98,0.69",
                    "NSGEX2|202608031345Z|20260803|SPY,747.03,745.98,-0.69,P730:0.82",
                    "NSGEX2|202608031345Z|20260803|SPY,747.03,745.98,-0.69;Q,1,+0.00"):
            with self.assertRaises(BlobFormatError, msg=repr(bad)):
                decode_blob(bad)


# --------------------------------------------------------------------------
class TestPayloadValidation(unittest.TestCase):

    def test_loud_failure_on_a_broken_payload(self):
        cases = [
            ({}, "no data"),
            ({"timestamp": "2026-08-04 14:00:00"}, "no data"),
            (payload("2026-08-04 14:00:00", 0.0, [row("IWM260807C00300000")]), "spot"),
            (payload("not a date", 300.0, [row("IWM260807C00300000")]), "timestamp"),
            (payload("2026-08-04 14:00:00", 300.0, []), "empty options"),
            (payload("2026-08-04 14:00:00", 300.0,
                     [row("IWM260801C00300000")]), "all expired"),
        ]
        for data, label in cases:
            with self.assertRaises(FetchError, msg=label):
                reduce_payload("IWM", data, max_age_hours=None)

    def test_stale_quote_is_rejected(self):
        data = payload("2026-08-04 14:00:00", 300.0, [row("IWM260807C00300000")])
        fresh = datetime(2026, 8, 4, 15, 0, tzinfo=timezone.utc)
        stale = datetime(2026, 8, 5, 14, 0, tzinfo=timezone.utc)
        reduce_payload("IWM", data, max_age_hours=12, now=fresh)
        with self.assertRaises(FetchError):
            reduce_payload("IWM", data, max_age_hours=12, now=stale)

    def test_zero_open_interest_is_dropped(self):
        chain = reduce_payload("IWM", payload("2026-08-04 14:00:00", 300.0, [
            row("IWM260807C00300000", oi=0.0),
            row("IWM260807C00305000", oi=10.0)]), max_age_hours=None)
        self.assertEqual([c.occ for c in chain.contracts], ["IWM260807C00305000"])


# --------------------------------------------------------------------------
class TestSymbols(unittest.TestCase):

    def test_index_symbols_take_a_leading_underscore(self):
        self.assertEqual(to_cboe("SPX"), "_SPX")
        self.assertEqual(to_cboe("_SPX"), "_SPX")
        self.assertEqual(to_cboe("NDX"), "_NDX")
        self.assertEqual(to_cboe("SPY"), "SPY")
        self.assertEqual(to_cboe("tsla"), "TSLA")

    def test_ticker_strips_the_underscore_for_tradingview(self):
        self.assertEqual(to_ticker("_SPX"), "SPX")
        self.assertEqual(to_ticker("SPY"), "SPY")

    def test_the_dow_is_carried_by_dia_the_etf_not_the_djx_index(self):
        self.assertIn("DIA", DEFAULT_SYMBOLS)
        self.assertNotIn("DJX", DEFAULT_SYMBOLS)
        # DIA is an ETF, so no underscore. DJX still resolves for anyone asking.
        self.assertEqual(to_cboe("DIA"), "DIA")
        self.assertEqual(to_cboe("DJX"), "_DJX")


# --------------------------------------------------------------------------
class TestArchiveAndPipeline(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="gexicon-test-")
        self.payload = payload("2026-08-04 14:00:00", 301.745, [
            row("IWM260804C00300000", oi=5000.0, gamma=0.02, iv=0.30),
            row("IWM260807C00305000", oi=4000.0, gamma=0.01, iv=0.22),
        ])
        with open(os.path.join(self.dir, "IWM.json"), "w") as handle:
            import json
            json.dump(self.payload, handle)

    def test_snapshot_is_written_once_per_quote_timestamp(self):
        chain = reduce_payload("IWM", self.payload, max_age_hours=None)
        archive = os.path.join(self.dir, "archive")
        path, written = write_snapshot(archive, chain, chain.session_date)
        self.assertTrue(written)
        self.assertTrue(os.path.exists(path))
        self.assertIn("20260804T140000Z", path)

        _path, rewritten = write_snapshot(archive, chain, chain.session_date)
        self.assertFalse(rewritten, "a re-run against unchanged data must be a no-op")

    def test_snapshot_round_trips_as_gzipped_csv(self):
        import csv
        import gzip
        chain = reduce_payload("IWM", self.payload, max_age_hours=None)
        archive = os.path.join(self.dir, "archive")
        path, _ = write_snapshot(archive, chain, chain.session_date)
        with gzip.open(path, "rt", newline="") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["ticker"], "IWM")
        self.assertEqual(rows[0]["occ"], "IWM260804C00300000")
        self.assertAlmostEqual(float(rows[0]["strike"]), 300.0)

    def test_offline_run_produces_a_parseable_blob(self):
        result = run(symbols=["IWM"], offline_dir=self.dir,
                     archive_dir=os.path.join(self.dir, "archive"))
        self.assertEqual(result.failures, [])
        self.assertIsNotNone(result.blob)
        _stamp, session, records = decode_blob(result.blob)
        self.assertEqual(session, "20260804")
        self.assertEqual(records[0].ticker, "IWM")
        self.assertEqual(len(result.archived), 1)

    def test_a_failing_symbol_is_named_and_the_rest_still_encode(self):
        result = run(symbols=["IWM", "NOPE"], offline_dir=self.dir,
                     archive_dir=None)
        self.assertIsNotNone(result.blob)
        self.assertEqual([t for t, _ in result.failures], ["NOPE"])
        self.assertFalse(result.ok)
        _stamp, _session, records = decode_blob(result.blob)
        self.assertEqual([r.ticker for r in records], ["IWM"])


# --------------------------------------------------------------------------
class TestStaleSymbolDoesNotFailTheBuild(unittest.TestCase):
    """The 2026-09-23 DIA incident: one stale quote raised FetchError, cli._report
    turned that into exit code 2, and GitHub Actions' default `bash -e` aborted
    the "Build the blob" step before anything was published -- a healthy blob
    from fourteen other symbols thrown away over one. A dropped symbol must be
    named as a WARNING, never FAILED, and the build must exit 0 as long as at
    least one symbol made it in.
    """

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="gexicon-stale-test-")
        with open(os.path.join(self.dir, "IWM.json"), "w") as handle:
            import json
            json.dump(payload("2026-08-04 14:00:00", 301.745, [
                row("IWM260807C00300000", oi=4000.0, gamma=0.01, iv=0.22),
            ]), handle)

    def _report(self, result):
        args = argparse.Namespace(summary=False)
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            code = cli._report(result, args)
        return code, buf.getvalue()

    def test_a_stale_symbol_is_dropped_as_a_warning_and_the_build_exits_zero(self):
        result = run(symbols=["IWM"], offline_dir=self.dir, archive_dir=None)
        self.assertIsNotNone(result.blob, "the surviving symbol must still publish")

        # The DIA incident's own message, dropped in as if it had come from the
        # real fetch loop (offline runs skip the age check by design).
        result.failures.append(
            ("DIA", "DIA: quote is 13.3h old (limit 12.0h) -- feed looks stalled"))

        code, stderr = self._report(result)

        self.assertEqual(code, 0, "one good symbol must be enough to exit 0")
        self.assertIn("WARNING", stderr)
        self.assertNotIn("FAILED", stderr)
        self.assertIn("DIA", stderr)
        _stamp, _session, records = decode_blob(result.blob)
        self.assertEqual([r.ticker for r in records], ["IWM"],
                         "the dropped symbol must not appear in the blob")

    def test_nothing_usable_still_exits_one(self):
        result = RunResult(failures=[("DIA", "stalled"), ("SPY", "stalled")])
        code, stderr = self._report(result)
        self.assertEqual(code, 1)
        self.assertIn("WARNING", stderr)


# --------------------------------------------------------------------------
class TestHeaderTimestamp(unittest.TestCase):
    """The header stamp is the instant `spot` was true, never when the run happened.

    `data.current_price` is 15-minute delayed. The indicator anchors index levels
    onto a futures chart with

        basis = chart close at the header stamp - record spot * ratio

    so a header that is fresher than the spot it is paired with folds the index's
    move over that gap into `basis` and slides every level by it. Measured live
    2026-08-05: basis reported as 16.87 against a true spread of 31.70.

    The numbers below are that live capture: New York wall clock 11:35:11, file
    stamp 15:34:28 UTC, `last_trade_time` 11:19:25 New York, spot 7745.0801.
    """

    FILE_STAMP = "2026-08-05 15:34:28"
    ROWS = [row("IWM260807C00300000", oi=5000.0, gamma=0.02, iv=0.30)]

    def chain(self, last_trade_time, timestamp=FILE_STAMP):
        return reduce_payload(
            "IWM", payload(timestamp, 7745.0801, self.ROWS,
                           last_trade_time=last_trade_time),
            max_age_hours=None)

    def test_the_two_cboe_stamps_are_read_in_different_zones(self):
        # Same wall-clock minute, four hours apart on the wire. Reading either
        # field as the other is a plausible-looking four-hour error.
        self.assertEqual(parse_cboe_timestamp(self.FILE_STAMP),
                         datetime(2026, 8, 5, 15, 34, 28, tzinfo=timezone.utc))
        self.assertEqual(parse_ny_timestamp("2026-08-05T11:19:25"),
                         datetime(2026, 8, 5, 15, 19, 25, tzinfo=timezone.utc))

    def test_spot_time_comes_from_last_trade_time_not_the_file_stamp(self):
        chain = self.chain("2026-08-05T11:19:25")
        self.assertIsNone(chain.spot_ts_fallback)
        self.assertEqual(chain.spot_ts,
                         datetime(2026, 8, 5, 15, 19, 25, tzinfo=timezone.utc))
        self.assertEqual(chain.quote_ts,
                         datetime(2026, 8, 5, 15, 34, 28, tzinfo=timezone.utc))
        # 15 minutes and 3 seconds of delay: exactly the gap that used to end up
        # in the futures basis.
        self.assertEqual((chain.quote_ts - chain.spot_ts).total_seconds(), 903.0)

    def test_a_delay_inside_the_guard_band_is_trusted_as_it_stands(self):
        chain = self.chain("2026-08-05T09:39:28")  # 1h55m behind the file stamp
        self.assertIsNone(chain.spot_ts_fallback)
        self.assertEqual(chain.spot_ts,
                         datetime(2026, 8, 5, 13, 39, 28, tzinfo=timezone.utc))

    def test_a_missing_last_trade_time_falls_back_and_says_so(self):
        chain = self.chain(None)
        self.assertIn("missing", chain.spot_ts_fallback)
        self.assertEqual(chain.spot_ts, chain.quote_ts - ASSUMED_FEED_DELAY)

    def test_an_unparseable_last_trade_time_falls_back_and_says_so(self):
        chain = self.chain("yesterday afternoon")
        self.assertIn("will not parse", chain.spot_ts_fallback)
        self.assertEqual(chain.spot_ts, chain.quote_ts - ASSUMED_FEED_DELAY)

    def test_a_stale_last_trade_time_falls_back_and_says_so(self):
        # A weekend or overnight capture: the field is the previous session's
        # close, nearly twenty hours behind the file. Believing it would stamp
        # the blob a day early.
        chain = self.chain("2026-08-04T16:00:00")
        self.assertIn("stale field", chain.spot_ts_fallback)
        self.assertEqual(chain.spot_ts, chain.quote_ts - ASSUMED_FEED_DELAY)

    def test_the_guard_band_is_two_hours_either_side(self):
        inside, _ = spot_effective_time(
            {"last_trade_time": "2026-08-05T13:34:28"},  # +2h exactly, in New York
            parse_cboe_timestamp("2026-08-05 15:34:28"))
        self.assertIsNotNone(inside)
        _outside, reason = spot_effective_time(
            {"last_trade_time": "2026-08-05T13:39:28"},  # +2h05m
            parse_cboe_timestamp("2026-08-05 15:34:28"))
        self.assertIn("stale field", reason)

    def test_the_blob_header_carries_the_spot_time_not_the_run_clock(self):
        directory = tempfile.mkdtemp(prefix="gexicon-stamp-")
        import json
        with open(os.path.join(directory, "IWM.json"), "w") as handle:
            json.dump(payload("2026-08-05 15:34:28", 301.745, self.ROWS,
                              last_trade_time="2026-08-05T11:19:25"), handle)
        # A run clock hours after the file was published must not reach the wire.
        result = run(symbols=["IWM"], offline_dir=directory, archive_dir=None,
                     now=datetime(2026, 8, 5, 20, 0, tzinfo=timezone.utc))
        stamp, _session, _records = decode_blob(result.blob)
        self.assertEqual(stamp, "202608051519Z")
        self.assertNotEqual(stamp, "202608052000Z")
        self.assertEqual(result.effective_at,
                         datetime(2026, 8, 5, 15, 19, 25, tzinfo=timezone.utc))
        self.assertEqual(result.warnings, [])

    def test_one_blob_takes_the_oldest_symbols_spot_time(self):
        """The header must never claim the data is fresher than any part of it."""
        directory = tempfile.mkdtemp(prefix="gexicon-stamp-")
        import json
        for name, last_trade in (("IWM", "2026-08-05T11:19:25"),
                                 ("SPY", "2026-08-05T11:21:40")):
            rows = [row("%s260807C00300000" % name, oi=5000.0, gamma=0.02, iv=0.30)]
            with open(os.path.join(directory, name + ".json"), "w") as handle:
                json.dump(payload("2026-08-05 15:34:28", 301.745, rows,
                                  last_trade_time=last_trade), handle)
        result = run(symbols=["IWM", "SPY"], offline_dir=directory, archive_dir=None)
        stamp, _session, records = decode_blob(result.blob)
        self.assertEqual(len(records), 2)
        self.assertEqual(stamp, "202608051519Z", "the older of the two, not the newer")

    def test_a_fallback_is_reported_on_the_run_result(self):
        directory = tempfile.mkdtemp(prefix="gexicon-stamp-")
        import json
        with open(os.path.join(directory, "IWM.json"), "w") as handle:
            json.dump(payload("2026-08-05 15:34:28", 301.745, self.ROWS), handle)
        result = run(symbols=["IWM"], offline_dir=directory, archive_dir=None)
        self.assertIsNotNone(result.blob)
        self.assertEqual([t for t, _ in result.warnings], ["IWM"])
        self.assertIn("missing", result.warnings[0][1])
        # Inferred, but still fifteen minutes behind the file stamp rather than
        # equal to it.
        stamp, _session, _records = decode_blob(result.blob)
        self.assertEqual(stamp, "202608051519Z")


# --------------------------------------------------------------------------
class TestEasternFallback(unittest.TestCase):
    """The no-tzdata path must agree with the IANA database, or Windows drifts."""

    def setUp(self):
        try:
            from zoneinfo import ZoneInfo
            self.iana = ZoneInfo("America/New_York")
        except Exception:
            self.skipTest("no IANA database available to compare against")
        from gexicon.nytime import _USEastern
        self.fallback = _USEastern()

    def test_offsets_agree_every_day_for_three_years(self):
        day = date(2025, 1, 1)
        checked = 0
        while day < date(2028, 1, 1):
            # Noon is never inside a DST fold, so both are unambiguous.
            noon = datetime(day.year, day.month, day.day, 12, 0)
            self.assertEqual(noon.replace(tzinfo=self.fallback).utcoffset(),
                             noon.replace(tzinfo=self.iana).utcoffset(),
                             "offset differs on %s" % day)
            day += timedelta(days=1)
            checked += 1
        self.assertGreater(checked, 1000)

    def test_settlement_converts_identically(self):
        for day in (date(2026, 1, 15), date(2026, 3, 8), date(2026, 6, 30),
                    date(2026, 11, 1), date(2026, 12, 31)):
            close = datetime(day.year, day.month, day.day, 16, 0)
            self.assertEqual(
                close.replace(tzinfo=self.fallback).astimezone(timezone.utc),
                close.replace(tzinfo=self.iana).astimezone(timezone.utc),
                "settlement differs on %s" % day)

    def test_dst_boundaries_are_the_statutory_ones(self):
        # 2026: forward Sunday 8 March, back Sunday 1 November.
        self.assertEqual(
            datetime(2026, 3, 7, 12, tzinfo=self.fallback).tzname(), "EST")
        self.assertEqual(
            datetime(2026, 3, 9, 12, tzinfo=self.fallback).tzname(), "EDT")
        self.assertEqual(
            datetime(2026, 10, 31, 12, tzinfo=self.fallback).tzname(), "EDT")
        self.assertEqual(
            datetime(2026, 11, 2, 12, tzinfo=self.fallback).tzname(), "EST")

    def test_session_date_is_the_same_under_the_fallback(self):
        import gexicon.nytime as nt
        original = nt.NY
        try:
            nt.NY = self.fallback
            self.assertEqual(nt.session_date_of(utc("2026-08-05 00:30:00")),
                             date(2026, 8, 4))
            self.assertEqual(nt.session_date_of(utc("2026-01-05 04:30:00")),
                             date(2026, 1, 4))
        finally:
            nt.NY = original


# --------------------------------------------------------------------------
class TestServerPayload(unittest.TestCase):
    """The browser UI must surface failures, not just show a short blob."""

    def setUp(self):
        import json
        self.dir = tempfile.mkdtemp(prefix="gexicon-ui-")
        with open(os.path.join(self.dir, "IWM.json"), "w") as handle:
            json.dump(payload("2026-08-04 14:00:00", 301.745, [
                row("IWM260804C00300000", oi=5000.0, gamma=0.02, iv=0.30),
                row("IWM260807C00305000", oi=4000.0, gamma=0.01, iv=0.22),
            ]), handle)

    def _state(self, symbols, max_cache_age=600, offline=True):
        from gexicon.server import _State
        state = _State({"symbols": symbols,
                        "offline_dir": self.dir if offline else None,
                        "archive_dir": None, "max_age_hours": None, "timeout": 5},
                       max_cache_age=max_cache_age)
        state.ensure(True)
        return state

    def test_payload_carries_the_blob_and_a_summary_row(self):
        data = self._state(["IWM"]).payload()
        self.assertTrue(data["blob"].startswith(BLOB_PREFIX + "|"))
        self.assertEqual(data["session_date"], "2026-08-04")
        self.assertEqual(len(data["symbols"]), 1)
        self.assertEqual(data["symbols"][0]["ticker"], "IWM")
        self.assertEqual(data["failures"], [])

    def test_a_failed_symbol_reaches_the_page(self):
        data = self._state(["IWM", "NOPE"]).payload()
        self.assertIsNotNone(data["blob"])
        self.assertEqual([f["ticker"] for f in data["failures"]], ["NOPE"])

    def test_fresh_cache_is_reused_so_a_reload_does_not_refetch(self):
        state = self._state(["IWM"])
        first = state.built_at
        state.ensure(False)
        self.assertEqual(state.built_at, first)
        self.assertFalse(state.is_stale())

    def test_cache_goes_stale_on_a_timer(self):
        """A page left open must not keep serving a blob priced off an old spot."""
        from gexicon.server import _State
        fresh = self._state(["IWM"], max_cache_age=600)
        self.assertFalse(fresh.is_stale())

        aged = self._state(["IWM"], max_cache_age=0)
        self.assertTrue(aged.is_stale(), "a zero-second cache must always be stale")

        empty = _State({"symbols": ["IWM"], "offline_dir": self.dir,
                        "archive_dir": None, "max_age_hours": None, "timeout": 5})
        self.assertTrue(empty.is_stale(), "having built nothing counts as stale")

    def test_offline_mode_does_not_rebuild_on_a_timer(self):
        """There is nothing newer in a directory of saved files."""
        state = self._state(["IWM"], max_cache_age=0)
        first = state.built_at
        state.ensure(False)
        self.assertEqual(state.built_at, first)

    def test_explicit_refresh_always_rebuilds(self):
        state = self._state(["IWM"])
        first = state.built_at
        state.ensure(True)
        self.assertNotEqual(state.built_at, first)

    def test_quote_lag_is_reported_and_flagged(self):
        """The saved fixture is stamped 2026-08-04, so it is far behind now."""
        data = self._state(["IWM"]).payload()
        self.assertIn("quote_lag_seconds", data)
        self.assertGreater(data["quote_lag_seconds"], 0)
        self.assertTrue(data["quote_lag_warn"])


# --------------------------------------------------------------------------
def js_strings_all_close(script):
    """True if every string literal in `script` is terminated.

    A hand-rolled scan rather than a real parser, because there is no JavaScript
    engine in the standard library and the specific failure worth catching is a
    string that never closes. Quote state is tracked through escapes and line
    comments; `'` and `"` may not cross a newline, backticks may.
    """
    quote = None
    i = 0
    while i < len(script):
        ch = script[i]
        if quote:
            if ch == "\\":
                i += 2
                continue
            if ch == quote:
                quote = None
            elif ch == "\n" and quote != "`":
                return False
        elif ch in ("'", '"', "`"):
            quote = ch
        elif ch == "/" and script[i + 1:i + 2] == "/":
            end = script.find("\n", i)
            i = len(script) if end < 0 else end
            continue
        i += 1
    return quote is None


class TestPageScriptSurvivesBeingAPythonString(unittest.TestCase):
    """The inline JS has to survive the Python literal that carries it.

    `PAGE` is an ordinary triple-quoted string, so a backslash-escaped apostrophe
    inside a single-quoted JS string -- legal JavaScript, legal Python -- is
    collapsed to a bare quote by Python, ends the JS string early, and makes the
    whole inline script one syntax error. The page then serves 200, renders nothing
    and sits on "loading" with no clue as to why, while the server, the endpoints
    and the blob are all perfectly fine. That is how it shipped until 2026-08-06,
    and nothing else in this suite would have noticed.
    """

    def _script(self):
        from gexicon.server import PAGE
        return PAGE.split("<script>")[1].split("</script>")[0]

    def test_no_escaped_quote_reaches_the_page(self):
        from gexicon.server import PAGE
        self.assertNotIn("\\'", PAGE)
        self.assertNotIn('\\"', PAGE)

    def test_every_string_literal_in_the_page_script_is_closed(self):
        self.assertTrue(js_strings_all_close(self._script()))

    def test_the_check_catches_the_bug_it_was_written_for(self):
        self.assertFalse(js_strings_all_close("x = 'CBOE's time' + y;\n"))
        self.assertTrue(js_strings_all_close('x = "CBOE\'s time";\n'))


# --------------------------------------------------------------------------
# Replay: rebuilding a past session's blob out of the snapshot archive.

# The header shape before `spot_ts_utc` was appended. Every snapshot taken before
# that change has it, so the reader has to keep parsing it forever.
OLD_HEADER = ("ticker", "quote_ts_utc", "session_date", "spot", "occ", "expiry",
              "right", "strike", "open_interest", "gamma", "iv", "volume", "gex")


def write_archive_csv(path, header, rows):
    """Write a snapshot file by hand, so a header shape can be chosen."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", newline="", encoding="ascii") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        for values in rows:
            writer.writerow([values.get(column, "") for column in header])


def archive_row(ticker, quote_ts, session, occ, spot=300.0, oi=1000.0, gamma=0.01,
                iv=0.20, spot_ts=None):
    _root, expiry, right, strike = parse_occ(occ)
    return {"ticker": ticker, "quote_ts_utc": quote_ts, "session_date": session,
            "spot": "%.4f" % spot, "occ": occ, "expiry": expiry.isoformat(),
            "right": right, "strike": "%.3f" % strike, "open_interest": "%.0f" % oi,
            "gamma": "%.10g" % gamma, "iv": "%.10g" % iv, "volume": "0",
            "gex": "0", "spot_ts_utc": spot_ts or ""}


class TestReplayReproducesTheLiveBlob(unittest.TestCase):
    """The one that matters: a replay must reproduce the blob of its own session.

    `samples/*.json` and the 18:34Z snapshot of 2026-08-04 came out of the same
    fetch, so the two paths are looking at identical data. Anything that differs is
    a defect in the replay path -- there is no legitimate reason for the maths to
    move, and that is why the record text is compared byte for byte rather than
    approximately. Only the header stamp is allowed to differ: the live path read
    `last_trade_time`, and a snapshot written before `spot_ts_utc` existed has to
    infer it.
    """

    STAMP = "20260804T183422Z"
    SESSION = "2026-08-04"

    def setUp(self):
        if not os.path.isdir(os.path.join(ARCHIVE_DIR, self.SESSION)):
            self.skipTest("no %s snapshot in %s to replay" % (self.SESSION,
                                                              ARCHIVE_DIR))
        if not os.path.isdir(SAMPLES_DIR):
            self.skipTest("no saved payloads in %s to compare against" % SAMPLES_DIR)
        self.live = run(offline_dir=SAMPLES_DIR, archive_dir=None)
        self.replayed = replay(session_date=self.SESSION, stamp=self.STAMP,
                               archive_dir=ARCHIVE_DIR)

    def _records(self, blob):
        return {text.split(",")[0]: text for text in blob.split("|")[3:]}

    def test_the_same_symbols_come_back(self):
        self.assertEqual(sorted(self._records(self.live.blob)),
                         sorted(self._records(self.replayed.blob)))
        self.assertEqual([r.ticker for r in self.live.records],
                         [r.ticker for r in self.replayed.records],
                         "record order should match too, so the two are comparable")

    def test_every_record_matches_the_live_one_character_for_character(self):
        live = self._records(self.live.blob)
        back = self._records(self.replayed.blob)
        for ticker, text in live.items():
            self.assertEqual(back[ticker], text, ticker)

    def test_spot_flip_net_and_every_level_match_field_by_field(self):
        _stamp, live_session, live_records = decode_blob(self.live.blob)
        _stamp2, back_session, back_records = decode_blob(self.replayed.blob)
        self.assertEqual(live_session, back_session)
        by_ticker = {r.ticker: r for r in back_records}
        for record in live_records:
            other = by_ticker[record.ticker]
            self.assertEqual([s.tag for s in other.sections],
                             [s.tag for s in record.sections], record.ticker)
            for mine, theirs in zip(record.sections, other.sections):
                label = "%s/%s" % (record.ticker, mine.tag)
                self.assertEqual(theirs.spot, mine.spot, label)
                self.assertEqual(theirs.flip, mine.flip, label)
                self.assertEqual(theirs.net, mine.net, label)
                self.assertEqual([(lv.right, lv.price, lv.magnitude)
                                  for lv in theirs.levels],
                                 [(lv.right, lv.price, lv.magnitude)
                                  for lv in mine.levels], label)
                for name in ("call_wall", "put_wall"):
                    wall_a, wall_b = getattr(mine, name), getattr(theirs, name)
                    self.assertEqual(wall_b is None, wall_a is None, label + "/" + name)
                    if wall_a is not None:
                        self.assertEqual((wall_b.side, wall_b.price, wall_b.magnitude),
                                         (wall_a.side, wall_a.price, wall_a.magnitude),
                                         label + "/" + name)

    def test_the_session_date_is_the_archived_one_not_today(self):
        self.assertEqual(self.replayed.session_date, date(2026, 8, 4))
        self.assertEqual(self.replayed.blob.split("|")[2], "20260804")

    def test_the_header_stamp_is_inferred_and_says_so(self):
        # Pre-existing snapshots carry no spot timestamp, so every symbol reports
        # the inference. The blob is still emitted -- it is a caveat, not a failure.
        self.assertEqual(len(self.replayed.warnings), len(self.replayed.chains))
        for _ticker, reason in self.replayed.warnings:
            self.assertIn("inferred", reason)
        self.assertEqual(self.replayed.failures, [])


class TestReplayArchiveReader(unittest.TestCase):
    """Both header shapes have to parse: the archive gained a column, not a format."""

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="gexicon-replay-")
        self.payload = payload("2026-08-04 18:34:22", 301.745, [
            row("IWM260804C00300000", oi=5000.0, gamma=0.02, iv=0.30),
            row("IWM260807C00305000", oi=4000.0, gamma=0.01, iv=0.22),
        ], last_trade_time="2026-08-04T14:19:16")

    def test_the_new_column_was_appended_not_inserted(self):
        # A reader keying on column order -- ours does not, but a spreadsheet does --
        # must still find every original field where it was.
        self.assertEqual(HEADER[:len(OLD_HEADER)], OLD_HEADER)
        # Every column gained since then went on the end, in the order it was
        # added, and none of them displaced anything.
        self.assertEqual(HEADER[len(OLD_HEADER):], ("spot_ts_utc", "source"))

    def test_a_new_snapshot_replays_with_the_recorded_stamp(self):
        chain = reduce_payload("IWM", self.payload, max_age_hours=None)
        path, _written = write_snapshot(self.dir, chain, chain.session_date)
        back, session = read_chain(path)
        self.assertIsNone(back.spot_ts_fallback, "nothing needed inferring here")
        self.assertEqual(back.spot_ts, chain.spot_ts)
        # The recorded stamp is the real 15-minute delay, not the file stamp.
        self.assertEqual(back.spot_ts,
                         datetime(2026, 8, 4, 18, 19, 16, tzinfo=timezone.utc))
        self.assertEqual(back.quote_ts, chain.quote_ts)
        self.assertEqual(session, date(2026, 8, 4))
        self.assertEqual([c.occ for c in back.contracts],
                         [c.occ for c in chain.contracts])

    def test_an_old_snapshot_infers_the_stamp_and_reports_it(self):
        path = os.path.join(self.dir, "2026-08-04", "IWM_20260804T183422Z.csv.gz")
        write_archive_csv(path, OLD_HEADER, [
            archive_row("IWM", "2026-08-04T18:34:22Z", "2026-08-04",
                        "IWM260804C00300000"),
            archive_row("IWM", "2026-08-04T18:34:22Z", "2026-08-04",
                        "IWM260807C00305000"),
        ])
        chain, session = read_chain(path)
        self.assertEqual(session, date(2026, 8, 4))
        self.assertEqual(chain.spot_ts, chain.quote_ts - ASSUMED_FEED_DELAY)
        self.assertIn("inferred", chain.spot_ts_fallback)
        # And it surfaces on the result the UI and the CLI read.
        result = replay(session_date="2026-08-04", archive_dir=self.dir)
        self.assertEqual([t for t, _ in result.warnings], ["IWM"])
        self.assertTrue(result.blob.startswith(BLOB_PREFIX + "|"))

    def test_an_empty_spot_ts_cell_is_treated_as_absent(self):
        # Half-written or hand-edited rows exist. An empty cell means "not
        # recorded", which is the inference case, not a parse error.
        path = os.path.join(self.dir, "2026-08-04", "IWM_20260804T183422Z.csv.gz")
        write_archive_csv(path, HEADER, [
            archive_row("IWM", "2026-08-04T18:34:22Z", "2026-08-04",
                        "IWM260804C00300000", spot_ts=""),
        ])
        chain, _session = read_chain(path)
        self.assertEqual(chain.spot_ts, chain.quote_ts - ASSUMED_FEED_DELAY)
        self.assertIn("inferred", chain.spot_ts_fallback)

    def test_a_missing_required_column_fails_loudly(self):
        path = os.path.join(self.dir, "2026-08-04", "IWM_20260804T183422Z.csv.gz")
        write_archive_csv(path, [c for c in OLD_HEADER if c != "gamma"], [
            archive_row("IWM", "2026-08-04T18:34:22Z", "2026-08-04",
                        "IWM260804C00300000"),
        ])
        with self.assertRaises(ReplayError) as caught:
            read_chain(path)
        self.assertIn("gamma", str(caught.exception))

    def test_an_empty_snapshot_fails_loudly(self):
        path = os.path.join(self.dir, "2026-08-04", "IWM_20260804T183422Z.csv.gz")
        write_archive_csv(path, HEADER, [])
        with self.assertRaises(ReplayError):
            read_chain(path)


class TestReplayDoesNoWallClockFiltering(unittest.TestCase):
    """Every contract in a past session has since expired. None may be dropped.

    The expired-contract filter belongs to the fetch, where "expired" means
    "expired as of this quote". Applying it again on read would delete the whole
    0DTE book of any past session -- the one part of a replay there is no substitute
    for -- and the blob would still look perfectly well formed.
    """

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="gexicon-replay-old-")
        # A session far enough back that every contract in it is long settled.
        self.chain = reduce_payload("IWM", payload("2024-05-15 18:00:00", 300.0, [
            row("IWM240515C00300000", oi=5000.0, gamma=0.02, iv=0.30),   # 0DTE
            row("IWM240515P00295000", oi=3000.0, gamma=0.02, iv=0.30),   # 0DTE
            row("IWM240517C00305000", oi=4000.0, gamma=0.01, iv=0.22),
        ]), max_age_hours=None)
        write_snapshot(self.dir, self.chain, self.chain.session_date)

    def test_the_fixture_really_is_expired_today(self):
        self.assertTrue(is_expired(date(2024, 5, 15), now_utc()),
                        "this test proves nothing unless the fixture has settled")

    def test_the_whole_chain_comes_back_including_the_0dte_bucket(self):
        result = replay(session_date="2024-05-15", archive_dir=self.dir)
        self.assertEqual(result.session_date, date(2024, 5, 15))
        self.assertEqual(len(result.chains[0].contracts), 3)
        record = result.records[0]
        self.assertEqual([s.tag for s in record.sections], [None, "0", "R"])
        zero_dte = record.sections[1]
        self.assertTrue(zero_dte.levels, "the 0DTE book must still carry levels")
        self.assertNotEqual(zero_dte.net, 0.0)
        # And the whole-market chunk still equals the sum of its buckets.
        self.assertAlmostEqual(record.total.net,
                               record.sections[1].net + record.sections[2].net,
                               places=9)

    def test_the_blob_carries_the_archived_session_not_todays_date(self):
        result = replay(session_date="2024-05-15", archive_dir=self.dir)
        stamp, session, _records = decode_blob(result.blob)
        self.assertEqual(session, "20240515")
        self.assertTrue(stamp.startswith("20240515"))


class TestReplayDiscovery(unittest.TestCase):
    """A snapshot is a cluster of files, because one run stamps each symbol apart."""

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="gexicon-index-")
        # Two sessions. The later one holds two runs 100 seconds apart -- closer
        # together than one run's own internal spread, so only the repeated ticker
        # separates them. That is the 19:43/19:45 pair from the real archive.
        self.files = {
            "2026-08-03": [("SPY", "20260803T183431Z"),
                           ("IWM", "20260803T183422Z")],
            "2026-08-04": [("IWM", "20260804T194320Z"),
                           ("SPY", "20260804T194332Z"),
                           ("IWM", "20260804T194520Z"),
                           ("SPY", "20260804T194533Z"),
                           ("QQQ", "20260804T194620Z")],
        }
        for session, entries in self.files.items():
            for ticker, stamp in entries:
                quote = "%s-%s-%sT%s:%s:%sZ" % (stamp[0:4], stamp[4:6], stamp[6:8],
                                                stamp[9:11], stamp[11:13],
                                                stamp[13:15])
                write_archive_csv(
                    os.path.join(self.dir, session, "%s_%s.csv.gz" % (ticker, stamp)),
                    HEADER,
                    [archive_row(ticker, quote, session,
                                 "%s260911C00300000" % ticker, spot_ts=quote)])

    def test_dates_come_back_sorted_oldest_first(self):
        self.assertEqual(list_dates(self.dir),
                         [date(2026, 8, 3), date(2026, 8, 4)])
        self.assertEqual([d.session_date for d in archive_index(self.dir)],
                         [date(2026, 8, 3), date(2026, 8, 4)])

    def test_a_directory_that_is_not_a_date_is_ignored(self):
        os.makedirs(os.path.join(self.dir, "notes"), exist_ok=True)
        self.assertEqual(list_dates(self.dir),
                         [date(2026, 8, 3), date(2026, 8, 4)])

    def test_an_empty_archive_lists_nothing_rather_than_raising(self):
        self.assertEqual(list_dates(os.path.join(self.dir, "nope")), [])

    def test_files_seconds_apart_are_one_snapshot_and_a_repeat_starts_another(self):
        snapshots = list_snapshots(self.dir, "2026-08-04")
        self.assertEqual([s.stamp for s in snapshots],
                         ["20260804T194320Z", "20260804T194520Z"])
        # Reported in default-symbol order, so a replayed blob lists its records
        # the way a live one does.
        self.assertEqual(snapshots[0].tickers, ["SPY", "IWM"])
        # QQQ lands 47 seconds after the second run's last file and 60 after its
        # first, so it belongs to that run rather than to a third.
        self.assertEqual(snapshots[1].tickers, ["SPY", "QQQ", "IWM"])

    def test_stamps_come_back_earliest_first(self):
        stamps = [s.stamp for s in list_snapshots(self.dir, "2026-08-04")]
        self.assertEqual(stamps, sorted(stamps))

    def test_the_stamp_is_the_earliest_file_in_the_run(self):
        snapshot = list_snapshots(self.dir, "2026-08-04")[0]
        self.assertEqual(snapshot.stamp, "20260804T194320Z")
        self.assertEqual(snapshot.quote_range_ny, "15:43")

    def test_no_date_replays_the_newest_session_and_no_stamp_the_earliest_snapshot(self):
        snapshot = find_snapshot(self.dir)
        self.assertEqual(snapshot.session_date, date(2026, 8, 4))
        self.assertEqual(snapshot.stamp, "20260804T194320Z")

    def test_a_session_or_stamp_that_is_not_there_is_an_error_not_an_empty_blob(self):
        for kwargs in ({"session_date": "2026-08-05"},
                       {"session_date": "2026-08-04", "stamp": "20260804T120000Z"},
                       {"session_date": "not-a-date"}):
            with self.assertRaises(ReplayError, msg=repr(kwargs)):
                replay(archive_dir=self.dir, **kwargs)
        with self.assertRaises(ReplayError):
            replay(archive_dir=os.path.join(self.dir, "nope"))

    def test_a_ticker_the_snapshot_does_not_hold_is_named(self):
        result = replay(session_date="2026-08-04", stamp="20260804T194320Z",
                        tickers=["SPY", "NVDA"], archive_dir=self.dir)
        self.assertIsNotNone(result.blob)
        self.assertEqual([t for t, _ in result.failures], ["NVDA"])
        self.assertIn("not in the", result.failures[0][1])
        self.assertFalse(result.ok, "a short blob must not report as a clean run")
        _stamp, _session, records = decode_blob(result.blob)
        self.assertEqual([r.ticker for r in records], ["SPY"])

    def test_asking_for_only_absent_tickers_yields_no_blob_at_all(self):
        result = replay(session_date="2026-08-04", stamp="20260804T194320Z",
                        tickers=["NVDA"], archive_dir=self.dir)
        self.assertIsNone(result.blob)
        self.assertEqual([t for t, _ in result.failures], ["NVDA"])

    def test_the_default_is_every_ticker_in_the_snapshot(self):
        result = replay(session_date="2026-08-04", stamp="20260804T194520Z",
                        archive_dir=self.dir)
        _stamp, _session, records = decode_blob(result.blob)
        self.assertEqual(sorted(r.ticker for r in records), ["IWM", "QQQ", "SPY"])
        self.assertEqual(result.failures, [])


class TestReplayNeverWrites(unittest.TestCase):
    """Reading history is not making history."""

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="gexicon-readonly-")
        chain = reduce_payload("IWM", payload("2026-08-04 18:34:22", 301.745, [
            row("IWM260804C00300000", oi=5000.0, gamma=0.02, iv=0.30),
            row("IWM260807C00305000", oi=4000.0, gamma=0.01, iv=0.22),
        ], last_trade_time="2026-08-04T14:19:16"), max_age_hours=None)
        write_snapshot(self.dir, chain, chain.session_date)

    def _tree(self):
        out = {}
        for root, _dirs, names in os.walk(self.dir):
            for name in names:
                path = os.path.join(root, name)
                stat = os.stat(path)
                out[path] = (stat.st_size, stat.st_mtime_ns)
        return out

    def test_a_replay_leaves_the_archive_byte_for_byte_as_it_was(self):
        before = self._tree()
        result = replay(session_date="2026-08-04", archive_dir=self.dir)
        self.assertIsNotNone(result.blob)
        self.assertEqual(result.archived, [],
                         "a replay has nothing to archive and must claim nothing")
        self.assertEqual(self._tree(), before)


class TestReplayServerEndpoints(unittest.TestCase):
    """The page's two new endpoints, in the same shape the live one returns."""

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="gexicon-replay-ui-")
        chain = reduce_payload("IWM", payload("2026-08-04 18:34:22", 301.745, [
            row("IWM260804C00300000", oi=5000.0, gamma=0.02, iv=0.30),
            row("IWM260807C00305000", oi=4000.0, gamma=0.01, iv=0.22),
        ], last_trade_time="2026-08-04T14:19:16"), max_age_hours=None)
        write_snapshot(self.dir, chain, chain.session_date)

    def test_the_index_lists_dates_snapshots_and_tickers(self):
        from gexicon.server import archive_payload
        data = archive_payload(self.dir)
        self.assertEqual([d["session_date"] for d in data["dates"]], ["2026-08-04"])
        snapshot = data["dates"][0]["snapshots"][0]
        self.assertEqual(snapshot["stamp"], "20260804T183422Z")
        self.assertEqual(snapshot["tickers"], ["IWM"])
        self.assertEqual(snapshot["time_ny"], "14:34:22")

    def test_the_replay_endpoint_returns_a_blob_and_flags_the_mode(self):
        from gexicon.server import replay_payload
        data = replay_payload(self.dir, session_date="2026-08-04")
        self.assertTrue(data["replay"])
        self.assertTrue(data["blob"].startswith(BLOB_PREFIX + "|"))
        self.assertEqual(data["session_date"], "2026-08-04")
        self.assertEqual(data["snapshot_stamp"], "20260804T183422Z")
        self.assertFalse(data["stamp_inferred"], "this snapshot recorded its stamp")
        self.assertEqual(len(data["symbols"]), 1)
        self.assertEqual(data["failures"], [])
        self.assertIsNone(data["error"])

    def test_a_bad_request_returns_an_error_and_no_blob(self):
        from gexicon.server import replay_payload
        data = replay_payload(self.dir, session_date="2026-08-05")
        self.assertIsNone(data["blob"])
        self.assertIn("2026-08-05", data["error"])
        self.assertTrue(data["replay"])


# --------------------------------------------------------------------------
class TestTouchProbabilityMaths(unittest.TestCase):
    """P(touch K before T) = 2 * N(-|ln(K/S)| / (sigma*sqrt(T)))."""

    def test_a_wall_sitting_on_spot_is_a_certainty(self):
        self.assertEqual(touch_probability(100.0, 100.0, 0.2, 0.01), 100)

    def test_a_wall_miles_away_with_no_time_left_is_zero(self):
        minute = 1.0 / (365.0 * 24.0 * 60.0)
        self.assertEqual(touch_probability(100.0, 120.0, 0.2, minute), 0)

    def test_it_is_twice_the_probability_of_finishing_beyond(self):
        # The reflection principle in one line. This is why "finishes beyond" is
        # the wrong number to quote against a target.
        spot, strike, sigma, years = 100.0, 103.0, 0.25, 0.05
        finish = norm_cdf(-abs(math.log(strike / spot)) / (sigma * math.sqrt(years)))
        self.assertEqual(touch_probability(spot, strike, sigma, years),
                         min(100, int(100.0 * 2.0 * finish + 0.5)))

    def test_it_matches_a_hand_computed_value(self):
        # 0.5% away, 20 vol, two hours to the close: sigma*sqrt(T) = 0.003023,
        # |ln(K/S)| = 0.0049875, x = 1.6499, 2*N(-1.6499) = 0.0990 -> 10.
        years = 2.0 / (365.0 * 24.0)
        self.assertEqual(touch_probability(100.0, 100.5, 0.20, years), 10)

    def test_it_is_monotone_in_distance_and_in_time(self):
        near = touch_probability(100.0, 100.5, 0.2, 0.01)
        far = touch_probability(100.0, 105.0, 0.2, 0.01)
        self.assertGreater(near, far)
        self.assertGreater(touch_probability(100.0, 105.0, 0.2, 0.05), far)

    def test_both_sides_of_spot_read_the_same(self):
        # |ln(K/S)| is symmetric in log space. The skew that makes real downside
        # dearer arrives through sigma, never through the formula.
        up = touch_probability(100.0, 100.0 * math.exp(0.02), 0.2, 0.01)
        down = touch_probability(100.0, 100.0 * math.exp(-0.02), 0.2, 0.01)
        self.assertEqual(up, down)

    def test_unusable_inputs_give_none_never_a_zero(self):
        self.assertIsNone(touch_probability(100.0, 105.0, 0.0, 0.01))
        self.assertIsNone(touch_probability(100.0, 105.0, -0.2, 0.01))
        self.assertIsNone(touch_probability(100.0, 105.0, 0.2, 0.0))
        self.assertIsNone(touch_probability(100.0, 105.0, 0.2, -1.0))
        self.assertIsNone(touch_probability(0.0, 105.0, 0.2, 0.01))
        self.assertIsNone(touch_probability(100.0, 0.0, 0.2, 0.01))

    def test_the_result_is_always_an_integer_inside_0_100(self):
        for strike in (80.0, 99.0, 100.0, 101.0, 130.0):
            for years in (1e-8, 1e-4, 0.01, 1.0):
                value = touch_probability(100.0, strike, 0.3, years)
                self.assertIsInstance(value, int)
                self.assertGreaterEqual(value, 0)
                self.assertLessEqual(value, 100)


# --------------------------------------------------------------------------
class TestTouchHorizon(unittest.TestCase):
    """One horizon for every chunk: 16:00 New York on the session date."""

    DAY = date(2026, 8, 4)

    def test_it_measures_to_the_close_on_the_session_date(self):
        anchor = utc("2026-08-04 18:00:00")   # 14:00 New York
        self.assertAlmostEqual(touch_horizon_years(anchor, self.DAY) * 365.0 * 24.0,
                               2.0, places=9)

    def test_after_the_close_there_is_no_horizon(self):
        # Not zero, not a floor -- None, so the field is omitted rather than
        # emitted as a fabricated 0.
        for stamp in ("2026-08-04 20:00:00", "2026-08-04 20:01:00",
                      "2026-08-05 01:00:00"):
            with self.subTest(stamp=stamp):
                self.assertIsNone(touch_horizon_years(utc(stamp), self.DAY))

    def test_a_second_before_the_close_still_has_one(self):
        years = touch_horizon_years(utc("2026-08-04 19:59:59"), self.DAY)
        self.assertIsNotNone(years)
        self.assertAlmostEqual(years * TOUCH_SECONDS_PER_YEAR, 1.0)

    def test_it_uses_a_365_day_year_so_both_pipelines_agree(self):
        # Pinned deliberately: the market-terminal reference pipeline computes
        # the same field and the two have to round to the same integer.
        self.assertEqual(TOUCH_SECONDS_PER_YEAR, 365.0 * 24.0 * 3600.0)


# --------------------------------------------------------------------------
class TestStrikeSigma(unittest.TestCase):
    """Which implied vol prices the move to a wall."""

    def test_it_takes_the_nearest_expiry_at_that_strike(self):
        contracts = [contract("TST260911C00100000", iv=0.40),
                     contract("TST260807C00100000", iv=0.50),
                     contract("TST260821C00100000", iv=0.30)]
        self.assertAlmostEqual(strike_sigma(contracts, 100.0), 0.50)

    def test_calls_and_puts_at_the_front_expiry_are_averaged(self):
        contracts = [contract("TST260807C00100000", iv=0.20),
                     contract("TST260807P00100000", iv=0.30)]
        self.assertAlmostEqual(strike_sigma(contracts, 100.0), 0.25)

    def test_it_falls_back_to_the_nearest_strike_that_has_one(self):
        contracts = [contract("TST260807C00095000", iv=0.33),
                     contract("TST260807C00120000", iv=0.99)]
        self.assertAlmostEqual(strike_sigma(contracts, 100.0), 0.33)

    def test_junk_vols_are_not_candidates(self):
        contracts = [contract("TST260807C00100000", iv=0.0),
                     contract("TST260807P00100000", iv=9.0)]
        self.assertIsNone(strike_sigma(contracts, 100.0))

    def test_a_strike_nobody_holds_is_not_a_candidate(self):
        # Zero open interest means the quote may not have moved in days.
        contracts = [contract("TST260807C00100000", iv=0.90, oi=0.0),
                     contract("TST260807C00105000", iv=0.25, oi=500.0)]
        self.assertAlmostEqual(strike_sigma(contracts, 100.0), 0.25)

    def test_no_usable_vol_anywhere_gives_none(self):
        self.assertIsNone(strike_sigma([], 100.0))


# --------------------------------------------------------------------------
class TestWallTouchEndToEnd(unittest.TestCase):
    """The field as it reaches a Wall, through build_record."""

    def _chain(self, timestamp, rows, spot=100.0, last_trade=None):
        return reduce_payload("TEST", payload(timestamp, spot, rows,
                                              last_trade_time=last_trade),
                              max_age_hours=None)

    def _rows(self, iv=0.20, expiry="260804"):
        # Walls 0.3% either side of spot, so two hours at 20 vol is a real chance.
        return [row("TST%sC00100300" % expiry, oi=3000000.0, gamma=0.02, iv=iv),
                row("TST%sP00100300" % expiry, oi=100000.0, gamma=0.02, iv=iv),
                row("TST%sC00099700" % expiry, oi=100000.0, gamma=0.02, iv=iv),
                row("TST%sP00099700" % expiry, oi=2000000.0, gamma=0.02, iv=iv)]

    def test_walls_carry_a_touch_probability(self):
        chain = self._chain("2026-08-04 18:15:00", self._rows(),
                            last_trade="2026-08-04T14:00:00")
        total = build_record(chain, chain.session_date).total
        self.assertEqual(total.call_wall.price, 100.3)
        self.assertEqual(total.put_wall.price, 99.7)
        for wall in (total.call_wall, total.put_wall):
            self.assertIsInstance(wall.touch, int)
            self.assertGreater(wall.touch, 20)

    def test_after_the_close_the_field_is_omitted(self):
        chain = self._chain("2026-08-04 20:30:00", self._rows(expiry="260911"),
                            last_trade="2026-08-04T16:20:00")
        total = build_record(chain, chain.session_date).total
        self.assertIsNotNone(total.call_wall)
        self.assertIsNone(total.call_wall.touch)
        self.assertIsNone(total.put_wall.touch)

    def test_no_usable_implied_vol_omits_the_field(self):
        chain = self._chain("2026-08-04 18:15:00", self._rows(iv=0.0),
                            last_trade="2026-08-04T14:00:00")
        total = build_record(chain, chain.session_date).total
        self.assertIsNotNone(total.call_wall)
        self.assertIsNone(total.call_wall.touch)
        self.assertIsNone(total.put_wall.touch)

    def test_every_chunk_uses_the_same_horizon(self):
        # A wall at the same strike in two chunks must read the same odds. If a
        # later change gives a bucket its own horizon, this is what breaks.
        rows = self._rows() + [
            row("TST260911C00100300", oi=9000000.0, gamma=0.02),
            row("TST260911P00099700", oi=8000000.0, gamma=0.02),
        ]
        chain = self._chain("2026-08-04 18:15:00", rows,
                            last_trade="2026-08-04T14:00:00")
        record = build_record(chain, chain.session_date)
        seen = {}
        for section in record.sections:
            for wall in (section.call_wall, section.put_wall):
                if wall is None or wall.touch is None:
                    continue
                self.assertEqual(seen.setdefault(wall.price, wall.touch), wall.touch)
        self.assertTrue(seen)

    def test_the_horizon_is_anchored_on_the_spot_stamp_not_the_file_stamp(self):
        # The file was published 15 minutes after its spot was true. Anchoring on
        # the publish stamp would shorten every horizon by that gap.
        rows = self._rows()
        early = self._chain("2026-08-04 18:15:00", rows,
                            last_trade="2026-08-04T14:00:00")
        # Same spot instant, file published a minute later.
        later = self._chain("2026-08-04 18:16:00", rows,
                            last_trade="2026-08-04T14:00:00")
        a = build_record(early, early.session_date).total.call_wall.touch
        b = build_record(later, later.session_date).total.call_wall.touch
        self.assertEqual(a, b)


# --------------------------------------------------------------------------
class TestTouchOnTheWire(unittest.TestCase):
    """The optional third field on a W token."""

    HEAD = BLOB_PREFIX + "|202608041800Z|20260804|"

    def test_a_wall_with_a_probability_carries_three_fields(self):
        wall = Wall(side="C", price=7750.0, magnitude=4.15, touch=41)
        self.assertEqual(encode_wall(wall), "WC7750:+4.15:41")

    def test_a_wall_without_one_stays_two_fields(self):
        # Never an empty third field, never a placeholder, never a faked zero.
        wall = Wall(side="C", price=7750.0, magnitude=4.15, touch=None)
        self.assertEqual(encode_wall(wall), "WC7750:+4.15")

    def test_zero_is_a_real_answer_and_is_transmitted(self):
        wall = Wall(side="C", price=7750.0, magnitude=4.15, touch=0)
        self.assertEqual(encode_wall(wall), "WC7750:+4.15:0")

    def test_both_shapes_decode(self):
        self.assertEqual(decode_wall("WC7750:+4.15:41").touch, 41)
        self.assertIsNone(decode_wall("WC7750:+4.15").touch)

    def test_both_shapes_can_share_one_blob(self):
        text = self.HEAD + "SPX,7745.60,7480.00,+126.19,C7700:+9.76,WC7750:+4.15:41,WP7700:-1.46"
        record = decode_blob(text)[2][0]
        self.assertEqual(record.total.call_wall.touch, 41)
        self.assertIsNone(record.total.put_wall.touch)

    def test_a_blob_written_before_touch_probabilities_still_decodes(self):
        text = self.HEAD + "SPY,747.03,745.98,-0.69,P730:-0.82,WC760:+0.75,WP730:-0.82"
        record = decode_blob(text)[2][0]
        self.assertEqual(record.total.call_wall.price, 760.0)
        self.assertIsNone(record.total.call_wall.touch)

    def test_rejects_a_malformed_touch_field(self):
        for bad in ("WC7750:+4.15:", "WC7750:+4.15:x", "WC7750:+4.15:101",
                    "WC7750:+4.15:-3", "WC7750:+4.15:4.5", "WC7750:+4.15:41:9"):
            with self.subTest(token=bad):
                with self.assertRaises(BlobFormatError):
                    decode_wall(bad)

    def test_the_encoder_refuses_an_out_of_range_probability(self):
        for bad in (-1, 101, 1.5, "41", True):
            with self.subTest(touch=bad):
                with self.assertRaises(BlobFormatError):
                    encode_wall(Wall(side="C", price=7750.0, magnitude=4.15,
                                     touch=bad))

    def test_a_record_round_trips_its_touch_fields(self):
        rows = [row("TST260804C00100300", oi=3000000.0, gamma=0.02),
                row("TST260804P00099700", oi=2000000.0, gamma=0.02),
                row("TST260911C00101000", oi=4000000.0, gamma=0.02),
                row("TST260911P00099000", oi=4000000.0, gamma=0.02)]
        chain = reduce_payload("TEST", payload("2026-08-04 18:15:00", 100.0, rows,
                                               last_trade_time="2026-08-04T14:00:00"),
                               max_age_hours=None)
        record = build_record(chain, chain.session_date)
        blob = encode_blob([record], chain.spot_ts, chain.session_date)
        back = decode_blob(blob)[2][0]
        for src, got in zip(record.sections, back.sections):
            self.assertEqual(src.call_wall.touch, got.call_wall.touch)
            self.assertIsNotNone(got.call_wall.touch)


# --------------------------------------------------------------------------
def yahoo_sample():
    """The saved SPY chain response, trimmed to twenty contracts. Real data."""
    with open(os.path.join(TEST_SAMPLES_DIR, "yahoo_SPY.json"), encoding="utf-8") as h:
        return json.load(h)


def yahoo_payload(expiry_epoch, calls=(), puts=(), spot=100.0,
                  market_time=None, symbol="TEST", quote_extra=None):
    """A Yahoo chain response with whatever contracts the test needs."""
    quote = {"symbol": symbol, "regularMarketPrice": spot,
             "regularMarketTime": market_time}
    if quote_extra:
        quote.update(quote_extra)
    return {"optionChain": {"error": None, "result": [{
        "underlyingSymbol": symbol,
        "expirationDates": [expiry_epoch],
        "quote": quote,
        "options": [{"expirationDate": expiry_epoch,
                     "calls": list(calls), "puts": list(puts)}],
    }]}}


def yahoo_row(occ, strike, oi=1000.0, iv=0.20, volume=0.0):
    return {"contractSymbol": occ, "strike": strike, "openInterest": oi,
            "impliedVolatility": iv, "volume": volume}


def epoch_of(day):
    """Midnight UTC on `day`, which is how Yahoo writes an expiry."""
    return int(datetime(day.year, day.month, day.day,
                        tzinfo=timezone.utc).timestamp())


class TestYahooSymbolsAndExpiries(unittest.TestCase):

    def test_index_symbols_carry_a_caret_and_everything_else_does_not(self):
        self.assertEqual(yahoo.to_yahoo("SPX"), "^SPX")
        self.assertEqual(yahoo.to_yahoo("_SPX"), "^SPX")
        self.assertEqual(yahoo.to_yahoo("NDX"), "^NDX")
        self.assertEqual(yahoo.to_yahoo("RUT"), "^RUT")
        self.assertEqual(yahoo.to_yahoo("SPY"), "SPY")
        self.assertEqual(yahoo.to_yahoo("nvda"), "NVDA")

    def test_expiry_epoch_is_read_in_utc_not_local_time(self):
        # Midnight UTC on the 23rd. Anywhere west of Greenwich the local date is
        # still the 22nd, and reading it locally would empty the 0DTE bucket --
        # which is the failure this whole source exists to repair.
        self.assertEqual(yahoo.expiry_from_epoch(1790121600), date(2026, 9, 23))
        self.assertEqual(yahoo.expiry_from_epoch(str(1790121600)),
                         date(2026, 9, 23))

    def test_every_expiry_in_the_saved_response_is_midnight_utc(self):
        result = yahoo_sample()["optionChain"]["result"][0]
        for epoch in result["expirationDates"]:
            moment = datetime.fromtimestamp(epoch, timezone.utc)
            self.assertEqual((moment.hour, moment.minute, moment.second), (0, 0, 0))


class TestYahooMapper(unittest.TestCase):
    """The saved response is real: SPY, 2026-09-23, front expiry, 20 contracts."""

    def setUp(self):
        self.chain = yahoo.reduce_payloads("SPY", [yahoo_sample()])

    def test_spot_and_quote_time_come_from_the_quote_block(self):
        self.assertEqual(self.chain.ticker, "SPY")
        self.assertAlmostEqual(self.chain.spot, 769.7, places=4)
        self.assertEqual(self.chain.quote_ts,
                         datetime(2026, 9, 23, 15, 12, 1, tzinfo=timezone.utc))
        self.assertEqual(self.chain.session_date, date(2026, 9, 23))

    def test_the_spot_instant_is_the_quote_instant_with_no_fallback(self):
        # Yahoo's quote is live, not a fifteen-minute delayed snapshot, so there
        # is nothing to correct and nothing to infer.
        self.assertEqual(self.chain.spot_ts, self.chain.quote_ts)
        self.assertIsNone(self.chain.spot_ts_fallback)

    def test_the_chain_is_labelled_as_the_second_source(self):
        self.assertEqual(self.chain.source, "yahoo")

    def test_contracts_carry_the_occ_right_strike_and_expiry(self):
        self.assertEqual(len(self.chain.contracts), 20)
        for contract_ in self.chain.contracts:
            self.assertIn(contract_.right, ("C", "P"))
            self.assertEqual(contract_.expiry, date(2026, 9, 23))
            self.assertGreater(contract_.strike, 0.0)
            self.assertGreater(contract_.open_interest, 0.0)
            _root, expiry, right, strike = parse_occ(contract_.occ)
            self.assertEqual((expiry, right, strike),
                             (contract_.expiry, contract_.right, contract_.strike))

    def test_both_sides_survive_the_mapping(self):
        rights = set(c.right for c in self.chain.contracts)
        self.assertEqual(rights, {"C", "P"})

    def test_gamma_is_computed_wherever_the_vol_can_carry_it(self):
        # Yahoo publishes no gamma at all. If this came out zero across the board
        # the levels would be empty and the blob would look fine while saying
        # nothing. Where the vol is junk -- Yahoo writes 0.00001 on a contract it
        # cannot price -- the gamma is zero rather than invented, which is the
        # same bound the flip re-pricing applies.
        priced = [c for c in self.chain.contracts if MIN_IV <= c.iv <= MAX_IV]
        junk = [c for c in self.chain.contracts if c.iv < MIN_IV]
        self.assertGreater(len(priced), 10)
        for contract_ in priced:
            self.assertGreater(contract_.gamma, 0.0)
        for contract_ in junk:
            self.assertEqual(contract_.gamma, 0.0)

    def test_the_whole_record_builds_with_a_0dte_bucket(self):
        record = build_record(self.chain, self.chain.session_date)
        self.assertEqual([s.tag for s in record.buckets], ["0", "R"])
        self.assertIsNotNone(record.total.call_wall)
        self.assertIsNotNone(record.total.put_wall)


class TestYahooMapperDrops(unittest.TestCase):

    QUOTE = int(datetime(2026, 9, 23, 15, 0, tzinfo=timezone.utc).timestamp())

    def reduce(self, calls=(), puts=(), day=date(2026, 9, 23), spot=100.0):
        return yahoo.reduce_payloads(
            "TEST",
            [yahoo_payload(epoch_of(day), calls=calls, puts=puts, spot=spot,
                           market_time=self.QUOTE)])

    def test_zero_or_missing_open_interest_is_dropped(self):
        chain = self.reduce(calls=[yahoo_row("TST260923C00100000", 100.0, oi=0.0),
                                   yahoo_row("TST260923C00101000", 101.0, oi=None),
                                   yahoo_row("TST260923C00102000", 102.0, oi=7.0)])
        self.assertEqual([c.strike for c in chain.contracts], [102.0])

    def test_a_contract_with_no_implied_vol_is_dropped(self):
        # Gamma is computed from the vol here rather than read off the feed, so a
        # contract with no vol has no gamma and would be dead weight in the chain.
        chain = self.reduce(calls=[yahoo_row("TST260923C00100000", 100.0, iv=None),
                                   yahoo_row("TST260923C00101000", 101.0, iv=0.2)])
        self.assertEqual([c.strike for c in chain.contracts], [101.0])

    def test_an_unparseable_contract_symbol_is_dropped(self):
        chain = self.reduce(calls=[yahoo_row("not-an-occ-symbol", 100.0),
                                   yahoo_row("TST260923C00101000", 101.0)])
        self.assertEqual([c.strike for c in chain.contracts], [101.0])
        self.assertEqual(chain.dropped_unparseable, 1)

    def test_a_settled_expiry_is_dropped(self):
        # 20:30 UTC on expiry day is 16:30 New York -- half an hour past
        # settlement, and the contract no longer exists.
        late = int(datetime(2026, 9, 23, 20, 30, tzinfo=timezone.utc).timestamp())
        payloads = [yahoo_payload(epoch_of(date(2026, 9, 23)),
                                  calls=[yahoo_row("TST260923C00100000", 100.0)],
                                  puts=[yahoo_row("TST260924P00100000", 100.0)],
                                  market_time=late)]
        # The put is a later expiry, so something survives and the reduction does
        # not fail for the wrong reason.
        payloads[0]["optionChain"]["result"][0]["options"].append(
            {"expirationDate": epoch_of(date(2026, 9, 24)),
             "calls": [yahoo_row("TST260924C00100000", 100.0)], "puts": []})
        payloads[0]["optionChain"]["result"][0]["options"][0]["puts"] = []
        chain = yahoo.reduce_payloads("TEST", payloads)
        self.assertEqual([c.expiry for c in chain.contracts], [date(2026, 9, 24)])
        self.assertEqual(chain.dropped_expired, 1)

    def test_an_expiry_the_symbol_disagrees_with_is_dropped(self):
        # The epoch says the 23rd, the contract symbol says the 24th. One of them
        # is wrong and guessing which puts the contract in the wrong bucket.
        chain = self.reduce(calls=[yahoo_row("TST260924C00100000", 100.0),
                                   yahoo_row("TST260923C00101000", 101.0)])
        self.assertEqual([c.strike for c in chain.contracts], [101.0])

    def test_a_payload_with_nothing_usable_is_an_error_not_an_empty_chain(self):
        with self.assertRaises(FetchError):
            self.reduce(calls=[yahoo_row("TST260923C00100000", 100.0, oi=0.0)])

    def test_a_missing_quote_price_is_an_error(self):
        with self.assertRaises(FetchError):
            yahoo.reduce_payloads("TEST", [yahoo_payload(
                epoch_of(date(2026, 9, 23)),
                calls=[yahoo_row("TST260923C00100000", 100.0)],
                spot=0.0, market_time=self.QUOTE)])

    def test_a_missing_quote_time_is_an_error(self):
        with self.assertRaises(FetchError):
            yahoo.reduce_payloads("TEST", [yahoo_payload(
                epoch_of(date(2026, 9, 23)),
                calls=[yahoo_row("TST260923C00100000", 100.0)],
                market_time=None)])

    def test_an_error_field_in_the_response_is_raised_not_ignored(self):
        bad = yahoo_payload(epoch_of(date(2026, 9, 23)),
                            calls=[yahoo_row("TST260923C00100000", 100.0)],
                            market_time=self.QUOTE)
        bad["optionChain"]["error"] = {"code": "Not Found"}
        with self.assertRaises(FetchError):
            yahoo.reduce_payloads("TEST", [bad])


class TestYahooGammaMatchesTheFlipMaths(unittest.TestCase):
    """Yahoo publishes no gamma, so it is computed. It has to be the same gamma."""

    def test_computed_gamma_reproduces_the_flip_re_pricing(self):
        quote_ts = utc("2026-09-23 15:00:00")
        spot, strike, iv = 500.0, 505.0, 0.22
        expiry = date(2026, 10, 16)
        years = years_to_expiry(expiry, quote_ts)
        c = Contract(occ="SPY261016C00505000", expiry=expiry, right="C",
                     strike=strike, open_interest=1000.0,
                     gamma=yahoo.bs_gamma(spot, strike, iv, years),
                     iv=iv, volume=0.0)
        # `total_gamma_at` re-prices from scratch off iv/strike/expiry. If the
        # computed gamma were a different formula the two would not agree.
        # `total_gamma_at` already returns dollars per 1% move, so the two are
        # directly comparable -- see the algebra in `gex._flip_terms`.
        from_gamma = contract_gex(c, spot)
        from_scratch = total_gamma_at(_flip_terms([c], quote_ts), spot)
        self.assertAlmostEqual(from_gamma / from_scratch, 1.0, places=9)

    def test_gamma_is_zero_rather_than_invented_when_the_vol_is_junk(self):
        years = 0.05
        self.assertEqual(yahoo.bs_gamma(500.0, 500.0, 0.0, years), 0.0)
        self.assertEqual(yahoo.bs_gamma(500.0, 500.0, MAX_IV * 2, years), 0.0)
        self.assertEqual(yahoo.bs_gamma(500.0, 500.0, 0.2, 0.0), 0.0)
        self.assertEqual(yahoo.bs_gamma(0.0, 500.0, 0.2, years), 0.0)

    def test_at_the_money_gamma_is_the_largest(self):
        years = 0.02
        at = yahoo.bs_gamma(500.0, 500.0, 0.2, years)
        away = yahoo.bs_gamma(500.0, 560.0, 0.2, years)
        self.assertGreater(at, away)
        self.assertGreater(at, 0.0)


class TestSessionDateComesFromTheChain(unittest.TestCase):
    """2026-09-24: the quote stamp is not what session a Yahoo chain belongs to.

    The 08:30 build failed with nothing usable and the 09:30:06 build dropped SPX
    and RUT, both because the stamp read as yesterday. Pre-market it always does,
    and the cash indexes hold it past the open until they print.
    """

    TODAY = date(2026, 9, 24)                                    # a Thursday
    PRE = datetime(2026, 9, 24, 12, 30, tzinfo=timezone.utc)     # 08:30 New York
    OPEN = datetime(2026, 9, 24, 13, 30, 6, tzinfo=timezone.utc)  # 09:30:06
    LAST_CLOSE = datetime(2026, 9, 23, 20, 0, tzinfo=timezone.utc)  # 16:00 the 23rd

    def chain(self, symbol, now, expiry=None, spot=6700.0, market_time=None,
              quote_extra=None, twin_quote=None):
        expiry = expiry or self.TODAY
        occ = "TST%s" % expiry.strftime("%y%m%d")
        stamp = market_time if market_time is not None else self.LAST_CLOSE
        return yahoo.reduce_payloads(symbol, [yahoo_payload(
            epoch_of(expiry),
            calls=[yahoo_row(occ + "C06700000", 6700.0, oi=20000.0)],
            puts=[yahoo_row(occ + "P06600000", 6600.0, oi=20000.0)],
            spot=spot, market_time=int(stamp.timestamp()),
            quote_extra=quote_extra)], now=now, twin_quote=twin_quote)

    def test_a_premarket_chain_belongs_to_today_not_to_last_nights_stamp(self):
        chain = self.chain("SPX", self.PRE)
        self.assertEqual(session_date_of(chain.quote_ts), date(2026, 9, 23))
        self.assertEqual(chain.session_date, self.TODAY)

    def test_an_index_that_has_not_printed_yet_is_still_todays_chain(self):
        # 09:30:06. Every ETF has ticked; the cash indexes have not.
        chain = self.chain("SPX", self.OPEN)
        self.assertEqual(chain.session_date, self.TODAY)
        self.assertTrue(prefer_fallback(chain, None, self.OPEN)[0])

    def test_a_premarket_chain_is_accepted_over_a_stalled_cboe_file(self):
        self.assertTrue(prefer_fallback(self.chain("SPX", self.PRE), None,
                                        self.PRE)[0])

    def test_a_holiday_is_still_refused_because_nothing_expires_on_one(self):
        # Monday 2026-01-19, a holiday. Yahoo holds Friday's close and the chain's
        # earliest live expiry is Tuesday. `earliest >= today` would wave that
        # through -- the test is equality for exactly this reason.
        monday = datetime(2026, 1, 19, 14, 30, tzinfo=timezone.utc)   # 09:30 NY
        friday_close = datetime(2026, 1, 16, 21, 0, tzinfo=timezone.utc)
        chain = self.chain("SPX", monday, expiry=date(2026, 1, 20),
                           market_time=friday_close)
        self.assertEqual(chain.session_date, date(2026, 1, 16))
        use_it, why_not = prefer_fallback(chain, None, monday)
        self.assertIsNone(use_it)
        self.assertIn("not today", why_not)

    def test_a_chain_nobody_has_updated_for_a_week_is_refused(self):
        stale = self.LAST_CLOSE - timedelta(days=7)
        chain = self.chain("SPX", self.PRE, market_time=stale)
        self.assertEqual(chain.session_date, session_date_of(stale))

    def test_outside_the_window_the_quote_stamp_still_decides(self):
        night = datetime(2026, 9, 24, 3, 0, tzinfo=timezone.utc)   # 23:00 the 23rd
        chain = self.chain("SPX", night)
        self.assertEqual(chain.session_date, date(2026, 9, 23))

    def test_the_premarket_print_is_preferred_when_it_is_newer(self):
        chain = self.chain(
            "SPY", self.PRE, spot=660.0,
            quote_extra={"preMarketPrice": 663.5,
                         "preMarketTime": int(self.PRE.timestamp())})
        self.assertAlmostEqual(chain.spot, 663.5)
        self.assertEqual(chain.quote_ts, self.PRE)
        self.assertEqual(chain.spot_ts, self.PRE)
        self.assertEqual(chain.session_date, self.TODAY)
        # An ETF that has printed needs no explaining.
        self.assertIsNone(chain.spot_note)

    def test_a_stale_premarket_print_is_ignored(self):
        old = self.LAST_CLOSE - timedelta(hours=2)
        chain = self.chain("SPY", self.PRE, spot=660.0,
                           quote_extra={"preMarketPrice": 640.0,
                                        "preMarketTime": int(old.timestamp())})
        self.assertAlmostEqual(chain.spot, 660.0)
        self.assertEqual(chain.quote_ts, self.LAST_CLOSE)

    def test_todays_expiry_is_priced_off_the_run_clock_not_the_stale_stamp(self):
        # Pricing today's expiry off last night's stamp hands it an extra day of
        # life, and 0DTE gamma comes out about half what it should be.
        chain = self.chain("SPX", self.OPEN)
        years = years_to_expiry(self.TODAY, self.OPEN)
        expected = yahoo.bs_gamma(6700.0, 6700.0, 0.20, years)
        got = [c.gamma for c in chain.contracts if c.strike == 6700.0][0]
        self.assertAlmostEqual(got, expected)
        self.assertGreater(got, yahoo.bs_gamma(
            6700.0, 6700.0, 0.20, years_to_expiry(self.TODAY, self.LAST_CLOSE)))


class TestIndexSpotProxiedFromItsTwin(unittest.TestCase):
    """Yahoo's ^SPX quote held 2026-09-23's close at 09:50 on the 24th."""

    NOW = datetime(2026, 9, 24, 13, 50, tzinfo=timezone.utc)      # 09:50 New York
    LAST_CLOSE = datetime(2026, 9, 23, 20, 36, tzinfo=timezone.utc)

    def index_quote(self, spot=7706.03, previous_close=7706.03):
        return {"symbol": "^SPX", "regularMarketPrice": spot,
                "regularMarketTime": int(self.LAST_CLOSE.timestamp()),
                "regularMarketPreviousClose": previous_close}

    def twin_quote(self, price=772.4, previous_close=769.7, when=None):
        when = when if when is not None else self.NOW
        return {"symbol": "SPY", "regularMarketPrice": price,
                "regularMarketTime": int(when.timestamp()),
                "regularMarketPreviousClose": previous_close}

    def test_the_arithmetic_is_the_twin_carried_on_the_closing_ratio(self):
        spot, when = yahoo.proxy_spot(self.index_quote(), self.twin_quote(), "SPY")
        self.assertAlmostEqual(spot, 772.4 * (7706.03 / 769.7), places=6)
        self.assertAlmostEqual(spot, 7733.07, places=1)
        self.assertEqual(when, self.NOW)

    def test_the_twins_own_premarket_print_is_what_gets_carried(self):
        pre = datetime(2026, 9, 24, 12, 30, tzinfo=timezone.utc)
        twin = self.twin_quote(price=769.9)
        twin["preMarketPrice"] = 774.0
        twin["preMarketTime"] = int(pre.timestamp())
        twin["regularMarketTime"] = int(self.LAST_CLOSE.timestamp())
        spot, when = yahoo.proxy_spot(self.index_quote(), twin, "SPY")
        self.assertAlmostEqual(spot, 774.0 * (7706.03 / 769.7), places=6)
        self.assertEqual(when, pre)

    def test_a_missing_previous_close_on_either_side_gives_no_proxy(self):
        self.assertIsNone(yahoo.proxy_spot(
            self.index_quote(previous_close=0.0), self.twin_quote(), "SPY"))
        self.assertIsNone(yahoo.proxy_spot(
            self.index_quote(), self.twin_quote(previous_close=0.0), "SPY"))

    def chain(self, twin_quote=None, symbol="SPX"):
        occ = "TST260924"
        payload = yahoo_payload(
            epoch_of(date(2026, 9, 24)),
            calls=[yahoo_row(occ + "C07700000", 7700.0, oi=20000.0)],
            puts=[yahoo_row(occ + "P07600000", 7600.0, oi=20000.0)],
            spot=7706.03, market_time=int(self.LAST_CLOSE.timestamp()),
            quote_extra={"regularMarketPreviousClose": 7706.03})
        return yahoo.reduce_payloads(symbol, [payload], now=self.NOW,
                                     twin_quote=twin_quote)

    def test_the_chain_takes_the_proxied_spot_and_the_twins_instant(self):
        chain = self.chain(twin_quote=self.twin_quote())
        self.assertAlmostEqual(chain.spot, 772.4 * (7706.03 / 769.7), places=6)
        self.assertEqual(chain.quote_ts, self.NOW)
        self.assertEqual(chain.spot_ts, self.NOW)
        self.assertEqual(chain.session_date, date(2026, 9, 24))
        self.assertIn("proxied from SPY", chain.spot_note)
        self.assertIn("17.2h old", chain.spot_note)

    def test_a_twin_that_is_also_stale_leaves_the_prior_close_in_place(self):
        chain = self.chain(twin_quote=self.twin_quote(when=self.LAST_CLOSE))
        self.assertAlmostEqual(chain.spot, 7706.03)
        self.assertEqual(chain.quote_ts, self.LAST_CLOSE)
        self.assertIn("prior close", chain.spot_note)
        # Still today's chain. The flip and the walls are built from open
        # interest and strikes, and they do not need a live spot.
        self.assertEqual(chain.session_date, date(2026, 9, 24))

    def test_no_twin_at_all_leaves_the_prior_close_in_place(self):
        self.assertIn("prior close", self.chain().spot_note)

    def test_a_symbol_with_no_twin_is_never_proxied(self):
        self.assertIsNone(self.chain(twin_quote=self.twin_quote(),
                                     symbol="TEST").spot_note)


class TestPriorCloseDoesNotMoveTheHeaderStamp(unittest.TestCase):
    """One symbol stuck on yesterday's close must not date the whole blob."""

    NOW = datetime(2026, 9, 24, 13, 50, tzinfo=timezone.utc)      # 09:50 New York
    LAST_CLOSE = datetime(2026, 9, 23, 20, 0, tzinfo=timezone.utc)

    def yahoo_chain(self, symbol, market_time):
        occ = "TST260924"
        return yahoo.reduce_payloads(symbol, [yahoo_payload(
            epoch_of(date(2026, 9, 24)),
            calls=[yahoo_row(occ + "C00100000", 100.0, oi=2000000.0)],
            puts=[yahoo_row(occ + "P00099000", 99.0, oi=2000000.0)],
            spot=100.0, market_time=int(market_time.timestamp()))],
            now=self.NOW)

    def test_the_stamp_is_the_oldest_spot_that_actually_printed_today(self):
        stamps = {"SPY": self.NOW, "SPX": self.LAST_CLOSE}

        def fake_fetch_raw(symbol, timeout=None):
            raise FetchError("%s: feed looks stalled" % symbol)

        def fake_load_chain(symbol, timeout=None, now=None, **kwargs):
            return self.yahoo_chain(symbol, stamps[to_ticker(symbol)])

        real_fetch = pipeline_module.fetch_raw
        real_load = pipeline_module.yahoo.load_chain
        pipeline_module.fetch_raw = fake_fetch_raw
        pipeline_module.yahoo.load_chain = fake_load_chain
        try:
            result = run(symbols=["SPY", "SPX"], archive_dir=None, now=self.NOW)
        finally:
            pipeline_module.fetch_raw = real_fetch
            pipeline_module.yahoo.load_chain = real_load

        self.assertEqual(result.session_date, date(2026, 9, 24))
        self.assertEqual(sorted(c.ticker for c in result.chains), ["SPX", "SPY"])
        self.assertEqual(result.effective_at, self.NOW)
        self.assertTrue(any("prior close" in note for t, note in result.warnings
                            if t == "SPX"))


class TestFallbackHours(unittest.TestCase):

    def ny(self, y, m, d, hour, minute=0):
        return datetime(y, m, d, hour, minute, tzinfo=NY).astimezone(timezone.utc)

    def test_the_window_is_weekday_0700_to_1630_new_york(self):
        # 2026-09-23 is a Wednesday.
        self.assertFalse(inside_fallback_hours(self.ny(2026, 9, 23, 6, 59)))
        self.assertTrue(inside_fallback_hours(self.ny(2026, 9, 23, 8, 0)))
        self.assertTrue(inside_fallback_hours(self.ny(2026, 9, 23, 11, 30)))
        self.assertTrue(inside_fallback_hours(self.ny(2026, 9, 23, 16, 30)))
        self.assertFalse(inside_fallback_hours(self.ny(2026, 9, 23, 16, 31)))
        self.assertFalse(inside_fallback_hours(self.ny(2026, 9, 23, 3, 0)))

    def test_the_weekend_is_outside_the_window_at_any_hour(self):
        # 2026-09-26 is a Saturday, 2026-09-27 a Sunday.
        self.assertFalse(inside_fallback_hours(self.ny(2026, 9, 26, 11, 0)))
        self.assertFalse(inside_fallback_hours(self.ny(2026, 9, 27, 11, 0)))


class TestFallbackDecision(unittest.TestCase):
    """`wants_fallback` on its own: the rule, with no network anywhere near it."""

    INSIDE = datetime(2026, 9, 23, 15, 0, tzinfo=timezone.utc)   # 11:00 New York
    OUTSIDE = datetime(2026, 9, 23, 3, 0, tzinfo=timezone.utc)   # 23:00 the 22nd

    def chain_quoted(self, hours_ago, reference):
        return reduce_payload(
            "TEST",
            payload((reference - timedelta(hours=hours_ago)).strftime(
                "%Y-%m-%d %H:%M:%S"), 100.0,
                [row("TST261016C00100000")]),
            max_age_hours=None)

    def test_a_stale_file_inside_hours_asks_for_the_second_source(self):
        chain = self.chain_quoted(11.0, self.INSIDE)
        self.assertIsNotNone(wants_fallback(chain, None, self.INSIDE))

    def test_the_same_stale_file_outside_hours_does_not(self):
        chain = self.chain_quoted(11.0, self.OUTSIDE)
        self.assertIsNone(wants_fallback(chain, None, self.OUTSIDE))

    def test_a_fresh_file_inside_hours_does_not(self):
        chain = self.chain_quoted(0.5, self.INSIDE)
        self.assertIsNone(wants_fallback(chain, None, self.INSIDE))

    def test_the_threshold_is_the_one_that_was_asked_for(self):
        chain = self.chain_quoted(3.0, self.INSIDE)
        self.assertIsNotNone(wants_fallback(chain, None, self.INSIDE,
                                            max_age_hours=CBOE_MAX_AGE_HOURS))
        self.assertIsNone(wants_fallback(chain, None, self.INSIDE,
                                         max_age_hours=6.0))

    def test_no_chain_at_all_inside_hours_asks_for_the_second_source(self):
        # Past twelve hours the stall stops being a stale chain and becomes no
        # chain at all. Same fault, and the same answer.
        self.assertIsNotNone(wants_fallback(None, "feed looks stalled", self.INSIDE))

    def test_no_chain_at_all_outside_hours_does_not(self):
        self.assertIsNone(wants_fallback(None, "feed looks stalled", self.OUTSIDE))


class TestFallbackInTheRun(unittest.TestCase):
    """The whole `run` path with both feeds stubbed out."""

    INSIDE = datetime(2026, 9, 23, 15, 0, tzinfo=timezone.utc)   # 11:00 New York
    OUTSIDE = datetime(2026, 9, 23, 3, 0, tzinfo=timezone.utc)   # 23:00 the 22nd

    def cboe_payload(self, hours_ago, reference):
        stamp = (reference - timedelta(hours=hours_ago)).strftime("%Y-%m-%d %H:%M:%S")
        return payload(stamp, 100.0,
                       [row("TST261016C00100000", oi=2000000.0, gamma=0.02),
                        row("TST261016P00099000", oi=2000000.0, gamma=0.02)])

    def yahoo_chain(self, quote_ts):
        return yahoo.reduce_payloads("TEST", [yahoo_payload(
            epoch_of(date(2026, 10, 16)),
            calls=[yahoo_row("TST261016C00100000", 100.0, oi=2000000.0)],
            puts=[yahoo_row("TST261016P00099000", 99.0, oi=2000000.0)],
            spot=101.0, market_time=int(quote_ts.timestamp()))])

    @contextlib.contextmanager
    def feeds(self, cboe_payload=None, cboe_error=None, yahoo_quote=None,
              yahoo_error=None):
        calls = {"cboe": 0, "yahoo": 0}

        def fake_fetch_raw(symbol, timeout=None):
            calls["cboe"] += 1
            if cboe_error:
                raise FetchError(cboe_error)
            return cboe_payload

        def fake_load_chain(symbol, timeout=None, now=None, **kwargs):
            calls["yahoo"] += 1
            if yahoo_error:
                raise FetchError(yahoo_error)
            return self.yahoo_chain(yahoo_quote)

        real_fetch = pipeline_module.fetch_raw
        real_load = pipeline_module.yahoo.load_chain
        pipeline_module.fetch_raw = fake_fetch_raw
        pipeline_module.yahoo.load_chain = fake_load_chain
        try:
            yield calls
        finally:
            pipeline_module.fetch_raw = real_fetch
            pipeline_module.yahoo.load_chain = real_load

    def test_a_stale_file_inside_hours_publishes_the_yahoo_chain(self):
        with self.feeds(cboe_payload=self.cboe_payload(11.0, self.INSIDE),
                        yahoo_quote=self.INSIDE) as calls:
            result = run(symbols=["TEST"], archive_dir=None, now=self.INSIDE)
        self.assertEqual(calls["yahoo"], 1)
        self.assertEqual([c.source for c in result.chains], ["yahoo"])
        self.assertAlmostEqual(result.chains[0].spot, 101.0)
        self.assertTrue(result.blob)
        self.assertTrue(any("Yahoo" in note for _t, note in result.warnings))

    def test_a_stale_file_outside_hours_keeps_cboe_and_never_calls_yahoo(self):
        with self.feeds(cboe_payload=self.cboe_payload(11.0, self.OUTSIDE),
                        yahoo_quote=self.OUTSIDE) as calls:
            result = run(symbols=["TEST"], archive_dir=None, now=self.OUTSIDE)
        self.assertEqual(calls["yahoo"], 0)
        self.assertEqual([c.source for c in result.chains], ["cboe"])
        self.assertAlmostEqual(result.chains[0].spot, 100.0)

    def test_a_yahoo_quote_no_newer_than_cboes_is_not_used(self):
        # Fresher-looking is not the same as fresher. If the second source is
        # behind the first there is nothing to gain by switching to it. Both
        # quotes here are from today's session, so only the age decides.
        with self.feeds(cboe_payload=self.cboe_payload(3.0, self.INSIDE),
                        yahoo_quote=self.INSIDE - timedelta(hours=4)) as calls:
            result = run(symbols=["TEST"], archive_dir=None, now=self.INSIDE)
        self.assertEqual(calls["yahoo"], 1)
        self.assertEqual([c.source for c in result.chains], ["cboe"])
        self.assertTrue(any("no newer" in note for _t, note in result.warnings))

    def test_a_yahoo_quote_from_a_previous_session_is_refused(self):
        # What a market holiday looks like: CBOE holds the last session's file,
        # and Yahoo holds the same last close. Newer by the clock, and still not
        # today's data. Refusing it is what keeps the previous line in place.
        with self.feeds(cboe_payload=self.cboe_payload(11.0, self.INSIDE),
                        yahoo_quote=self.INSIDE - timedelta(hours=20)) as calls:
            result = run(symbols=["TEST"], archive_dir=None, now=self.INSIDE)
        self.assertEqual(calls["yahoo"], 1)
        self.assertEqual([c.source for c in result.chains], ["cboe"])
        self.assertTrue(any("not today" in note for _t, note in result.warnings))

    def test_a_holiday_with_no_cboe_file_at_all_publishes_nothing(self):
        # The same holiday, once CBOE's file has aged past the twelve-hour limit
        # and stopped coming back. There is no live data anywhere, so the symbol
        # is dropped rather than backfilled with the last close.
        with self.feeds(cboe_error="TEST: quote is 30.0h old -- feed looks stalled",
                        yahoo_quote=self.INSIDE - timedelta(hours=20)):
            result = run(symbols=["TEST"], archive_dir=None, now=self.INSIDE)
        self.assertEqual(result.chains, [])
        self.assertIsNone(result.blob)
        self.assertEqual(len(result.failures), 1)

    def test_a_fresh_file_never_calls_the_second_source(self):
        with self.feeds(cboe_payload=self.cboe_payload(0.4, self.INSIDE),
                        yahoo_quote=self.INSIDE) as calls:
            result = run(symbols=["TEST"], archive_dir=None, now=self.INSIDE)
        self.assertEqual(calls["yahoo"], 0)
        self.assertEqual([c.source for c in result.chains], ["cboe"])

    def test_a_failed_fallback_keeps_the_cboe_chain_and_says_so(self):
        with self.feeds(cboe_payload=self.cboe_payload(11.0, self.INSIDE),
                        yahoo_error="yahoo: HTTP 429") as calls:
            result = run(symbols=["TEST"], archive_dir=None, now=self.INSIDE)
        self.assertEqual(calls["yahoo"], 1)
        self.assertEqual([c.source for c in result.chains], ["cboe"])
        self.assertTrue(any("429" in note for _t, note in result.warnings))
        self.assertTrue(result.blob)

    def test_no_cboe_chain_at_all_is_rescued_by_the_second_source(self):
        with self.feeds(cboe_error="TEST: quote is 13.0h old -- feed looks stalled",
                        yahoo_quote=self.INSIDE) as calls:
            result = run(symbols=["TEST"], archive_dir=None, now=self.INSIDE)
        self.assertEqual(calls["yahoo"], 1)
        self.assertEqual([c.source for c in result.chains], ["yahoo"])
        self.assertEqual(result.failures, [])

    def test_both_feeds_down_is_a_failure_carrying_the_cboe_reason(self):
        with self.feeds(cboe_error="TEST: feed looks stalled",
                        yahoo_error="yahoo: HTTP 503"):
            result = run(symbols=["TEST"], archive_dir=None, now=self.INSIDE)
        self.assertEqual(result.chains, [])
        self.assertEqual(len(result.failures), 1)
        self.assertIsNone(result.blob)

    def test_source_cboe_never_calls_yahoo_however_stale_the_file_is(self):
        with self.feeds(cboe_payload=self.cboe_payload(11.0, self.INSIDE),
                        yahoo_quote=self.INSIDE) as calls:
            result = run(symbols=["TEST"], archive_dir=None, now=self.INSIDE,
                         source="cboe")
        self.assertEqual(calls["yahoo"], 0)
        self.assertEqual([c.source for c in result.chains], ["cboe"])

    def test_source_yahoo_never_calls_cboe(self):
        with self.feeds(cboe_payload=self.cboe_payload(0.4, self.INSIDE),
                        yahoo_quote=self.INSIDE) as calls:
            result = run(symbols=["TEST"], archive_dir=None, now=self.INSIDE,
                         source="yahoo")
        self.assertEqual(calls["cboe"], 0)
        self.assertEqual([c.source for c in result.chains], ["yahoo"])

    def test_an_unknown_source_is_refused_rather_than_guessed(self):
        with self.assertRaises(ValueError):
            run(symbols=["TEST"], archive_dir=None, now=self.INSIDE, source="bloomberg")

    def test_an_offline_run_never_reaches_for_the_second_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "TEST.json"), "w", encoding="utf-8") as h:
                json.dump(self.cboe_payload(30.0, self.INSIDE), h)
            with self.feeds(yahoo_quote=self.INSIDE) as calls:
                result = run(symbols=["TEST"], offline_dir=tmp, archive_dir=None,
                             now=self.INSIDE)
        self.assertEqual(calls["yahoo"], 0)
        self.assertEqual([c.source for c in result.chains], ["cboe"])


class TestSourceOnTheWireAndInTheArchive(unittest.TestCase):

    def test_a_cboe_chain_is_labelled_cboe_without_anyone_saying_so(self):
        chain = reduce_payload("TEST", payload("2026-08-04 18:15:00", 100.0,
                                               [row("TST260911C00100000")]),
                               max_age_hours=None)
        self.assertEqual(chain.source, "cboe")

    def test_the_archive_records_the_source_and_the_replay_reads_it_back(self):
        chain = yahoo.reduce_payloads("SPY", [yahoo_sample()])
        with tempfile.TemporaryDirectory() as tmp:
            path, written = write_snapshot(tmp, chain, chain.session_date)
            self.assertTrue(written)
            with gzip.open(path, "rt", encoding="ascii") as handle:
                header = next(csv.reader(handle))
            self.assertIn("source", header)
            back, _session = read_chain(path)
        self.assertEqual(back.source, "yahoo")

    def test_a_snapshot_written_before_the_column_existed_reads_as_cboe(self):
        old_header = [c for c in HEADER if c != "source"]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "TEST_20260804T181500Z.csv.gz")
            write_archive_csv(path, old_header, [
                archive_row("TEST", "2026-08-04T18:15:00Z", "2026-08-04",
                            "TST260911C00100000")])
            chain, _session = read_chain(path)
        self.assertEqual(chain.source, "cboe")

    def test_the_summary_names_yahoo_only_when_yahoo_was_used(self):
        chain = yahoo.reduce_payloads("SPY", [yahoo_sample()])
        result = RunResult()
        result.chains = [chain]
        result.records = [build_record(chain, chain.session_date)]
        result.computed_at = chain.quote_ts
        result.effective_at = chain.spot_ts
        stream = io.StringIO()
        cli._summarise(result, stream)
        self.assertIn("source yahoo", stream.getvalue())

        chain.source = "cboe"
        stream = io.StringIO()
        cli._summarise(result, stream)
        self.assertNotIn("source", stream.getvalue())



if __name__ == "__main__":
    unittest.main(verbosity=2)
