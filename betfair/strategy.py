"""
betfair/strategy.py

Bet qualification and stake calculation — v3.1 evidence-based rules.

Changes from v3 (validated on clean backtest, 348 races):

  STAKING:
  - Chase: £4 (128.6% ROI, 35 races, Kelly 5.2% bankroll)
  - Hurdle: £2 (7.6% ROI, marginal but positive)
  - Flat: £2 (negative ROI on win bets alone; P1 only)
  - No variable staking by SP, tier, or market rank — insufficient data
    to justify reducing stakes on short-priced horses or increasing on
    long-priced ones. Sample sizes too small to trade away simplicity.

  BET STRUCTURE (Strategy H — +£148 vs +£92 baseline over 348 races):
  - Flat:    P1 win only (£2)
  - Chase:   P1 win (£4) + P1 place (£2, 8+ runners)
  - NH Flat: P1 win (£2) + P1 place (£2, 8+ runners)
  - Hurdle:  P1 win (£2) + P2 win (£2) + P1 place (£2) + P2 place (£2)
             (place bets 8+ runners only)

  WHY HURDLE P2 WIN:
  - Hurdle P2 wins at 27% vs P1 at 25% — P2 actually outperforms P1
  - P2 win returns +£0.796/bet on hurdles vs -£0.930/bet on chase
  - Chase P2 win actively harmful — excluded entirely

  WHY CHASE £4:
  - Chase P1 win: +£3.253/bet, 34% win rate, avg SP 7.46
  - Chase ELITE/STRONG: +£4.973/bet, 248.7% ROI
  - Kelly quarter-fraction = £1.04/£20 bankroll ~ £4 practical stake

  WHY NOT MORE COMPLEX:
  - SP-band staking, tier-based staking, market-rank staking all tested
  - Chase type is the single cleanest signal to stake on
  - Adding more conditions risks overfitting on 348 races
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from predict_v2 import (
    TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_WEAK, TIER_SKIP,
)

# ── Price gates ────────────────────────────────────────────────────────────────

MIN_PICK1_PRICE = 1.10
MIN_PICK2_PRICE = 1.10
MIN_BACK_PRICE  = 1.05
MIN_LIQUIDITY   = 2.00

# ── Going / surface filters ────────────────────────────────────────────────────

SKIP_GOING_KEYS = {"heavy", "soft to heavy", "heavy to soft"}

ATTRITION_VENUES  = {"fairyhouse", "cork", "punchestown", "naas", "leopardstown"}
ATTRITION_GOING   = {"soft", "yielding to soft", "soft to heavy", "heavy"}
ATTRITION_DIST_F  = 20.0

# ── Stakes ─────────────────────────────────────────────────────────────────────

FLAT_STAKE        = 2.0
CHASE_WIN_STAKE   = 4.0   # Chase P1 win — 128.6% ROI over 35 races
PLACE_STAKE       = 2.0   # All place bets flat £2

TIER_STAKE_THRESHOLDS = {
    TIER_ELITE:  [(0, 2.0)],
    TIER_STRONG: [(0, 2.0)],
    TIER_GOOD:   [(0, 2.0)],
    TIER_STD:    [(0, 2.0)],
}

BET_TIERS       = {TIER_ELITE, TIER_STRONG, TIER_GOOD}
PLACE_BET_TIERS = {TIER_ELITE, TIER_STRONG, TIER_GOOD}

MIN_RUNNERS_FOR_PLACE = 8


def _is_jump_race(race: dict) -> bool:
    rtype = (race.get("type") or "").lower()
    return any(t in rtype for t in ("chase", "hurdle", "nh flat", "national hunt"))


def _is_hurdle(race: dict) -> bool:
    rtype = (race.get("type") or "").lower()
    return "hurdle" in rtype


def _is_chase(race: dict) -> bool:
    rtype = (race.get("type") or "").lower()
    return "chase" in rtype


def _is_nhflat(race: dict) -> bool:
    rtype = (race.get("type") or "").lower()
    return "nh flat" in rtype or "national hunt flat" in rtype


def get_stake(profit: float, tier: int) -> float:
    return FLAT_STAKE


def win_stake_for_pick(sp: float, score: float, is_chase: bool = False) -> float:
    """
    P1 win stake.
    Chase: £4 (Kelly-justified, 128.6% ROI over 35 races)
    All others: £2 flat
    """
    if not sp or sp < MIN_PICK1_PRICE:
        return 0.0
    return CHASE_WIN_STAKE if is_chase else FLAT_STAKE


def place_stake_for_pick(score: float, tier: int, sp: float = 0.0,
                          is_jump: bool = False, n_runners: int = 0,
                          is_chase: bool = False) -> float:
    """
    P1 place stake.
    Flat: none (-£0.127/bet)
    Chase: £2 (+£0.377/bet)
    NH Flat: £2 (positive, small sample)
    Hurdle: £2 (+£0.162/bet on P2; P1 place included in Strategy H)
    All require 8+ runners.
    """
    if not is_jump:
        return 0.0
    if tier not in PLACE_BET_TIERS:
        return 0.0
    if n_runners < MIN_RUNNERS_FOR_PLACE:
        return 0.0
    return PLACE_STAKE


def p2_win_stake_for_pick(sp: float, score: float,
                           is_hurdle: bool = False) -> float:
    """
    P2 win stake — hurdle only.
    Hurdle P2 win: +£0.796/bet, 27% win rate
    Chase P2 win: -£0.930/bet — excluded
    Flat P2 win: -£0.504/bet — excluded
    """
    if not is_hurdle:
        return 0.0
    if not sp or sp < MIN_PICK2_PRICE:
        return 0.0
    return FLAT_STAKE


def p2_place_stake(sp: float, is_jump: bool = False,
                   n_runners: int = 0, is_hurdle: bool = False) -> float:
    """
    P2 place stake — hurdle only, 8+ runners.
    Hurdle P2 place: +£0.120/bet
    Chase P2 place: -£0.118/bet — excluded
    NH Flat P2 place: -£0.225/bet — excluded
    """
    if not is_hurdle:
        return 0.0
    if n_runners < MIN_RUNNERS_FOR_PLACE:
        return 0.0
    if not sp or sp < MIN_PICK2_PRICE:
        return 0.0
    return PLACE_STAKE


def get_place_stake(profit: float, tier: int = TIER_STD) -> float:
    return PLACE_STAKE


def next_tier_threshold(profit: float, tier: int) -> float:
    return 0.0


def min_liquidity_for_price(price: float, stake: float) -> float:
    multiplier = min(price / 5.0, 4.0)
    return max(MIN_LIQUIDITY, round(stake * multiplier, 2))


# ── Race qualification ─────────────────────────────────────────────────────────

def _is_attrition_risk(race: dict) -> bool:
    course    = (race.get("course") or "").lower()
    going     = (race.get("going")  or "").lower()
    race_type = (race.get("type")   or "").lower()
    dist_f    = race.get("dist_f")  or 0.0

    try:
        dist_f = float(str(dist_f).replace("f", "").strip())
    except (ValueError, TypeError):
        dist_f = 0.0

    is_irish   = any(v in course for v in ATTRITION_VENUES)
    is_nh      = any(t in race_type for t in ("hurdle", "chase", "nh flat"))
    is_soft    = any(g in going for g in ATTRITION_GOING)
    is_staying = dist_f >= ATTRITION_DIST_F

    return is_irish and is_nh and is_soft and is_staying


def qualifies(race: dict) -> bool:
    going = (race.get("going") or "").lower()
    tier  = race.get("tier")

    if any(k in going for k in SKIP_GOING_KEYS):
        return False
    if _is_attrition_risk(race):
        return False
    if tier not in BET_TIERS:
        return False
    if not race.get("top1"):
        return False
    return True


def should_back_pick1(pick1_price) -> bool:
    if not pick1_price:
        return False
    return pick1_price >= MIN_PICK1_PRICE


def should_back_pick2(pick2_price) -> bool:
    if not pick2_price:
        return False
    return pick2_price >= MIN_PICK2_PRICE


def should_place_bet(tier: int, n_runners: int) -> bool:
    return tier in PLACE_BET_TIERS and n_runners >= MIN_RUNNERS_FOR_PLACE


def pick_stakes(
    profit:       float,
    tier:         int,
    pick1_price,
    pick2_price,
    n_runners:    int = 0,
    is_jump:      bool = False,
    is_hurdle:    bool = False,
    is_chase:     bool = False,
) -> tuple:
    """
    Return (stake_p1_win, stake_p2_win, stake_p1_place, stake_p2_place).

    Strategy H:
    - Flat:    P1 win £2
    - Chase:   P1 win £4 + P1 place £2 (8+ runners)
    - NH Flat: P1 win £2 + P1 place £2 (8+ runners)
    - Hurdle:  P1 win £2 + P2 win £2 + P1 place £2 + P2 place £2 (8+ runners)
    """
    if tier not in BET_TIERS:
        return 0.0, 0.0, 0.0, 0.0

    has_place = is_jump and n_runners >= MIN_RUNNERS_FOR_PLACE

    s1_win = win_stake_for_pick(pick1_price, 0.0, is_chase=is_chase) \
             if should_back_pick1(pick1_price) else 0.0

    s2_win = p2_win_stake_for_pick(pick2_price, 0.0, is_hurdle=is_hurdle) \
             if should_back_pick2(pick2_price) else 0.0

    s1_plc = PLACE_STAKE if has_place else 0.0

    s2_plc = p2_place_stake(pick2_price, is_jump=is_jump,
                             n_runners=n_runners, is_hurdle=is_hurdle) \
             if should_back_pick2(pick2_price) else 0.0

    return s1_win, s2_win, s1_plc, s2_plc


def apply_liquidity(
    stake_a:  float,
    stake_b:  float,
    liq_a:    float,
    liq_b:    float,
    redirect: bool = False,
) -> tuple:
    if stake_a == 0 and stake_b == 0:
        return 0.0, 0.0, True, "zero stakes"
    if redirect:
        return 0.0, stake_b, False, ""
    return stake_a, stake_b, False, ""
