"""
betfair/strategy.py

Bet qualification and stake calculation — v3 evidence-based rules.

Changes from previous version (validated on clean backtest, 520 races):
  - FLAT STAKES: £2 on everything. Variable staking amplified losses —
    biggest stakes went on longest-priced losers, smallest on short-priced
    winners. Flat £2 gives +£218 over 520 races vs variable staking which
    adds noise and variance without improving expectancy.

  - PLACE BETS — JUMPS ONLY: Place bets on flat races lose -£0.13/bet
    across 373 races. Place bets on hurdles return +£0.46/bet and chases
    +£0.25/bet. Jump place bets retained, flat place bets removed entirely.

  - P2 WIN BETS REMOVED: P2 win bets add noise. Win bet is P1 only.
    P2 place bet retained for jump races where P2 also has place value.

Betting rules:
  ELITE/STRONG/GOOD: WIN P1 (flat £2)
  ELITE/STRONG/GOOD + jump race + 8+ runners: PLACE P1 + PLACE P2 (£2 each)
  STANDARD/WEAK/SKIP: No bet

Minimum price: 1.10 (back almost anything)
No variable staking. No score-gap logic. No redirects.
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

# ── Stake ──────────────────────────────────────────────────────────────────────

FLAT_STAKE = 2.0

# Tier-specific stake thresholds kept for display/briefing compatibility
# All return flat £2 regardless of profit pot
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


def get_stake(profit: float, tier: int) -> float:
    """Flat £2 regardless of profit. Kept for display compatibility."""
    return FLAT_STAKE


def win_stake_for_pick(sp: float, score: float) -> float:
    """
    Flat £2 win stake for P1.
    Previous variable staking (£2/£4/£6 by SP band) amplified losses
    on longer-priced losers without improving expectancy.
    Only gate is minimum price.
    """
    if not sp or sp < MIN_PICK1_PRICE:
        return 0.0
    return FLAT_STAKE


def place_stake_for_pick(score: float, tier: int, sp: float = 0.0,
                          is_jump: bool = False, n_runners: int = 0) -> float:
    """
    Place bet on P1 for jump races only (hurdles +£0.46/bet, chases +£0.25/bet).
    Flat place bets lose -£0.13/bet across 373 races — excluded.
    Minimum 8 runners for place market to pay 3 places.
    """
    if not is_jump:
        return 0.0
    if tier not in PLACE_BET_TIERS:
        return 0.0
    if n_runners < MIN_RUNNERS_FOR_PLACE:
        return 0.0
    return FLAT_STAKE


def p2_place_stake(sp: float, is_jump: bool = False,
                   n_runners: int = 0) -> float:
    """
    P2 place bet for jump races only, 8+ runners.
    P2 win bets removed — added noise without improving expectancy.
    """
    if not is_jump:
        return 0.0
    if n_runners < MIN_RUNNERS_FOR_PLACE:
        return 0.0
    if not sp or sp < MIN_PICK2_PRICE:
        return 0.0
    return FLAT_STAKE


def p2_win_stake_for_pick(sp: float, score: float) -> float:
    """P2 win bets removed. Returns 0. Kept for API compatibility."""
    return 0.0


def get_place_stake(profit: float, tier: int = TIER_STD) -> float:
    """Legacy function — retained for display/briefing compatibility."""
    return FLAT_STAKE


def next_tier_threshold(profit: float, tier: int) -> float:
    """Returns 0 — no stake escalation thresholds in flat staking."""
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
    """
    Return True if the race passes all pre-bet filters.
    """
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
    """Place bets gated by jump race check in place_stake_for_pick."""
    return tier in PLACE_BET_TIERS and n_runners >= MIN_RUNNERS_FOR_PLACE


def pick_stakes(
    profit:       float,
    tier:         int,
    pick1_price,
    pick2_price,
    n_runners:    int = 0,
    is_jump:      bool = False,
) -> tuple:
    """
    Return (stake_p1_win, stake_p2_win, stake_place).

    v3 rules:
    - P1 win: flat £2 if price >= 1.10
    - P2 win: always 0 (removed)
    - Place: flat £2 on P1 for jump races with 8+ runners only
    """
    if tier not in BET_TIERS:
        return 0.0, 0.0, 0.0

    s1 = FLAT_STAKE if should_back_pick1(pick1_price) else 0.0
    s2 = 0.0  # P2 win bets removed
    sp = FLAT_STAKE if (is_jump and should_place_bet(tier, n_runners)) else 0.0

    return s1, s2, sp


def apply_liquidity(
    stake_a:  float,
    stake_b:  float,
    liq_a:    float,
    liq_b:    float,
    redirect: bool = False,
) -> tuple:
    """BSP bets — liquidity checks bypassed. Kept for compatibility."""
    if stake_a == 0 and stake_b == 0:
        return 0.0, 0.0, True, "zero stakes"
    if redirect:
        return 0.0, stake_b, False, ""
    return stake_a, stake_b, False, ""
