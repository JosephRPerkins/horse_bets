"""
betfair/strategy.py

Bet qualification and stake calculation — System C.

Tier-specific independent stake cascades:
  ELITE:  £2 → £4 at £30 profit,  £4 → £6 at £60 profit
  STRONG: £2 → £4 at £50 profit,  £4 → £6 at £100 profit
  GOOD:   £2 → £4 at £75 profit,  £4 → £6 at £150 profit

Each tier tracks its own profit independently. A STRONG winning streak
does not inflate GOOD stakes, and vice versa.

Betting rules:
  ELITE + STRONG: WIN P1, WIN P2, PLACE P1, PLACE P2 (8+ runners only)
  GOOD:           WIN P1, WIN P2 only (no place bets)
  STANDARD/WEAK:  No bet
  SKIP:           No bet

No odds-on skips. No score-gap redirects. No P2 price redirects.
P1 and P2 are always backed at the same stake when both qualify on price.
Place bets use the same stake as win bets for that tier.

Minimum prices:
  P1: 1.20 (back anything with meaningful odds)
  P2: 2.00 (avoid backing near-certainties as value picks)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from predict_v2 import (
    TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_WEAK, TIER_SKIP,
)

# ── Price gates ────────────────────────────────────────────────────────────────

MIN_PICK1_PRICE = 1.20   # back P1 at almost any odds
MIN_PICK2_PRICE = 2.00   # P2 must offer some value
MIN_BACK_PRICE  = 1.50   # exchange minimum meaningful odds
MIN_LIQUIDITY   = 2.00   # minimum matched volume before placing

# ── Going / surface filters ────────────────────────────────────────────────────

SKIP_GOING_KEYS = {"heavy", "soft to heavy", "heavy to soft"}

# Irish NH races in attrition conditions — historically unpredictable
ATTRITION_VENUES  = {"fairyhouse", "cork", "punchestown", "naas", "leopardstown"}
ATTRITION_GOING   = {"soft", "yielding to soft", "soft to heavy", "heavy"}
ATTRITION_DIST_F  = 20.0

# ── Tier-specific stake cascades ───────────────────────────────────────────────
# Format: [(min_profit, stake), ...]
# Profit tracked independently per tier.

TIER_STAKE_THRESHOLDS = {
    TIER_ELITE:  [(0, 2.0), (30,  4.0), (60,  6.0)],
    TIER_STRONG: [(0, 2.0), (50,  4.0), (100, 6.0)],
    TIER_GOOD:   [(0, 2.0), (75,  4.0), (150, 6.0)],
}

# Bet tiers — races in these tiers qualify for betting
BET_TIERS = {TIER_ELITE, TIER_STRONG, TIER_GOOD}

# Place bet tiers — only ELITE and STRONG get place bets
PLACE_BET_TIERS = {TIER_ELITE, TIER_STRONG}

# Minimum runners for place bets (Betfair pays 3 places at 8+)
MIN_RUNNERS_FOR_PLACE = 8


def get_stake(profit: float, tier: int) -> float:
    """
    Return per-horse win stake based on this tier's cumulative profit.
    Each tier has its own independent profit pot.
    """
    thresholds = TIER_STAKE_THRESHOLDS.get(tier, [(0, 2.0)])
    stake = thresholds[0][1]
    for min_profit, s in thresholds:
        if profit >= min_profit:
            stake = s
    return stake


def get_place_stake(profit: float, tier: int = TIER_STD) -> float:
    """
    Place stake equals win stake for the tier.
    Returns 0 if tier doesn't qualify for place bets.
    """
    if tier not in PLACE_BET_TIERS:
        return 0.0
    return get_stake(profit, tier)


def next_tier_threshold(profit: float, tier: int) -> float:
    """
    Return the next profit milestone for this tier's stake to increase.
    Returns current threshold if already at maximum.
    """
    thresholds = TIER_STAKE_THRESHOLDS.get(tier, [(0, 2.0)])
    for min_profit, _ in thresholds:
        if profit < min_profit:
            return float(min_profit)
    return float(thresholds[-1][0])


def min_liquidity_for_price(price: float, stake: float) -> float:
    """
    Minimum matched volume required before placing.
    Scales with price — higher-priced horses need more liquidity.
    """
    multiplier = min(price / 5.0, 4.0)
    return max(MIN_LIQUIDITY, round(stake * multiplier, 2))


# ── Race qualification ─────────────────────────────────────────────────────────

def _is_attrition_risk(race: dict) -> bool:
    """Irish NH races in soft/heavy going at staying distances."""
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

    Filters applied:
    - Going: skip heavy/soft-to-heavy
    - Attrition: skip Irish NH staying races in soft ground
    - Tier: must be ELITE, STRONG, or GOOD
    - Picks: must have both top1 and top2 populated
    - Field: must have runners list (for RPR check)
    """
    going = (race.get("going") or "").lower()
    tier  = race.get("tier")

    if any(k in going for k in SKIP_GOING_KEYS):
        return False

    if _is_attrition_risk(race):
        return False

    if tier not in BET_TIERS:
        return False

    if not race.get("top1") or not race.get("top2"):
        return False

    return True


def should_back_pick1(pick1_price) -> bool:
    """P1 qualifies if it meets the minimum price."""
    if not pick1_price:
        return False
    return pick1_price >= MIN_PICK1_PRICE


def should_back_pick2(pick2_price) -> bool:
    """P2 qualifies if it meets the minimum price."""
    if not pick2_price:
        return False
    return pick2_price >= MIN_PICK2_PRICE


def should_place_bet(tier: int, n_runners: int) -> bool:
    """
    Place bets only on ELITE and STRONG, with 8+ runners.
    With fewer than 8 runners Betfair pays only 2 places (5-7)
    or win only (<=4), reducing value significantly.
    """
    return tier in PLACE_BET_TIERS and n_runners >= MIN_RUNNERS_FOR_PLACE


def pick_stakes(
    profit:       float,
    tier:         int,
    pick1_price,
    pick2_price,
    n_runners:    int = 0,
) -> tuple:
    """
    Return (stake_p1_win, stake_p2_win, stake_place).

    profit:      this tier's cumulative net profit
    tier:        System C tier (ELITE/STRONG/GOOD)
    pick1_price: SP or exchange price for P1
    pick2_price: SP or exchange price for P2
    n_runners:   actual runners in race (for place bet gate)

    Rules:
    - Both P1 and P2 backed at same stake if they meet price gates
    - Place bets only on ELITE+STRONG, 8+ runners
    - No redirects, no odds-on skips, no gap-based switching
    - Returns (0, 0, 0) if tier doesn't qualify
    """
    if tier not in BET_TIERS:
        return 0.0, 0.0, 0.0

    stake = get_stake(profit, tier)

    p1_ok = should_back_pick1(pick1_price)
    p2_ok = should_back_pick2(pick2_price)

    s1 = stake if p1_ok else 0.0
    s2 = stake if p2_ok else 0.0

    # Place bets: same stake, ELITE+STRONG, 8+ runners
    sp = stake if should_place_bet(tier, n_runners) else 0.0

    return s1, s2, sp

def apply_liquidity(
    stake_a:  float,
    stake_b:  float,
    liq_a:    float,
    liq_b:    float,
    redirect: bool = False,) -> tuple:
    """
    Liquidity stub — all bets placed as BSP so liquidity checks are bypassed.
    Always returns full stakes as placeable. Kept for compatibility with
    betfair_main.py until a full refactor removes liquidity logic entirely.
    Returns (actual_a, actual_b, skipped=False, reason="").
    """
    if stake_a == 0 and stake_b == 0:
        return 0.0, 0.0, True, "zero stakes"
    if redirect:
        return 0.0, stake_b, False, ""
    return stake_a, stake_b, False, ""
