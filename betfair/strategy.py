"""
betfair/strategy.py

Bet qualification and stake calculation — v3.2 evidence-based rules.

════════════════════════════════════════════════════════════════════════
CHANGES FROM v3.1 (validated on 442 races, Apr 10 – May 22 2026)
════════════════════════════════════════════════════════════════════════

  STAKING CHANGES:
  - Chase: £4 → £6  (ROI +100.5% over 41 races, Kelly 17.8% bankroll)
    Previous £4 was already justified; £6 is still conservative vs Kelly.
    Chase P1 win: +£2.010/bet at £2 flat → scales linearly with stake.
  - All other stakes unchanged at £2 flat.

  NEW QUALIFYING RACES (expansion validated on 442-race dataset):
  - STD tier AW races (all classes): +£0.724/bet over 105 races (37% win)
    Stake: £3 (stronger signal than STD jump; AW consistency vs turf).
    AW venues: Wolverhampton, Kempton, Chelmsford, Lingfield, Newcastle,
    Dundalk, Southwell.
  - STD tier jump races — hurdle + chase only, class 3/4/5/IRE:
    +£0.363/bet over 170 races (31% win). Stake: £2.
    Excluded: NH Flat STD (-£0.893/bet), Class 1 jump STD (-£0.836/bet),
    Class 2 jump STD (insufficient sample, precautionary).
  These add ~10 bets/day. Combined system: +£566 over 520 bets (+£1.089/bet)
  vs original +£163 over 348 bets (+£0.469/bet).

  NEW EXCLUSION FILTERS:
  - Class 2 all races: -£0.746/bet over 24 races (-37.3% ROI). EXCLUDED.
  - Class 5 flat:      -£0.463/bet over 53 races (-23.2% ROI). EXCLUDED.
  - Dead zone:  flat (non-jump) races with SP 3-5/1 AND 9+ runners.
    -£0.630/bet over 58 races (-31.5% ROI). Turf flat cls4 3-5/1 is
    0% win rate over 12 races. AW flat 3-5/1 n>8 also -£0.961/bet.
    Model has no edge vs market at this price range in large flat fields.
    Removing adds +£36 and improves ROI from 23.5% → 34.5%.

  RETAINED FROM v3.1:
  - Hurdle P2 win: £2 (+£0.254/bet over 69 races full dataset)
  - Hurdle/chase P1+P2 place bets: £2 (8+ runners)
  - Not heavy going filter
  - Irish staying chase filter
  - Class 5 flat exclusion (now formalised in qualifies())

  UNCHANGED BET STRUCTURE (Strategy H):
  - Flat/AW bet-tier:  P1 win (£2)
  - STD AW:            P1 win (£3), no place bets
  - STD jump:          P1 win (£2), no place bets
  - Chase bet-tier:    P1 win (£6) + P1 place (£2, 8+ runners)
  - NH Flat bet-tier:  P1 win (£2) + P1 place (£2, 8+ runners)
  - Hurdle bet-tier:   P1 win (£2) + P2 win (£2) + P1 place (£2)
                       + P2 place (£2) (8+ runners only)
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

ATTRITION_VENUES = {"fairyhouse", "cork", "punchestown", "naas", "leopardstown"}
ATTRITION_GOING  = {"soft", "yielding to soft", "soft to heavy", "heavy"}
ATTRITION_DIST_F = 20.0

# ── All-weather venues ─────────────────────────────────────────────────────────
# Used for STD AW qualification and dead zone filter.

AW_VENUES = {
    "wolverhampton", "kempton", "chelmsford", "lingfield",
    "newcastle", "dundalk", "southwell",
}

# ── Stakes ─────────────────────────────────────────────────────────────────────

FLAT_STAKE       = 2.0
CHASE_WIN_STAKE  = 6.0   # Chase P1 win — +100.5% ROI, Kelly 17.8% bankroll
STD_AW_STAKE     = 3.0   # STD AW P1 win — +£0.724/bet over 105 races
STD_JUMP_STAKE   = 2.0   # STD jump P1 win — +£0.363/bet over 170 races
PLACE_STAKE      = 2.0   # All place bets flat £2

TIER_STAKE_THRESHOLDS = {
    TIER_ELITE:  [(0, 2.0)],
    TIER_STRONG: [(0, 2.0)],
    TIER_GOOD:   [(0, 2.0)],
    TIER_STD:    [(0, 2.0)],
}

# BET_TIERS: tiers that qualify under standard rules.
# TIER_STD is handled separately via STD AW / STD jump logic in qualifies().
BET_TIERS       = {TIER_ELITE, TIER_STRONG, TIER_GOOD}
PLACE_BET_TIERS = {TIER_ELITE, TIER_STRONG, TIER_GOOD}

MIN_RUNNERS_FOR_PLACE = 8

# ── Dead zone filter constants ─────────────────────────────────────────────────
# Flat races (non-jump) priced 3-5/1 with 9+ runners: -£0.630/bet, -31.5% ROI.
# Removing saves £36 over dataset. AW flat included — same pattern.

DEAD_ZONE_SP_LO      = 3.0
DEAD_ZONE_SP_HI      = 5.0
DEAD_ZONE_MIN_RNRS   = 9


# ── Race type helpers ──────────────────────────────────────────────────────────

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


def _is_aw_race(race: dict) -> bool:
    """True if race is at an all-weather venue."""
    course  = (race.get("course") or "").lower()
    surface = (race.get("surface") or "").lower()
    # Strip country suffixes before matching
    for suffix in (" (ire)", " (gb)", " (usa)", " (fr)"):
        course = course.replace(suffix, "")
    course = course.strip()
    return (
        any(v in course for v in AW_VENUES)
        or "aw" in surface
        or "artificial" in surface
    )


def _race_class(race: dict) -> str:
    """Return normalised class string e.g. '2', '5', '' (Irish)."""
    cls = str(race.get("race_class") or race.get("class") or "")
    return cls.replace("Class", "").strip()


# ── Stake functions ────────────────────────────────────────────────────────────

def get_stake(profit: float, tier: int) -> float:
    return FLAT_STAKE


def win_stake_for_pick(sp: float, score: float, is_chase: bool = False) -> float:
    """
    P1 win stake.
    Chase:  £6 (+100.5% ROI over 41 races, Kelly-justified at 17.8% bankroll)
    Others: £2 flat
    """
    if not sp or sp < MIN_PICK1_PRICE:
        return 0.0
    return CHASE_WIN_STAKE if is_chase else FLAT_STAKE


def place_stake_for_pick(score: float, tier: int, sp: float = 0.0,
                          is_jump: bool = False, n_runners: int = 0,
                          is_chase: bool = False) -> float:
    """
    P1 place stake — bet-tier jump races only, 8+ runners.
    Flat: none (-£0.127/bet historically)
    Chase/hurdle/NH Flat: £2
    STD tier races: no place bets regardless of race type.
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
    P2 win stake — bet-tier hurdle races only.
    Hurdle P2 win: +£0.254/bet over 69 races (full dataset)
    Chase P2 win: -£0.930/bet — excluded
    Flat P2 win:  -£0.504/bet — excluded
    STD tier: no P2 win bets
    """
    if not is_hurdle:
        return 0.0
    if not sp or sp < MIN_PICK2_PRICE:
        return 0.0
    return FLAT_STAKE


def p2_place_stake(sp: float, is_jump: bool = False,
                   n_runners: int = 0, is_hurdle: bool = False) -> float:
    """
    P2 place stake — bet-tier hurdle races only, 8+ runners.
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


def std_win_stake(race: dict) -> float:
    """
    Win stake for STD tier expansion races.
    AW:   £3 (+£0.724/bet over 105 races)
    Jump: £2 (+£0.363/bet over 170 races)
    """
    if _is_aw_race(race):
        return STD_AW_STAKE
    if _is_jump_race(race):
        return STD_JUMP_STAKE
    return 0.0


def next_tier_threshold(profit: float, tier: int) -> float:
    return 0.0


def min_liquidity_for_price(price: float, stake: float) -> float:
    multiplier = min(price / 5.0, 4.0)
    return max(MIN_LIQUIDITY, round(stake * multiplier, 2))


# ── Race qualification ─────────────────────────────────────────────────────────

def _is_attrition_risk(race: dict) -> bool:
    """Irish staying NH races in soft/heavy going — unpredictable form."""
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


def _is_class_excluded(race: dict) -> bool:
    """
    Class-based exclusions validated on full dataset:
    - Class 2: -£0.746/bet, -37.3% ROI (24 races)
    - Class 5 flat: -£0.463/bet, -23.2% ROI (53 races)
    """
    cls     = _race_class(race)
    is_jump = _is_jump_race(race)

    if cls == "2":
        return True
    if cls == "5" and not is_jump:
        return True
    return False


def _is_dead_zone(race: dict, pick1_sp: float = None) -> bool:
    """
    Dead zone: flat (non-jump) races priced 3-5/1 with 9+ runners.
    -£0.630/bet over 58 races (-31.5% ROI). Model has no edge vs market
    at this price range in large flat fields — both turf and AW.

    pick1_sp is the live/morning SP of P1 at bet time. If not available
    at qualification time (morning briefing), we cannot apply this filter
    and return False to err on the side of inclusion.
    """
    if pick1_sp is None:
        return False

    is_jump   = _is_jump_race(race)
    if is_jump:
        return False

    n_runners = len(race.get("runners") or race.get("all_runners") or [])
    if n_runners == 0:
        n_runners = int(race.get("field_size") or 0)

    return (
        DEAD_ZONE_SP_LO <= pick1_sp < DEAD_ZONE_SP_HI
        and n_runners >= DEAD_ZONE_MIN_RNRS
    )


def _is_std_aw_qualifying(race: dict) -> bool:
    """
    STD tier AW races — all classes.
    +£0.724/bet over 105 races, 37% win rate.
    AW consistency makes ratings signals more reliable than variable turf.
    """
    return (
        race.get("tier") == TIER_STD
        and _is_aw_race(race)
        and race.get("top1") is not None
    )


def _is_std_jump_qualifying(race: dict) -> bool:
    """
    STD tier jump races — hurdle and chase only, class 3/4/5/IRE.
    +£0.363/bet over 170 races, 31% win rate.

    Excluded:
    - NH Flat STD: -£0.893/bet (poor signal on this race type)
    - Class 1 jump STD: -£0.836/bet (too competitive for model)
    - Class 2 jump STD: precautionary (same pattern as Class 2 overall)
    """
    if race.get("tier") != TIER_STD:
        return False
    if not race.get("top1"):
        return False

    rtype = (race.get("type") or "").lower()
    is_hurdle = "hurdle" in rtype
    is_chase  = "chase" in rtype
    if not (is_hurdle or is_chase):
        return False   # NH Flat excluded

    cls = _race_class(race)
    if cls in ("1", "2"):
        return False   # Class 1 and 2 excluded

    return True


def qualifies(race: dict, pick1_sp: float = None) -> bool:
    """
    Return True if this race should receive a bet.

    Qualification path A — bet-tier (ELITE/STRONG/GOOD):
      Pass going filters, pass class exclusions, pass dead zone filter.

    Qualification path B — STD tier AW:
      All-weather venue, any class, STD tier only.

    Qualification path C — STD tier jump:
      Hurdle or chase only, class 3/4/5/IRE, STD tier only.

    Dead zone filter (path A only):
      Flat races priced 3-5/1 with 9+ runners are excluded regardless of
      tier. Applied at bet time when SP is known; skipped at morning
      briefing when SP is not yet available.
    """
    going = (race.get("going") or "").lower()
    tier  = race.get("tier")

    # ── Universal filters (apply to all paths) ────────────────────────────────
    if any(k in going for k in SKIP_GOING_KEYS):
        return False
    if _is_attrition_risk(race):
        return False
    if not race.get("top1"):
        return False

    # ── Path A: standard bet tiers ────────────────────────────────────────────
    if tier in BET_TIERS:
        if _is_class_excluded(race):
            return False
        if _is_dead_zone(race, pick1_sp):
            return False
        return True

    # ── Path B: STD AW ────────────────────────────────────────────────────────
    if _is_std_aw_qualifying(race):
        return True

    # ── Path C: STD jump ──────────────────────────────────────────────────────
    if _is_std_jump_qualifying(race):
        return True

    return False


# ── Back-compat alias (betfair_main calls qualifies(race) without sp) ─────────
# Morning briefing calls qualifies(race) with no SP — dead zone filter is
# skipped, so these races appear in the briefing. At bet time betfair_main
# passes the live price and the filter is applied correctly.


# ── Staking helpers ────────────────────────────────────────────────────────────

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

    Strategy H (bet-tier races):
    - Flat/AW:  P1 win £2
    - Chase:    P1 win £6 + P1 place £2 (8+ runners)
    - NH Flat:  P1 win £2 + P1 place £2 (8+ runners)
    - Hurdle:   P1 win £2 + P2 win £2 + P1 place £2 + P2 place £2 (8+ rnrs)

    STD tier races use std_win_stake() directly in betfair_main — this
    function returns zero stakes for TIER_STD to avoid double-firing.
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
