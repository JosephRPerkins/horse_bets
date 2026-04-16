"""
betfair/strategy.py

v3 bet qualification and stake calculation.

Winnings-driven staking:
  Stakes are determined by cumulative NET PROFIT, not total Betfair balance.
  Initial deposit never compounds — only actual winnings scale stakes up.
  At zero or negative profit stakes stay at 2/horse.

Odds-on handling by tier:
  SUPREME  — back Pick 1 at any price regardless of Pick 2 price
  STRONG   — skip race entirely if Pick 1 is odds-on
  GOOD     — redirect to Pick 2 ONLY if score >= 5 AND price >= 4.0
  STANDARD — same as GOOD

Pick 2 price gate:
  MIN_PICK2_PRICE applies to all tiers EXCEPT SUPREME.
  For SUPREME, Pick 1 is always backed even if Pick 2 is below minimum.
  For non-SUPREME races, if Pick 2 is below minimum but Pick 1 qualifies,
  Pick 1 is backed solo (stake_pick2 = 0) rather than skipping entirely.

Stake tiers:
  Thresholds scaled to account for place bets (£2 win + £2 place per horse).
  Total exposure per qualifying race ≈ £8 at base tier.
  Thresholds raised accordingly so compounding is not triggered prematurely.

Place bet stakes:
  Scale with win stake tier (same per-horse amount).
  Capped at £2 for GOOD/SKIP tier races (same cap as win bets).
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from predict_v2 import TIER_STRONG, TIER_SUPREME, TIER_STD, TIER_SKIP, TIER_GOOD

SKIP_SURFACES   = {"AW"}
SKIP_GOING_KEYS = {"heavy", "soft to heavy", "heavy to soft"}

ATTRITION_VENUES = {"fairyhouse", "cork", "punchestown", "naas", "leopardstown"}
ATTRITION_GOING  = {"soft", "yielding to soft", "soft to heavy", "heavy"}
ATTRITION_DIST_F = 20.0

MIN_PICK1_PRICE = 2.0
MIN_PICK2_PRICE = 4.0   # applies to non-SUPREME tiers only

MIN_PICK2_SCORE_FOR_REDIRECT = 5
MIN_PICK2_PRICE_FOR_REDIRECT = 4.0

MIN_BACK_PRICE = 1.5
MIN_LIQUIDITY  = 2.0

TOPUP_WARNING  = 10.0
TOPUP_CRITICAL = 4.0

TIER1_CAP_TIERS = {TIER_GOOD, TIER_SKIP}

# Winnings-driven cascade — thresholds are cumulative PROFIT not total balance.
# Scaled to account for place bets: total exposure per race ≈ £8 at base tier
# (£2 win P1 + £2 win P2 + £2 place P1 + £2 place P2).
# Thresholds raised ~2x vs original to avoid premature tier jumps.
# Format: (min_profit, per_horse_stake, redirect_single_stake)
STAKE_TIERS = [
    (0,    2,   4),
    (40,   4,   8),
    (80,   8,   16),
    (160,  16,  32),
    (320,  32,  64),
    (640,  64,  128),
    (1280, 128, 256),
    (2560, 256, 512),
    (5120, 512, 1024),
]

STOP_FLOOR = 0.0


def get_stake(profit: float) -> float:
    """Return per-horse win stake based on cumulative NET PROFIT."""
    if profit <= 0:
        return 2.0
    stake = 2.0
    for min_profit, s, _ in STAKE_TIERS:
        if profit >= min_profit:
            stake = float(s)
    return stake


def get_place_stake(profit: float, tier: int = TIER_STD) -> float:
    """
    Return per-horse place stake.
    Matches win stake but capped at £2 for GOOD/SKIP tiers.
    """
    if tier in TIER1_CAP_TIERS:
        return 2.0
    return get_stake(profit)


def get_redirect_stake(profit: float) -> float:
    if profit <= 0:
        return 4.0
    redirect = 4.0
    for min_profit, _, r in STAKE_TIERS:
        if profit >= min_profit:
            redirect = float(r)
    return redirect


def get_tsr_stake(profit: float) -> float:
    stakes  = [float(s) for _, s, _ in STAKE_TIERS]
    current = get_stake(profit)
    try:
        idx = stakes.index(current)
        return stakes[min(idx + 1, len(stakes) - 1)]
    except ValueError:
        return current


def is_tsr_trigger(race: dict) -> bool:
    return race.get("tier") == TIER_SUPREME


def _is_attrition_risk(race: dict) -> bool:
    course     = (race.get("course") or "").lower()
    going      = (race.get("going") or "").lower()
    race_type  = (race.get("type") or "").lower()
    dist_f     = race.get("dist_f") or 0.0
    is_irish   = any(v in course for v in ATTRITION_VENUES)
    is_nh      = "hurdle" in race_type or "chase" in race_type or "nh flat" in race_type
    is_soft    = any(g in going for g in ATTRITION_GOING)
    is_staying = float(dist_f) >= ATTRITION_DIST_F
    return is_irish and is_nh and is_soft and is_staying


def qualifies(race: dict) -> bool:
    surface = race.get("surface", "")
    going   = (race.get("going") or "").lower()
    if surface in SKIP_SURFACES:
        return False
    if any(k in going for k in SKIP_GOING_KEYS):
        return False
    if _is_attrition_risk(race):
        return False
    if not race.get("top1") or not race.get("top2"):
        return False
    return True


def should_back_pick1(pick1_price, tier: int = TIER_STD) -> bool:
    if not pick1_price:
        return False
    if tier == TIER_SUPREME:
        return pick1_price > 1.0
    return pick1_price >= MIN_PICK1_PRICE


def should_back_pick2(pick2_price, tier: int = TIER_STD) -> bool:
    """
    Returns True if Pick 2 meets the minimum price gate.
    SUPREME: gate does not apply — Pick 1 always backed regardless.
    All other tiers: Pick 2 must be >= MIN_PICK2_PRICE.
    """
    if not pick2_price:
        return False
    if tier == TIER_SUPREME:
        return True
    return pick2_price >= MIN_PICK2_PRICE


def pick_stakes(profit: float, tsr: bool,
                pick1_price, pick2_price,
                tier: int = TIER_STD,
                pick2_score: int = 0) -> tuple:
    """
    Return (stake_pick1, stake_pick2).

    profit:       cumulative net profit — drives stake tier
    pick2_score:  model score for Pick 2 — gates redirect decisions

    Key behaviours:
    - SUPREME: always backs Pick 1. Pick 2 backed only if >= MIN_PICK2_PRICE.
    - STRONG odds-on P1: skip entirely (0, 0)
    - GOOD/SKIP: capped at £2/horse
    - Non-SUPREME with Pick 2 below minimum but Pick 1 qualifying:
      backs Pick 1 solo (s1, 0) rather than skipping entirely.

    Returns (0.0, 0.0) only when race should be skipped completely.
    Returns (s1, 0.0) when Pick 1 should be backed solo.
    """
    if tier in TIER1_CAP_TIERS:
        base     = 2.0
        redirect = 4.0
    else:
        base     = get_stake(profit)
        redirect = get_redirect_stake(profit)

    p1_qualifies = should_back_pick1(pick1_price, tier)
    p1_odds_on   = pick1_price is not None and pick1_price < MIN_PICK1_PRICE
    p2_ok        = pick2_price is not None and pick2_price >= MIN_PICK2_PRICE

    # ── SUPREME ───────────────────────────────────────────────────────────────
    # Always back Pick 1. Back Pick 2 only if price is viable.
    if tier == TIER_SUPREME:
        s1 = get_tsr_stake(profit) if tier not in TIER1_CAP_TIERS else base
        s2 = base if p2_ok else 0.0
        return s1, s2

    # ── STRONG odds-on P1: skip entirely ─────────────────────────────────────
    if p1_odds_on and tier == TIER_STRONG:
        return 0.0, 0.0

    # ── Normal P1 qualifies ───────────────────────────────────────────────────
    if p1_qualifies:
        s2 = base if p2_ok else 0.0
        return base, s2

    # ── P1 odds-on for non-SUPREME/STRONG: redirect to P2 if strong ──────────
    if p1_odds_on:
        p2_strong = (
            pick2_score >= MIN_PICK2_SCORE_FOR_REDIRECT
            and pick2_price is not None
            and pick2_price >= MIN_PICK2_PRICE_FOR_REDIRECT
        )
        if p2_strong:
            return 0.0, redirect
        return 0.0, 0.0

    # ── P1 below minimum (not odds-on): redirect to P2 if strong ─────────────
    p2_strong = (
        pick2_score >= MIN_PICK2_SCORE_FOR_REDIRECT
        and pick2_price is not None
        and pick2_price >= MIN_PICK2_PRICE_FOR_REDIRECT
    )
    if p2_strong:
        return 0.0, redirect
    return 0.0, 0.0


def apply_liquidity(stake_a: float, stake_b: float,
                    liq_a: float, liq_b: float,
                    redirect: bool) -> tuple:
    if redirect:
        actual_b = min(stake_b, liq_b)
        if actual_b < MIN_LIQUIDITY:
            return 0.0, 0.0, True, f"Pick 2 liquidity £{liq_b:.2f} < £{MIN_LIQUIDITY:.0f}"
        return 0.0, actual_b, False, ""
    else:
        # Solo Pick 1 bet (stake_b == 0) — only check Pick 1 liquidity
        if stake_b == 0:
            actual_a = min(stake_a, liq_a) if liq_a > 0 else stake_a
            if actual_a < MIN_LIQUIDITY and liq_a > 0:
                return 0.0, 0.0, True, f"Pick 1 liquidity £{liq_a:.2f} < £{MIN_LIQUIDITY:.0f}"
            return actual_a, 0.0, False, ""
        # Both picks
        safe   = min(liq_a, liq_b) if liq_a > 0 and liq_b > 0 else stake_a
        actual = min(stake_a, safe)
        if actual < MIN_LIQUIDITY and (liq_a > 0 or liq_b > 0):
            return 0.0, 0.0, True, (
                f"Liquidity too low — P1: £{liq_a:.2f} P2: £{liq_b:.2f} "
                f"(need >= £{MIN_LIQUIDITY:.0f})"
            )
        actual_b = min(stake_b, liq_b) if liq_b > 0 else stake_b
        return actual, actual_b, False, ""


def check_topup_alerts(balance: float, profit: float,
                       prev_stake) -> list:
    alerts     = []
    curr_stake = get_stake(profit)

    if balance <= 0:
        alerts.append(
            f"🛑 <b>Session halted</b> — balance £{balance:.2f}\n"
            f"Top up to continue. Minimum £2 to place a bet."
        )
    elif balance < TOPUP_CRITICAL:
        needed = TOPUP_WARNING - balance
        alerts.append(
            f"🚨 <b>Critical: £{balance:.2f} remaining</b>\n"
            f"Top up £{needed:.2f} to clear the warning threshold."
        )
    elif balance < TOPUP_WARNING:
        needed = 20.0 - balance
        alerts.append(
            f"⚠️ <b>Balance at £{balance:.2f}</b>\n"
            f"Top up £{needed:.2f} to reach the £4/horse tier."
        )

    if prev_stake is not None and curr_stake != prev_stake:
        if curr_stake > prev_stake:
            alerts.append(
                f"📈 <b>Tier up</b> — stakes now £{curr_stake:.0f}/horse\n"
                f"Cumulative profit: £{profit:.2f}"
            )
        else:
            alerts.append(
                f"📉 <b>Tier down</b> — stakes now £{curr_stake:.0f}/horse\n"
                f"Cumulative profit: £{profit:.2f}"
            )

    return alerts


def stake_display(profit: float) -> str:
    s = get_stake(profit)
    r = get_redirect_stake(profit)
    p = get_place_stake(profit)
    return (
        f"Win: £{s:.0f}/horse | Place: £{p:.0f}/horse | "
        f"Redirect: £{r:.0f} (P2 score>=5, 3/1+)"
    )
