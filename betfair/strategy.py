"""
betfair/strategy.py

v3 bet qualification and stake calculation.

Winnings-driven staking:
  Stakes are determined by cumulative NET PROFIT, not total Betfair balance.
  Initial deposit never compounds — only actual winnings scale stakes up.
  At zero profit (start, or after losses wipe all profit) stakes stay at £2.

Odds-on handling by tier:
  SUPREME  → back Pick 1 at any price (TSR solo confidence)
  STRONG   → skip race entirely if Pick 1 is odds-on
  GOOD     → redirect Pick 1 stake to Pick 2 ONLY if Pick 2 score >= 5
             AND Pick 2 price >= 4.0. Otherwise skip.
  STANDARD → same as GOOD
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from predict_v2 import TIER_STRONG, TIER_SUPREME, TIER_STD, TIER_SKIP, TIER_GOOD

# ── Qualification filters ─────────────────────────────────────────────────────

SKIP_SURFACES   = {"AW"}
SKIP_GOING_KEYS = {"heavy", "soft to heavy", "heavy to soft"}

ATTRITION_VENUES = {"fairyhouse", "cork", "punchestown", "naas", "leopardstown"}
ATTRITION_GOING  = {"soft", "yielding to soft", "soft to heavy", "heavy"}
ATTRITION_DIST_F = 20.0

# Minimum prices
MIN_PICK1_PRICE = 2.0
MIN_PICK2_PRICE = 4.0

# Redirect thresholds for GOOD/STANDARD when Pick 1 is odds-on
# Pick 2 must meet BOTH to justify a redirected bet
MIN_PICK2_SCORE_FOR_REDIRECT = 5
MIN_PICK2_PRICE_FOR_REDIRECT = 4.0

# ── Staking ───────────────────────────────────────────────────────────────────

MIN_BACK_PRICE = 1.5
MIN_LIQUIDITY  = 2.0

TOPUP_WARNING  = 10.0
TOPUP_CRITICAL = 4.0

TIER1_CAP_TIERS = {TIER_GOOD, TIER_SKIP}

# Winnings-driven cascade — thresholds are cumulative PROFIT not total balance
# (min_profit, per_horse_stake, redirect_single_stake)
STAKE_TIERS = [
    (0,    2,   4),
    (20,   4,   8),
    (40,   8,   16),
    (80,   16,  32),
    (160,  32,  64),
    (320,  64,  128),
    (640,  128, 256),
    (1280, 256, 512),
    (2560, 512, 1024),
]

STOP_FLOOR = 0.0


def get_stake(profit: float) -> float:
    """
    Return per-horse stake based on cumulative NET PROFIT.

    At zero or negative profit always returns 2.0 (base tier).
    Stakes only grow once profit exceeds each threshold.
    """
    if profit <= 0:
        return 2.0
    stake = 2.0
    for min_profit, s, _ in STAKE_TIERS:
        if profit >= min_profit:
            stake = float(s)
    return stake


def get_redirect_stake(profit: float) -> float:
    """Redirected single stake based on cumulative profit."""
    if profit <= 0:
        return 4.0
    redirect = 4.0
    for min_profit, _, r in STAKE_TIERS:
        if profit >= min_profit:
            redirect = float(r)
    return redirect


def get_tsr_stake(profit: float) -> float:
    """TSR>OR trigger: one tier higher stake for Pick 1 when SUPREME."""
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


def should_back_pick1(pick1_price: float | None, tier: int = TIER_STD) -> bool:
    if not pick1_price:
        return False
    if tier == TIER_SUPREME:
        return pick1_price > 1.0
    return pick1_price >= MIN_PICK1_PRICE


def should_back_pick2(pick2_price: float | None) -> bool:
    if not pick2_price:
        return False
    return pick2_price >= MIN_PICK2_PRICE


def pick_stakes(profit: float, tsr: bool,
                pick1_price: float | None,
                pick2_price: float | None,
                tier: int = TIER_STD,
                pick2_score: int = 0) -> tuple[float, float]:
    """
    Return (stake_pick1, stake_pick2).

    profit:      cumulative net profit — drives stake tier
    pick2_score: model score for Pick 2 — gates redirect decisions

    Odds-on handling:
      SUPREME  → back at any price
      STRONG   → skip entirely if Pick 1 odds-on
      GOOD     → redirect to Pick 2 only if score >= 5 AND price >= 4.0
      STANDARD → same as GOOD

    Returns (0.0, 0.0) to signal race should be skipped.
    """
    if not should_back_pick2(pick2_price):
        return 0.0, 0.0

    if tier in TIER1_CAP_TIERS:
        base     = 2.0
        redirect = 4.0
    else:
        base     = get_stake(profit)
        redirect = get_redirect_stake(profit)

    p1_qualifies = should_back_pick1(pick1_price, tier)
    p1_odds_on   = pick1_price is not None and pick1_price < MIN_PICK1_PRICE

    if p1_qualifies:
        s1 = get_tsr_stake(profit) if (tsr and tier not in TIER1_CAP_TIERS) else base
        s2 = base
        return s1, s2

    elif p1_odds_on:
        if tier == TIER_SUPREME:
            # Always back both at any price
            s1 = get_tsr_stake(profit) if tier not in TIER1_CAP_TIERS else base
            s2 = base
            return s1, s2

        elif tier == TIER_STRONG:
            # Skip entirely — redirecting consistently loses
            return 0.0, 0.0

        else:
            # GOOD/STANDARD: only redirect if Pick 2 is genuinely strong
            # Backing a weak Pick 2 just because Pick 1 is odds-on loses money
            p2_strong = (
                pick2_score >= MIN_PICK2_SCORE_FOR_REDIRECT
                and pick2_price is not None
                and pick2_price >= MIN_PICK2_PRICE_FOR_REDIRECT
            )
            if p2_strong:
                return 0.0, redirect
            else:
                return 0.0, 0.0

    else:
        # Pick 1 has no price — only back Pick 2 if it's strong enough
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
                    redirect: bool) -> tuple[float, float, bool, str]:
    if redirect:
        actual_b = min(stake_b, liq_b)
        if actual_b < MIN_LIQUIDITY:
            return 0.0, 0.0, True, f"Pick 2 liquidity £{liq_b:.2f} < £{MIN_LIQUIDITY:.0f}"
        return 0.0, actual_b, False, ""
    else:
        safe   = min(liq_a, liq_b)
        actual = min(stake_a, safe)
        if actual < MIN_LIQUIDITY:
            return 0.0, 0.0, True, (
                f"Liquidity too low — P1: £{liq_a:.2f} P2: £{liq_b:.2f} "
                f"(need >= £{MIN_LIQUIDITY:.0f})"
            )
        return actual, actual, False, ""


def check_topup_alerts(balance: float, profit: float,
                       prev_stake: float | None) -> list[str]:
    """
    Return notification strings for low balance or tier changes.

    balance: actual Betfair balance (for low balance warnings)
    profit:  cumulative profit (for tier change detection)
    """
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
    return (
        f"Pick 1: £{s:.0f} (2/1+ only, any price for SUPREME) | "
        f"Pick 2: £{s:.0f} (redirect £{r:.0f} if P2 score>=5 and 3/1+)"
    )
