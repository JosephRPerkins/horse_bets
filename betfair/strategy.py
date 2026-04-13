"""
betfair/strategy.py

v3 bet qualification and stake calculation.

Strategy: Pick 2 as primary back, Pick 1 only at 2/1+.

Backtest findings (30 days, 322 races, bot-matched picks):
  - All tiers profitable flat-staked at £2/horse:
      Supreme 100% | Strong 82% | Good 76% | Standard 68% | Skip 66%
  - Good + Skip capped at Tier 1 (£2/horse) to limit downside variance
  - AW surface and Heavy going excluded (structural, not tier-based)
  - Irish staying chases on soft/heavy excluded (attrition risk)

Rules:
  - All tiers qualify (Good/Skip capped at Tier 1 stake)
  - Pick 2 is primary — must be 3/1+ (≥ 4.0 dec) or race is skipped
  - SUPREME: Pick 1 always backed regardless of price (TSR solo is
    strong enough to back at any odds). Pick 2 backed at tier stake.
  - STRONG: Pick 1 backed at 2/1+ only. If Pick 1 is odds-on the race
    is SKIPPED entirely — redirecting to Pick 2 consistently loses when
    the identified winner is odds-on.
  - GOOD/STANDARD: If Pick 1 is odds-on its stake is redirected to Pick 2.
  - Both picks always staked equally at min(tier_stake, liquidity_both)
  - Liquidity < £2 on either pick → skip race
  - Top-up notifications at £10 (warning) and £4 (critical)

Odds-on handling by tier:
  SUPREME  → back Pick 1 at any price (TSR solo confidence)
  STRONG   → skip race if Pick 1 odds-on (don't redirect)
  GOOD     → redirect Pick 1 stake to Pick 2 if odds-on
  STANDARD → redirect Pick 1 stake to Pick 2 if odds-on
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from predict_v2 import TIER_STRONG, TIER_SUPREME, TIER_STD, TIER_SKIP, TIER_GOOD

# ── Qualification filters ─────────────────────────────────────────────────────

SKIP_SURFACES   = {"AW"}
SKIP_GOING_KEYS = {"heavy", "soft to heavy", "heavy to soft"}

# Irish staying chases with soft/heavy going → mass non-completions
ATTRITION_VENUES = {"fairyhouse", "cork", "punchestown", "naas", "leopardstown"}
ATTRITION_GOING  = {"soft", "yielding to soft", "soft to heavy", "heavy"}
ATTRITION_DIST_F = 20.0    # 2m4f in furlongs

# Minimum prices
MIN_PICK1_PRICE = 2.0    # Pick 1 must be 2/1+ for GOOD/STANDARD (redirect if odds-on)
                         # Not applied to SUPREME (always backed)
                         # STRONG skips entirely if Pick 1 is odds-on
MIN_PICK2_PRICE = 4.0    # Pick 2 must be 3/1+ (skip race if shorter)

# ── Staking ───────────────────────────────────────────────────────────────────

MIN_BACK_PRICE = 1.5    # absolute floor — don't back near-certs
MIN_LIQUIDITY  = 2.0    # skip race if either pick has less than this available

# Top-up notification thresholds
TOPUP_WARNING  = 10.0
TOPUP_CRITICAL = 4.0

# Tiers capped at Tier 1 stake regardless of balance
TIER1_CAP_TIERS = {TIER_GOOD, TIER_SKIP}

# Doubling cascade — no artificial ceiling, liquidity is the real cap
# (min_balance, per_horse_stake, redirect_single_stake)
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

STOP_FLOOR = 0.0    # no hard floor — bot keeps betting at £2 as long as balance > 0


def get_stake(balance: float) -> float:
    """Return the per-horse stake for the current balance tier."""
    stake = 2.0
    for min_bal, s, _ in STAKE_TIERS:
        if balance >= min_bal:
            stake = float(s)
    return stake


def get_redirect_stake(balance: float) -> float:
    """Return the redirected-single stake (odds-on Pick 1 → all to Pick 2)."""
    redirect = 4.0
    for min_bal, _, r in STAKE_TIERS:
        if balance >= min_bal:
            redirect = float(r)
    return redirect


def get_tsr_stake(balance: float) -> float:
    """TSR>OR trigger: one tier higher stake for Pick 1 when SUPREME."""
    stakes = [float(s) for _, s, _ in STAKE_TIERS]
    current = get_stake(balance)
    try:
        idx = stakes.index(current)
        return stakes[min(idx + 1, len(stakes) - 1)]
    except ValueError:
        return current


def is_tsr_trigger(race: dict) -> bool:
    """SUPREME tier = TSR>OR solo trigger fired."""
    return race.get("tier") == TIER_SUPREME


def _is_attrition_risk(race: dict) -> bool:
    course    = (race.get("course") or "").lower()
    going     = (race.get("going") or "").lower()
    race_type = (race.get("type") or "").lower()
    dist_f    = race.get("dist_f") or 0.0
    is_irish  = any(v in course for v in ATTRITION_VENUES)
    is_nh     = "hurdle" in race_type or "chase" in race_type or "nh flat" in race_type
    is_soft   = any(g in going for g in ATTRITION_GOING)
    is_staying = float(dist_f) >= ATTRITION_DIST_F
    return is_irish and is_nh and is_soft and is_staying


def qualifies(race: dict) -> bool:
    """Return True if a race passes all structural qualifying filters."""
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
    """
    Determine whether Pick 1 should be backed given its price and race tier.

    SUPREME: back at any price > 1.0 — TSR solo confidence justifies it.
    All others: must be 2/1+ (≥ 2.0 dec).
    """
    if not pick1_price:
        return False
    if tier == TIER_SUPREME:
        return pick1_price > 1.0   # back at any price for SUPREME
    return pick1_price >= MIN_PICK1_PRICE


def should_back_pick2(pick2_price: float | None) -> bool:
    if not pick2_price:
        return False
    return pick2_price >= MIN_PICK2_PRICE


def pick_stakes(balance: float, tsr: bool,
                pick1_price: float | None,
                pick2_price: float | None,
                tier: int = TIER_STD) -> tuple[float, float]:
    """
    Return (stake_pick1, stake_pick2).

    Odds-on handling by tier:
      SUPREME  → back Pick 1 at any price (TSR solo confidence)
      STRONG   → skip race entirely if Pick 1 is odds-on (return 0, 0)
      GOOD     → redirect Pick 1 stake to Pick 2 if odds-on
      STANDARD → redirect Pick 1 stake to Pick 2 if odds-on

    Pick 2 is always the anchor — race is skipped if Pick 2 price gate fails.
    Good/Skip races capped at Tier 1 (£2/horse) regardless of balance.
    TSR trigger gives Pick 1 one tier bump when it qualifies on SUPREME.

    NOTE: Returned stakes are the tier stakes before liquidity adjustment.
    Caller must apply min(stake, liquidity_both) and enforce MIN_LIQUIDITY.

    Returns (0.0, 0.0) if:
      - Pick 2 price gate fails
      - STRONG tier with odds-on Pick 1 (skip entirely)
    """
    if not should_back_pick2(pick2_price):
        return 0.0, 0.0

    # Good/Skip capped at Tier 1 regardless of balance
    if tier in TIER1_CAP_TIERS:
        base     = 2.0
        redirect = 4.0
    else:
        base     = get_stake(balance)
        redirect = get_redirect_stake(balance)

    p1_qualifies = should_back_pick1(pick1_price, tier)
    p1_odds_on   = pick1_price is not None and pick1_price < MIN_PICK1_PRICE

    if p1_qualifies:
        # Pick 1 qualifies — back both
        s1 = get_tsr_stake(balance) if (tsr and tier not in TIER1_CAP_TIERS) else base
        s2 = base
        return s1, s2

    elif p1_odds_on:
        # Pick 1 is odds-on — behaviour depends on tier
        if tier == TIER_SUPREME:
            # SUPREME: back at any price — TSR solo is strong enough
            s1 = get_tsr_stake(balance) if tier not in TIER1_CAP_TIERS else base
            s2 = base
            return s1, s2
        elif tier == TIER_STRONG:
            # STRONG: skip race entirely — redirecting consistently loses
            # when the model's identified winner is odds-on
            return 0.0, 0.0
        else:
            # GOOD/STANDARD: redirect Pick 1 stake to Pick 2
            return 0.0, redirect

    else:
        # Pick 1 has no price — redirect to Pick 2
        return 0.0, redirect


def apply_liquidity(stake_a: float, stake_b: float,
                    liq_a: float, liq_b: float,
                    redirect: bool) -> tuple[float, float, bool, str]:
    """
    Apply liquidity constraints and enforce equal stakes on both picks.

    redirect=True means stake_a==0 (odds-on), only check Pick 2 liquidity.

    Returns (actual_a, actual_b, skipped, reason).
    """
    if redirect:
        # Single bet on Pick 2 only
        actual_b = min(stake_b, liq_b)
        if actual_b < MIN_LIQUIDITY:
            return 0.0, 0.0, True, f"Pick 2 liquidity £{liq_b:.2f} < £{MIN_LIQUIDITY:.0f}"
        return 0.0, actual_b, False, ""
    else:
        safe = min(liq_a, liq_b)
        actual = min(stake_a, safe)   # both picks always equal
        if actual < MIN_LIQUIDITY:
            return 0.0, 0.0, True, (
                f"Liquidity too low — P1: £{liq_a:.2f} P2: £{liq_b:.2f} "
                f"(need ≥ £{MIN_LIQUIDITY:.0f})"
            )
        return actual, actual, False, ""


def check_topup_alerts(balance: float, prev_stake: float | None) -> list[str]:
    """
    Return list of notification strings to send if thresholds crossed.
    prev_stake: the per-horse stake from the previous bet (for tier change detection).
    """
    alerts = []
    curr_stake = get_stake(balance)

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
            alerts.append(f"📈 <b>Tier up</b> — stakes now £{curr_stake:.0f}/horse")
        else:
            alerts.append(f"📉 <b>Tier down</b> — stakes now £{curr_stake:.0f}/horse")

    return alerts


def stake_display(balance: float) -> str:
    s = get_stake(balance)
    r = get_redirect_stake(balance)
    return (
        f"Pick 2: £{s:.0f} (or £{r:.0f} if Pick 1 odds-on) | "
        f"Pick 1: £{s:.0f} (2/1+ only, any price for SUPREME)"
    )
