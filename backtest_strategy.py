"""
backtest_strategy.py

Simulates the Betfair WIN + PLACE strategy across all available historical data
in data/raw/YYYY-MM-DD.json.

Strategy being tested:
  - Back Pick 1 AND Pick 2 to WIN on Betfair exchange
  - Place bet on Pick 1 AND Pick 2 using estimated place odds from SP
  - Tiered staking: £2 (0-74), £4 (75-149), £6 (150+)
  - Good/Skip races capped at £2/horse regardless of balance
  - Price gates: Pick 2 must be >= 4.0 dec, Pick 1 must be >= 2.0 dec
  - Odds-on Pick 1: cancel Pick 1 win stake, double it onto Pick 2
  - Two-horse race rule: place-only when both picks short and field drops away
  - Attrition filter: skip Irish NH staying races on soft/heavy
  - Skip Heavy going
  - Win = horse finishes 1st (position == "1")
  - Place = horse finishes within standard place terms
  - Commission: 5% on winnings
  - Place odds estimated via logit-shift from win SP

Run:
    python backtest_strategy.py
    python backtest_strategy.py --start-balance 50
    python backtest_strategy.py --daily
    python backtest_strategy.py --daily --reset-daily
    python backtest_strategy.py --tier supreme
    python backtest_strategy.py --last 14
    python backtest_strategy.py --no-place     # win bets only
"""

import os
import sys
import json
import glob
import math
import argparse
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from predict    import score_runner, place_terms, dist_furlongs, to_float, to_int, is_numeric
from predict_v2 import race_confidence, TIER_SUPREME, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP, TIER_LABELS

COMMISSION      = 0.05
MIN_PICK1_PRICE = 2.0
MIN_PICK2_PRICE = 4.0
TIER1_STAKE     = 2

STAKE_TIERS = [
    (0,    2),
    (75,   4),
    (150,  6),
]

ATTRITION_VENUES = {"fairyhouse", "cork", "punchestown", "naas", "leopardstown"}
ATTRITION_GOING  = {"soft", "yielding to soft", "soft to heavy", "heavy"}
ATTRITION_DIST_F = 20.0

# Logit-shift constants for place odds estimation
LOGIT_A = {2: 0.9, 3: 1.3, 4: 1.6, 5: 2.0, 6: 2.3, 7: 2.8}


def get_stake(balance: float) -> int:
    """Return per-horse stake based on current balance. Max £6/horse."""
    stake = 2
    for min_bal, s in STAKE_TIERS:
        if balance >= min_bal:
            stake = s
    return stake


def estimate_place_odds(sp_dec: float, n_places: int) -> float:
    """
    Estimate Betfair place market odds from win SP using logit-shift model.
    This is an approximation — real place odds vary but this gives a
    reasonable estimate for backtesting purposes.
    """
    if sp_dec <= 1.0:
        return 1.01
    n = min(max(n_places, 2), 7)
    a = LOGIT_A[n]
    try:
        p_win = 1.0 / sp_dec
        lt    = math.log(p_win / (1.0 - p_win)) + a
        p_top = 1.0 / (1.0 + math.exp(-lt))
        return max(round(1.0 / p_top, 2), 1.01)
    except (ValueError, ZeroDivisionError):
        return 1.01


def is_attrition_risk(race: dict) -> bool:
    course     = (race.get("course") or "").lower()
    going      = (race.get("going") or "").lower()
    rtype      = (race.get("type") or "").lower()
    dist_f     = to_float(str(race.get("dist_f", "")).replace("f", ""))
    is_irish   = any(v in course for v in ATTRITION_VENUES)
    is_nh      = any(k in rtype for k in ("hurdle", "chase", "nh flat"))
    is_soft    = any(g in going for g in ATTRITION_GOING)
    is_staying = (dist_f or 0) >= ATTRITION_DIST_F
    return is_irish and is_nh and is_soft and is_staying


def qualifies(race: dict, tier: int) -> bool:
    going   = (race.get("going") or "").lower()
    if any(k in going for k in ("heavy", "soft to heavy", "heavy to soft")):
        return False
    if is_attrition_risk(race):
        return False
    return True


def analyse_race(race: dict) -> dict | None:
    runners = race.get("runners") or []
    if len(runners) < 2:
        return None
    scored = []
    for r in runners:
        sc = score_runner(r, race)
        scored.append({**r, "score": sc})
    scored.sort(key=lambda x: x["score"], reverse=True)

    race_for_tier = {**race, "runners": scored}
    win_score     = scored[0]["score"] if scored else 0
    tier, _       = race_confidence(race_for_tier, win_score)

    top1 = scored[0] if len(scored) > 0 else {}
    top2 = scored[1] if len(scored) > 1 else {}
    top3 = scored[2] if len(scored) > 2 else {}

    return {
        "tier": tier,
        "top1": top1,
        "top2": top2,
        "top3": top3,
        "race": race,
    }


def simulate_race(analysis: dict, balance: float, include_place: bool = True):
    """
    Simulate win + place bets for one race.

    Returns:
        win_pnl, place_pnl, stake_a, stake_b, outcome, reason
    """
    tier = analysis["tier"]
    top1 = analysis["top1"]
    top2 = analysis["top2"]
    top3 = analysis["top3"]
    race = analysis["race"]

    if not qualifies(race, tier):
        return None, None, 0, 0, "FILTER_SKIP", "surface/going/attrition"

    sp_a  = to_float(top1.get("sp_dec"))
    sp_b  = to_float(top2.get("sp_dec"))
    sp_c  = to_float(top3.get("sp_dec")) if top3 else None

    base = TIER1_STAKE if tier in (TIER_GOOD, TIER_SKIP) else get_stake(balance)

    # ── Two-horse race rule ───────────────────────────────────────────────────
    # P1 odds-on, P2 short, P3 well behind — place only
    place_only = False
    if tier in (TIER_GOOD, TIER_STRONG, TIER_SUPREME) and sp_a and sp_b and sp_c:
        scenario1 = (sp_a < MIN_PICK1_PRICE and sp_b < MIN_PICK2_PRICE
                     and sp_c >= sp_b * 3.0)
        scenario2 = (sp_a < 1.5 and sp_b >= MIN_PICK2_PRICE
                     and sp_c >= sp_b * 2.0)
        if scenario1 or scenario2:
            place_only = True

    # ── Win bet stakes ────────────────────────────────────────────────────────
    if place_only:
        stake_a = 0
        stake_b = 0
        redirect = False
    elif sp_b is None or sp_b < MIN_PICK2_PRICE:
        # Check solo P1 — back P1 only if it qualifies
        if sp_a and sp_a >= MIN_PICK1_PRICE:
            stake_a  = base
            stake_b  = 0
            redirect = False
        else:
            return None, None, 0, 0, "PRICE_SKIP", f"Pick2 {sp_b} < {MIN_PICK2_PRICE}"
    elif sp_a is None or sp_a < MIN_PICK1_PRICE:
        # Odds-on redirect
        stake_a  = 0
        stake_b  = base * 2
        redirect = True
    else:
        stake_a  = base
        stake_b  = base
        redirect = False

    # ── Finish positions ──────────────────────────────────────────────────────
    pos_a = top1.get("position", "")
    pos_b = top2.get("position", "")
    won_a = is_numeric(pos_a) and int(pos_a) == 1
    won_b = is_numeric(pos_b) and int(pos_b) == 1

    n_runners = len(race.get("runners") or [])
    std_places = place_terms(n_runners) if n_runners else 1
    placed_a   = is_numeric(pos_a) and int(pos_a) <= std_places
    placed_b   = is_numeric(pos_b) and int(pos_b) <= std_places

    # ── Win P&L ───────────────────────────────────────────────────────────────
    win_pnl = 0.0
    if not place_only:
        if redirect:
            if won_b:
                win_pnl = round(stake_b * (sp_b - 1) * (1 - COMMISSION), 2)
            else:
                win_pnl = -stake_b
        else:
            if won_a:
                win_pnl += round(stake_a * (sp_a - 1) * (1 - COMMISSION), 2)
            else:
                win_pnl -= stake_a
            if stake_b > 0:
                if won_b:
                    win_pnl += round(stake_b * (sp_b - 1) * (1 - COMMISSION), 2)
                else:
                    win_pnl -= stake_b

    # ── Place P&L ─────────────────────────────────────────────────────────────
    place_pnl = 0.0
    if include_place and sp_a and sp_b:
        p_stake = TIER1_STAKE if tier in (TIER_GOOD, TIER_SKIP) else base

        # Horses to place bet on
        if place_only:
            horses = [(sp_a, placed_a), (sp_b, placed_b)]
        elif redirect:
            horses = [(sp_b, placed_b)]
        elif stake_b == 0:
            horses = [(sp_a, placed_a)]
        else:
            horses = [(sp_a, placed_a), (sp_b, placed_b)]

        for sp, placed in horses:
            if sp is None:
                continue
            place_odds = estimate_place_odds(sp, std_places)
            if placed:
                place_pnl += round(p_stake * (place_odds - 1) * (1 - COMMISSION), 2)
            else:
                place_pnl -= p_stake

    # ── Outcome label ─────────────────────────────────────────────────────────
    if place_only:
        outcome = "PLACE_ONLY"
    elif won_a and won_b:
        outcome = "WIN_BOTH"
    elif won_a:
        outcome = "WIN_A"
    elif won_b:
        outcome = "WIN_B"
    else:
        outcome = "LOSS"

    return win_pnl, place_pnl, stake_a, stake_b, outcome, ""


def run_backtest(start_balance: float = 50.0,
                 daily: bool = False,
                 tier_filter: int | None = None,
                 show_races: bool = False,
                 reset_daily: bool = False,
                 last_n: int | None = None,
                 include_place: bool = True):

    data_dir = os.path.join(os.path.dirname(__file__), "data", "raw")
    files    = sorted(glob.glob(os.path.join(data_dir, "*.json")))
    if last_n:
        files = files[-last_n:]
    if not files:
        print("No data files found in data/raw/")
        return

    balance        = start_balance
    total_win_pnl  = 0.0
    total_pl_pnl   = 0.0
    total_races    = 0
    total_bet      = 0
    total_wins     = 0
    total_staked   = 0.0
    tier_stats     = defaultdict(lambda: {"races": 0, "wins": 0,
                                          "win_pnl": 0.0, "place_pnl": 0.0})

    place_label = "WIN + PLACE" if include_place else "WIN ONLY"
    mode_label  = f"DAILY RESET (£{start_balance:.0f}/day)" if reset_daily else "COMPOUNDING"

    print(f"\n{'='*70}")
    print(f"  BETFAIR STRATEGY BACKTEST — {len(files)} days  [{mode_label}]")
    print(f"  Starting balance: £{start_balance:.2f}  |  {place_label}")
    print(f"  Strategy: Back P1+P2 to win | Good/Skip → £2 cap | Tiers: £75=£4, £150=£6")
    print(f"{'='*70}\n")

    for fpath in files:
        date_str = os.path.basename(fpath).replace(".json", "")
        with open(fpath) as f:
            data = json.load(f)
        races = data.get("results", [])

        day_win_pnl  = 0.0
        day_pl_pnl   = 0.0
        day_bet      = 0
        day_wins     = 0
        day_staked   = 0.0
        peak_stake   = get_stake(balance)

        if reset_daily:
            balance = start_balance

        day_races = []

        for race in races:
            total_races += 1
            analysis = analyse_race(race)
            if not analysis:
                continue

            tier = analysis["tier"]
            if tier_filter is not None and tier != tier_filter:
                continue

            win_pnl, place_pnl, stake_a, stake_b, outcome, reason = simulate_race(
                analysis, balance, include_place=include_place
            )
            if win_pnl is None:
                continue

            staked       = stake_a + stake_b
            combined_pnl = round(win_pnl + place_pnl, 2)

            day_staked   += staked
            total_staked += staked
            day_bet      += 1
            total_bet    += 1

            won = outcome in ("WIN_A", "WIN_B", "WIN_BOTH")
            if won:
                day_wins   += 1
                total_wins += 1

            day_win_pnl += win_pnl
            day_pl_pnl  += place_pnl
            balance      = round(balance + combined_pnl, 2)

            cur_stake = get_stake(balance)
            if cur_stake > peak_stake:
                peak_stake = cur_stake

            tier_stats[tier]["races"]     += 1
            tier_stats[tier]["win_pnl"]   += win_pnl
            tier_stats[tier]["place_pnl"] += place_pnl
            if won:
                tier_stats[tier]["wins"] += 1

            if show_races:
                top1 = analysis["top1"]
                top2 = analysis["top2"]
                icon = "✅" if won else "❌"
                lbl  = TIER_LABELS.get(tier, "?")
                day_races.append(
                    f"  {icon} {lbl} {race.get('off','?')} {race.get('course','?')}\n"
                    f"     ⭐{top1.get('horse','?')} ({top1.get('sp','?')}) "
                    f"→ pos {top1.get('position','?')} | stake £{stake_a}\n"
                    f"     🔵{top2.get('horse','?')} ({top2.get('sp','?')}) "
                    f"→ pos {top2.get('position','?')} | stake £{stake_b}\n"
                    f"     Win: {'+' if win_pnl>=0 else ''}£{win_pnl:.2f} "
                    f"Place: {'+' if place_pnl>=0 else ''}£{place_pnl:.2f} "
                    f"| Balance: £{balance:.2f}"
                )

        day_combined = round(day_win_pnl + day_pl_pnl, 2)
        total_win_pnl = round(total_win_pnl + day_win_pnl, 2)
        total_pl_pnl  = round(total_pl_pnl + day_pl_pnl, 2)

        if daily or reset_daily:
            sign = "+" if day_combined >= 0 else ""
            sr   = f"{day_wins}/{day_bet}" if day_bet else "0/0"
            print(
                f"  {date_str}  {sign}£{day_combined:7.2f}  "
                f"SR: {sr:<7}  "
                f"EOD bal: £{balance:7.2f}  "
                f"(peak £{peak_stake}/horse)"
                + (f"  [W:{'+' if day_win_pnl>=0 else ''}£{day_win_pnl:.2f} "
                   f"P:{'+' if day_pl_pnl>=0 else ''}£{day_pl_pnl:.2f}]"
                   if include_place else "")
            )
            if show_races:
                for line in day_races:
                    print(line)

    # ── Summary ───────────────────────────────────────────────────────────────
    total_pnl    = round(total_win_pnl + total_pl_pnl, 2)
    total_staked_inc_place = total_staked * 2 if include_place else total_staked

    print(f"\n{'='*70}")
    print(f"  OVERALL RESULTS")
    print(f"{'='*70}")
    print(f"  Days:           {len(files)}")
    print(f"  Races bet:      {total_bet}")
    if total_bet:
        print(f"  Win SR:         {total_wins}/{total_bet} ({total_wins/total_bet*100:.1f}%)")
    print(f"  Win staked:     £{total_staked:.2f}")
    print(f"  Win P&L:        {'+' if total_win_pnl>=0 else ''}£{total_win_pnl:.2f}")
    if include_place:
        print(f"  Place P&L:      {'+' if total_pl_pnl>=0 else ''}£{total_pl_pnl:.2f}")
        print(f"  Combined P&L:   {'+' if total_pnl>=0 else ''}£{total_pnl:.2f}")
        if total_staked:
            print(f"  Win ROI:        {total_win_pnl/total_staked*100:+.1f}%")
            print(f"  Place ROI:      {total_pl_pnl/total_staked*100:+.1f}%  (vs win stakes)")
            print(f"  Combined ROI:   {total_pnl/total_staked*100:+.1f}%  (vs win stakes)")
    else:
        if total_staked:
            print(f"  ROI:            {total_win_pnl/total_staked*100:+.1f}%")
    print(f"  Closing bal:    £{balance:.2f}  (started £{start_balance:.2f})")
    print(f"  Balance x:      {balance/start_balance:.1f}x")

    print(f"\n  By tier:")
    for tier in [TIER_SUPREME, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP]:
        s = tier_stats[tier]
        if s["races"] == 0:
            continue
        lbl      = TIER_LABELS.get(tier, str(tier))
        sr       = f"{s['wins']}/{s['races']} ({s['wins']/s['races']*100:.0f}%)"
        win_sign = "+" if s["win_pnl"] >= 0 else ""
        if include_place:
            pl_sign = "+" if s["place_pnl"] >= 0 else ""
            print(f"    {lbl}  SR: {sr:<16} "
                  f"Win: {win_sign}£{s['win_pnl']:.2f}  "
                  f"Place: {pl_sign}£{s['place_pnl']:.2f}  "
                  f"Combined: {'+' if s['win_pnl']+s['place_pnl']>=0 else ''}£{s['win_pnl']+s['place_pnl']:.2f}")
        else:
            print(f"    {lbl}  SR: {sr:<16} P&L: {win_sign}£{s['win_pnl']:.2f}")

    print(f"\n  Current tier:   £{get_stake(balance)}/horse at £{balance:.2f}")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Betfair WIN+PLACE strategy backtest")
    parser.add_argument("--start-balance", type=float, default=50.0)
    parser.add_argument("--daily",         action="store_true",
                        help="Show per-day P&L breakdown")
    parser.add_argument("--races",         action="store_true",
                        help="Show every race (implies --daily)")
    parser.add_argument("--reset-daily",   action="store_true",
                        help="Reset balance to start-balance each day")
    parser.add_argument("--last",          type=int, metavar="N",
                        help="Only run last N days")
    parser.add_argument("--tier",          choices=["supreme","strong","good","standard","skip"],
                        help="Filter to one tier only")
    parser.add_argument("--no-place",      action="store_true",
                        help="Win bets only — skip place bet simulation")
    args = parser.parse_args()

    tier_map = {
        "supreme":  TIER_SUPREME,
        "strong":   TIER_STRONG,
        "good":     TIER_GOOD,
        "standard": TIER_STD,
        "skip":     TIER_SKIP,
    }
    tier_filter = tier_map.get(args.tier) if args.tier else None

    run_backtest(
        start_balance = args.start_balance,
        daily         = args.daily or args.races,
        tier_filter   = tier_filter,
        show_races    = args.races,
        reset_daily   = args.reset_daily,
        last_n        = args.last,
        include_place = not args.no_place,
    )
