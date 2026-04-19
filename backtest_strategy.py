"""
backtest_strategy.py

Simulates the Betfair WIN strategy across all available historical data
in data/raw/YYYY-MM-DD.json.

Strategy being tested:
  - Back Pick 1 AND Pick 2 to WIN on Betfair exchange
  - Tiered staking: doubles with balance, starting £2/horse from £0
  - Good/Skip races capped at £2/horse regardless of balance
  - Price gates: Pick 2 must be >= 4.0 dec, Pick 1 must be >= 2.0 dec
  - Odds-on Pick 1: cancel Pick 1 stake, double it onto Pick 2
  - Attrition filter: skip Irish NH staying races on soft/heavy
  - Skip AW surface, Heavy going
  - Win = horse finishes 1st (position == "1")
  - Commission: 5% on winnings

Run:
    python backtest_strategy.py
    python backtest_strategy.py --start-balance 10
    python backtest_strategy.py --daily         # show per-day breakdown
    python backtest_strategy.py --tier supreme  # filter to one tier
"""

import os
import sys
import json
import glob
import argparse
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from predict    import score_runner, place_terms, dist_furlongs, to_float, to_int, is_numeric
from predict_v2 import race_confidence, TIER_SUPREME, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP, TIER_LABELS

COMMISSION      = 0.05
MIN_PICK1_PRICE = 2.0
MIN_PICK2_PRICE = 4.0
TIER1_STAKE     = 2

TIER1_STAKE     = 2
STAKE_TIERS = [
    (0,    2),
    (75,   4),
    (150,  6),
]

ATTRITION_VENUES = {"fairyhouse", "cork", "punchestown", "naas", "leopardstown"}
ATTRITION_GOING  = {"soft", "yielding to soft", "soft to heavy", "heavy"}
ATTRITION_DIST_F = 20.0


def get_stake(balance: float) -> int:
    """Return per-horse stake based on current balance. Max £6/horse."""
    stake = 2
    for min_bal, s in STAKE_TIERS:
        if balance >= min_bal:
            stake = s
    return stake


def is_attrition_risk(race: dict) -> bool:
    course    = (race.get("course") or "").lower()
    going     = (race.get("going") or "").lower()
    rtype     = (race.get("type") or "").lower()
    dist_f    = to_float(str(race.get("dist_f", "")).replace("f", ""))
    is_irish  = any(v in course for v in ATTRITION_VENUES)
    is_nh     = any(k in rtype for k in ("hurdle", "chase", "nh flat"))
    is_soft   = any(g in going for g in ATTRITION_GOING)
    is_staying = (dist_f or 0) >= ATTRITION_DIST_F
    return is_irish and is_nh and is_soft and is_staying


def qualifies(race: dict, tier: int) -> bool:
    surface = race.get("surface", "")
    going   = (race.get("going") or "").lower()
    if surface == "AW":
        return False
    if any(k in going for k in ("heavy", "soft to heavy", "heavy to soft")):
        return False
    if is_attrition_risk(race):
        return False
    return True


def _normalise_runner(r: dict) -> dict:
    """
    Strip fields the live bot cannot see at bet time, matching core/api_client.py
    normalise_runner() behaviour:

      - trainer_14d.pl set to 0.0  (racecard API does not return P&L, only runs/wins)
      - jockey_14d passed through as-is (API does return jockey P&L)
      - sp_dec kept as-is (we use actual SP — see NOTE below)

    NOTE: The live bot uses pre-race Bet365 odds, not final SP.  Using actual SP
    makes price gates (Pick2 >= 4.0) slightly different vs live.  We accept this
    caveat — fixing it would require pre-race price data we don't have in the raw
    files.  The trainer_pl fix is the material one (affects pick selection).
    """
    t14 = dict(r.get("trainer_14d") or {})
    t14["pl"] = 0.0   # bot cannot see trainer P&L at bet time
    return {**r, "trainer_14d": t14}


def analyse_race(race: dict):
    """
    Score runners, find top1/top2, assign tier.
    Applies the same normalisation as the live bot so picks match.
    Returns dict with all info needed for simulation, or None if < 2 runners.
    """
    runners = race.get("runners", [])
    if len(runners) < 2:
        return None

    scored = []
    for r in runners:
        norm = _normalise_runner(r)
        sc, sigs = score_runner(norm)
        scored.append((sc, to_float(norm.get("sp_dec"), 999), norm))
    scored.sort(key=lambda x: (-x[0], x[1]))

    top1 = scored[0][2]
    top2 = scored[1][2]

    win_score = scored[0][0]
    race_for_tier = {**race, "runners": runners}
    tier, _ = race_confidence(race_for_tier, win_score)

    return {
        "race":      race,
        "top1":      top1,
        "top2":      top2,
        "tier":      tier,
        "win_score": win_score,
        "n_runners": len(runners),
    }


def simulate_race(analysis: dict, balance: float):
    """
    Given an analysed race and current balance, determine stake and P&L.
    Returns (pnl, stake_a, stake_b, outcome_label, skip_reason).
    """
    tier  = analysis["tier"]
    top1  = analysis["top1"]
    top2  = analysis["top2"]
    race  = analysis["race"]

    if not qualifies(race, tier):
        return None, 0, 0, "FILTER_SKIP", f"surface/going/attrition"

    sp_a = to_float(top1.get("sp_dec"))
    sp_b = to_float(top2.get("sp_dec"))

    if sp_b is None or sp_b < MIN_PICK2_PRICE:
        return None, 0, 0, "PRICE_SKIP", f"Pick2 {sp_b} < {MIN_PICK2_PRICE}"

    base = TIER1_STAKE if tier in (TIER_GOOD, TIER_SKIP) else get_stake(balance)

    # Odds-on redirect
    if sp_a is None or sp_a < MIN_PICK1_PRICE:
        stake_a = 0
        stake_b = base * 2
        redirect = True
    else:
        stake_a = base
        stake_b = base
        redirect = False

    pos_a = top1.get("position", "")
    pos_b = top2.get("position", "")

    won_a = is_numeric(pos_a) and int(pos_a) == 1
    won_b = is_numeric(pos_b) and int(pos_b) == 1

    pnl = 0.0
    if redirect:
        if won_b:
            pnl = round(stake_b * (sp_b - 1) * (1 - COMMISSION), 2)
        else:
            pnl = -stake_b
    else:
        if won_a:
            pnl += round(stake_a * (sp_a - 1) * (1 - COMMISSION), 2)
        else:
            pnl -= stake_a
        if won_b:
            pnl += round(stake_b * (sp_b - 1) * (1 - COMMISSION), 2)
        else:
            pnl -= stake_b

    total_staked = stake_a + stake_b
    if won_a and won_b:
        outcome = "WIN_BOTH"
    elif won_a:
        outcome = "WIN_A"
    elif won_b:
        outcome = "WIN_B"
    else:
        outcome = "LOSS"

    return pnl, stake_a, stake_b, outcome, ""


def run_backtest(start_balance: float = 10.0,
                 daily: bool = False,
                 tier_filter: int | None = None,
                 show_races: bool = False,
                 reset_daily: bool = False,
                 last_n: int | None = None):

    data_dir = os.path.join(os.path.dirname(__file__), "data", "raw")
    files    = sorted(glob.glob(os.path.join(data_dir, "*.json")))
    if last_n:
        files = files[-last_n:]

    if not files:
        print("No data files found in data/raw/")
        return

    balance       = start_balance
    total_pnl     = 0.0
    total_races   = 0
    total_bet     = 0
    total_wins    = 0
    total_staked  = 0.0

    tier_stats = defaultdict(lambda: {"races": 0, "wins": 0, "pnl": 0.0})

    mode_label = "DAILY RESET (£10/day)" if reset_daily else "COMPOUNDING"
    print(f"\n{'='*70}")
    print(f"  BETFAIR WIN STRATEGY BACKTEST — {len(files)} days  [{mode_label}]")
    print(f"  Starting balance: £{start_balance:.2f}")
    print(f"  Strategy: Back P1+P2 to win | Good/Skip → £2 cap")
    print(f"{'='*70}\n")

    for fpath in files:
        date_str = os.path.basename(fpath).replace(".json", "")
        with open(fpath) as f:
            data = json.load(f)

        races = data.get("results", [])
        day_pnl      = 0.0
        day_bet      = 0
        day_wins     = 0
        day_staked   = 0.0
        if reset_daily:
            balance = start_balance
        day_bal_open = balance

        day_races = []

        for race in races:
            total_races += 1
            analysis = analyse_race(race)
            if not analysis:
                continue

            tier = analysis["tier"]
            if tier_filter is not None and tier != tier_filter:
                continue

            pnl, stake_a, stake_b, outcome, reason = simulate_race(analysis, balance)
            if pnl is None:
                continue

            staked = stake_a + stake_b
            day_staked  += staked
            total_staked += staked
            day_bet     += 1
            total_bet   += 1

            won = outcome in ("WIN_A", "WIN_B", "WIN_BOTH")
            if won:
                day_wins   += 1
                total_wins += 1

            day_pnl += pnl
            balance  = round(balance + pnl, 2)

            tier_stats[tier]["races"] += 1
            tier_stats[tier]["pnl"]   += pnl
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
                    f"     P&L: {'+' if pnl>=0 else ''}£{pnl:.2f} | Balance: £{balance:.2f}"
                )

        total_pnl = round(total_pnl + day_pnl, 2)

        if daily or reset_daily:
            sign     = "+" if day_pnl >= 0 else ""
            sr       = f"{day_wins}/{day_bet}" if day_bet else "0/0"
            tier_str = f"£{get_stake(balance)}/horse" if not reset_daily else f"peak £{get_stake(balance)}/horse"
            print(
                f"  {date_str}  {sign}£{day_pnl:7.2f}  "
                f"SR: {sr:<7}  "
                f"EOD bal: £{balance:7.2f}  "
                f"({tier_str})"
            )
            if show_races:
                for line in day_races:
                    print(line)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  OVERALL RESULTS")
    print(f"{'='*70}")
    print(f"  Days:          {len(files)}")
    print(f"  Races bet:     {total_bet}")
    print(f"  Wins:          {total_wins} ({total_wins/total_bet*100:.1f}%)" if total_bet else "  Wins:          0")
    print(f"  Total staked:  £{total_staked:.2f}")
    print(f"  Net P&L:       {'+' if total_pnl>=0 else ''}£{total_pnl:.2f}")
    print(f"  ROI:           {total_pnl/total_staked*100:+.1f}%" if total_staked else "  ROI: n/a")
    print(f"  Closing bal:   £{balance:.2f}  (started £{start_balance:.2f})")
    print(f"  Balance x:     {balance/start_balance:.1f}x")

    print(f"\n  By tier:")
    for tier in [TIER_SUPREME, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP]:
        s = tier_stats[tier]
        if s["races"] == 0:
            continue
        lbl  = TIER_LABELS.get(tier, str(tier))
        sr   = f"{s['wins']}/{s['races']} ({s['wins']/s['races']*100:.0f}%)"
        sign = "+" if s["pnl"] >= 0 else ""
        roi  = s["pnl"] / (s["races"] * 4) * 100  # rough: avg £4 staked per race
        print(f"    {lbl}  SR: {sr:<16} P&L: {sign}£{s['pnl']:.2f}")

    print(f"\n  Compounding breakdown:")
    for (min_bal, stake) in STAKE_TIERS:
        next_tier = f"£{STAKE_TIERS[STAKE_TIERS.index((min_bal,stake))+1][0]}" if STAKE_TIERS.index((min_bal,stake)) + 1 < len(STAKE_TIERS) else "+"
        if min_bal <= balance:
            pass  # would show history but we don't track tier timestamps
    print(f"    Current tier: £{get_stake(balance)}/horse at £{balance:.2f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Betfair WIN strategy backtest")
    parser.add_argument("--start-balance", type=float, default=10.0)
    parser.add_argument("--daily",       action="store_true", help="Show per-day P&L")
    parser.add_argument("--races",       action="store_true", help="Show every race (implies --daily)")
    parser.add_argument("--reset-daily", action="store_true", help="Reset balance to start each day")
    parser.add_argument("--last",        type=int, default=None, metavar="N", help="Only run last N days")
    parser.add_argument("--tier",        choices=["supreme","strong","good","standard","skip"],
                        help="Filter to one tier only")
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
    )
