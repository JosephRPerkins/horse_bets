"""
scratch_clean_vs_contaminated.py
=================================
Runs two backtests on the same dates and compares results:

  A) CLEAN — uses pre-race card data for model inputs (RPR, OR, TSR,
     form, trainer/jockey stats) and post-race results for outcomes only.
     This is what the model actually has available at bet time.

  B) CONTAMINATED — uses post-race results data for both inputs and
     outcomes. Racing Post updates RPR after races run, so this
     inadvertently uses look-ahead information.

The gap between A and B shows how much of the historical edge
was an artefact of backtest contamination.

Only dates where BOTH a card file and a results file exist are included.

Run from ~/horse_bets_v3:
  python3 scratch_clean_vs_contaminated.py
"""

import json, os, sys
from collections import defaultdict
sys.path.insert(0, os.path.dirname(__file__))

from predict_v2 import (
    get_blended_picks, TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD,
    _sp_free_score,
)
from betfair.strategy import win_stake_for_pick
from predict import place_terms

COMMISSION = 0.05
BET_TIERS  = {TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD}
TIER_NAMES = {
    TIER_ELITE:  "ELITE",
    TIER_STRONG: "STRONG",
    TIER_GOOD:   "GOOD",
    TIER_STD:    "STD",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def tof(v):
    try:
        f = float(str(v).strip())
        return f if f > 0 else None
    except: return None

def get_pos(r):
    try: return int(str(r.get("position", "")).strip())
    except: return None

def field_ok(runners, race, is_card=False):
    """Apply the same race filters as the live bot."""
    n = len(runners)
    if n < 2: return False
    rt = str(race.get("type", "") or "").lower()
    is_jump = any(t in rt for t in ("chase", "hurdle", "nh flat", "national hunt"))
    cls = str(
        race.get("race_class", "") or race.get("class", "") or ""
    ).replace("Class", "").strip()
    if cls in ("1", "2") and is_jump: return False
    if is_jump and n > 12: return False
    if not is_jump and n > 20: return False
    return True

def runner_won(runner, results_by_id):
    """Look up actual finishing position from results file."""
    hid = runner.get("horse_id", "")
    return results_by_id.get(hid)

def build_results_index(results_races):
    """
    Build lookup: race_id -> {horse_id -> position}
    Also build: (course, off) -> {horse_id -> (position, bsp)}
    """
    by_race  = {}
    by_label = {}
    for race in results_races:
        rid   = race.get("race_id", "")
        off   = str(race.get("off", "") or "")
        course = str(race.get("course", "") or "")
        key   = (course.lower().strip(), off.strip())
        runners = race.get("runners", [])
        horse_map = {}
        for r in runners:
            hid = r.get("horse_id", "")
            pos = get_pos(r)
            bsp = tof(r.get("bsp") or r.get("bsp_dec"))
            if hid and pos is not None:
                horse_map[hid] = {"pos": pos, "bsp": bsp}
        if rid:
            by_race[rid] = horse_map
        by_label[key] = horse_map
    return by_race, by_label

# ── Load date pairs ───────────────────────────────────────────────────────────

CARDS_DIR   = "data/cards"
RESULTS_DIR = "data/raw"

card_dates    = set()
results_dates = set()

for fp in os.listdir(CARDS_DIR):
    if fp.endswith(".json") and fp[:4].isdigit():
        card_dates.add(fp.replace(".json", ""))

for fp in os.listdir(RESULTS_DIR):
    if fp.endswith(".json") and fp[:4].isdigit():
        results_dates.add(fp.replace(".json", ""))

common_dates = sorted(card_dates & results_dates)
print(f"Dates with both card and results: {len(common_dates)}")
print(f"  {common_dates[0]} to {common_dates[-1]}")
print()

# ── Run both backtests ────────────────────────────────────────────────────────

def run_backtest(label, use_cards_for_inputs):
    """
    Run backtest. If use_cards_for_inputs=True, use card runner data
    for model inputs and results for outcomes. Otherwise use results
    for both (contaminated).
    """
    records = []

    for date_str in common_dates:
        # Load results file (always needed for outcomes)
        try:
            with open(f"{RESULTS_DIR}/{date_str}.json") as f:
                results_data = json.load(f)
        except: continue
        results_races = results_data.get("results") or results_data.get("races") or []
        _, results_by_label = build_results_index(results_races)

        if use_cards_for_inputs:
            # Load card file for model inputs
            try:
                with open(f"{CARDS_DIR}/{date_str}.json") as f:
                    card_data = json.load(f)
            except: continue
            input_races = card_data.get("races") or card_data.get("racecards") or []
            runner_key  = "all_runners"
        else:
            input_races = results_races
            runner_key  = "runners"

        for race in input_races:
            runners = race.get(runner_key, [])
            if not runners: continue

            # For card races, we need to match to results by course+off
            off    = str(race.get("off", "") or "")
            course = str(race.get("course", "") or "")
            label_key = (course.lower().strip(), off.strip())
            result_map = results_by_label.get(label_key, {})

            if not field_ok(runners, race, is_card=use_cards_for_inputs):
                continue

            # For contaminated backtest, need at least one result
            if not use_cards_for_inputs:
                if not any(get_pos(r) == 1 for r in runners):
                    continue
            else:
                # For clean backtest, verify results exist and have a winner
                if not any(
                    v.get("pos") == 1 for v in result_map.values()
                ):
                    continue

            raw_meta = {
                "class":   str(race.get("race_class", "") or race.get("class", "") or ""),
                "surface": race.get("surface", "Turf") or "Turf",
                "type":    race.get("type", "") or "",
            }

            tc, p1, p2, _ = get_blended_picks(
                runners, mw_p1=0.1, mw_p2=0.4, raw_race=raw_meta
            )
            if not p1 or tc not in BET_TIERS: continue

            p1score = _sp_free_score(p1)
            if p1score < 3: continue

            # Get SP — for clean backtest use BSP from results, fallback to morning odds
            if use_cards_for_inputs:
                hid = p1.get("horse_id", "")
                res = result_map.get(hid, {})
                bsp = res.get("bsp")
                p1pos = res.get("pos")
                # Use BSP as the settlement price
                p1sp = bsp
                if not p1sp:
                    # Fallback: use morning decimal odds
                    odds = p1.get("odds", [])
                    if odds:
                        for o in odds:
                            d = tof(o.get("decimal"))
                            if d and d > 1.0:
                                p1sp = d
                                break
                if not p1sp or p1pos is None: continue
            else:
                p1sp  = tof(p1.get("sp_dec") or p1.get("bsp_dec"))
                p1pos = get_pos(p1)
                if not p1sp or p1pos is None: continue

            stake = win_stake_for_pick(p1sp, p1score)
            if stake == 0: continue

            won  = p1pos == 1
            pnl  = round(stake * (p1sp - 1) * (1 - COMMISSION), 2) if won else -stake

            records.append({
                "date":  date_str,
                "tier":  tc,
                "sp":    p1sp,
                "score": p1score,
                "stake": stake,
                "won":   won,
                "pnl":   pnl,
            })

    return records

# ── Run and compare ───────────────────────────────────────────────────────────

print("Running contaminated backtest (results data for inputs + outcomes)...")
contaminated = run_backtest("contaminated", use_cards_for_inputs=False)
print(f"  {len(contaminated)} qualifying bets")

print("Running clean backtest (card data for inputs, results for outcomes)...")
clean = run_backtest("clean", use_cards_for_inputs=True)
print(f"  {len(clean)} qualifying bets")
print()

def summarise(label, records):
    if not records: return
    n    = len(records)
    wins = sum(1 for r in records if r["won"])
    pnl  = sum(r["pnl"] for r in records)
    staked = sum(r["stake"] for r in records)
    print(f"{'='*65}")
    print(f"{label}")
    print(f"{'='*65}")
    print(f"  Bets:        {n}")
    print(f"  Wins:        {wins} ({wins/n*100:.1f}%)")
    print(f"  Total staked: £{staked:.2f}")
    print(f"  Total P&L:   £{pnl:+.2f}")
    print(f"  ROI:         {pnl/staked*100:.1f}%")
    print(f"  Per bet:     £{pnl/n:+.3f}")
    print()

    # By tier
    print(f"  {'Tier':<10} {'N':>5} {'Win%':>6} {'P&L':>10} {'Per bet':>8}")
    print(f"  {'-'*42}")
    for tier in [TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD]:
        b = [r for r in records if r["tier"] == tier]
        if not b: continue
        bw = sum(1 for r in b if r["won"])
        bp = sum(r["pnl"] for r in b)
        print(f"  {TIER_NAMES[tier]:<10} {len(b):>5} {bw/len(b)*100:>5.1f}% {bp:>+10.2f} {bp/len(b):>+8.3f}")
    print()

    # By SP band
    print(f"  {'SP band':<12} {'N':>5} {'Win%':>6} {'P&L':>10} {'Per bet':>8}")
    print(f"  {'-'*44}")
    for lo, hi, lbl in [
        (1.0, 2.0,  "<2/1"),
        (2.0, 3.0,  "2-3/1"),
        (3.0, 5.0,  "3-5/1"),
        (5.0, 8.0,  "5-8/1"),
        (8.0, 13.0, "8-13/1"),
        (13.0, 999, "13/1+"),
    ]:
        b = [r for r in records if lo <= r["sp"] < hi]
        if not b: continue
        bw = sum(1 for r in b if r["won"])
        bp = sum(r["pnl"] for r in b)
        print(f"  {lbl:<12} {len(b):>5} {bw/len(b)*100:>5.1f}% {bp:>+10.2f} {bp/len(b):>+8.3f}")
    print()

    # Daily P&L
    by_date = defaultdict(float)
    for r in records:
        by_date[r["date"]] += r["pnl"]
    daily = list(by_date.values())
    neg   = [d for d in daily if d < 0]
    print(f"  Days:        {len(daily)}")
    print(f"  Loss days:   {len(neg)} ({len(neg)/len(daily)*100:.0f}%)")
    print(f"  Best day:    £{max(daily):+.2f}")
    print(f"  Worst day:   £{min(daily):+.2f}")
    print(f"  Avg day:     £{sum(daily)/len(daily):+.2f}")
    print()

summarise("A) CONTAMINATED BACKTEST (results data — what we've been using)", contaminated)
summarise("B) CLEAN BACKTEST (card data for inputs — genuine pre-race signals)", clean)

# ── Head to head on same dates ────────────────────────────────────────────────
print("="*65)
print("HEAD TO HEAD SUMMARY")
print("="*65)
c_wins = sum(1 for r in contaminated if r["won"])
k_wins = sum(1 for r in clean       if r["won"])
c_pnl  = sum(r["pnl"] for r in contaminated)
k_pnl  = sum(r["pnl"] for r in clean)
c_n    = len(contaminated)
k_n    = len(clean)

print(f"  {'Metric':<25} {'Contaminated':>15} {'Clean':>15}")
print(f"  {'-'*57}")
print(f"  {'Bets':<25} {c_n:>15} {k_n:>15}")
print(f"  {'Win rate':<25} {c_wins/c_n*100:>14.1f}% {k_wins/k_n*100:>14.1f}%")
print(f"  {'Total P&L':<25} {'£'+f'{c_pnl:+.2f}':>15} {'£'+f'{k_pnl:+.2f}':>15}")
print(f"  {'Per bet':<25} {'£'+f'{c_pnl/c_n:+.3f}':>15} {'£'+f'{k_pnl/k_n:+.3f}':>15}")
print(f"  {'ROI':<25} {c_pnl/sum(r['stake'] for r in contaminated)*100:>14.1f}% {k_pnl/sum(r['stake'] for r in clean)*100:>14.1f}%")
print()
print("The gap between these two figures is the contamination effect.")
print("The clean figure is what you should expect going forward.")
print()
print("Done.")
