"""
scratch_clean_vs_contaminated.py
=================================
Runs two backtests on the same dates and compares results:

  A) CLEAN — uses pre-race card data for model inputs (RPR, OR, TSR,
     form, trainer/jockey stats) and post-race results for outcomes only.

  B) CONTAMINATED — uses post-race results data for both inputs and
     outcomes. Racing Post updates RPR after races run, so this
     inadvertently uses look-ahead information.

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

COMMISSION = 0.05
BET_TIERS  = {TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD}
TIER_NAMES = {TIER_ELITE:"ELITE", TIER_STRONG:"STRONG", TIER_GOOD:"GOOD", TIER_STD:"STD"}

def tof(v):
    try:
        f = float(str(v).strip())
        return f if f > 0 else None
    except: return None

def get_pos(r):
    try: return int(str(r.get("position","")).strip())
    except: return None

def normalise_time(t):
    """Convert both 1:50 (12hr results) and 13:50 (24hr card) to 13:50."""
    if not t: return ""
    t = str(t).strip()
    parts = t.split(":")
    if len(parts) != 2: return t
    try: h, m = int(parts[0]), int(parts[1])
    except: return t
    if 1 <= h <= 9:
        h += 12
    return f"{h:02d}:{m:02d}"

def field_ok(runners, race):
    n = len(runners)
    if n < 2: return False
    rt = str(race.get("type","") or "").lower()
    is_jump = any(t in rt for t in ("chase","hurdle","nh flat","national hunt"))
    cls = str(race.get("race_class","") or race.get("class","") or "").replace("Class","").strip()
    if cls in ("1","2") and is_jump: return False
    if is_jump and n > 12: return False
    if not is_jump and n > 20: return False
    return True

def build_results_index(results_races):
    by_label = {}
    for race in results_races:
        off    = normalise_time(race.get("off",""))
        course = str(race.get("course","") or "").lower().strip()
        key    = (course, off)
        horse_map = {}
        for r in race.get("runners",[]):
            hid = r.get("horse_id","")
            pos = get_pos(r)
            bsp = tof(r.get("bsp") or r.get("bsp_dec")) or tof(r.get("sp_dec"))
            if hid and pos is not None:
                horse_map[hid] = {"pos": pos, "bsp": bsp}
        by_label[key] = horse_map
    return by_label

CARDS_DIR   = "data/cards"
RESULTS_DIR = "data/raw"

card_dates    = {fp.replace(".json","") for fp in os.listdir(CARDS_DIR)    if fp.endswith(".json") and fp[:4].isdigit()}
results_dates = {fp.replace(".json","") for fp in os.listdir(RESULTS_DIR) if fp.endswith(".json") and fp[:4].isdigit()}
common_dates  = sorted(card_dates & results_dates)

print(f"Dates with both card and results: {len(common_dates)}")
print(f"  {common_dates[0]} to {common_dates[-1]}")
print()

def run_backtest(use_cards):
    records = []
    for date_str in common_dates:
        try:
            with open(f"{RESULTS_DIR}/{date_str}.json") as f: rd = json.load(f)
        except: continue
        results_races    = rd.get("results") or rd.get("races") or []
        results_by_label = build_results_index(results_races)

        if use_cards:
            try:
                with open(f"{CARDS_DIR}/{date_str}.json") as f: cd = json.load(f)
            except: continue
            input_races = cd.get("races") or cd.get("racecards") or []
            rkey = "all_runners"
        else:
            input_races = results_races
            rkey = "runners"

        for race in input_races:
            runners = race.get(rkey,[])
            if not runners: continue

            off    = normalise_time(race.get("off",""))
            course = str(race.get("course","") or "").lower().strip()
            key    = (course, off)
            rmap   = results_by_label.get(key,{})

            if not field_ok(runners, race): continue

            if use_cards:
                if not any(v.get("pos")==1 for v in rmap.values()): continue
            else:
                if not any(get_pos(r)==1 for r in runners): continue

            raw_meta = {
                "class":   str(race.get("race_class","") or race.get("class","") or ""),
                "surface": race.get("surface","Turf") or "Turf",
                "type":    race.get("type","") or "",
            }

            tc, p1, _, _ = get_blended_picks(runners, mw_p1=0.1, mw_p2=0.4, raw_race=raw_meta)
            if not p1 or tc not in BET_TIERS: continue

            p1score = _sp_free_score(p1)
            if p1score < 3: continue

            if use_cards:
                hid   = p1.get("horse_id","")
                res   = rmap.get(hid,{})
                p1sp  = res.get("bsp")
                p1pos = res.get("pos")
                if not p1sp:
                    for o in (p1.get("odds") or []):
                        d = tof(o.get("decimal"))
                        if d and d > 1.0: p1sp = d; break
                if not p1sp or p1pos is None: continue
            else:
                p1sp  = tof(p1.get("sp_dec") or p1.get("bsp_dec"))
                p1pos = get_pos(p1)
                if not p1sp or p1pos is None: continue

            stake = win_stake_for_pick(p1sp, p1score)
            if stake == 0: continue

            won = p1pos == 1
            pnl = round(stake*(p1sp-1)*(1-COMMISSION),2) if won else -stake
            records.append({"date":date_str,"tier":tc,"sp":p1sp,"score":p1score,"stake":stake,"won":won,"pnl":pnl})

    return records

print("Running contaminated backtest...")
contaminated = run_backtest(use_cards=False)
print(f"  {len(contaminated)} qualifying bets")

print("Running clean backtest...")
clean = run_backtest(use_cards=True)
print(f"  {len(clean)} qualifying bets")

# Diagnose matching if clean is very low
if len(clean) < len(contaminated) * 0.3:
    print()
    print("WARNING: Clean bets much lower than contaminated — checking match rate...")
    try:
        with open(f"{CARDS_DIR}/{common_dates[-1]}.json") as f: cd = json.load(f)
        with open(f"{RESULTS_DIR}/{common_dates[-1]}.json") as f: rd = json.load(f)
        card_races   = cd.get("races") or []
        result_races = rd.get("results") or rd.get("races") or []
        rkeys = {(str(r.get("course","")).lower().strip(), normalise_time(r.get("off",""))) for r in result_races}
        matched = sum(1 for r in card_races if (str(r.get("course","")).lower().strip(), normalise_time(r.get("off",""))) in rkeys)
        print(f"  Last date ({common_dates[-1]}): {matched}/{len(card_races)} card races matched to results")
        print(f"  Sample result keys: {list(rkeys)[:5]}")
        print(f"  Sample card keys:   {[(str(r.get('course','')).lower().strip(), normalise_time(r.get('off',''))) for r in card_races[:5]]}")
    except Exception as e:
        print(f"  Diagnostic failed: {e}")
    print()

print()

def summarise(label, records):
    if not records: print(f"{label}: no records\n"); return
    n=len(records); wins=sum(1 for r in records if r["won"])
    pnl=sum(r["pnl"] for r in records); staked=sum(r["stake"] for r in records)
    print(f"{'='*65}")
    print(f"{label}")
    print(f"{'='*65}")
    print(f"  Bets:         {n}")
    print(f"  Wins:         {wins} ({wins/n*100:.1f}%)")
    print(f"  Total staked: £{staked:.2f}")
    print(f"  Total P&L:    £{pnl:+.2f}")
    print(f"  ROI:          {pnl/staked*100:.1f}%")
    print(f"  Per bet:      £{pnl/n:+.3f}")
    print()
    print(f"  {'Tier':<10} {'N':>5} {'Win%':>6} {'P&L':>10} {'Per bet':>8}")
    print(f"  {'-'*44}")
    for tier in [TIER_ELITE,TIER_STRONG,TIER_GOOD,TIER_STD]:
        b=[r for r in records if r["tier"]==tier]
        if not b: continue
        bw=sum(1 for r in b if r["won"]); bp=sum(r["pnl"] for r in b)
        print(f"  {TIER_NAMES[tier]:<10} {len(b):>5} {bw/len(b)*100:>5.1f}% {bp:>+10.2f} {bp/len(b):>+8.3f}")
    print()
    print(f"  {'SP band':<12} {'N':>5} {'Win%':>6} {'P&L':>10} {'Per bet':>8}")
    print(f"  {'-'*46}")
    for lo,hi,lbl in [(1,2,"<2/1"),(2,3,"2-3/1"),(3,5,"3-5/1"),(5,8,"5-8/1"),(8,13,"8-13/1"),(13,999,"13/1+")]:
        b=[r for r in records if lo<=r["sp"]<hi]
        if not b: continue
        bw=sum(1 for r in b if r["won"]); bp=sum(r["pnl"] for r in b)
        print(f"  {lbl:<12} {len(b):>5} {bw/len(b)*100:>5.1f}% {bp:>+10.2f} {bp/len(b):>+8.3f}")
    print()
    by_date=defaultdict(float)
    for r in records: by_date[r["date"]]+=r["pnl"]
    daily=list(by_date.values()); neg=[d for d in daily if d<0]
    print(f"  Days: {len(daily)}  Loss days: {len(neg)} ({len(neg)/len(daily)*100:.0f}%)  Best: £{max(daily):+.2f}  Worst: £{min(daily):+.2f}  Avg: £{sum(daily)/len(daily):+.2f}")
    print()

summarise("A) CONTAMINATED (results for inputs + outcomes)", contaminated)
summarise("B) CLEAN (card data for inputs, results for outcomes)", clean)

print("="*65)
print("HEAD TO HEAD SUMMARY")
print("="*65)
c_n=len(contaminated); k_n=len(clean)
c_w=sum(1 for r in contaminated if r["won"]); k_w=sum(1 for r in clean if r["won"])
c_pnl=sum(r["pnl"] for r in contaminated); k_pnl=sum(r["pnl"] for r in clean)
c_s=sum(r["stake"] for r in contaminated); k_s=sum(r["stake"] for r in clean)
print(f"  {'Metric':<25} {'Contaminated':>15} {'Clean':>15}")
print(f"  {'-'*57}")
print(f"  {'Bets':<25} {c_n:>15} {k_n:>15}")
if c_n and k_n:
    print(f"  {'Win rate':<25} {c_w/c_n*100:>14.1f}% {k_w/k_n*100:>14.1f}%")
    print(f"  {'Total P&L':<25} {'£'+f'{c_pnl:+.2f}':>15} {'£'+f'{k_pnl:+.2f}':>15}")
    print(f"  {'Per bet':<25} {'£'+f'{c_pnl/c_n:+.3f}':>15} {'£'+f'{k_pnl/k_n:+.3f}':>15}")
    print(f"  {'ROI':<25} {c_pnl/c_s*100:>14.1f}% {k_pnl/k_s*100:>14.1f}%")
print()
if k_n < c_n * 0.5:
    print("NOTE: Clean bets still much lower than contaminated — card-results matching incomplete.")
else:
    print("The gap between these figures is the contamination effect.")
    print("The clean figure is the realistic expectation going forward.")
print()
print("Done.")
