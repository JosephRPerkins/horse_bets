"""
scratch_variance_check.py
=========================
Compares variance of recommended strategy vs current flat £2 approach.
Checks losing run lengths, daily P&L distribution, and stake breakdown.

Run from ~/horse_bets_v3:
  python3 scratch_variance_check.py
"""

import json, os, sys
from collections import defaultdict
sys.path.insert(0, os.path.dirname(__file__))
from predict import place_terms
from predict_v2 import get_blended_picks, TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD, _sp_free_score

COMMISSION = 0.05
BET_TIERS  = {TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD}

def tof(v):
    try:
        f = float(str(v).strip())
        return f if f > 0 else None
    except: return None

def get_pos(r):
    try: return int(str(r.get("position","")).strip())
    except: return None

def field_ok(runners, race):
    n = len(runners)
    if n < 2: return False
    rt = str(race.get("type","") or "").lower()
    is_jump = any(t in rt for t in ("chase","hurdle","nh flat","national hunt"))
    cls = str(race.get("class","") or "").replace("Class","").strip()
    if cls in ("1","2") and is_jump: return False
    if is_jump and n > 12: return False
    if not is_jump and n > 20: return False
    return True

def band_stake(sp):
    """Recommended variable stake. 0 = skip."""
    if not sp or sp < 3.0: return 0
    if sp < 6.0: return 2
    if sp < 10.0: return 4
    return 6

# ── Load all qualifying races chronologically ─────────────────────────────────

print("Loading races...")
all_races = []
for fp in sorted(os.listdir("data/raw")):
    if not fp.endswith(".json"): continue
    date_str = fp.replace(".json","")
    try:
        with open(f"data/raw/{fp}") as f: d = json.load(f)
    except: continue
    for race in (d.get("results") or d.get("races") or []):
        if not field_ok(race.get("runners",[]), race): continue
        runners = race.get("runners",[])
        if not any(get_pos(r) == 1 for r in runners): continue
        race["_date"] = date_str
        all_races.append(race)

print(f"Loaded {len(all_races)} races")
print()

# ── Build bet sequences ───────────────────────────────────────────────────────

rec_bets  = []   # recommended: score>=3, variable stake
curr_bets = []   # current: flat £2 P1 always

for race in all_races:
    runners  = race.get("runners",[])
    raw_meta = {
        "class":   str(race.get("class","") or ""),
        "surface": race.get("surface","Turf") or "Turf",
        "type":    race.get("type","") or "",
    }
    tc, p1, p2, _ = get_blended_picks(runners, mw_p1=0.1, mw_p2=0.4, raw_race=raw_meta)
    if not p1 or tc not in BET_TIERS: continue

    p1sp  = tof(p1.get("sp_dec")); p1pos = get_pos(p1)
    if p1pos is None or not p1sp: continue

    score = _sp_free_score(p1)
    won   = p1pos == 1
    date  = race["_date"]

    # Current: flat £2 always
    curr_pnl = round(2*(p1sp-1)*(1-COMMISSION),2) if won else -2.0
    curr_bets.append({"date":date,"won":won,"stake":2,"sp":p1sp,"pnl":curr_pnl})

    # Recommended: score>=3 + variable stake
    s = band_stake(p1sp)
    if score >= 3 and s > 0:
        rec_pnl = round(s*(p1sp-1)*(1-COMMISSION),2) if won else -s
        rec_bets.append({"date":date,"won":won,"stake":s,"sp":p1sp,"pnl":rec_pnl})

# ── Losing run analysis ───────────────────────────────────────────────────────

def losing_runs(bets):
    runs = []; current = 0
    for b in bets:
        if not b["won"]:
            current += 1
        else:
            if current > 0: runs.append(current)
            current = 0
    if current > 0: runs.append(current)
    return runs

def analyse(bets, label):
    if not bets: return
    n = len(bets)
    wins = sum(1 for b in bets if b["won"])
    total_pnl = sum(b["pnl"] for b in bets)
    total_staked = sum(b["stake"] for b in bets)
    avg_stake = total_staked / n

    runs = losing_runs(bets)
    max_run = max(runs) if runs else 0
    avg_run = sum(runs)/len(runs) if runs else 0

    # Cost of max losing run
    # Use avg stake on losses for realistic estimate
    loss_stakes = [b["stake"] for b in bets if not b["won"]]
    avg_loss_stake = sum(loss_stakes)/len(loss_stakes) if loss_stakes else 0
    max_run_cost_avg = max_run * avg_loss_stake
    max_run_cost_worst = max_run * max(b["stake"] for b in bets)

    # Daily P&L
    by_date = defaultdict(float)
    for b in bets: by_date[b["date"]] += b["pnl"]
    daily = sorted(by_date.values())
    neg_days = sum(1 for p in daily if p < 0)
    zero_days = sum(1 for p in daily if p == 0)

    print(f"  {label}")
    print(f"  {'—'*55}")
    print(f"  Total bets:          {n:>6,}")
    print(f"  Win rate:            {wins/n*100:>6.0f}%  ({wins}/{n})")
    print(f"  Total P&L:           £{total_pnl:>+8.2f}")
    print(f"  Total staked:        £{total_staked:>8.2f}")
    print(f"  ROI:                 {total_pnl/total_staked*100:>6.1f}%")
    print(f"  Avg stake per bet:   £{avg_stake:>6.2f}")
    print(f"  Per bet P&L:         £{total_pnl/n:>+6.2f}")
    print()
    print(f"  Losing runs:")
    print(f"    Total runs:        {len(runs):>6}")
    print(f"    Avg run length:    {avg_run:>6.1f}")
    print(f"    Max run:           {max_run:>6}")
    print(f"    Runs >= 5:         {sum(1 for r in runs if r>=5):>6}")
    print(f"    Runs >= 10:        {sum(1 for r in runs if r>=10):>6}")
    print(f"    Runs >= 15:        {sum(1 for r in runs if r>=15):>6}")
    print(f"    Max run cost (avg stake £{avg_loss_stake:.2f}): £{max_run_cost_avg:.0f}")
    print(f"    Max run cost (worst case): £{max_run_cost_worst:.0f}")
    print()
    print(f"  Daily P&L ({len(daily)} days with bets):")
    print(f"    Loss days:         {neg_days:>6} ({neg_days/len(daily)*100:.0f}%)")
    print(f"    Zero days:         {zero_days:>6}")
    print(f"    Best day:          £{max(daily):>+8.2f}")
    print(f"    Worst day:         £{min(daily):>+8.2f}")
    print(f"    Avg day:           £{sum(daily)/len(daily):>+8.2f}")

    # SP breakdown
    bands = [(3,6,"3-6/1  £2"),(6,10,"6-10/1 £4"),(10,999,"10/1+  £6")]
    print()
    print(f"  SP distribution (recommended only):")
    for lo,hi,lbl in bands:
        band = [b for b in bets if lo <= b["sp"] < hi]
        if not band: continue
        bw = sum(1 for b in band if b["won"])
        print(f"    {lbl}: {len(band):>4} bets ({len(band)/n*100:.0f}%), "
              f"{bw/len(band)*100:.0f}% win rate, "
              f"P&L £{sum(b['pnl'] for b in band):>+8.2f}")

    print()

print("=" * 60)
print("RECOMMENDED: score>=3, variable stake (£2/£4/£6)")
print("=" * 60)
analyse(rec_bets, "Recommended strategy")

print("=" * 60)
print("CURRENT: flat £2 P1 always")
print("=" * 60)
analyse(curr_bets, "Current strategy")

# ── Head-to-head summary ──────────────────────────────────────────────────────

print("=" * 60)
print("HEAD-TO-HEAD COMPARISON")
print("=" * 60)

rec_runs  = losing_runs(rec_bets)
curr_runs = losing_runs(curr_bets)

rec_daily  = defaultdict(float)
curr_daily = defaultdict(float)
for b in rec_bets:  rec_daily[b["date"]]  += b["pnl"]
for b in curr_bets: curr_daily[b["date"]] += b["pnl"]

print(f"  {'Metric':<30} {'Recommended':>14} {'Current':>12}")
print(f"  {'-'*58}")
print(f"  {'Bets':<30} {len(rec_bets):>14,} {len(curr_bets):>12,}")
print(f"  {'Win rate':<30} {sum(1 for b in rec_bets if b['won'])/len(rec_bets)*100:>13.0f}% {sum(1 for b in curr_bets if b['won'])/len(curr_bets)*100:>11.0f}%")
print(f"  {'Avg stake':<30} £{sum(b['stake'] for b in rec_bets)/len(rec_bets):>12.2f} £{2.0:>10.2f}")
print(f"  {'Max losing run':<30} {max(rec_runs) if rec_runs else 0:>14} {max(curr_runs) if curr_runs else 0:>12}")
print(f"  {'Max run cost':<30} £{max(rec_runs)*sum(b['stake'] for b in rec_bets if not b['won'])/max(1,sum(1 for b in rec_bets if not b['won'])):>11.0f} £{max(curr_runs)*2:>10.0f}")
print(f"  {'Loss days %':<30} {sum(1 for p in rec_daily.values() if p<0)/len(rec_daily)*100:>13.0f}% {sum(1 for p in curr_daily.values() if p<0)/len(curr_daily)*100:>11.0f}%")
print(f"  {'Worst day':<30} £{min(rec_daily.values()):>12.2f} £{min(curr_daily.values()):>10.2f}")
print(f"  {'Total P&L':<30} £{sum(b['pnl'] for b in rec_bets):>+12.2f} £{sum(b['pnl'] for b in curr_bets):>+10.2f}")
print(f"  {'ROI':<30} {sum(b['pnl'] for b in rec_bets)/sum(b['stake'] for b in rec_bets)*100:>13.1f}% {sum(b['pnl'] for b in curr_bets)/sum(b['stake'] for b in curr_bets)*100:>11.1f}%")
print()
print("NOTE: P&L inflated by backtest SP contamination.")
print("Focus on losing runs, win rates, and ROI relativities.")
