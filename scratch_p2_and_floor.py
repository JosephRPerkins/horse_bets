"""
scratch_p2_and_floor.py
=======================
1. P2 win bet analysis — is there value at certain SP/score thresholds?
2. P1 floor comparison — what changes if we lower from 3/1 to 2/1?

Run from ~/horse_bets_v3:
  python3 scratch_p2_and_floor.py
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

def pct(a, b): return f"{a/b*100:.0f}%" if b else "—"
def sgn(v):    return f"+£{v:.2f}" if v >= 0 else f"-£{abs(v):.2f}"

def win_return(sp, stake):
    return round(stake * (sp - 1) * (1 - COMMISSION), 2)

def losing_runs(seq):
    runs = []; cur = 0
    for won in seq:
        if not won: cur += 1
        else:
            if cur > 0: runs.append(cur)
            cur = 0
    if cur > 0: runs.append(cur)
    return runs

# ── Load races ────────────────────────────────────────────────────────────────

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

# ── Build records ─────────────────────────────────────────────────────────────

records = []
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
    p2sp  = tof(p2.get("sp_dec")) if p2 else None
    p2pos = get_pos(p2) if p2 else None
    if p1pos is None: continue

    records.append({
        "date":    race["_date"],
        "tier":    tc,
        "p1sp":    p1sp,   "p1won": p1pos == 1,
        "p2sp":    p2sp,   "p2won": p2pos == 1 if p2pos else False,
        "p1score": _sp_free_score(p1),
        "p2score": _sp_free_score(p2) if p2 else 0,
    })

print(f"Records: {len(records)}")
print()

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — P2 WIN BET ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════

print("=" * 70)
print("SECTION 1 — P2 WIN BET ANALYSIS")
print("=" * 70)
print()

# Overall P2 stats
p2_all = [r for r in records if r["p2sp"]]
p2_wins = [r for r in p2_all if r["p2won"]]
print(f"P2 overall: {len(p2_wins)}/{len(p2_all)} ({pct(len(p2_wins),len(p2_all))}) win rate")
print(f"P2 avg SP:  {sum(r['p2sp'] for r in p2_all)/len(p2_all):.2f}")
print(f"P2 avg winner SP: {sum(r['p2sp'] for r in p2_wins)/len(p2_wins):.2f}" if p2_wins else "")
print()

# P2 by SP band
print("P2 by SP band (£2 flat stake):")
print(f"  {'Band':<14} {'N':>5} {'Wins':>5} {'Win%':>6} {'AvgWinSP':>9} {'P&L':>10} {'Per bet':>8} {'Breakeven':>10}")
print(f"  {'-'*70}")
bands = [(1.0,2.0,"<2/1"),(2.0,3.0,"2-3/1"),(3.0,5.0,"3-5/1"),
         (5.0,8.0,"5-8/1"),(8.0,13.0,"8-13/1"),(13.0,999,"13/1+")]
for lo,hi,lbl in bands:
    recs = [r for r in p2_all if lo<=r["p2sp"]<hi]
    if not recs: continue
    n = len(recs); w = sum(1 for r in recs if r["p2won"])
    wins = [r for r in recs if r["p2won"]]
    avg_win_sp = sum(r["p2sp"] for r in wins)/len(wins) if wins else 0
    pnl = sum(win_return(r["p2sp"],2) if r["p2won"] else -2 for r in recs)
    be = 1/(avg_win_sp*0.95)*100 if avg_win_sp else 0
    print(f"  {lbl:<14} {n:>5} {w:>5} {w/n*100:>5.0f}% {avg_win_sp:>9.2f} {pnl:>+10.2f} {pnl/n:>+8.3f} {be:>9.0f}%")

print()

# P2 by score threshold
print("P2 by score threshold (£2 flat stake, all SPs):")
print(f"  {'Score':<12} {'N':>5} {'Win%':>6} {'P&L':>10} {'Per bet':>8}")
print(f"  {'-'*45}")
for min_score in [0, 1, 2, 3, 4, 5]:
    recs = [r for r in p2_all if r["p2score"] >= min_score]
    if not recs: continue
    n = len(recs); w = sum(1 for r in recs if r["p2won"])
    pnl = sum(win_return(r["p2sp"],2) if r["p2won"] else -2 for r in recs)
    print(f"  score>={min_score:<6} {n:>5} {w/n*100:>5.0f}% {pnl:>+10.2f} {pnl/n:>+8.3f}")

print()

# P2 combined score + SP filter
print("P2 combined score + SP filter (£2 stake, best combinations):")
print(f"  {'Rule':<30} {'N':>5} {'Win%':>6} {'P&L':>10} {'Per bet':>8}")
print(f"  {'-'*60}")
combos = []
for ms in [0, 2, 3, 4]:
    for msp in [1.0, 2.0, 3.0, 4.0, 5.0]:
        recs = [r for r in p2_all if r["p2score"]>=ms and (r["p2sp"] or 0)>=msp]
        if len(recs) < 30: continue
        n = len(recs); w = sum(1 for r in recs if r["p2won"])
        pnl = sum(win_return(r["p2sp"],2) if r["p2won"] else -2 for r in recs)
        combos.append((ms, msp, n, w, pnl, pnl/n))

combos.sort(key=lambda x: -x[5])
for ms,msp,n,w,pnl,ppb in combos[:15]:
    lbl = f"score>={ms} AND SP>={msp:.0f}"
    print(f"  {lbl:<30} {n:>5} {w/n*100:>5.0f}% {pnl:>+10.2f} {ppb:>+8.3f}")

print()

# Losing run analysis for best P2 strategy
best_ms, best_msp = 3, 3.0
p2_seq = [r["p2won"] for r in p2_all if r["p2score"]>=best_ms and (r["p2sp"] or 0)>=best_msp]
runs = losing_runs(p2_seq)
print(f"Losing runs for P2 score>={best_ms} AND SP>={best_msp:.0f} ({len(p2_seq)} bets):")
if runs:
    print(f"  Max run: {max(runs)}  Avg: {sum(runs)/len(runs):.1f}  "
          f"Runs>=5: {sum(1 for r in runs if r>=5)}  Runs>=10: {sum(1 for r in runs if r>=10)}")
    print(f"  Max run cost at £2: £{max(runs)*2}")

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — P1 FLOOR COMPARISON (2/1 vs 3/1)
# ══════════════════════════════════════════════════════════════════════════════

print()
print("=" * 70)
print("SECTION 2 — P1 FLOOR COMPARISON")
print("=" * 70)
print()

def simulate_p1(records, min_sp, min_score=3, stake_fn=None):
    """Simulate P1 win betting with given floor and optional variable stake."""
    n = wins = 0; pnl = 0.0; seq = []
    for r in records:
        sp = r["p1sp"]; score = r["p1score"]
        if not sp or score < min_score or sp < min_sp: continue
        s = stake_fn(sp) if stake_fn else 2.0
        n += 1
        won = r["p1won"]
        seq.append(won)
        if won: wins += 1; pnl += win_return(sp, s)
        else: pnl -= s
    runs = losing_runs(seq)
    return {
        "n": n, "wins": wins, "pnl": pnl,
        "win_rate": wins/n*100 if n else 0,
        "per_bet": pnl/n if n else 0,
        "max_run": max(runs) if runs else 0,
        "runs_5": sum(1 for r in runs if r>=5),
        "runs_10": sum(1 for r in runs if r>=10),
        "avg_run": sum(runs)/len(runs) if runs else 0,
    }

def var_stake(sp):
    if sp < 6: return 2
    if sp < 10: return 4
    return 6

configs = [
    ("Current: score>=3, SP>=3, var stake", 3.0, 3, var_stake),
    ("Lower floor: score>=3, SP>=2, var stake",  2.0, 3, var_stake),
    ("Lower floor: score>=3, SP>=2, flat £2",    2.0, 3, None),
    ("Current floor: score>=3, SP>=3, flat £2",  3.0, 3, None),
    ("No floor: score>=3, SP>=1, var stake",     1.0, 3, var_stake),
    ("No floor: score>=3, SP>=1, flat £2",       1.0, 3, None),
    ("Higher floor: score>=3, SP>=4, var stake", 4.0, 3, var_stake),
    ("Higher floor: score>=3, SP>=5, var stake", 5.0, 3, var_stake),
]

print(f"  {'Config':<45} {'N':>5} {'Win%':>6} {'P&L':>10} {'Per bet':>8} {'MaxRun':>7} {'R>=5':>5} {'R>=10':>6}")
print(f"  {'-'*95}")

for name, min_sp, min_score, sfn in configs:
    s = simulate_p1(records, min_sp, min_score, sfn)
    max_cost = s["max_run"] * (6 if sfn else 2)
    print(f"  {name:<45} {s['n']:>5} {s['win_rate']:>5.0f}% "
          f"{s['pnl']:>+10.2f} {s['per_bet']:>+8.3f} "
          f"{s['max_run']:>7} {s['runs_5']:>5} {s['runs_10']:>6}   "
          f"max cost £{max_cost}")

print()

# What races does lowering to 2/1 add?
print("RACES ADDED by lowering P1 floor from 3/1 to 2/1 (score>=3):")
print("(These are currently skipped but would get a win bet at £2)")
print()
added = [r for r in records if r["p1score"]>=3 and r["p1sp"] and 2.0<=r["p1sp"]<3.0]
if added:
    n = len(added); w = sum(1 for r in added if r["p1won"])
    pnl = sum(win_return(r["p1sp"],2) if r["p1won"] else -2 for r in added)
    avg_sp = sum(r["p1sp"] for r in added)/len(added)
    print(f"  Total races: {n}  Win rate: {pct(w,n)}  Avg SP: {avg_sp:.2f}  P&L: {sgn(pnl)}  Per bet: {sgn(pnl/n)}")
    print()
    # SP distribution
    for lo,hi,lbl in [(2.0,2.5,"2.0-2.5/1"),(2.5,3.0,"2.5-3.0/1")]:
        sub = [r for r in added if lo<=r["p1sp"]<hi]
        if not sub: continue
        sw = sum(1 for r in sub if r["p1won"])
        sp_pnl = sum(win_return(r["p1sp"],2) if r["p1won"] else -2 for r in sub)
        print(f"  {lbl}: n={len(sub)} win={pct(sw,len(sub))} P&L={sgn(sp_pnl)} per_bet={sgn(sp_pnl/len(sub))}")

print()

# Breakeven at 2/1 prices
avg_sp_20 = sum(r["p1sp"] for r in added if r["p1won"])/sum(1 for r in added if r["p1won"]) if any(r["p1won"] for r in added) else 0
be = 1/(avg_sp_20*0.95)*100 if avg_sp_20 else 0
print(f"  Avg winner SP in 2-3/1 band: {avg_sp_20:.2f}")
print(f"  Breakeven win rate needed: {be:.0f}%")
print(f"  Actual win rate: {pct(sum(1 for r in added if r['p1won']),len(added))}")
print()
print("NOTE: P&L inflated by backtest contamination. Win rates and relative")
print("      comparisons are the reliable signals.")
print()
print("Done.")
