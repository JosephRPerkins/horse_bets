"""
scratch_full_analysis.py
========================
Comprehensive analysis using raw results files only.
No tier tracker dependency — retroactively classifies all races.

Covers:
  1. System C vs old score_runner win rates per tier
  2. SP band profitability
  3. Staking strategy comparison
  4. mw_p1 sweep (P1 win rate at different market weights)
  5. Best/worst performing days

Run from ~/horse_bets_v3:
  python3 scratch_full_analysis.py 2>&1 | tee full_analysis_output.txt
"""

import json, os, sys, re
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from predict import score_runner
from predict_v2 import get_blended_picks, TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP, TIER_LABELS

COMMISSION = 0.05
STAKE      = 2.0
MW_SWEEP   = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
TIER_NAMES = {TIER_ELITE:"ELITE", TIER_STRONG:"STRONG", TIER_GOOD:"GOOD", TIER_STD:"STD"}
BET_TIERS  = {TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD}

def tof(v):
    try:
        f = float(str(v).strip())
        return f if f > 0 else None
    except: return None

def get_pos(r):
    try: return int(str(r.get("position","")).strip())
    except: return None

def pnl(won, sp, stake=STAKE):
    if sp is None: return None
    return round(stake * (sp - 1) * (1 - COMMISSION), 2) if won else -stake

def field_ok(runners, race):
    n = len(runners)
    if n < 2: return False
    race_type = str(race.get("type","") or "").lower()
    is_jump   = any(t in race_type for t in ("chase","hurdle","nh flat","national hunt"))
    cls = str(race.get("class","") or "").replace("Class","").strip()
    if cls in ("1","2") and is_jump: return False
    if is_jump and n > 12: return False
    if not is_jump and n > 20: return False
    return True

# ── Load all raw results ──────────────────────────────────────────────────────

all_races = []
for fp in sorted(os.listdir("data/raw")):
    if not fp.endswith(".json"): continue
    date_str = fp.replace(".json","")
    try:
        with open(f"data/raw/{fp}") as f: d = json.load(f)
    except: continue
    races = d.get("results") or d.get("races") or []
    for race in races:
        if not field_ok(race.get("runners",[]), race): continue
        runners = race.get("runners",[])
        if not any(get_pos(r) == 1 for r in runners): continue  # skip no-result
        race["_date"] = date_str
        all_races.append(race)

print(f"Loaded {len(all_races)} qualifying races with results across {len(set(r['_date'] for r in all_races))} days")
print(f"Date range: {min(r['_date'] for r in all_races)} to {max(r['_date'] for r in all_races)}")
print()

# ── Build picks for each race ─────────────────────────────────────────────────

records = []
for race in all_races:
    runners  = race.get("runners",[])
    date_str = race["_date"]
    raw_meta = {
        "class":   str(race.get("class","") or ""),
        "surface": race.get("surface","Turf") or "Turf",
        "type":    race.get("type","") or "",
    }

    # System C picks (current mw=0.1 for P1 since that's what's deployed)
    tc, cp1, cp2, _ = get_blended_picks(runners, mw_p1=0.1, mw_p2=0.4, raw_race=raw_meta)

    # Old system picks (pure score_runner)
    scored = sorted(runners, key=lambda r: (-score_runner(r)[0], tof(r.get("sp_dec")) or 999))
    op1 = scored[0] if scored else None
    op2 = scored[1] if len(scored) > 1 else None

    if not cp1 or not op1: continue
    if tc not in BET_TIERS: continue

    cp1sp = tof(cp1.get("sp_dec")); cp1pos = get_pos(cp1)
    cp2sp = tof(cp2.get("sp_dec")) if cp2 else None; cp2pos = get_pos(cp2) if cp2 else None
    op1sp = tof(op1.get("sp_dec")); op1pos = get_pos(op1)
    op2sp = tof(op2.get("sp_dec")) if op2 else None; op2pos = get_pos(op2) if op2 else None

    if cp1pos is None or op1pos is None: continue

    same_p1 = (cp1.get("horse_id","") == op1.get("horse_id",""))

    records.append({
        "date":    date_str,
        "tier":    tc,
        "cp1sp":   cp1sp, "cp1won": cp1pos==1, "cp1pos": cp1pos,
        "cp2sp":   cp2sp, "cp2won": cp2pos==1 if cp2pos else False,
        "op1sp":   op1sp, "op1won": op1pos==1, "op1pos": op1pos,
        "op2sp":   op2sp, "op2won": op2pos==1 if op2pos else False,
        "same_p1": same_p1,
        "n_runners": len(runners),
    })

print(f"Records with picks: {len(records)}")
print()

# ── 1. System C vs Old — overall and per tier ─────────────────────────────────

print("=" * 70)
print("1. SYSTEM C vs OLD SYSTEM — P1 win rates")
print("=" * 70)
print()

def pct(a,b): return f"{a/b*100:.0f}%" if b else "—"

overall = {"n":0,"cw":0,"ow":0,"same":0,"diff":0,"cw_diff":0,"ow_diff":0}
tier_stats = defaultdict(lambda: {"n":0,"cw":0,"ow":0,"same":0,"diff":0,"cw_diff":0,"ow_diff":0})

for r in records:
    tn = TIER_NAMES.get(r["tier"],"?")
    for s in (overall, tier_stats[tn]):
        s["n"]  += 1
        s["cw"] += int(r["cp1won"])
        s["ow"] += int(r["op1won"])
        if r["same_p1"]:
            s["same"] += 1
        else:
            s["diff"]    += 1
            s["cw_diff"] += int(r["cp1won"])
            s["ow_diff"] += int(r["op1won"])

s = overall
print(f"  Overall: {s['n']} races")
print(f"    System C P1 win:  {pct(s['cw'],s['n'])} ({s['cw']}/{s['n']})")
print(f"    Old sys  P1 win:  {pct(s['ow'],s['n'])} ({s['ow']}/{s['n']})")
print(f"    Same pick:        {pct(s['same'],s['n'])} ({s['same']}/{s['n']})")
print(f"    When differ ({s['diff']} races):")
print(f"      SysC wins: {pct(s['cw_diff'],s['diff'])}")
print(f"      Old  wins: {pct(s['ow_diff'],s['diff'])}")
print()

for tn in ("ELITE","STRONG","GOOD","STD"):
    s = tier_stats[tn]
    if not s["n"]: continue
    print(f"  {tn}: {s['n']} races  SysC={pct(s['cw'],s['n'])}  Old={pct(s['ow'],s['n'])}  "
          f"same={pct(s['same'],s['n'])}  "
          f"differ: SysC={pct(s['cw_diff'],s['diff'])} Old={pct(s['ow_diff'],s['diff'])} ({s['diff']} races)")

# ── 2. SP band profitability ──────────────────────────────────────────────────

print()
print("=" * 70)
print("2. SP BAND PROFITABILITY (System C P1, flat £2)")
print("=" * 70)

bands = [(1.0,2.0,"<2/1"),(2.0,3.0,"2-3/1"),(3.0,5.0,"3-5/1"),
         (5.0,8.0,"5-8/1"),(8.0,13.0,"8-13/1"),(13.0,999,"13/1+")]

print(f"\n  {'Band':<10} {'N':>5} {'Wins':>5} {'Win%':>6} {'AvgSP':>7} {'P&L':>9} {'Per bet':>8} {'BEvn':>7}")
print(f"  {'-'*62}")
for lo,hi,lbl in bands:
    recs = [r for r in records if r["cp1sp"] and lo<=r["cp1sp"]<hi]
    if not recs: continue
    n = len(recs); w = sum(1 for r in recs if r["cp1won"])
    wins = [r for r in recs if r["cp1won"]]
    avg_sp = sum(r["cp1sp"] for r in wins)/len(wins) if wins else 0
    total_pnl = sum(pnl(r["cp1won"],r["cp1sp"]) for r in recs if r["cp1sp"])
    be = 1/(avg_sp*0.95)*100 if avg_sp else 0
    print(f"  {lbl:<10} {n:>5} {w:>5} {w/n*100:>5.0f}% {avg_sp:>7.2f} {total_pnl:>+9.2f} {total_pnl/n:>+8.3f} {be:>6.0f}%")

# Same for old system
print(f"\n  OLD SYSTEM for comparison:")
print(f"  {'Band':<10} {'N':>5} {'Wins':>5} {'Win%':>6} {'AvgSP':>7} {'P&L':>9} {'Per bet':>8}")
print(f"  {'-'*55}")
for lo,hi,lbl in bands:
    recs = [r for r in records if r["op1sp"] and lo<=r["op1sp"]<hi]
    if not recs: continue
    n = len(recs); w = sum(1 for r in recs if r["op1won"])
    wins = [r for r in recs if r["op1won"]]
    avg_sp = sum(r["op1sp"] for r in wins)/len(wins) if wins else 0
    total_pnl = sum(pnl(r["op1won"],r["op1sp"]) for r in recs if r["op1sp"])
    print(f"  {lbl:<10} {n:>5} {w:>5} {w/n*100:>5.0f}% {avg_sp:>7.2f} {total_pnl:>+9.2f} {total_pnl/n:>+8.3f}")

# ── 3. mw_p1 sweep ────────────────────────────────────────────────────────────

print()
print("=" * 70)
print("3. mw_p1 SWEEP — P1 win rate at each market weight")
print("=" * 70)
print()

mw_results = {}
for mw in MW_SWEEP:
    stats = defaultdict(lambda: {"n":0,"p1w":0,"p2w":0,"either":0,"p1pnl":0.0})
    for race in all_races:
        runners  = race.get("runners",[])
        raw_meta = {"class": str(race.get("class","") or ""),
                    "surface": race.get("surface","Turf") or "Turf",
                    "type": race.get("type","") or ""}
        if mw == 0.0:
            scored = sorted(runners, key=lambda r: (-score_runner(r)[0], tof(r.get("sp_dec")) or 999))
            p1 = scored[0] if scored else None
            p2 = scored[1] if len(scored)>1 else None
            tc2, _, _, _ = get_blended_picks(runners, mw_p1=0.3, mw_p2=0.4, raw_race=raw_meta)
            tc = tc2
        elif mw == 1.0:
            by_price = sorted([r for r in runners if tof(r.get("sp_dec"))],
                               key=lambda r: tof(r.get("sp_dec")))
            p1 = by_price[0] if by_price else None
            p2 = by_price[1] if len(by_price)>1 else None
            tc2, _, _, _ = get_blended_picks(runners, mw_p1=0.7, mw_p2=0.4, raw_meta=raw_meta) if raw_meta else (TIER_STD,None,None,None)
            tc = tc2
        else:
            tc, p1, p2, _ = get_blended_picks(runners, mw_p1=mw, mw_p2=0.4, raw_race=raw_meta)

        if not p1 or tc not in BET_TIERS: continue
        p1sp = tof(p1.get("sp_dec")); p1pos = get_pos(p1)
        p2sp = tof(p2.get("sp_dec")) if p2 else None; p2pos = get_pos(p2) if p2 else None
        if p1pos is None: continue

        tn = TIER_NAMES.get(tc,"?")
        p1w = p1pos==1; p2w = p2pos==1 if p2pos else False
        p1pl = pnl(p1w,p1sp) if p1sp else 0

        for bucket in ("all", tn):
            s = stats[bucket]
            s["n"]     += 1
            s["p1w"]   += int(p1w)
            s["p2w"]   += int(p2w)
            s["either"]+= int(p1w or p2w)
            s["p1pnl"] += p1pl or 0

    mw_results[mw] = stats

print(f"  {'mw':>5}  {'N':>5}  {'P1 win%':>8}  {'Either%':>8}  {'P1 P&L':>9}")
print(f"  {'-'*45}")
for mw in MW_SWEEP:
    s = mw_results[mw]["all"]
    n = s["n"]
    lbl = f"{mw:.1f}"
    if mw == 0.0: lbl = "0.0 (SR)"
    if mw == 1.0: lbl = "1.0 (Mkt)"
    print(f"  {lbl:>9}  {n:>5}  {pct(s['p1w'],n):>8}  {pct(s['either'],n):>8}  {s['p1pnl']:>+9.2f}")

# ── 4. Staking strategies ─────────────────────────────────────────────────────

print()
print("=" * 70)
print("4. STAKING STRATEGY COMPARISON (win bets only, £2 stake)")
print("=" * 70)
print()

strategies = [
    ("Both P1+P2 always",           lambda r: (2, 2)),
    ("P1 only",                     lambda r: (2, 0)),
    ("P2 only",                     lambda r: (0, 2)),
    ("P1 win if SP>=2, else skip",  lambda r: (2 if (r["cp1sp"] or 0)>=2 else 0, 0)),
    ("P1 win if SP>=3, else skip",  lambda r: (2 if (r["cp1sp"] or 0)>=3 else 0, 0)),
    ("P1 win if SP>=5, else skip",  lambda r: (2 if (r["cp1sp"] or 0)>=5 else 0, 0)),
    ("Both only if P1 SP>=3",       lambda r: (2,2) if (r["cp1sp"] or 0)>=3 else (0,0)),
    ("Longer-priced of P1/P2 only", lambda r: (2,0) if (r["cp1sp"] or 0)>=(r["cp2sp"] or 0) else (0,2)),
    ("OLD: P1 only, pure SR",       lambda r: ("old",0)),
    ("OLD: Both P1+P2, pure SR",    lambda r: ("old","old")),
]

print(f"  {'Strategy':<40} {'Bets':>5} {'Wins':>5} {'Win%':>6} {'P&L':>10} {'Per bet':>8}")
print(f"  {'-'*76}")

for name, stake_fn in strategies:
    n = w = 0; total_pnl = 0.0
    for r in records:
        s1, s2 = stake_fn(r)

        # P1
        if s1 == "old":
            sp, won = r["op1sp"], r["op1won"]
            s1 = STAKE
        else:
            sp, won = r["cp1sp"], r["cp1won"]

        if s1 and s1 > 0 and sp:
            n += 1
            if won: w += 1; total_pnl += s1*(sp-1)*(1-COMMISSION)
            else:   total_pnl -= s1

        # P2
        if s2 == "old":
            sp2, won2 = r["op2sp"], r["op2won"]
            s2 = STAKE
        else:
            sp2, won2 = r["cp2sp"], r["cp2won"]

        if s2 and s2 > 0 and sp2:
            n += 1
            if won2: w += 1; total_pnl += s2*(sp2-1)*(1-COMMISSION)
            else:    total_pnl -= s2

    if n == 0: continue
    print(f"  {name:<40} {n:>5} {w:>5} {w/n*100:>5.0f}% {total_pnl:>+10.2f} {total_pnl/n:>+8.3f}")

# ── 5. Daily P&L ──────────────────────────────────────────────────────────────

print()
print("=" * 70)
print("5. DAILY P&L — System C P1+P2 flat £2")
print("=" * 70)
print()

by_date = defaultdict(lambda: {"n":0,"p1w":0,"p2w":0,"p1pnl":0.0,"p2pnl":0.0})
for r in records:
    d = r["date"]
    by_date[d]["n"]    += 1
    by_date[d]["p1w"]  += int(r["cp1won"])
    by_date[d]["p2w"]  += int(r["cp2won"])
    p1pl = pnl(r["cp1won"],r["cp1sp"]) or 0
    p2pl = pnl(r["cp2won"],r["cp2sp"]) if r["cp2sp"] else 0
    by_date[d]["p1pnl"] += p1pl
    by_date[d]["p2pnl"] += p2pl or 0

print(f"  {'Date':<12} {'N':>4} {'P1w%':>6} {'P2w%':>6} {'P1 P&L':>9} {'P2 P&L':>9} {'Total':>9}")
print(f"  {'-'*62}")
cum = 0
for date in sorted(by_date.keys()):
    s = by_date[date]
    n = s["n"]; total = s["p1pnl"]+s["p2pnl"]; cum += total
    print(f"  {date:<12} {n:>4} {pct(s['p1w'],n):>6} {pct(s['p2w'],n):>6} "
          f"{s['p1pnl']:>+9.2f} {s['p2pnl']:>+9.2f} {total:>+9.2f}")
print(f"  {'CUMULATIVE':<12} {'':>4} {'':>6} {'':>6} {'':>9} {'':>9} {cum:>+9.2f}")

print()
print("Done.")
