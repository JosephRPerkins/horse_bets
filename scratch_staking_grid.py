"""
scratch_staking_grid.py
=======================
Tests staking rules against raw results data.
Betfair minimum bet = £2. All non-zero stakes are £2 minimum.

Sections:
  A. Win bet SP thresholds (P1 only)
  B. Win both P1+P2 with independent SP thresholds
  C. SP-free score + SP threshold combined
  D. Variable stakes by SP band (£2 minimum)
  E. Variable stakes by tier
  F. Combined tier + SP + place bets
  G. Your idea variants

Run from ~/horse_bets_v3:
  python3 scratch_staking_grid.py 2>&1 | tee staking_grid_output.txt
"""

import json, os, sys
from collections import defaultdict
sys.path.insert(0, os.path.dirname(__file__))
from predict import score_runner, place_terms
from predict_v2 import (
    get_blended_picks, TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD,
    _sp_free_score,
)

COMMISSION = 0.05
BET_TIERS  = {TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD}
TIER_NAMES = {TIER_ELITE:"ELITE", TIER_STRONG:"STRONG", TIER_GOOD:"GOOD", TIER_STD:"STD"}
MIN_STAKE  = 2.0   # Betfair exchange minimum

def tof(v):
    try:
        f = float(str(v).strip())
        return f if f > 0 else None
    except: return None

def get_pos(r):
    try: return int(str(r.get("position","")).strip())
    except: return None

def stake(s):
    """Enforce Betfair minimum — any non-zero stake becomes at least £2."""
    return max(MIN_STAKE, float(s)) if s and s > 0 else 0.0

def win_return(sp, s):
    return round(s * (sp - 1) * (1 - COMMISSION), 2)

def place_return(sp, s):
    """Approximate place return using 1/4 odds model."""
    place_sp = round((sp - 1) * 0.25 + 1, 2)
    return round(s * (place_sp - 1) * (1 - COMMISSION), 2)

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

print(f"Loaded {len(all_races)} qualifying races with results")

# ── Build enriched records ────────────────────────────────────────────────────

print("Building picks...")
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

    n      = len(runners)
    places = place_terms(n)

    records.append({
        "date":     race["_date"],
        "tier":     tc,
        "n":        n,
        "places":   places,
        "p1sp":     p1sp,
        "p1won":    p1pos == 1,
        "p1placed": p1pos <= places if p1pos else False,
        "p2sp":     p2sp,
        "p2won":    p2pos == 1 if p2pos else False,
        "p2placed": p2pos <= places if p2pos else False,
        "p1score":  _sp_free_score(p1),
        "p2score":  _sp_free_score(p2) if p2 else 0,
    })

print(f"Records: {len(records)} across {len(set(r['date'] for r in records))} days\n")

# ── Simulator ─────────────────────────────────────────────────────────────────

def simulate(records, win_fn, place_fn=None, label=""):
    """
    win_fn(rec)   -> (p1_win_stake, p2_win_stake)  — use 0 to skip
    place_fn(rec) -> (p1_place_stake, p2_place_stake) or None
    All non-zero stakes are enforced to Betfair minimum £2.
    """
    nw = np_ = ww = pw = 0
    wpnl = ppnl = staked = 0.0

    for rec in records:
        ws1_raw, ws2_raw = win_fn(rec)
        ws1 = stake(ws1_raw); ws2 = stake(ws2_raw)

        if ws1 and rec["p1sp"]:
            nw += 1; staked += ws1
            if rec["p1won"]: ww += 1; wpnl += win_return(rec["p1sp"], ws1)
            else: wpnl -= ws1

        if ws2 and rec["p2sp"]:
            nw += 1; staked += ws2
            if rec["p2won"]: ww += 1; wpnl += win_return(rec["p2sp"], ws2)
            else: wpnl -= ws2

        if place_fn:
            ps1_raw, ps2_raw = place_fn(rec)
            ps1 = stake(ps1_raw); ps2 = stake(ps2_raw)

            if ps1 and rec["p1sp"] and rec["n"] > 4:
                np_ += 1; staked += ps1
                if rec["p1placed"]: pw += 1; ppnl += place_return(rec["p1sp"], ps1)
                else: ppnl -= ps1

            if ps2 and rec["p2sp"] and rec["n"] > 4:
                np_ += 1; staked += ps2
                if rec["p2placed"]: pw += 1; ppnl += place_return(rec["p2sp"], ps2)
                else: ppnl -= ps2

    total_bets = nw + np_
    total_pnl  = wpnl + ppnl
    return {
        "label":    label,
        "bets":     total_bets,
        "staked":   staked,
        "wins":     ww + pw,
        "win_rate": ww / nw * 100 if nw else 0,
        "win_pnl":  wpnl,
        "place_pnl": ppnl,
        "total_pnl": total_pnl,
        "per_bet":  total_pnl / total_bets if total_bets else 0,
        "roi":      total_pnl / staked * 100 if staked else 0,
    }

def print_table(results, sort_by="per_bet", top_n=None):
    results = sorted(results, key=lambda x: -x[sort_by])
    if top_n: results = results[:top_n]
    print(f"  {'Strategy':<52} {'Bets':>5} {'Win%':>6} {'W P&L':>9} {'P P&L':>9} {'Per bet':>8} {'ROI':>7}")
    print(f"  {'-'*98}")
    for r in results:
        wr = f"{r['win_rate']:.0f}%"
        print(f"  {r['label']:<52} {r['bets']:>5} {wr:>6} "
              f"{sgn(r['win_pnl']):>9} {sgn(r['place_pnl']):>9} "
              f"{sgn(r['per_bet']):>8} {r['roi']:>6.1f}%")

# ── A. Win bet SP thresholds ──────────────────────────────────────────────────

print("=" * 75)
print("A. WIN BET SP THRESHOLDS — P1 win bet only, no place bets")
print("   All stakes £2 (Betfair minimum). Bets skipped below threshold.")
print("=" * 75)

results_a = []
for min_sp in [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0, 6.0, 8.0, 10.0]:
    r = simulate(records,
        win_fn=lambda rec, m=min_sp: (
            2 if (rec["p1sp"] or 0) >= m else 0, 0),
        label=f"P1 win £2, SP >= {min_sp:.1f}")
    results_a.append(r)
print_table(results_a, sort_by="per_bet")

# ── B. Win both P1+P2 — SP thresholds ────────────────────────────────────────

print()
print("=" * 75)
print("B. WIN BOTH P1+P2 — independent SP thresholds, £2 each")
print("=" * 75)

results_b = []
for m1 in [1.0, 2.0, 3.0, 4.0, 5.0]:
    for m2 in [1.0, 2.0, 3.0, 4.0, 5.0]:
        r = simulate(records,
            win_fn=lambda rec, a=m1, b=m2: (
                2 if (rec["p1sp"] or 0) >= a else 0,
                2 if (rec["p2sp"] or 0) >= b else 0),
            label=f"P1 SP>={m1:.0f} + P2 SP>={m2:.0f}")
        results_b.append(r)
print_table(results_b, sort_by="per_bet", top_n=15)

# ── C. Score + SP threshold ───────────────────────────────────────────────────

print()
print("=" * 75)
print("C. SP-FREE SCORE + SP THRESHOLD — P1 win bet £2 only")
print("=" * 75)

results_c = []
for min_score in [0, 1, 2, 3, 4, 5]:
    for min_sp in [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0]:
        r = simulate(records,
            win_fn=lambda rec, ms=min_score, msp=min_sp: (
                2 if (rec["p1score"] >= ms and (rec["p1sp"] or 0) >= msp) else 0, 0),
            label=f"score>={min_score} AND SP>={min_sp:.0f}")
        results_c.append(r)
print_table(results_c, sort_by="per_bet", top_n=20)

# ── D. Variable stakes by SP band ────────────────────────────────────────────

print()
print("=" * 75)
print("D. VARIABLE STAKES BY SP BAND (£2 minimum on all non-zero stakes)")
print("=" * 75)

def band_stake(sp, bands):
    """bands = [(max_sp, raw_stake), ...]. Returns enforced stake."""
    if not sp: return 0
    for max_sp, s in bands:
        if sp < max_sp: return stake(s)
    return stake(bands[-1][1])

band_configs = [
    ("Skip <3/1 | £2 at 3-6/1 | £3 at 6-10/1 | £4 at 10/1+",
        [(3.0,0),(6.0,2),(10.0,3),(999,4)]),
    ("Skip <3/1 | £2 at 3-6/1 | £4 at 6-10/1 | £6 at 10/1+",
        [(3.0,0),(6.0,2),(10.0,4),(999,6)]),
    ("Skip <2/1 | £2 at 2-5/1 | £3 at 5-10/1 | £4 at 10/1+",
        [(2.0,0),(5.0,2),(10.0,3),(999,4)]),
    ("Skip <2/1 | £2 at 2-4/1 | £3 at 4-8/1 | £4 at 8/1+",
        [(2.0,0),(4.0,2),(8.0,3),(999,4)]),
    ("£2 <3/1 | £3 at 3-6/1 | £4 at 6-10/1 | £6 at 10/1+",
        [(3.0,2),(6.0,3),(10.0,4),(999,6)]),
    ("Flat £2 all (baseline)",
        [(999,2)]),
]

results_d = []
for name, bands in band_configs:
    r = simulate(records,
        win_fn=lambda rec, b=bands: (
            band_stake(rec["p1sp"], b),
            band_stake(rec["p2sp"], b)),
        label=name)
    results_d.append(r)
print_table(results_d, sort_by="per_bet")

# ── E. Variable stakes by tier ────────────────────────────────────────────────

print()
print("=" * 75)
print("E. VARIABLE STAKES BY TIER (non-zero enforced to £2 minimum)")
print("=" * 75)

tier_configs = [
    ("ELITE £4 | STRONG £3 | GOOD £2 | STD £2",
        {TIER_ELITE:4, TIER_STRONG:3, TIER_GOOD:2, TIER_STD:2}),
    ("ELITE £3 | STRONG £2 | GOOD £2 | STD £2",
        {TIER_ELITE:3, TIER_STRONG:2, TIER_GOOD:2, TIER_STD:2}),
    ("Flat £2 all tiers",
        {TIER_ELITE:2, TIER_STRONG:2, TIER_GOOD:2, TIER_STD:2}),
    ("ELITE £2 | STRONG £2 | GOOD £2 | STD skip",
        {TIER_ELITE:2, TIER_STRONG:2, TIER_GOOD:2, TIER_STD:0}),
    ("ELITE £2 | STRONG £2 | GOOD skip | STD skip",
        {TIER_ELITE:2, TIER_STRONG:2, TIER_GOOD:0, TIER_STD:0}),
    ("ELITE £2 only | all others skip",
        {TIER_ELITE:2, TIER_STRONG:0, TIER_GOOD:0, TIER_STD:0}),
]

results_e = []
for name, ts in tier_configs:
    r = simulate(records,
        win_fn=lambda rec, t=ts: (t.get(rec["tier"],0), t.get(rec["tier"],0)),
        label=name)
    results_e.append(r)
print_table(results_e, sort_by="per_bet")

# ── F. Combined rules ─────────────────────────────────────────────────────────

print()
print("=" * 75)
print("F. COMBINED — tier + SP + score + place bets (£2 minimum)")
print("=" * 75)

def make(wf, pf=None, lbl=""):
    return simulate(records, win_fn=wf, place_fn=pf, label=lbl)

results_f = [
    make(lambda r: (2,2), None,
         "Baseline: win P1+P2 flat £2, no place"),
    make(lambda r: (2,2), lambda r: (2,2),
         "Win P1+P2 + place P1+P2 flat £2"),
    make(lambda r: (2 if (r["p1sp"] or 0)>=3 else 0, 0), lambda r: (0,2),
         "Win P1 if SP>=3 + place P2 always"),
    make(lambda r: (2 if (r["p1sp"] or 0)>=3 else 0, 0), lambda r: (0, 2 if (r["p2sp"] or 0)>=2 else 0),
         "Win P1 if SP>=3 + place P2 if SP>=2"),
    make(lambda r: (2 if (r["p1sp"] or 0)>=5 else 0, 0), lambda r: (0,2),
         "Win P1 if SP>=5 + place P2 always"),
    make(lambda r: (2 if r["p1score"]>=3 and (r["p1sp"] or 0)>=3 else 0, 0), lambda r: (0,2),
         "Win P1 if score>=3 AND SP>=3 + place P2"),
    make(lambda r: (2 if r["p1score"]>=3 and (r["p1sp"] or 0)>=3 else 0, 0),
         lambda r: (0, 2 if r["p2score"]>=2 else 0),
         "Win P1 score>=3+SP>=3 + place P2 score>=2"),
    make(lambda r: (2 if r["p1score"]>=3 and (r["p1sp"] or 0)>=4 else 0, 0),
         lambda r: (0, 2 if r["p2score"]>=2 else 0),
         "Win P1 score>=3+SP>=4 + place P2 score>=2"),
    make(lambda r: (2 if r["p1score"]>=3 and (r["p1sp"] or 0)>=5 else 0, 0),
         lambda r: (0, 2 if r["p2score"]>=2 else 0),
         "Win P1 score>=3+SP>=5 + place P2 score>=2"),
    make(lambda r: (
            band_stake(r["p1sp"],[(3.0,0),(6.0,2),(10.0,3),(999,4)]) if r["p1score"]>=3 else 0, 0),
         lambda r: (0, 2 if r["p2score"]>=2 else 0),
         "Win P1 score>=3 var stake + place P2 score>=2"),
    make(lambda r: (2 if r["tier"] in (TIER_ELITE,TIER_STRONG) else 0,
                    2 if r["tier"] == TIER_ELITE else 0),
         lambda r: (0, 2 if r["tier"] in (TIER_GOOD,TIER_STD) else 0),
         "ELITE win both | STRONG win P1 | GOOD/STD place P2"),
    make(lambda r: (2 if r["tier"] in (TIER_ELITE,TIER_STRONG) and (r["p1sp"] or 0)>=3 else 0, 0),
         lambda r: (0, 2),
         "Win P1 if ELITE/STRONG AND SP>=3 + place P2 always"),
]
print_table(results_f, sort_by="per_bet")

# ── G. Your idea variants ─────────────────────────────────────────────────────

print()
print("=" * 75)
print("G. YOUR IDEA — short favs different treatment to longer shots")
print("=" * 75)

results_g = [
    make(lambda r: (2,2), lambda r: (2,2),
         "Current: win+place both always (£2 each)"),
    make(lambda r: (2 if (r["p1sp"] or 0)>=3 else 0, 0), lambda r: (0,2),
         "Win P1 if SP>=3, place P2 always"),
    make(lambda r: (2 if r["p1score"]>=3 else 0, 0), lambda r: (0, 2 if r["p2score"]>=2 else 0),
         "Win P1 if score>=3, place P2 if score>=2"),
    make(lambda r: (0, 0),
         lambda r: (2 if (r["p1sp"] or 0)<3 else 0, 2 if (r["p1sp"] or 0)>=3 else 0),
         "P1<3: place only | P1>=3: win only"),
    make(lambda r: (2 if (r["p1sp"] or 0)>=3 else 0, 2 if (r["p2sp"] or 0)>=3 else 0),
         lambda r: (2 if (r["p1sp"] or 0)<3 else 0, 2 if (r["p2sp"] or 0)<3 else 0),
         "SP>=3: win bet | SP<3: place bet (both picks)"),
    make(lambda r: (2 if r["tier"]==TIER_ELITE and (r["p1sp"] or 0)>=2 else
                    2 if r["tier"]==TIER_STRONG and (r["p1sp"] or 0)>=3 else
                    2 if r["tier"] in (TIER_GOOD,TIER_STD) and (r["p1sp"] or 0)>=4 else 0, 0),
         lambda r: (0, 2),
         "Tier-scaled SP floor for win + place P2 always"),
    make(lambda r: (
            band_stake(r["p1sp"],[(3.0,0),(6.0,2),(10.0,3),(999,4)]), 0),
         lambda r: (0, 2),
         "Skip P1<3 | var win stake | place P2 always £2"),
    make(lambda r: (
            band_stake(r["p1sp"],[(3.0,0),(6.0,2),(10.0,3),(999,4)]) if r["p1score"]>=3 else 0, 0),
         lambda r: (0, 2 if r["p2score"]>=2 else 0),
         "P1 score>=3 var win stake | P2 score>=2 place"),
]
print_table(results_g, sort_by="per_bet")

print()
print("NOTE: P&L figures inflated by backtest SP contamination.")
print("Focus on win rates, per-bet rankings, and ROI relativities.")
print("Done.")
