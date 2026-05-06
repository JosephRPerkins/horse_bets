"""
scratch_staking_grid.py
=======================
Tests a wide grid of staking rules against the raw results data.

Dimensions tested:
  1. Win bet SP thresholds (min price to place win bet)
  2. Place bet SP thresholds (min price to place place bet)
  3. Score thresholds (SP-free score minimum for P1)
  4. Variable stakes by SP band
  5. Variable stakes by tier
  6. Combinations of the above

All using System C picks (mw_p1=0.1, mw_p2=0.4) since mw makes no difference.
P&L figures use post-race SP so are inflated — focus on win rates and relative rankings.

Run from ~/horse_bets_v3:
  python3 scratch_staking_grid.py 2>&1 | tee staking_grid_output.txt
"""

import json, os, sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from predict import score_runner
from predict_v2 import (
    get_blended_picks, TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD,
    TIER_LABELS, _sp_free_score
)

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

def win_pnl(sp, stake):
    return round(stake * (sp - 1) * (1 - COMMISSION), 2)

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

# ── Load all raw results ──────────────────────────────────────────────────────

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
print()

# ── Build enriched race records ───────────────────────────────────────────────

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

    # SP-free score for P1 and P2
    p1score = _sp_free_score(p1)
    p2score = _sp_free_score(p2) if p2 else 0

    # Place terms
    n = len(runners)
    from predict import place_terms
    places = place_terms(n)

    records.append({
        "date":    race["_date"],
        "tier":    tc,
        "n":       n,
        "places":  places,
        "p1sp":    p1sp,   "p1won":    p1pos == 1,  "p1placed": p1pos <= places if p1pos else False,
        "p2sp":    p2sp,   "p2won":    p2pos == 1 if p2pos else False,
                           "p2placed": p2pos <= places if p2pos else False,
        "p1score": p1score,
        "p2score": p2score,
    })

print(f"Records built: {len(records)}")
print()

# ── Simulator ─────────────────────────────────────────────────────────────────

def simulate(records, win_stake_fn, place_stake_fn=None, label=""):
    """
    win_stake_fn(rec)   -> (p1_win_stake, p2_win_stake)
    place_stake_fn(rec) -> (p1_place_stake, p2_place_stake) or None to skip
    Returns: dict of stats
    """
    n_races = len(records)
    n_win_bets = n_place_bets = 0
    win_pnl_total = place_pnl_total = 0.0
    win_wins = place_wins = 0
    total_staked = 0.0

    for rec in records:
        ws1, ws2 = win_stake_fn(rec)

        # P1 win bet
        if ws1 and rec["p1sp"]:
            n_win_bets += 1; total_staked += ws1
            if rec["p1won"]:
                win_wins += 1
                win_pnl_total += win_pnl(rec["p1sp"], ws1)
            else:
                win_pnl_total -= ws1

        # P2 win bet
        if ws2 and rec["p2sp"]:
            n_win_bets += 1; total_staked += ws2
            if rec["p2won"]:
                win_wins += 1
                win_pnl_total += win_pnl(rec["p2sp"], ws2)
            else:
                win_pnl_total -= ws2

        # Place bets
        if place_stake_fn:
            ps1, ps2 = place_stake_fn(rec)

            if ps1 and rec["p1sp"]:
                n_place_bets += 1; total_staked += ps1
                if rec["p1placed"]:
                    place_wins += 1
                    # Approximate place return: win_sp * 0.25 (1/4 odds)
                    place_price = round((rec["p1sp"] - 1) * 0.25 + 1, 2)
                    place_pnl_total += win_pnl(place_price, ps1)
                else:
                    place_pnl_total -= ps1

            if ps2 and rec["p2sp"]:
                n_place_bets += 1; total_staked += ps2
                if rec["p2placed"]:
                    place_wins += 1
                    place_price = round((rec["p2sp"] - 1) * 0.25 + 1, 2)
                    place_pnl_total += win_pnl(place_price, ps2)
                else:
                    place_pnl_total -= ps2

    total_bets = n_win_bets + n_place_bets
    total_pnl  = win_pnl_total + place_pnl_total

    return {
        "label":         label,
        "n_races":       n_races,
        "n_win_bets":    n_win_bets,
        "n_place_bets":  n_place_bets,
        "total_bets":    total_bets,
        "total_staked":  total_staked,
        "win_wins":      win_wins,
        "place_wins":    place_wins,
        "win_pnl":       win_pnl_total,
        "place_pnl":     place_pnl_total,
        "total_pnl":     total_pnl,
        "per_race":      total_pnl / n_races if n_races else 0,
        "per_bet":       total_pnl / total_bets if total_bets else 0,
        "win_rate":      win_wins / n_win_bets * 100 if n_win_bets else 0,
        "roi":           total_pnl / total_staked * 100 if total_staked else 0,
    }

def print_results(results, sort_by="per_bet", top_n=None):
    results = sorted(results, key=lambda x: -x[sort_by])
    if top_n: results = results[:top_n]
    print(f"  {'Strategy':<50} {'Bets':>5} {'Win%':>6} {'P&L':>10} {'Per bet':>8} {'ROI':>7}")
    print(f"  {'-'*88}")
    for r in results:
        wr = f"{r['win_rate']:.0f}%" if r['n_win_bets'] else "—"
        print(f"  {r['label']:<50} {r['total_bets']:>5} {wr:>6} "
              f"{sgn(r['total_pnl']):>10} {sgn(r['per_bet']):>8} {r['roi']:>6.1f}%")

# ── Section A: Win bet SP thresholds ─────────────────────────────────────────

print("=" * 70)
print("A. WIN BET SP THRESHOLDS — P1 only, no place bets")
print("=" * 70)
print()

results_a = []
for min_sp in [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0, 6.0, 8.0]:
    r = simulate(records,
        win_stake_fn=lambda rec, m=min_sp: (
            2.0 if (rec["p1sp"] or 0) >= m else 0,
            0
        ),
        label=f"P1 win only, SP >= {min_sp:.1f}"
    )
    results_a.append(r)

print_results(results_a, sort_by="per_bet")

# ── Section B: Win both P1+P2 with SP thresholds ─────────────────────────────

print()
print("=" * 70)
print("B. WIN BOTH P1+P2 — minimum SP thresholds")
print("=" * 70)
print()

results_b = []
for min_p1 in [1.0, 2.0, 3.0, 4.0, 5.0]:
    for min_p2 in [1.0, 2.0, 3.0, 4.0, 5.0]:
        r = simulate(records,
            win_stake_fn=lambda rec, m1=min_p1, m2=min_p2: (
                2.0 if (rec["p1sp"] or 0) >= m1 else 0,
                2.0 if (rec["p2sp"] or 0) >= m2 else 0,
            ),
            label=f"P1 SP>={min_p1:.0f} + P2 SP>={min_p2:.0f}"
        )
        results_b.append(r)

print_results(results_b, sort_by="per_bet", top_n=15)

# ── Section C: Score thresholds ───────────────────────────────────────────────

print()
print("=" * 70)
print("C. SP-FREE SCORE THRESHOLDS — P1 win bet only")
print("=" * 70)
print()

results_c = []
for min_score in [0, 1, 2, 3, 4, 5]:
    for min_sp in [1.0, 2.0, 3.0, 4.0, 5.0]:
        r = simulate(records,
            win_stake_fn=lambda rec, ms=min_score, msp=min_sp: (
                2.0 if (rec["p1score"] >= ms and (rec["p1sp"] or 0) >= msp) else 0,
                0
            ),
            label=f"P1 score>={min_score} AND SP>={min_sp:.0f}"
        )
        results_c.append(r)

print_results(results_c, sort_by="per_bet", top_n=15)

# ── Section D: Variable stakes by SP band ────────────────────────────────────

print()
print("=" * 70)
print("D. VARIABLE STAKES BY SP BAND")
print("   (£1 odds-on, £2 evens-3/1, £3 3/1-6/1, £4 6/1+)")
print("=" * 70)
print()

def sp_band_stake(sp, bands):
    """bands = [(max_sp, stake), ...] sorted by max_sp ascending, last is default"""
    if not sp: return 0
    for max_sp, stake in bands:
        if sp < max_sp: return stake
    return bands[-1][1]

band_configs = [
    ("Skip <2, £2 2-5, £3 5-10, £4 10+",
        [(2.0,0),(5.0,2),(10.0,3),(999,4)]),
    ("£1 <2, £2 2-4, £3 4-8, £4 8+",
        [(2.0,1),(4.0,2),(8.0,3),(999,4)]),
    ("£2 <3, £3 3-6, £4 6-10, £5 10+",
        [(3.0,2),(6.0,3),(10.0,4),(999,5)]),
    ("Skip <3, £2 3-6, £3 6-10, £4 10+",
        [(3.0,0),(6.0,2),(10.0,3),(999,4)]),
    ("Skip <2, £2 2-4, £2 4+",
        [(2.0,0),(4.0,2),(999,2)]),
    ("Flat £2 all",
        [(999,2)]),
]

results_d = []
for name, bands in band_configs:
    # P1 and P2 both with band stakes
    r = simulate(records,
        win_stake_fn=lambda rec, b=bands: (
            sp_band_stake(rec["p1sp"], b),
            sp_band_stake(rec["p2sp"], b),
        ),
        label=name
    )
    results_d.append(r)

print_results(results_d, sort_by="per_bet")

# ── Section E: Variable stakes by tier ───────────────────────────────────────

print()
print("=" * 70)
print("E. VARIABLE STAKES BY TIER")
print("=" * 70)
print()

tier_configs = [
    ("ELITE £3, STRONG £2, GOOD £1, STD £1",
        {TIER_ELITE:3, TIER_STRONG:2, TIER_GOOD:1, TIER_STD:1}),
    ("ELITE £4, STRONG £3, GOOD £2, STD £1",
        {TIER_ELITE:4, TIER_STRONG:3, TIER_GOOD:2, TIER_STD:1}),
    ("ELITE £2, STRONG £2, GOOD £2, STD £2",
        {TIER_ELITE:2, TIER_STRONG:2, TIER_GOOD:2, TIER_STD:2}),
    ("ELITE £3, STRONG £2, GOOD £2, STD skip",
        {TIER_ELITE:3, TIER_STRONG:2, TIER_GOOD:2, TIER_STD:0}),
    ("ELITE £2 only, others skip",
        {TIER_ELITE:2, TIER_STRONG:0, TIER_GOOD:0, TIER_STD:0}),
    ("ELITE+STRONG £2, GOOD+STD skip",
        {TIER_ELITE:2, TIER_STRONG:2, TIER_GOOD:0, TIER_STD:0}),
]

results_e = []
for name, tier_stakes in tier_configs:
    r = simulate(records,
        win_stake_fn=lambda rec, ts=tier_stakes: (
            ts.get(rec["tier"], 0),
            ts.get(rec["tier"], 0),
        ),
        label=name
    )
    results_e.append(r)

print_results(results_e, sort_by="per_bet")

# ── Section F: Combined — tier + SP threshold ─────────────────────────────────

print()
print("=" * 70)
print("F. COMBINED — Tier filter + SP threshold + place bets")
print("   Place bets use approximate 1/4 odds model")
print("=" * 70)
print()

combos = [
    # (label, p1_win_cond, p2_win_cond, p1_place_cond, p2_place_cond, win_stake, place_stake)
    ("Current: win+place both always",
        lambda r: True, lambda r: True,
        lambda r: True, lambda r: True, 2, 2),
    ("Win P1 if SP>=3, place P2 always",
        lambda r: (r["p1sp"] or 0)>=3, lambda r: False,
        lambda r: False, lambda r: True, 2, 2),
    ("Win P1 if SP>=3, place both always",
        lambda r: (r["p1sp"] or 0)>=3, lambda r: False,
        lambda r: True, lambda r: True, 2, 2),
    ("Win P1+P2 if SP>=3, place both",
        lambda r: (r["p1sp"] or 0)>=3, lambda r: (r["p2sp"] or 0)>=3,
        lambda r: True, lambda r: True, 2, 2),
    ("Win P1 if SP>=5, place P2 always",
        lambda r: (r["p1sp"] or 0)>=5, lambda r: False,
        lambda r: False, lambda r: True, 2, 2),
    ("Win both if SP>=3, no place",
        lambda r: (r["p1sp"] or 0)>=3, lambda r: (r["p2sp"] or 0)>=3,
        lambda r: False, lambda r: False, 2, 0),
    ("ELITE+STRONG win both, GOOD+STD place only",
        lambda r: r["tier"] in (TIER_ELITE,TIER_STRONG),
        lambda r: r["tier"] in (TIER_ELITE,TIER_STRONG),
        lambda r: True, lambda r: True, 2, 2),
    ("Win P1 if score>=3 and SP>=2, place P2 always",
        lambda r: r["p1score"]>=3 and (r["p1sp"] or 0)>=2, lambda r: False,
        lambda r: False, lambda r: True, 2, 2),
    ("Win P1 if score>=3 and SP>=3, place both",
        lambda r: r["p1score"]>=3 and (r["p1sp"] or 0)>=3, lambda r: False,
        lambda r: True, lambda r: True, 2, 2),
    ("ELITE win both £3, STRONG win P1 £2 + place P2 £2, GOOD place only £2",
        lambda r: r["tier"] in (TIER_ELITE,TIER_STRONG),
        lambda r: r["tier"] == TIER_ELITE,
        lambda r: r["tier"] in (TIER_GOOD,TIER_STD),
        lambda r: r["tier"] in (TIER_STRONG,TIER_GOOD,TIER_STD),
        3, 2),
]

results_f = []
for item in combos:
    label = item[0]
    p1w_cond, p2w_cond, p1p_cond, p2p_cond, ws, ps = item[1:]

    def make_win_fn(c1, c2, s):
        return lambda rec: (s if c1(rec) else 0, s if c2(rec) else 0)
    def make_place_fn(c1, c2, s):
        if s == 0: return None
        return lambda rec: (s if c1(rec) else 0, s if c2(rec) else 0)

    r = simulate(records,
        win_stake_fn=make_win_fn(p1w_cond, p2w_cond, ws),
        place_stake_fn=make_place_fn(p1p_cond, p2p_cond, ps),
        label=label
    )
    results_f.append(r)

print_results(results_f, sort_by="per_bet")

# ── Section G: Your specific idea ────────────────────────────────────────────
print()
print("=" * 70)
print("G. YOUR IDEA — 'short fav win only, longer shot place only'")
print("   Variants around that theme")
print("=" * 70)
print()

your_ideas = [
    ("If P1 short (<2): win only. If P1 long (>=2): win+place",
        lambda r: (2, 2),
        lambda r: (0, 2) if (r["p1sp"] or 0) < 2 else (2, 2)),
    ("P1<3: win bet only. P1>=3: win+place",
        lambda r: (2, 2),
        lambda r: (0, 0) if (r["p1sp"] or 0) < 3 else (2, 2)),
    ("P1<2: skip. P1 2-4: place only. P1>=4: win+place",
        lambda r: (0 if (r["p1sp"] or 0) < 2 else 0,
                   2 if (r["p1sp"] or 0) >= 4 else 0),
        lambda r: (2 if 2 <= (r["p1sp"] or 0) < 4 else
                   (2 if (r["p1sp"] or 0) >= 4 else 0), 2)),
    ("Win P1 only if >=3. Place P2 only if >=3",
        lambda r: (2 if (r["p1sp"] or 0) >= 3 else 0, 0),
        lambda r: (0, 2 if (r["p2sp"] or 0) >= 3 else 0)),
    ("Win P1 if >=3 + place P2 always (your original idea)",
        lambda r: (2 if (r["p1sp"] or 0) >= 3 else 0, 0),
        lambda r: (0, 2)),
    ("Win P1 if score>=3. Place P2 if score>=2",
        lambda r: (2 if r["p1score"] >= 3 else 0, 0),
        lambda r: (0, 2 if r["p2score"] >= 2 else 0)),
    ("ELITE: win+place both. STRONG: win P1 + place P2. GOOD/STD: place P2 only",
        lambda r: (2 if r["tier"]==TIER_ELITE else
                   (2 if r["tier"]==TIER_STRONG else 0),
                   2 if r["tier"]==TIER_ELITE else 0),
        lambda r: (2 if r["tier"]==TIER_ELITE else 0,
                   2 if r["tier"] in (TIER_ELITE,TIER_STRONG,TIER_GOOD,TIER_STD) else 0)),
    ("Win P1+P2 if ELITE, Win P1 if STRONG, place P2 if GOOD/STD",
        lambda r: (2, 2 if r["tier"]==TIER_ELITE else 0),
        lambda r: (0, 2 if r["tier"] in (TIER_GOOD,TIER_STD) else 0)),
]

results_g = []
for item in your_ideas:
    label, win_fn, place_fn = item
    r = simulate(records, win_stake_fn=win_fn, place_stake_fn=place_fn, label=label)
    results_g.append(r)

print_results(results_g, sort_by="per_bet")

print()
print("NOTE: P&L figures use post-race SP (inflated by backtest contamination).")
print("Focus on win rates, per-bet rankings, and ROI relativities — not absolute P&L.")
print("Done.")
