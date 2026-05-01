"""
scratch_mw_grid.py
==================
Full grid sweep: every combination of mw_p1 and mw_p2 (0.0 to 1.0 in 0.1 steps)
tested against real settled bet data from the tier tracker.

For each (mw_p1, mw_p2) pair, calculates P1 win%, P2 win%, and either% 
across all matched races, and per tier.

Output: ranked table of best combinations overall and per tier.

Run from ~/horse_bets_v3:
  python3 scratch_mw_grid.py 2>&1 | tee mw_grid_output.txt
"""

import json, os, sys, re
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from predict import score_runner
from predict_v2 import get_blended_picks, TIER_ELITE, TIER_STRONG, TIER_GOOD

MW_VALUES = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
TIER_NAMES = {TIER_ELITE: "ELITE", TIER_STRONG: "STRONG", TIER_GOOD: "GOOD"}

def tof(v):
    try:
        f = float(str(v).strip())
        return f if f > 0 else None
    except: return None

def get_pos(r):
    try: return int(str(r.get("position","")).strip())
    except: return None

def conv_off(off_raw):
    if not off_raw or ":" not in off_raw: return off_raw
    try:
        parts = str(off_raw).strip().split(":")
        h = int(parts[0]); m = int(parts[1])
        if h < 9: h += 12
        return f"{h:02d}:{m:02d}"
    except: return off_raw

def norm_course(c):
    return (c or "").lower().replace(" ","").replace("-","").replace("(aw)","").replace("(ire)","")

# ── Load and deduplicate tier tracker ─────────────────────────────────────────

with open("data/logs/tier_performance.json") as f:
    tracker = json.load(f)

seen = set(); deduped = []
for e in tracker:
    key = (e.get("date",""), e.get("off",""), e.get("course",""))
    if key not in seen: seen.add(key); deduped.append(e)
tracker = deduped
print(f"Tier tracker: {len(tracker)} races (after dedup)")

# ── Build race lookup ─────────────────────────────────────────────────────────

print("Loading raw results...")
race_lookup = {}
for entry in tracker:
    date_str = entry.get("date","")
    off      = entry.get("off","")
    course   = entry.get("course","")
    key      = (date_str, off, course)
    raw_path = f"data/raw/{date_str}.json"
    if not os.path.exists(raw_path): continue
    with open(raw_path) as f: d = json.load(f)
    races = d.get("results") or d.get("races") or []
    tc = norm_course(course)
    for race in races:
        rc = norm_course(race.get("course",""))
        off_bst = conv_off(str(race.get("off") or ""))
        if tc in rc and off_bst == off:
            race_lookup[key] = race
            break

print(f"Matched: {len(race_lookup)}/{len(tracker)} races")
print(f"Running {len(MW_VALUES)*len(MW_VALUES)} combinations...")
print()

# ── Grid sweep ────────────────────────────────────────────────────────────────

# grid[mw_p1][mw_p2] = {"all": stats, "ELITE": stats, "STRONG": stats, "GOOD": stats}
def empty():
    return {"n":0, "p1w":0, "p2w":0, "either":0}

grid = {}
for mw1 in MW_VALUES:
    grid[mw1] = {}
    for mw2 in MW_VALUES:
        grid[mw1][mw2] = {
            "all":    empty(),
            "ELITE":  empty(),
            "STRONG": empty(),
            "GOOD":   empty(),
        }

for entry in tracker:
    date_str = entry.get("date","")
    off      = entry.get("off","")
    course   = entry.get("course","")
    key      = (date_str, off, course)

    raw_race = race_lookup.get(key)
    if not raw_race: continue

    runners = raw_race.get("runners",[])
    if len(runners) < 2: continue

    raw_meta = {
        "class":   str(raw_race.get("class","") or ""),
        "surface": raw_race.get("surface","Turf") or "Turf",
        "type":    raw_race.get("type","") or "",
    }

    # Pre-compute all mw_p1 picks (P1 horse for each mw_p1)
    p1_by_mw = {}
    for mw1 in MW_VALUES:
        if mw1 == 0.0:
            scored = sorted(runners, key=lambda r: (-score_runner(r)[0], tof(r.get("sp_dec")) or 999))
            p1 = scored[0] if scored else None
            # Use get_blended_picks just for tier
            tc, _, _, _ = get_blended_picks(runners, mw_p1=0.1, mw_p2=0.4, raw_race=raw_meta)
        elif mw1 == 1.0:
            by_price = sorted([r for r in runners if tof(r.get("sp_dec"))],
                              key=lambda r: tof(r.get("sp_dec")))
            p1 = by_price[0] if by_price else None
            tc, _, _, _ = get_blended_picks(runners, mw_p1=0.9, mw_p2=0.4, raw_race=raw_meta)
        else:
            tc, p1, _, _ = get_blended_picks(runners, mw_p1=mw1, mw_p2=0.4, raw_race=raw_meta)
        p1_by_mw[mw1] = (tc, p1)

    # For each (mw_p1, mw_p2) pair, get P2 excluding the mw_p1 P1
    for mw1 in MW_VALUES:
        tc, p1 = p1_by_mw[mw1]
        if not p1: continue
        if tc not in (TIER_ELITE, TIER_STRONG, TIER_GOOD): continue

        p1_hid  = p1.get("horse_id","")
        p1pos   = get_pos(p1)
        if p1pos is None: continue
        p1w = p1pos == 1

        tier_name = TIER_NAMES.get(tc, "OTHER")

        for mw2 in MW_VALUES:
            # Get P2 at this mw_p2, excluding P1
            if mw2 == 0.0:
                scored2 = sorted(runners, key=lambda r: (-score_runner(r)[0], tof(r.get("sp_dec")) or 999))
                p2 = next((r for r in scored2 if r.get("horse_id","") != p1_hid), None)
            elif mw2 == 1.0:
                by_price2 = sorted([r for r in runners if tof(r.get("sp_dec")) and r.get("horse_id","") != p1_hid],
                                   key=lambda r: tof(r.get("sp_dec")))
                p2 = by_price2[0] if by_price2 else None
            else:
                _, _, p2_blend, _ = get_blended_picks(runners, mw_p1=mw1, mw_p2=mw2, raw_race=raw_meta)
                # p2_blend already excludes p1 from get_blended_picks
                # but p1 may differ from our mw1 p1 — re-select excluding our p1
                if p2_blend and p2_blend.get("horse_id","") == p1_hid:
                    # find next best at mw2
                    from predict_v2 import _blend_runners
                    p2 = None  # fallback
                else:
                    p2 = p2_blend

            p2pos = get_pos(p2) if p2 else None
            p2w   = p2pos == 1 if p2pos is not None else False

            s_all  = grid[mw1][mw2]["all"]
            s_tier = grid[mw1][mw2][tier_name]

            for s in (s_all, s_tier):
                s["n"]      += 1
                s["p1w"]    += int(p1w)
                s["p2w"]    += int(p2w)
                s["either"] += int(p1w or p2w)

# ── Output ────────────────────────────────────────────────────────────────────

def pct(a, b):
    return f"{a/b*100:.0f}%" if b else "—"

def score(s):
    """Combined score: weight P1 and either% equally."""
    n = s["n"]
    if n < 10: return 0
    return (s["p1w"] + s["either"]) / (2 * n)

# Print top 20 combinations overall
print("=" * 70)
print("TOP 20 COMBINATIONS — overall (min 50 races)")
print("Ranked by P1 win% + Either% combined score")
print("=" * 70)
print(f"  {'mw_p1':>6} {'mw_p2':>6} {'N':>5} {'P1win':>7} {'P2win':>7} {'Either':>7} {'Score':>7}")
print(f"  {'-'*50}")

combos = []
for mw1 in MW_VALUES:
    for mw2 in MW_VALUES:
        s = grid[mw1][mw2]["all"]
        if s["n"] >= 50:
            combos.append((mw1, mw2, s))

combos.sort(key=lambda x: -score(x[2]))
for mw1, mw2, s in combos[:20]:
    n = s["n"]
    sc = score(s)
    print(f"  {mw1:>6.1f} {mw2:>6.1f} {n:>5} {pct(s['p1w'],n):>7} {pct(s['p2w'],n):>7} {pct(s['either'],n):>7} {sc:>7.3f}")

# Per tier top 10
for tier_name in ("ELITE", "STRONG", "GOOD"):
    print()
    print(f"{'='*70}")
    print(f"TOP 10 COMBINATIONS — {tier_name} (min 15 races)")
    print(f"{'='*70}")
    print(f"  {'mw_p1':>6} {'mw_p2':>6} {'N':>5} {'P1win':>7} {'P2win':>7} {'Either':>7} {'Score':>7}")
    print(f"  {'-'*50}")

    tier_combos = []
    for mw1 in MW_VALUES:
        for mw2 in MW_VALUES:
            s = grid[mw1][mw2][tier_name]
            if s["n"] >= 15:
                tier_combos.append((mw1, mw2, s))

    tier_combos.sort(key=lambda x: -score(x[2]))
    for mw1, mw2, s in tier_combos[:10]:
        n = s["n"]
        sc = score(s)
        print(f"  {mw1:>6.1f} {mw2:>6.1f} {n:>5} {pct(s['p1w'],n):>7} {pct(s['p2w'],n):>7} {pct(s['either'],n):>7} {sc:>7.3f}")

# Current config for reference
print()
print("=" * 70)
print("CURRENT CONFIG PERFORMANCE")
print("=" * 70)
s = grid[0.6][0.4]["all"]
n = s["n"]
print(f"  Current (mw_p1=0.6, mw_p2=0.4): n={n} P1={pct(s['p1w'],n)} P2={pct(s['p2w'],n)} Either={pct(s['either'],n)}")
s = grid[0.1][0.4]["all"]
n = s["n"]
print(f"  Proposed (mw_p1=0.1, mw_p2=0.4): n={n} P1={pct(s['p1w'],n)} P2={pct(s['p2w'],n)} Either={pct(s['either'],n)}")
s = grid[0.0][0.4]["all"]
n = s["n"]
print(f"  Pure stats P1 (mw_p1=0.0, mw_p2=0.4): n={n} P1={pct(s['p1w'],n)} P2={pct(s['p2w'],n)} Either={pct(s['either'],n)}")

print()
print("Done.")
