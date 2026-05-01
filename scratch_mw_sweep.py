"""
scratch_mw_sweep.py
===================
Tests different mw_p1 values against real settled bet data from the tier tracker.
No backtest — uses only races the bot actually settled.

For each mw_p1 value, recalculates what get_blended_picks would have picked
and compares to actual results.

Run from ~/horse_bets_v3:
  python3 scratch_mw_sweep.py 2>&1 | tee mw_sweep_output.txt
"""

import json, os, sys, re
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from predict import score_runner
from predict_v2 import get_blended_picks, TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_LABELS

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

def norm_name(n):
    return re.sub(r"\s*\([A-Z]{2,3}\)\s*$","",n).strip().lower()[:20]

# ── Load and deduplicate tier tracker ─────────────────────────────────────────

with open("data/logs/tier_performance.json") as f:
    tracker = json.load(f)

seen = set(); deduped = []
for e in tracker:
    key = (e.get("date",""), e.get("off",""), e.get("course",""))
    if key not in seen: seen.add(key); deduped.append(e)
tracker = deduped
print(f"Tier tracker: {len(tracker)} races (after dedup)")

# ── Build lookup: (date, off, course) -> raw race with runners+positions ──────

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

matched = len(race_lookup)
print(f"Matched: {matched}/{len(tracker)} races")
print()

# ── Sweep mw_p1 values ────────────────────────────────────────────────────────

MW_VALUES = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]

# Also test pure score_runner (mw=0.0) and pure market (mw=1.0)
MW_VALUES = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]

results = {}

for mw in MW_VALUES:
    stats = {
        "all":    {"n":0,"p1w":0,"p2w":0,"either":0},
        "ELITE":  {"n":0,"p1w":0,"p2w":0,"either":0},
        "STRONG": {"n":0,"p1w":0,"p2w":0,"either":0},
        "GOOD":   {"n":0,"p1w":0,"p2w":0,"either":0},
    }

    for entry in tracker:
        date_str = entry.get("date","")
        off      = entry.get("off","")
        course   = entry.get("course","")
        key      = (date_str, off, course)
        orig_tier = entry.get("tier", 0)

        raw_race = race_lookup.get(key)
        if not raw_race: continue

        runners = raw_race.get("runners",[])
        if len(runners) < 2: continue

        raw_meta = {
            "class":   str(raw_race.get("class","") or ""),
            "surface": raw_race.get("surface","Turf") or "Turf",
            "type":    raw_race.get("type","") or "",
        }

        # Get picks at this mw_p1
        if mw == 0.0:
            # Pure score_runner
            scored = sorted(runners, key=lambda r: (-score_runner(r)[0], tof(r.get("sp_dec")) or 999))
            p1 = scored[0] if scored else None
            p2 = scored[1] if len(scored) > 1 else None
            # Use original tier from tracker
            tc = orig_tier
        elif mw == 1.0:
            # Pure market (lowest SP)
            by_price = sorted([r for r in runners if tof(r.get("sp_dec"))],
                              key=lambda r: tof(r.get("sp_dec")))
            p1 = by_price[0] if by_price else None
            p2 = by_price[1] if len(by_price) > 1 else None
            tc = orig_tier
        else:
            tc, p1, p2, _ = get_blended_picks(
                runners, mw_p1=mw, mw_p2=0.40, raw_race=raw_meta
            )

        if not p1: continue
        if tc not in (TIER_ELITE, TIER_STRONG, TIER_GOOD): continue

        p1pos = get_pos(p1)
        p2pos = get_pos(p2) if p2 else None
        if p1pos is None: continue

        p1w = p1pos == 1
        p2w = p2pos == 1 if p2pos is not None else False
        tier_name = {TIER_ELITE:"ELITE", TIER_STRONG:"STRONG", TIER_GOOD:"GOOD"}.get(tc, "OTHER")

        for bucket in ("all", tier_name):
            s = stats[bucket]
            s["n"]      += 1
            s["p1w"]    += int(p1w)
            s["p2w"]    += int(p2w)
            s["either"] += int(p1w or p2w)

    results[mw] = stats

# ── Print results ─────────────────────────────────────────────────────────────

def pct(a, b):
    return f"{a/b*100:.0f}%" if b else "—"

print("=" * 90)
print("MW SWEEP — P1 win rate by market weight (real settled data only)")
print("mw=0.0 = pure score_runner | mw=1.0 = pure market favourite")
print("=" * 90)
print()

# Overall
print(f"  {'mw':>5}  {'N':>5}  {'P1 win%':>8}  {'P2 win%':>8}  {'Either%':>8}")
print(f"  {'-'*45}")
for mw in MW_VALUES:
    s = results[mw]["all"]
    n = s["n"]
    label = f"{mw:.1f}"
    if mw == 0.0: label = "0.0 (pure stats)"
    if mw == 1.0: label = "1.0 (pure mkt) "
    print(f"  {label:>16}  {n:>5}  {pct(s['p1w'],n):>8}  {pct(s['p2w'],n):>8}  {pct(s['either'],n):>8}")

print()

# Per tier
for tier_name in ("ELITE", "STRONG", "GOOD"):
    print(f"  {tier_name}")
    print(f"  {'mw':>5}  {'N':>5}  {'P1 win%':>8}  {'P2 win%':>8}  {'Either%':>8}")
    print(f"  {'-'*45}")
    for mw in MW_VALUES:
        s = results[mw][tier_name]
        n = s["n"]
        if not n: continue
        label = f"{mw:.1f}"
        if mw == 0.0: label = "0.0 (pure stats)"
        if mw == 1.0: label = "1.0 (pure mkt) "
        print(f"  {label:>16}  {n:>5}  {pct(s['p1w'],n):>8}  {pct(s['p2w'],n):>8}  {pct(s['either'],n):>8}")
    print()

# ── Best mw per tier ──────────────────────────────────────────────────────────

print("=" * 60)
print("OPTIMAL mw_p1 PER TIER (highest P1 win rate)")
print("=" * 60)
for tier_name in ("all", "ELITE", "STRONG", "GOOD"):
    best_mw = max(MW_VALUES, key=lambda mw: results[mw][tier_name]["p1w"] /
                  max(results[mw][tier_name]["n"], 1))
    s = results[best_mw][tier_name]
    print(f"  {tier_name:<8} best mw={best_mw:.1f}  "
          f"P1 win={pct(s['p1w'],s['n'])}  n={s['n']}")

print()
print("Done.")
