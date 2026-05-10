"""
scratch_oddon_filter.py
=======================
Analyses races currently skipped by the SP>=2 floor filter.
Are we missing value by skipping odds-on picks?

Run from ~/horse_bets_v3:
  python3 scratch_oddon_filter.py
"""

import json, os, sys
sys.path.insert(0, os.path.dirname(__file__))
from predict_v2 import get_blended_picks, TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD, _sp_free_score
from betfair.strategy import win_stake_for_pick

BET_TIERS  = {TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD}
COMMISSION = 0.05

def tof(v):
    try:
        f = float(str(v).strip())
        return f if f > 0 else None
    except: return None

def get_pos(r):
    try: return int(str(r.get("position", "")).strip())
    except: return None

def field_ok(runners, race):
    n = len(runners)
    if n < 2: return False
    rt = str(race.get("type", "") or "").lower()
    is_jump = any(t in rt for t in ("chase", "hurdle", "nh flat", "national hunt"))
    cls = str(race.get("class", "") or "").replace("Class", "").strip()
    if cls in ("1", "2") and is_jump: return False
    if is_jump and n > 12: return False
    if not is_jump and n > 20: return False
    return True

skipped = []

for fp in sorted(os.listdir("data/raw")):
    if not fp.endswith(".json"): continue
    try:
        with open(f"data/raw/{fp}") as f: d = json.load(f)
    except: continue
    for race in (d.get("results") or d.get("races") or []):
        runners = race.get("runners", [])
        if not field_ok(runners, race): continue
        if not any(get_pos(r) == 1 for r in runners): continue
        raw_meta = {
            "class":   str(race.get("class", "") or ""),
            "surface": race.get("surface", "Turf") or "Turf",
            "type":    race.get("type", "") or "",
        }
        tc, p1, _, _ = get_blended_picks(runners, mw_p1=0.1, mw_p2=0.4, raw_race=raw_meta)
        if not p1 or tc not in BET_TIERS: continue
        sp    = tof(p1.get("sp_dec"))
        pos   = get_pos(p1)
        score = _sp_free_score(p1)
        if not sp or pos is None or score < 3: continue
        if sp >= 2.0: continue  # only skipped races (odds-on)
        won  = pos == 1
        pnl  = round(2 * (sp - 1) * (1 - COMMISSION), 2) if won else -2.0
        skipped.append({"sp": sp, "score": score, "won": won, "pnl": pnl, "tier": tc})

n    = len(skipped)
wins = sum(1 for r in skipped if r["won"])
pnl  = sum(r["pnl"] for r in skipped)

print(f"Races skipped by SP<2 floor: {n}")
print(f"Win rate:  {wins}/{n} ({wins/n*100:.0f}%)")
print(f"P&L (£2 flat): {pnl:+.2f}")
print(f"Per bet:   {pnl/n:+.3f}")
print()

print("By SP band:")
for lo, hi, lbl in [(0, 1.25, "<5/4"), (1.25, 1.5, "5/4-6/4"), (1.5, 2.0, "6/4-2/1")]:
    b = [r for r in skipped if lo <= r["sp"] < hi]
    if not b: continue
    bw = sum(1 for r in b if r["won"])
    bp = sum(r["pnl"] for r in b)
    be = round(1 / ((sum(r["sp"] for r in b) / len(b)) * (1 - COMMISSION)) * 100, 1)
    print(f"  {lbl:<12} n={len(b):>4}  wins={bw:>4} ({bw/len(b)*100:.0f}%)  "
          f"breakeven={be:.0f}%  P&L={bp:>+8.2f}  per bet={bp/len(b):>+7.3f}")

print()
print("By score:")
for s in range(3, 10):
    b = [r for r in skipped if r["score"] == s]
    if len(b) < 5: continue
    bw = sum(1 for r in b if r["won"])
    bp = sum(r["pnl"] for r in b)
    print(f"  score={s}  n={len(b):>4}  wins={bw:>4} ({bw/len(b)*100:.0f}%)  "
          f"P&L={bp:>+8.2f}  per bet={bp/len(b):>+7.3f}")

print()
print("Done.")
