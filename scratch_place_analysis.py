"""
scratch_place_analysis.py
=========================
Analyses P2 place bet performance by:
1. Score gap (why is P2 ranked second?)
2. SP band
3. Whether P2 has stronger stats than P1

Run from ~/horse_bets_v3:
  python3 scratch_place_analysis.py
"""

import json, os, sys
sys.path.insert(0, os.path.dirname(__file__))
from predict_v2 import get_blended_picks, TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD, _sp_free_score
from predict import place_terms

BET_TIERS  = {TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD}
COMMISSION = 0.05

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

def est_place_pnl(p2sp, placed, stake=2):
    """Estimate place P&L using 1/4 odds model."""
    place_sp = round((p2sp - 1) * 0.25 + 1, 2)
    return round(stake * (place_sp - 1) * (1 - COMMISSION), 2) if placed else -stake

# ── Load records ──────────────────────────────────────────────────────────────

print("Loading races...")
records = []
for fp in sorted(os.listdir("data/raw")):
    if not fp.endswith(".json"): continue
    date = fp.replace(".json","")
    try:
        with open(f"data/raw/{fp}") as f: d = json.load(f)
    except: continue
    for race in (d.get("results") or d.get("races") or []):
        runners = race.get("runners",[])
        if not field_ok(runners, race): continue
        if not any(get_pos(r)==1 for r in runners): continue
        raw_meta = {
            "class":   str(race.get("class","") or ""),
            "surface": race.get("surface","Turf") or "Turf",
            "type":    race.get("type","") or "",
        }
        tc, p1, p2, _ = get_blended_picks(runners, mw_p1=0.1, mw_p2=0.4, raw_race=raw_meta)
        if not p1 or not p2 or tc not in BET_TIERS: continue
        p1sp = tof(p1.get("sp_dec")); p1pos = get_pos(p1)
        p2sp = tof(p2.get("sp_dec")); p2pos = get_pos(p2)
        if not p1sp or not p2sp or p1pos is None or p2pos is None: continue
        p1score = _sp_free_score(p1)
        p2score = _sp_free_score(p2)
        if p1score < 3: continue  # only races where we'd bet

        n       = len(runners)
        places  = place_terms(n)
        p2placed = p2pos <= places

        records.append({
            "date":        date,
            "tier":        tc,
            "p1sp":        p1sp,
            "p1score":     p1score,
            "p1won":       p1pos == 1,
            "p2sp":        p2sp,
            "p2score":     p2score,
            "p2placed":    p2placed,
            "p2won":       p2pos == 1,
            "score_gap":   p1score - p2score,
            "price_ratio": p2sp / p1sp,
            "n":           n,
            "places":      places,
        })

print(f"Records: {len(records)}\n")

def pct(a, b): return f"{a/b*100:.0f}%" if b else "—"

# ── 1. By score gap ───────────────────────────────────────────────────────────

print("=" * 70)
print("1. P2 PLACE PERFORMANCE BY SCORE GAP")
print("   Score gap = P1 score minus P2 score")
print("   Negative = P2 has stronger stats, market weight pushed P1 ahead")
print("=" * 70)
print()
print(f"  {'Bucket':<42} {'N':>5} {'Place%':>7} {'Win%':>6} {'Per bet':>8}")
print(f"  {'-'*70}")

buckets = [
    ("P2 much stronger stats (gap<=-2)", lambda r: r["score_gap"] <= -2),
    ("P2 slightly stronger stats (gap=-1)", lambda r: r["score_gap"] == -1),
    ("Equal stats (gap=0)", lambda r: r["score_gap"] == 0),
    ("P1 slightly stronger (gap=+1)", lambda r: r["score_gap"] == 1),
    ("P1 stronger (gap=+2)", lambda r: r["score_gap"] == 2),
    ("P1 much stronger (gap>=+3)", lambda r: r["score_gap"] >= 3),
]

for label, fn in buckets:
    band = [r for r in records if fn(r)]
    if not band: continue
    n = len(band)
    placed = sum(1 for r in band if r["p2placed"])
    won    = sum(1 for r in band if r["p2won"])
    pnl    = sum(est_place_pnl(r["p2sp"], r["p2placed"]) for r in band)
    print(f"  {label:<42} {n:>5} {pct(placed,n):>7} {pct(won,n):>6} {pnl/n:>+8.3f}")

print()

# ── 2. By P2 SP band ──────────────────────────────────────────────────────────

print("=" * 70)
print("2. P2 PLACE PERFORMANCE BY SP BAND")
print("=" * 70)
print()
print(f"  {'Band':<14} {'N':>5} {'Place%':>7} {'Win%':>6} {'AvgSP':>7} {'P&L':>9} {'Per bet':>8}")
print(f"  {'-'*60}")

sp_bands = [
    ("<2/1",   1.0,  2.0),
    ("2-3/1",  2.0,  3.0),
    ("3-5/1",  3.0,  5.0),
    ("5-8/1",  5.0,  8.0),
    ("8/1+",   8.0, 999),
]

for lbl, lo, hi in sp_bands:
    band = [r for r in records if lo <= r["p2sp"] < hi]
    if not band: continue
    n      = len(band)
    placed = sum(1 for r in band if r["p2placed"])
    won    = sum(1 for r in band if r["p2won"])
    avg_sp = sum(r["p2sp"] for r in band) / n
    pnl    = sum(est_place_pnl(r["p2sp"], r["p2placed"]) for r in band)
    print(f"  {lbl:<14} {n:>5} {pct(placed,n):>7} {pct(won,n):>6} {avg_sp:>7.2f} {pnl:>+9.2f} {pnl/n:>+8.3f}")

print()

# ── 3. Combined: score gap + SP band ─────────────────────────────────────────

print("=" * 70)
print("3. COMBINED: SCORE GAP + SP BAND")
print("   Finding the sweet spot for P2 place bets")
print("=" * 70)
print()
print(f"  {'Rule':<45} {'N':>5} {'Place%':>7} {'Win%':>6} {'Per bet':>8}")
print(f"  {'-'*73}")

rules = [
    ("P2 score>=3 AND P2 SP>=3 (current P2 win rule)",
        lambda r: r["p2score"] >= 3 and r["p2sp"] >= 3.0),
    ("P2 score>=3 AND P2 SP>=2",
        lambda r: r["p2score"] >= 3 and r["p2sp"] >= 2.0),
    ("P2 score>=3 AND P2 SP<3 (short-priced P2)",
        lambda r: r["p2score"] >= 3 and r["p2sp"] < 3.0),
    ("P2 score>=4 AND P2 SP>=2",
        lambda r: r["p2score"] >= 4 and r["p2sp"] >= 2.0),
    ("P2 score>=2 AND P2 SP<2 (odds-on P2)",
        lambda r: r["p2score"] >= 2 and r["p2sp"] < 2.0),
    ("P2 score<P1 score (P2 weaker stats)",
        lambda r: r["score_gap"] > 0),
    ("P2 score>=P1 score (P2 equal/stronger stats)",
        lambda r: r["score_gap"] <= 0),
    ("P2 shorter than P1 (market prefers P2)",
        lambda r: r["p2sp"] < r["p1sp"]),
    ("P2 longer than P1 (market agrees stats)",
        lambda r: r["p2sp"] >= r["p1sp"]),
    ("P2 score>=3 AND P2 SP>=5 (longer odds P2)",
        lambda r: r["p2score"] >= 3 and r["p2sp"] >= 5.0),
    ("score_gap>=2 AND P2 SP>=3 (P1 clearly better stats)",
        lambda r: r["score_gap"] >= 2 and r["p2sp"] >= 3.0),
    ("All P2 place bets (current — score>=2)",
        lambda r: r["p2score"] >= 2),
]

for label, fn in rules:
    band = [r for r in records if fn(r)]
    if len(band) < 20: continue
    n      = len(band)
    placed = sum(1 for r in band if r["p2placed"])
    won    = sum(1 for r in band if r["p2won"])
    pnl    = sum(est_place_pnl(r["p2sp"], r["p2placed"]) for r in band)
    print(f"  {label:<45} {n:>5} {pct(placed,n):>7} {pct(won,n):>6} {pnl/n:>+8.3f}")

print()
print("NOTE: P&L estimated using 1/4 odds model. Relative rankings are reliable.")
print("Done.")
