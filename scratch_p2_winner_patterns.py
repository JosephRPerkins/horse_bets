"""
scratch_p2_winner_patterns.py
==============================
When P1 loses and P2 wins, what distinguishes those races?
Goal: find rules to identify races where we should back P2 instead of P1.
NOT backing both — switching the single bet to P2.

Run from ~/horse_bets_v3:
  python3 scratch_p2_winner_patterns.py
"""

import json, os, sys
from collections import defaultdict
sys.path.insert(0, os.path.dirname(__file__))
from predict_v2 import get_blended_picks, TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD, _sp_free_score
from predict import score_runner

BET_TIERS  = {TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD}
TIER_NAMES = {TIER_ELITE:"ELITE", TIER_STRONG:"STRONG", TIER_GOOD:"GOOD", TIER_STD:"STD"}
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

def band_stake(sp):
    if not sp or sp < 2.0: return 0
    if sp < 6.0: return 2
    if sp < 10.0: return 4
    return 6

# ── Load races ────────────────────────────────────────────────────────────────

print("Loading races...")
p1_won = []    # races where P1 won (our bet was right)
p2_won = []    # races where P1 lost and P2 won (we had the right horse second)
neither = []   # races where neither won

for fp in sorted(os.listdir("data/raw")):
    if not fp.endswith(".json"): continue
    date = fp.replace(".json", "")
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
        tc, p1, p2, _ = get_blended_picks(runners, mw_p1=0.1, mw_p2=0.4, raw_race=raw_meta)
        if not p1 or not p2 or tc not in BET_TIERS: continue

        p1sp   = tof(p1.get("sp_dec")); p1pos = get_pos(p1)
        p2sp   = tof(p2.get("sp_dec")); p2pos = get_pos(p2)
        if not p1sp or not p2sp or p1pos is None or p2pos is None: continue

        p1score = _sp_free_score(p1)
        p2score = _sp_free_score(p2)

        # Only races where we would actually bet
        if band_stake(p1sp) == 0 or p1score < 3: continue

        # SP ratio: P2 price / P1 price (>1 means P2 is longer)
        sp_ratio = round(p2sp / p1sp, 2)

        # Score gap: P1 score - P2 score (positive means P1 is stronger stats)
        score_gap = p1score - p2score

        rec = {
            "date":      date,
            "tier":      tc,
            "p1sp":      p1sp,
            "p2sp":      p2sp,
            "p1score":   p1score,
            "p2score":   p2score,
            "sp_ratio":  sp_ratio,
            "score_gap": score_gap,
            "n_runners": len(runners),
        }

        if p1pos == 1:
            p1_won.append(rec)
        elif p2pos == 1:
            p2_won.append(rec)
        else:
            neither.append(rec)

total = len(p1_won) + len(p2_won) + len(neither)
print(f"Total qualifying races: {total}")
print(f"  P1 won:    {len(p1_won)} ({len(p1_won)/total*100:.0f}%)")
print(f"  P2 won:    {len(p2_won)} ({len(p2_won)/total*100:.0f}%)")
print(f"  Neither:   {len(neither)} ({len(neither)/total*100:.0f}%)")
print()

def pct(a, b): return f"{a/b*100:.0f}%" if b else "—"
def sgn(v):    return f"+£{v:.2f}" if v >= 0 else f"-£{abs(v):.2f}"

# ── 1. SP ratio analysis ──────────────────────────────────────────────────────
# When P2 wins, is it usually priced close to P1 or much longer?

print("=" * 65)
print("1. SP RATIO (P2 price / P1 price)")
print("   <1.0 = P2 shorter than P1 (unusual)")
print("   1.0-1.5 = similar prices")
print("   >2.0 = P2 significantly longer")
print("=" * 65)
print()

ratio_bands = [
    (0.0,  0.8,  "P2 shorter (<0.8x)"),
    (0.8,  1.2,  "Similar (0.8-1.2x)"),
    (1.2,  1.5,  "P2 slightly longer (1.2-1.5x)"),
    (1.5,  2.0,  "P2 longer (1.5-2.0x)"),
    (2.0,  3.0,  "P2 much longer (2-3x)"),
    (3.0, 99.0,  "P2 very long (3x+)"),
]

print(f"  {'SP ratio band':<32} {'P1 wins':>8} {'P2 wins':>8} {'Neither':>8} {'P2 win%':>8}")
print(f"  {'-'*67}")
for lo, hi, lbl in ratio_bands:
    r_p1 = [r for r in p1_won  if lo <= r["sp_ratio"] < hi]
    r_p2 = [r for r in p2_won  if lo <= r["sp_ratio"] < hi]
    r_ne = [r for r in neither if lo <= r["sp_ratio"] < hi]
    n = len(r_p1) + len(r_p2) + len(r_ne)
    if n < 5: continue
    print(f"  {lbl:<32} {len(r_p1):>8} {len(r_p2):>8} {len(r_ne):>8} {pct(len(r_p2), len(r_p2)+len(r_p1)):>8}")

print()

# ── 2. Score gap analysis ─────────────────────────────────────────────────────
# When P1 and P2 have similar scores, does P2 win more?

print("=" * 65)
print("2. SCORE GAP (P1 score minus P2 score)")
print("   0 = same score, positive = P1 stronger stats")
print("=" * 65)
print()

print(f"  {'Score gap':<20} {'P1 wins':>8} {'P2 wins':>8} {'Neither':>8} {'P2 win%':>8}")
print(f"  {'-'*52}")
for gap in sorted(set(r["score_gap"] for r in p1_won + p2_won + neither)):
    r_p1 = [r for r in p1_won  if r["score_gap"] == gap]
    r_p2 = [r for r in p2_won  if r["score_gap"] == gap]
    r_ne = [r for r in neither if r["score_gap"] == gap]
    n = len(r_p1) + len(r_p2) + len(r_ne)
    if n < 10: continue
    print(f"  P1-P2={gap:>+3} ({abs(gap)} {'P1 stronger' if gap>0 else 'P2 stronger' if gap<0 else 'equal':>12})  {len(r_p1):>8} {len(r_p2):>8} {len(r_ne):>8} {pct(len(r_p2), len(r_p2)+len(r_p1)):>8}")

print()

# ── 3. When P2 SP is shorter than P1 SP ──────────────────────────────────────

print("=" * 65)
print("3. KEY FINDING — when P2 is shorter-priced than P1")
print("   (Market disagrees with our stats ranking)")
print("=" * 65)
print()

p2_shorter_p1won = [r for r in p1_won  if r["p2sp"] < r["p1sp"]]
p2_shorter_p2won = [r for r in p2_won  if r["p2sp"] < r["p1sp"]]
p2_shorter_neith = [r for r in neither if r["p2sp"] < r["p1sp"]]
n_shorter = len(p2_shorter_p1won) + len(p2_shorter_p2won) + len(p2_shorter_neith)

p2_longer_p1won  = [r for r in p1_won  if r["p2sp"] >= r["p1sp"]]
p2_longer_p2won  = [r for r in p2_won  if r["p2sp"] >= r["p1sp"]]
p2_longer_neith  = [r for r in neither if r["p2sp"] >= r["p1sp"]]
n_longer = len(p2_longer_p1won) + len(p2_longer_p2won) + len(p2_longer_neith)

print(f"  When P2 is SHORTER than P1 ({n_shorter} races — market prefers P2):")
print(f"    P1 wins: {len(p2_shorter_p1won)} ({pct(len(p2_shorter_p1won), n_shorter)})")
print(f"    P2 wins: {len(p2_shorter_p2won)} ({pct(len(p2_shorter_p2won), n_shorter)})")
print(f"    Neither: {len(p2_shorter_neith)} ({pct(len(p2_shorter_neith), n_shorter)})")
print()
print(f"  When P2 is LONGER than P1 ({n_longer} races — market agrees with stats):")
print(f"    P1 wins: {len(p2_longer_p1won)} ({pct(len(p2_longer_p1won), n_longer)})")
print(f"    P2 wins: {len(p2_longer_p2won)} ({pct(len(p2_longer_p2won), n_longer)})")
print(f"    Neither: {len(p2_longer_neith)} ({pct(len(p2_longer_neith), n_longer)})")

print()

# ── 4. Switching rule — back P2 when market prefers it ───────────────────────

print("=" * 65)
print("4. SWITCHING RULE — back P2 instead of P1 when P2 is shorter")
print("   Compare: always P1 vs switch to P2 when P2 shorter-priced")
print("=" * 65)
print()

# Always P1
always_p1_pnl = 0.0
for r in p1_won + p2_won + neither:
    s = band_stake(r["p1sp"])
    if r in p1_won: always_p1_pnl += s*(r["p1sp"]-1)*(1-COMMISSION)
    else: always_p1_pnl -= s
n_all = len(p1_won) + len(p2_won) + len(neither)

# Switch to P2 when P2 is shorter-priced
switch_pnl = 0.0
switched = not_switched = 0
for group, outcome in [(p1_won,"p1"), (p2_won,"p2"), (neither,"neither")]:
    for r in group:
        if r["p2sp"] < r["p1sp"]:
            # Back P2 instead
            s = band_stake(r["p2sp"])
            switched += 1
            if outcome == "p2": switch_pnl += s*(r["p2sp"]-1)*(1-COMMISSION)
            else: switch_pnl -= s
        else:
            # Back P1 as normal
            s = band_stake(r["p1sp"])
            not_switched += 1
            if outcome == "p1": switch_pnl += s*(r["p1sp"]-1)*(1-COMMISSION)
            else: switch_pnl -= s

print(f"  Always P1:          {n_all} bets  P&L {sgn(always_p1_pnl)}  per bet {sgn(always_p1_pnl/n_all)}")
print(f"  Switch when P2<P1:  {n_all} bets  P&L {sgn(switch_pnl)}  per bet {sgn(switch_pnl/n_all)}")
print(f"  (Switched {switched} races, kept P1 in {not_switched} races)")
print()

# ── 5. Tighter switching rules ────────────────────────────────────────────────

print("=" * 65)
print("5. REFINED SWITCHING RULES")
print("   Only switch when P2 is meaningfully shorter AND scores well")
print("=" * 65)
print()

switching_rules = [
    ("P2 < P1 (any)",
        lambda r: r["p2sp"] < r["p1sp"]),
    ("P2 < P1 by >20%",
        lambda r: r["p2sp"] < r["p1sp"] * 0.8),
    ("P2 < P1 by >30%",
        lambda r: r["p2sp"] < r["p1sp"] * 0.7),
    ("P2 < P1 AND P2 score>=3",
        lambda r: r["p2sp"] < r["p1sp"] and r["p2score"] >= 3),
    ("P2 < P1 AND P2 score>=4",
        lambda r: r["p2sp"] < r["p1sp"] and r["p2score"] >= 4),
    ("P2 < P1 by >20% AND P2 score>=3",
        lambda r: r["p2sp"] < r["p1sp"] * 0.8 and r["p2score"] >= 3),
    ("P2 < P1 by >30% AND P2 score>=3",
        lambda r: r["p2sp"] < r["p1sp"] * 0.7 and r["p2score"] >= 3),
    ("Never switch (baseline)",
        lambda r: False),
]

print(f"  {'Rule':<40} {'Switched':>8} {'P&L':>10} {'Per bet':>8}")
print(f"  {'-'*70}")

for rule_name, switch_fn in switching_rules:
    pnl = 0.0; n_sw = 0
    for group, outcome in [(p1_won,"p1"), (p2_won,"p2"), (neither,"neither")]:
        for r in group:
            if switch_fn(r):
                s = band_stake(r["p2sp"])
                n_sw += 1
                if outcome == "p2": pnl += s*(r["p2sp"]-1)*(1-COMMISSION)
                else: pnl -= s if s else 0
            else:
                s = band_stake(r["p1sp"])
                if outcome == "p1": pnl += s*(r["p1sp"]-1)*(1-COMMISSION)
                else: pnl -= s
    print(f"  {rule_name:<40} {n_sw:>8} {sgn(pnl):>10} {sgn(pnl/n_all):>8}")

print()
print("NOTE: P&L inflated by backtest contamination.")
print("Focus on relative rankings — which rule beats the baseline.")
print("Done.")
