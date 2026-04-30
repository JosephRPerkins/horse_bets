"""
scratch_hybrid_v2.py
====================
Tests the correct hybrid: old score_runner P1+P2 picks,
filtered through System C tier qualification.

Systems compared:
  A) Old system   — score_runner P1+P2, old tier logic (all qualifying)
  B) System C     — blended P1+P2, System C tier filter
  C) Hybrid v2    — score_runner P1+P2, System C tier filter (the untested one)
  D) Hybrid v2 ELITE+STRONG only — same but skip GOOD tier

Run from ~/horse_bets_v3:
  python3 scratch_hybrid_v2.py 2>&1 | tee hybrid_v2_output.txt
"""

import json, glob, os, sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from predict import score_runner, place_terms, SIGNAL_WEIGHTS
from predict_v2 import get_blended_picks, TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP

COMMISSION  = 0.05
STAKE       = 2.0

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
    cls = str(race.get("class","") or "").replace("Class","").strip()
    if cls in ("1","2"): return False
    if n > 12 or n < 2: return False
    return True

def pnl(won, sp):
    if sp is None: return None
    return round(STAKE * (sp-1) * (1-COMMISSION), 2) if won else -STAKE

def empty():
    return {"n":0,"p1w":0,"p2w":0,"either":0,"neither":0,
            "p1pnl":0.0,"p2pnl":0.0,
            "p1_prices":[],"p2_prices":[]}

TIER_NAMES = {TIER_ELITE:"ELITE", TIER_STRONG:"STRONG", TIER_GOOD:"GOOD"}

# ── Load data ─────────────────────────────────────────────────────────────────

all_races = []
for fp in sorted(glob.glob("data/raw/*.json")):
    date_str = os.path.basename(fp).replace(".json","")
    try:
        with open(fp) as f: d = json.load(f)
    except: continue
    for race in (d.get("results") or d.get("races") or []):
        race["_date"] = date_str
        all_races.append(race)

print(f"Loaded {len(all_races)} races")
print()

# ── Run all systems ───────────────────────────────────────────────────────────

# Stats per system, per tier
stats = {
    "old":       defaultdict(empty),   # tier = "all" or "ELITE" etc per old logic
    "sysc":      defaultdict(empty),   # tier = TIER_ELITE etc
    "hybrid_v2": defaultdict(empty),   # tier = TIER_ELITE etc (SysC filter, old picks)
    "hybrid_es": defaultdict(empty),   # ELITE+STRONG only version
}

# Also per date window
WINDOWS = {
    "Mar16-Apr12": ("2026-03-16","2026-04-12"),
    "Apr13-Apr26": ("2026-04-13","2026-04-26"),
    "Apr27-Apr29": ("2026-04-27","2026-04-29"),
}
win_stats = {sys_name: {w: empty() for w in WINDOWS}
             for sys_name in ("old","sysc","hybrid_v2","hybrid_es")}

skipped = 0

for race in all_races:
    runners = race.get("runners",[])
    if not field_ok(runners, race):
        skipped += 1
        continue

    date_str = race.get("_date","")
    raw_meta = {
        "class":   str(race.get("class","") or ""),
        "surface": race.get("surface","Turf") or "Turf",
        "type":    race.get("type","") or "",
    }

    # ── Old system picks ──────────────────────────────────────────────────────
    scored = sorted(runners,
        key=lambda r: (-score_runner(r)[0], tof(r.get("sp_dec")) or 999))
    op1 = scored[0] if scored else None
    op2 = scored[1] if len(scored) > 1 else None

    # ── System C picks + tier ─────────────────────────────────────────────────
    tc, cp1, cp2, _ = get_blended_picks(
        runners, mw_p1=0.60, mw_p2=0.40, raw_race=raw_meta
    )

    def record(sys_name, tier_key, p1, p2):
        if not p1: return
        p1sp  = tof(p1.get("sp_dec"))
        p2sp  = tof(p2.get("sp_dec")) if p2 else None
        p1pos = get_pos(p1)
        p2pos = get_pos(p2) if p2 else None
        if p1pos is None: return          # no result — skip

        p1w   = p1pos == 1
        p2w   = p2pos == 1 if p2pos is not None else False
        p1pl  = pnl(p1w, p1sp)
        p2pl  = pnl(p2w, p2sp) if p2sp and p2pos is not None else None

        s = stats[sys_name][tier_key]
        s["n"]      += 1
        s["p1w"]    += p1w
        s["p2w"]    += p2w
        s["either"] += p1w or p2w
        s["neither"]+= not (p1w or p2w)
        if p1pl is not None: s["p1pnl"] += p1pl
        if p2pl is not None: s["p2pnl"] += p2pl
        if p1sp: s["p1_prices"].append(p1sp)
        if p2sp: s["p2_prices"].append(p2sp)

        for wname,(wstart,wend) in WINDOWS.items():
            if wstart <= date_str <= wend:
                ws = win_stats[sys_name][wname]
                ws["n"]      += 1
                ws["p1w"]    += p1w
                ws["p2w"]    += p2w
                ws["either"] += p1w or p2w
                ws["neither"]+= not (p1w or p2w)
                if p1pl is not None: ws["p1pnl"] += p1pl
                if p2pl is not None: ws["p2pnl"] += p2pl

    # Old system — all qualifying (no tier filter, old going/class rules)
    if op1:
        record("old", "all", op1, op2)

    # System C — blended picks, SysC tier
    if tc in (TIER_ELITE, TIER_STRONG, TIER_GOOD) and cp1:
        record("sysc", TIER_NAMES[tc], cp1, cp2)

    # Hybrid v2 — SysC tier filter, but OLD picks
    if tc in (TIER_ELITE, TIER_STRONG, TIER_GOOD) and op1:
        record("hybrid_v2", TIER_NAMES[tc], op1, op2)
        # ELITE+STRONG only
        if tc in (TIER_ELITE, TIER_STRONG):
            record("hybrid_es", TIER_NAMES[tc], op1, op2)

print(f"Skipped {skipped} races (Class 1/2, >12 runners, <2 runners)")
print()

# ── Output helpers ────────────────────────────────────────────────────────────

def pct(a,b): return f"{a/b*100:.0f}%" if b else "—"
def sgn(v):   return f"+£{v:.2f}" if v>=0 else f"-£{abs(v):.2f}"
def avg(lst):  return sum(lst)/len(lst) if lst else 0

def print_stats(label, s_dict, tiers=None):
    """Print stats for a system, aggregating across specified tier keys."""
    if tiers is None:
        tiers = list(s_dict.keys())
    rows = [s_dict[t] for t in tiers if s_dict[t]["n"] > 0]
    if not rows: return
    tot_n    = sum(r["n"] for r in rows)
    tot_p1w  = sum(r["p1w"] for r in rows)
    tot_p2w  = sum(r["p2w"] for r in rows)
    tot_ei   = sum(r["either"] for r in rows)
    tot_ni   = sum(r["neither"] for r in rows)
    tot_p1pl = sum(r["p1pnl"] for r in rows)
    tot_p2pl = sum(r["p2pnl"] for r in rows)
    all_p1   = [p for r in rows for p in r["p1_prices"]]
    all_p2   = [p for r in rows for p in r["p2_prices"]]
    print(f"  {label:<35} {tot_n:>5} {pct(tot_p1w,tot_n):>7} {pct(tot_p2w,tot_n):>7} "
          f"{pct(tot_ei,tot_n):>7} {pct(tot_ni,tot_n):>8} "
          f"{sgn(tot_p1pl):>9} {sgn(tot_p2pl):>9} {sgn(tot_p1pl+tot_p2pl):>9} "
          f"  P1avg={avg(all_p1):.1f} P2avg={avg(all_p2):.1f}")

# ── Full dataset ──────────────────────────────────────────────────────────────

print("=" * 120)
print("FULL DATASET — All 45 days — Flat £2 stake")
print("=" * 120)
hdr = f"  {'System':<35} {'N':>5} {'P1win':>7} {'P2win':>7} {'Either':>7} {'Neither':>8} {'P1 P&L':>9} {'P2 P&L':>9} {'Total':>9}   Avg SPs"
print(hdr)
print("-"*120)

print_stats("Old system (score_runner, all)", stats["old"], ["all"])
print()

# System C per tier then total
for tname in ("ELITE","STRONG","GOOD"):
    print_stats(f"  System C — {tname}", stats["sysc"], [tname])
print_stats("  System C TOTAL", stats["sysc"], ["ELITE","STRONG","GOOD"])
print()

# Hybrid v2 per tier then total
for tname in ("ELITE","STRONG","GOOD"):
    print_stats(f"  Hybrid v2 — {tname}", stats["hybrid_v2"], [tname])
print_stats("  Hybrid v2 TOTAL (E+S+G)", stats["hybrid_v2"], ["ELITE","STRONG","GOOD"])
print_stats("  Hybrid v2 ELITE+STRONG only", stats["hybrid_es"], ["ELITE","STRONG"])

# ── By time window ────────────────────────────────────────────────────────────

print()
print("=" * 100)
print("BY TIME WINDOW")
print("=" * 100)

for wname in WINDOWS:
    print(f"\n  {wname}")
    print(f"  {'System':<35} {'N':>5} {'P1win':>7} {'P2win':>7} {'Either':>7} {'P1 P&L':>9} {'Total':>9}")
    print(f"  {'-'*80}")

    def pw(sys_name, label):
        ws = win_stats[sys_name][wname]
        n = ws["n"]
        if not n: return
        print(f"  {label:<35} {n:>5} {pct(ws['p1w'],n):>7} {pct(ws['p2w'],n):>7} "
              f"{pct(ws['either'],n):>7} {sgn(ws['p1pnl']):>9} {sgn(ws['p1pnl']+ws['p2pnl']):>9}")

    pw("old",       "Old system")
    pw("sysc",      "System C")
    pw("hybrid_v2", "Hybrid v2 (SR picks, SysC tier)")
    pw("hybrid_es", "Hybrid v2 ELITE+STRONG only")

# ── P1 overlap analysis ───────────────────────────────────────────────────────

print()
print("=" * 80)
print("P1 AGREEMENT: How often do SysC and Old pick the same P1?")
print("=" * 80)

same = diff = 0
same_p1w = diff_p1w = same_p1pnl = diff_p1pnl = 0.0
same_n = diff_n = 0

for race in all_races:
    runners = race.get("runners",[])
    if not field_ok(runners, race): continue

    raw_meta = {
        "class":   str(race.get("class","") or ""),
        "surface": race.get("surface","Turf") or "Turf",
        "type":    race.get("type","") or "",
    }
    tc, cp1, cp2, _ = get_blended_picks(runners, mw_p1=0.60, mw_p2=0.40, raw_race=raw_meta)
    if tc not in (TIER_ELITE, TIER_STRONG, TIER_GOOD) or not cp1: continue

    scored = sorted(runners,
        key=lambda r: (-score_runner(r)[0], tof(r.get("sp_dec")) or 999))
    op1 = scored[0] if scored else None
    if not op1: continue

    cp1sp = tof(cp1.get("sp_dec"))
    op1sp = tof(op1.get("sp_dec"))
    cp1pos = get_pos(cp1)
    op1pos = get_pos(op1)
    if cp1pos is None or op1pos is None: continue

    if cp1.get("horse_id","") == op1.get("horse_id",""):
        same += 1
        same_n += 1
        same_p1w += cp1pos == 1
        if cp1sp: same_p1pnl += pnl(cp1pos==1, cp1sp)
    else:
        diff += 1
        diff_n += 1
        # Track both: what SysC picked vs what old picked
        diff_p1w += op1pos == 1   # old system's pick won
        if op1sp: diff_p1pnl += pnl(op1pos==1, op1sp)

total_agree = same + diff
print(f"\n  Same P1:    {same}/{total_agree} ({same/total_agree*100:.0f}%)")
print(f"  Differ P1:  {diff}/{total_agree} ({diff/total_agree*100:.0f}%)")
print()
print(f"  When they AGREE:  P1 win% = {same_p1w/same_n*100:.0f}%  P&L = {sgn(same_p1pnl)}")
print(f"  When they DIFFER: OLD P1 wins at {diff_p1w/diff_n*100:.0f}%  P&L = {sgn(diff_p1pnl)}")

# Also check SysC P1 when they differ
diff_sysc_p1w = 0.0
diff_sysc_pnl = 0.0
for race in all_races:
    runners = race.get("runners",[])
    if not field_ok(runners, race): continue
    raw_meta = {"class": str(race.get("class","") or ""), "surface": race.get("surface","Turf") or "Turf", "type": race.get("type","") or ""}
    tc, cp1, cp2, _ = get_blended_picks(runners, mw_p1=0.60, mw_p2=0.40, raw_race=raw_meta)
    if tc not in (TIER_ELITE, TIER_STRONG, TIER_GOOD) or not cp1: continue
    scored = sorted(runners, key=lambda r: (-score_runner(r)[0], tof(r.get("sp_dec")) or 999))
    op1 = scored[0] if scored else None
    if not op1: continue
    if cp1.get("horse_id","") == op1.get("horse_id",""): continue  # same, skip
    cp1sp = tof(cp1.get("sp_dec"))
    cp1pos = get_pos(cp1)
    if cp1pos is None: continue
    diff_sysc_p1w += cp1pos == 1
    if cp1sp: diff_sysc_pnl += pnl(cp1pos==1, cp1sp)

if diff_n:
    print(f"  When they DIFFER: SysC P1 wins at {diff_sysc_p1w/diff_n*100:.0f}%  P&L = {sgn(diff_sysc_pnl)}")

print()
print("Done.")
