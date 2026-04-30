"""
scratch_full_comparison.py
==========================
Compares three systems across all available raw data (Mar 16 – Apr 29):

  A) Old system  — pure score_runner P1+P2, old tier logic
  B) System C    — get_blended_picks mw=0.60/0.40
  C) Hybrid      — System C P1 + pure score_runner P2

Also checks field coverage (RPR, OR, SP) per date range to understand
data quality windows.

Run from ~/horse_bets_v3:
  python3 scratch_full_comparison.py 2>&1 | tee comparison_output.txt
"""

import json, glob, os, sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from predict import score_runner, place_terms, SIGNAL_WEIGHTS
from predict_v2 import get_blended_picks, TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP

COMMISSION = 0.05

# ── Helpers ───────────────────────────────────────────────────────────────────

def tof(v):
    try:
        f = float(str(v).strip())
        return f if f > 0 else None
    except: return None

def pos(r):
    try: return int(str(r.get("position","")).strip())
    except: return None

def place_spots(n):
    return 1 if n<=4 else (2 if n<=7 else 3)

def sp_free_score(runner):
    SP_SIGNALS = {"sp_odds_on","sp_2_to_4","sp_4_to_6"}
    sc, sigs = score_runner(runner)
    return sc - sum(SIGNAL_WEIGHTS.get(s,0) for s in sigs if s in SP_SIGNALS)

def rpr_coverage(runners):
    if not runners: return 0.0
    return sum(1 for r in runners if str(r.get("rpr") or "").strip() not in ("","–","-")) / len(runners)

def field_qualifies(runners, race):
    n = len(runners)
    cls = str(race.get("class","") or "").replace("Class","").strip()
    if cls in ("1","2"): return False
    if n > 12: return False
    if n < 2: return False
    return True

# ── Load all raw data ─────────────────────────────────────────────────────────

all_races = []
date_files = sorted(glob.glob("data/raw/*.json"))

for fp in date_files:
    date_str = os.path.basename(fp).replace(".json","")
    try:
        with open(fp) as f: d = json.load(f)
    except: continue
    races = d.get("results") or d.get("races") or []
    for race in races:
        race["_date"] = date_str
        all_races.append(race)

print(f"Loaded {len(all_races)} races across {len(date_files)} days")
print(f"Date range: {date_files[0].split('/')[-1].replace('.json','')} → {date_files[-1].split('/')[-1].replace('.json','')}")
print()

# ── Field coverage check ──────────────────────────────────────────────────────

print("=" * 65)
print("FIELD COVERAGE BY MONTH")
print("=" * 65)

coverage = defaultdict(lambda: {"races":0,"rpr_ok":0,"sp_ok":0,"or_ok":0})
for race in all_races:
    runners = race.get("runners",[])
    if not runners: continue
    month = race["_date"][:7]
    coverage[month]["races"] += 1
    rpr_ok = sum(1 for r in runners if str(r.get("rpr") or "").strip() not in ("","–","-"))
    sp_ok  = sum(1 for r in runners if tof(r.get("sp_dec")))
    or_ok  = sum(1 for r in runners if tof(r.get("or") or r.get("ofr")))
    coverage[month]["rpr_ok"] += (rpr_ok / len(runners))
    coverage[month]["sp_ok"]  += (sp_ok  / len(runners))
    coverage[month]["or_ok"]  += (or_ok  / len(runners))

print(f"  {'Month':<12} {'Races':>6} {'RPR cov':>9} {'SP cov':>9} {'OR cov':>9}")
print(f"  {'-'*50}")
for month in sorted(coverage):
    c = coverage[month]
    n = c["races"]
    print(f"  {month:<12} {n:>6} {c['rpr_ok']/n*100:>8.0f}% {c['sp_ok']/n*100:>8.0f}% {c['or_ok']/n*100:>8.0f}%")
print()

# ── Main comparison ───────────────────────────────────────────────────────────

# Track stats per system per tier/category
def empty_stats():
    return {"n":0,"p1w":0,"p2w":0,"either":0,"neither":0,
            "p1_pnl":0.0,"p2_pnl":0.0}

old_stats   = defaultdict(empty_stats)   # key = "all" or date range
sysc_stats  = defaultdict(empty_stats)   # key = tier int
hybrid_stats= defaultdict(empty_stats)   # key = tier int

# Also track by date window: pre-Apr13 (good RPR), Apr13-26 (poor RPR), Apr27+ (live)
WINDOWS = {
    "Mar16-Apr12": ("2026-03-16","2026-04-12"),
    "Apr13-Apr26": ("2026-04-13","2026-04-26"),
    "Apr27-Apr29": ("2026-04-27","2026-04-29"),
}

old_by_window   = {w: empty_stats() for w in WINDOWS}
sysc_by_window  = {w: defaultdict(empty_stats) for w in WINDOWS}
hybrid_by_window= {w: defaultdict(empty_stats) for w in WINDOWS}

skipped = 0
processed = 0

for race in all_races:
    runners = race.get("runners",[])
    if not field_qualifies(runners, race):
        skipped += 1
        continue

    date_str = race.get("_date","")
    n = len(runners)
    ps = place_spots(n)

    raw_meta = {
        "class":   str(race.get("class","") or ""),
        "surface": race.get("surface","Turf") or "Turf",
        "type":    race.get("type","") or "",
    }

    # ── Old system ────────────────────────────────────────────────────────────
    scored = sorted(runners,
        key=lambda r: (-score_runner(r)[0], tof(r.get("sp_dec")) or 999))
    op1 = scored[0] if scored else None
    op2 = scored[1] if len(scored)>1 else None

    if op1:
        op1_sp = tof(op1.get("sp_dec"))
        op2_sp = tof(op2.get("sp_dec")) if op2 else None
        p1w = pos(op1) == 1
        p2w = pos(op2) == 1 if op2 else False
        p1_pnl = (op1_sp-1)*(1-COMMISSION)*2 if (p1w and op1_sp) else -2.0
        p2_pnl = (op2_sp-1)*(1-COMMISSION)*2 if (p2w and op2_sp) else (-2.0 if op2_sp else 0)

        s = old_stats["all"]
        s["n"]+=1; s["p1w"]+=p1w; s["p2w"]+=p2w
        s["either"]+=p1w or p2w; s["neither"]+=not(p1w or p2w)
        s["p1_pnl"]+=p1_pnl; s["p2_pnl"]+=p2_pnl

        for wname,(wstart,wend) in WINDOWS.items():
            if wstart <= date_str <= wend:
                ws = old_by_window[wname]
                ws["n"]+=1; ws["p1w"]+=p1w; ws["p2w"]+=p2w
                ws["either"]+=p1w or p2w; ws["neither"]+=not(p1w or p2w)
                ws["p1_pnl"]+=p1_pnl; ws["p2_pnl"]+=p2_pnl

    # ── System C ──────────────────────────────────────────────────────────────
    tc, cp1, cp2, _ = get_blended_picks(runners, mw_p1=0.60, mw_p2=0.40, raw_race=raw_meta)

    if cp1 and tc in (TIER_ELITE, TIER_STRONG, TIER_GOOD):
        cp1_sp = tof(cp1.get("sp_dec"))
        cp2_sp = tof(cp2.get("sp_dec")) if cp2 else None
        p1w = pos(cp1) == 1
        p2w = pos(cp2) == 1 if cp2 else False
        p1_pnl = (cp1_sp-1)*(1-COMMISSION)*2 if (p1w and cp1_sp) else -2.0
        p2_pnl = (cp2_sp-1)*(1-COMMISSION)*2 if (p2w and cp2_sp) else (-2.0 if cp2_sp else 0)

        s = sysc_stats[tc]
        s["n"]+=1; s["p1w"]+=p1w; s["p2w"]+=p2w
        s["either"]+=p1w or p2w; s["neither"]+=not(p1w or p2w)
        s["p1_pnl"]+=p1_pnl; s["p2_pnl"]+=p2_pnl

        for wname,(wstart,wend) in WINDOWS.items():
            if wstart <= date_str <= wend:
                ws = sysc_by_window[wname][tc]
                ws["n"]+=1; ws["p1w"]+=p1w; ws["p2w"]+=p2w
                ws["either"]+=p1w or p2w; ws["neither"]+=not(p1w or p2w)
                ws["p1_pnl"]+=p1_pnl; ws["p2_pnl"]+=p2_pnl

        # ── Hybrid: System C P1, pure score_runner P2 ─────────────────────────
        # P2 = highest score_runner that is not P1
        cp1_hid = cp1.get("horse_id","")
        hp2 = next((r for r in scored if r.get("horse_id","") != cp1_hid), None)
        hp2_sp = tof(hp2.get("sp_dec")) if hp2 else None
        hp2w = pos(hp2) == 1 if hp2 else False
        hp2_pnl = (hp2_sp-1)*(1-COMMISSION)*2 if (hp2w and hp2_sp) else (-2.0 if hp2_sp else 0)

        s = hybrid_stats[tc]
        s["n"]+=1; s["p1w"]+=p1w; s["p2w"]+=hp2w
        s["either"]+=p1w or hp2w; s["neither"]+=not(p1w or hp2w)
        s["p1_pnl"]+=p1_pnl; s["p2_pnl"]+=hp2_pnl

        for wname,(wstart,wend) in WINDOWS.items():
            if wstart <= date_str <= wend:
                ws = hybrid_by_window[wname][tc]
                ws["n"]+=1; ws["p1w"]+=p1w; ws["p2w"]+=hp2w
                ws["either"]+=p1w or hp2w; ws["neither"]+=not(p1w or hp2w)
                ws["p1_pnl"]+=p1_pnl; ws["p2_pnl"]+=hp2_pnl

    processed += 1

print(f"Processed {processed} qualifying races | Skipped {skipped}")
print()

# ── Output ────────────────────────────────────────────────────────────────────

def pct(a,b): return f"{a/b*100:.0f}%" if b else "—"
def sgn(v): return f"+£{v:.2f}" if v>=0 else f"-£{abs(v):.2f}"

TIER_NAMES = {TIER_ELITE:"ELITE", TIER_STRONG:"STRONG", TIER_GOOD:"GOOD"}

print("=" * 65)
print("FULL DATASET — OLD vs SYSTEM C vs HYBRID (flat £2 stake)")
print("=" * 65)
print()
print(f"  {'System':<22} {'N':>5} {'P1win':>7} {'P2win':>7} {'Either':>7} {'Neither':>8} {'P1 P&L':>9} {'P2 P&L':>9} {'Total':>9}")
print(f"  {'-'*85}")

# Old system
s = old_stats["all"]
n = s["n"]
print(f"  {'Old (score_runner)':<22} {n:>5} {pct(s['p1w'],n):>7} {pct(s['p2w'],n):>7} "
      f"{pct(s['either'],n):>7} {pct(s['neither'],n):>8} "
      f"{sgn(s['p1_pnl']):>9} {sgn(s['p2_pnl']):>9} {sgn(s['p1_pnl']+s['p2_pnl']):>9}")

# System C
print(f"  {'System C':<22}")
sysc_total_p1=sysc_total_p2=sysc_total_n=0
for tc in (TIER_ELITE, TIER_STRONG, TIER_GOOD):
    s = sysc_stats[tc]
    n = s["n"]
    if not n: continue
    sysc_total_n+=n; sysc_total_p1+=s["p1_pnl"]; sysc_total_p2+=s["p2_pnl"]
    print(f"    {TIER_NAMES[tc]:<20} {n:>5} {pct(s['p1w'],n):>7} {pct(s['p2w'],n):>7} "
          f"{pct(s['either'],n):>7} {pct(s['neither'],n):>8} "
          f"{sgn(s['p1_pnl']):>9} {sgn(s['p2_pnl']):>9} {sgn(s['p1_pnl']+s['p2_pnl']):>9}")
print(f"  {'  TOTAL':<22} {sysc_total_n:>5} {'':>7} {'':>7} {'':>7} {'':>8} "
      f"{sgn(sysc_total_p1):>9} {sgn(sysc_total_p2):>9} {sgn(sysc_total_p1+sysc_total_p2):>9}")

# Hybrid
print(f"  {'Hybrid (SysC P1+SR P2)':<22}")
hyb_total_p1=hyb_total_p2=hyb_total_n=0
for tc in (TIER_ELITE, TIER_STRONG, TIER_GOOD):
    s = hybrid_stats[tc]
    n = s["n"]
    if not n: continue
    hyb_total_n+=n; hyb_total_p1+=s["p1_pnl"]; hyb_total_p2+=s["p2_pnl"]
    print(f"    {TIER_NAMES[tc]:<20} {n:>5} {pct(s['p1w'],n):>7} {pct(s['p2w'],n):>7} "
          f"{pct(s['either'],n):>7} {pct(s['neither'],n):>8} "
          f"{sgn(s['p1_pnl']):>9} {sgn(s['p2_pnl']):>9} {sgn(s['p1_pnl']+s['p2_pnl']):>9}")
print(f"  {'  TOTAL':<22} {hyb_total_n:>5} {'':>7} {'':>7} {'':>7} {'':>8} "
      f"{sgn(hyb_total_p1):>9} {sgn(hyb_total_p2):>9} {sgn(hyb_total_p1+hyb_total_p2):>9}")

print()
print("=" * 65)
print("BY TIME WINDOW")
print("=" * 65)

for wname in WINDOWS:
    print(f"\n  {wname}")
    print(f"  {'System':<22} {'N':>5} {'P1win':>7} {'P2win':>7} {'Either':>7} {'P1 P&L':>9} {'Total':>9}")
    print(f"  {'-'*68}")

    # Old
    s = old_by_window[wname]
    n = s["n"]
    if n:
        print(f"  {'Old':<22} {n:>5} {pct(s['p1w'],n):>7} {pct(s['p2w'],n):>7} "
              f"{pct(s['either'],n):>7} {sgn(s['p1_pnl']):>9} {sgn(s['p1_pnl']+s['p2_pnl']):>9}")

    # System C
    sc_n = sum(sysc_by_window[wname][tc]["n"] for tc in (TIER_ELITE,TIER_STRONG,TIER_GOOD))
    sc_p1 = sum(sysc_by_window[wname][tc]["p1_pnl"] for tc in (TIER_ELITE,TIER_STRONG,TIER_GOOD))
    sc_p2 = sum(sysc_by_window[wname][tc]["p2_pnl"] for tc in (TIER_ELITE,TIER_STRONG,TIER_GOOD))
    sc_p1w = sum(sysc_by_window[wname][tc]["p1w"] for tc in (TIER_ELITE,TIER_STRONG,TIER_GOOD))
    sc_p2w = sum(sysc_by_window[wname][tc]["p2w"] for tc in (TIER_ELITE,TIER_STRONG,TIER_GOOD))
    sc_ei  = sum(sysc_by_window[wname][tc]["either"] for tc in (TIER_ELITE,TIER_STRONG,TIER_GOOD))
    if sc_n:
        print(f"  {'System C':<22} {sc_n:>5} {pct(sc_p1w,sc_n):>7} {pct(sc_p2w,sc_n):>7} "
              f"{pct(sc_ei,sc_n):>7} {sgn(sc_p1):>9} {sgn(sc_p1+sc_p2):>9}")

    # Hybrid
    hy_n  = sum(hybrid_by_window[wname][tc]["n"] for tc in (TIER_ELITE,TIER_STRONG,TIER_GOOD))
    hy_p1 = sum(hybrid_by_window[wname][tc]["p1_pnl"] for tc in (TIER_ELITE,TIER_STRONG,TIER_GOOD))
    hy_p2 = sum(hybrid_by_window[wname][tc]["p2_pnl"] for tc in (TIER_ELITE,TIER_STRONG,TIER_GOOD))
    hy_p1w= sum(hybrid_by_window[wname][tc]["p1w"] for tc in (TIER_ELITE,TIER_STRONG,TIER_GOOD))
    hy_p2w= sum(hybrid_by_window[wname][tc]["p2w"] for tc in (TIER_ELITE,TIER_STRONG,TIER_GOOD))
    hy_ei = sum(hybrid_by_window[wname][tc]["either"] for tc in (TIER_ELITE,TIER_STRONG,TIER_GOOD))
    if hy_n:
        print(f"  {'Hybrid':<22} {hy_n:>5} {pct(hy_p1w,hy_n):>7} {pct(hy_p2w,hy_n):>7} "
              f"{pct(hy_ei,hy_n):>7} {sgn(hy_p1):>9} {sgn(hy_p1+hy_p2):>9}")

print()
print("=" * 65)
print("P2 PRICE DISTRIBUTION — old vs System C vs Hybrid")
print("(Shows whether old/hybrid P2 genuinely finds longer prices)")
print("=" * 65)

old_p2_prices = []
sysc_p2_prices = []
hybrid_p2_prices = []

for race in all_races:
    runners = race.get("runners",[])
    if not field_qualifies(runners, race): continue
    raw_meta = {
        "class": str(race.get("class","") or ""),
        "surface": race.get("surface","Turf") or "Turf",
        "type": race.get("type","") or "",
    }
    scored = sorted(runners,
        key=lambda r: (-score_runner(r)[0], tof(r.get("sp_dec")) or 999))

    # Old P2
    if len(scored) > 1:
        sp = tof(scored[1].get("sp_dec"))
        if sp: old_p2_prices.append(sp)

    # System C P2
    tc, cp1, cp2, _ = get_blended_picks(runners, mw_p1=0.60, mw_p2=0.40, raw_race=raw_meta)
    if cp2 and tc in (TIER_ELITE,TIER_STRONG,TIER_GOOD):
        sp = tof(cp2.get("sp_dec"))
        if sp: sysc_p2_prices.append(sp)

    # Hybrid P2 (score_runner excluding P1)
    if cp1 and tc in (TIER_ELITE,TIER_STRONG,TIER_GOOD):
        cp1_hid = cp1.get("horse_id","")
        hp2 = next((r for r in scored if r.get("horse_id","") != cp1_hid), None)
        if hp2:
            sp = tof(hp2.get("sp_dec"))
            if sp: hybrid_p2_prices.append(sp)

def price_dist(prices):
    if not prices: return "—"
    avg = sum(prices)/len(prices)
    under2 = sum(1 for p in prices if p < 2.0)/len(prices)*100
    u3_5   = sum(1 for p in prices if 2.0<=p<5.0)/len(prices)*100
    u10    = sum(1 for p in prices if 5.0<=p<10.0)/len(prices)*100
    over10 = sum(1 for p in prices if p>=10.0)/len(prices)*100
    return f"avg={avg:.1f}  <2/1={under2:.0f}%  2-5/1={u3_5:.0f}%  5-10/1={u10:.0f}%  10/1+={over10:.0f}%"

print(f"\n  Old P2    (n={len(old_p2_prices)}):    {price_dist(old_p2_prices)}")
print(f"  SysC P2   (n={len(sysc_p2_prices)}):  {price_dist(sysc_p2_prices)}")
print(f"  Hybrid P2 (n={len(hybrid_p2_prices)}): {price_dist(hybrid_p2_prices)}")
print()
print("Done.")
