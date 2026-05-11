"""
scratch_clean_analysis.py
=========================
Comprehensive clean backtest using pre-race card data for model inputs
and post-race results for outcomes only. No look-ahead contamination.

Cards provide: pre-race RPR, OR, TSR, form, trainer/jockey stats,
               morning bookmaker prices (sp_dec), pre-baked scores
Results provide: actual finishing positions, settled BSP

Ten sections:
  1.  Signal audit        — which card fields are populated
  2.  Score analysis      — current vs SP-free score, signal breakdown
  3.  Market weight sweep — mw_p1 0.0-1.0, pre-race prices
  4.  Score margin        — P1 score gap vs field as filter
  5.  SP floor            — odds-on and floor variations
  6.  SP drift            — morning price vs settled BSP
  7.  Staking             — flat vs variable by SP band
  8.  Place bets          — score gap + SP filters
  9.  Tier definitions    — current vs market-relative
  10. Daily P&L           — variance across card dates

Run from ~/horse_bets_v3:
  python3 scratch_clean_analysis.py 2>&1 | tee clean_analysis_output.txt
"""

import json, os, sys
from collections import defaultdict
sys.path.insert(0, os.path.dirname(__file__))
from predict_v2 import _sp_free_score, get_blended_picks, TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD
from predict import score_runner, SIGNAL_WEIGHTS

COMMISSION = 0.05
BET_TIERS  = {TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD}
TIER_NAMES = {TIER_ELITE:"ELITE", TIER_STRONG:"STRONG", TIER_GOOD:"GOOD", TIER_STD:"STD"}
CARDS_DIR  = "data/cards"
RAW_DIR    = "data/raw"
SP_SIGNALS = {"sp_odds_on", "sp_2_to_4", "sp_4_to_6"}

# ── Helpers ───────────────────────────────────────────────────────────────────

def tof(v):
    try:
        f = float(str(v).strip())
        return f if f > 0 else None
    except: return None

def get_pos(r):
    try: return int(str(r.get("position","")).strip())
    except: return None

def strip(name):
    return (name or "").split(" (")[0].strip().lower()

def norm_time(t):
    if not t: return ""
    parts = str(t).strip().split(":")
    if len(parts) != 2: return str(t).strip()
    try:
        h, m = int(parts[0]), int(parts[1])
        if 1 <= h <= 9: h += 12
        return f"{h:02d}:{m:02d}"
    except: return str(t).strip()

def field_ok(runners, race):
    n = len(runners)
    if n < 2: return False
    rt = str(race.get("type","") or "").lower()
    is_jump = any(t in rt for t in ("chase","hurdle","nh flat","national hunt"))
    cls = str(race.get("race_class","") or race.get("class","") or "").replace("Class","").strip()
    if cls in ("1","2") and is_jump: return False
    if is_jump and n > 12: return False
    if not is_jump and n > 20: return False
    return True

def place_spots(n):
    if n <= 4: return 1
    if n <= 7: return 2
    if n <= 11: return 3
    return 4

def place_divisor(n):
    if n <= 4:  return None
    if n <= 7:  return 4.0
    if n <= 11: return 5.0
    return 6.0

def pct(a, b): return f"{a/b*100:.1f}%" if b else "—"
def sgn(v):    return f"+£{v:.2f}" if v >= 0 else f"-£{abs(v):.2f}"

def norm_score(runner):
    """SP-free score from card runner."""
    sc, sigs = score_runner(runner)
    return sc - sum(SIGNAL_WEIGHTS.get(s,0) for s in sigs if s in SP_SIGNALS)

def normalise(val, vals, scale=10.0):
    valid = [v for v in vals if v is not None]
    if not valid or len(valid) < 2: return scale/2
    lo, hi = min(valid), max(valid)
    return scale/2 if hi==lo else ((val-lo)/(hi-lo))*scale

def stats_score_card(runner, rprs, ors, tsrs):
    """Pure stats score from card runner fields."""
    s = 0.0
    rpr = tof(runner.get("rpr"))
    or_ = tof(runner.get("ofr") or runner.get("or"))
    ts  = tof(runner.get("ts") or runner.get("tsr"))
    if rpr: s += normalise(rpr, rprs, 10.0)
    if or_: s += normalise(or_, ors,  10.0)
    if ts:  s += normalise(ts,  tsrs,  5.0)
    if rpr and or_ and rpr > or_: s += 2.0
    fd = runner.get("form_detail") or {}
    if isinstance(fd, dict):
        plc4 = fd.get("placed_last_4",0) or 0
        bad  = fd.get("bad_recent",0) or 0
        if plc4 >= 3:   s += 2.0
        elif plc4 >= 2: s += 1.0
        if bad == 0 and runner.get("form",""): s += 1.0
    for f14 in [runner.get("trainer_14d"), runner.get("jockey_14d")]:
        if not isinstance(f14, dict): continue
        ae   = f14.get("ae",0) or 0
        runs = f14.get("runs",0) or 0
        if runs >= 3:
            if   ae >= 2.0 and runs >= 5: s += 3
            elif ae >= 1.5 and runs >= 5: s += 2
            elif ae >= 1.0 and runs >= 5: s += 1
    return s

def rank_card_runners(runners, going, mw):
    """Rank card runners by blended stats+market score."""
    n = len(runners)
    if n < 2: return runners
    rprs = [tof(r.get("rpr")) for r in runners]
    ors  = [tof(r.get("ofr") or r.get("or")) for r in runners]
    tsrs = [tof(r.get("ts") or r.get("tsr")) for r in runners]
    stats = [(stats_score_card(r, rprs, ors, tsrs), r) for r in runners]
    stats.sort(key=lambda x: -x[0])
    sr = {r.get("horse_id",""):i+1 for i,(_,r) in enumerate(stats)}
    mkt = [(tof(r.get("sp_dec")) or 999, r) for r in runners]
    mkt.sort(key=lambda x: x[0])
    mr = {r.get("horse_id",""):i+1 for i,(_,r) in enumerate(mkt)}
    blended = []
    for ss, r in stats:
        hid = r.get("horse_id","")
        s_n = (sr.get(hid,n)-1)/max(n-1,1)
        m_n = (mr.get(hid,n)-1)/max(n-1,1)
        blended.append(((1-mw)*s_n + mw*m_n, sr.get(hid,n), mr.get(hid,n), ss, r))
    blended.sort(key=lambda x: x[0])
    return [r for _,_,_,_,r in blended]

# ── Load card dates and build results index ───────────────────────────────────

card_dates = sorted(
    fp.replace(".json","")
    for fp in os.listdir(CARDS_DIR)
    if fp.endswith(".json") and fp[:4].isdigit()
    and os.path.exists(f"{RAW_DIR}/{fp}")
)

print(f"Card dates with matching results: {len(card_dates)}")
print(f"  {card_dates[0]} to {card_dates[-1]}")
print()

# Build results index: (course_lower, norm_time) -> {horse_id -> {pos, bsp, sp}}
results_index = {}
for date_str in card_dates:
    with open(f"{RAW_DIR}/{date_str}.json") as f: d = json.load(f)
    for race in (d.get("results") or d.get("races") or []):
        course = race.get("course","").lower().strip()
        off    = norm_time(race.get("off",""))
        key    = (date_str, course, off)
        hmap   = {}
        for r in race.get("runners",[]):
            hid = r.get("horse_id","")
            pos = get_pos(r)
            bsp = tof(r.get("bsp") or r.get("bsp_dec"))
            sp  = tof(r.get("sp_dec"))
            if hid and pos is not None:
                hmap[hid] = {"pos":pos, "bsp":bsp or sp, "sp":sp}
        results_index[key] = hmap

# ── Build master record set ───────────────────────────────────────────────────

print("Building records from card data...")
records = []

for date_str in card_dates:
    with open(f"{CARDS_DIR}/{date_str}.json") as f: d = json.load(f)
    for race in (d.get("races") or []):
        runners = race.get("all_runners",[])
        if not runners: continue
        if not field_ok(runners, race): continue

        course = race.get("course","").lower().strip()
        off    = norm_time(race.get("off",""))
        rkey   = (date_str, course, off)
        rmap   = results_index.get(rkey, {})
        if not rmap: continue
        if not any(v["pos"]==1 for v in rmap.values()): continue

        n       = len(runners)
        ps      = place_spots(n)
        div     = place_divisor(n)
        going   = race.get("going","")
        raw_meta = {
            "class":   str(race.get("race_class","") or race.get("class","") or ""),
            "surface": race.get("surface","Turf") or "Turf",
            "type":    race.get("type","") or "",
        }

        # Pre-race morning prices
        morning_price = {r.get("horse_id",""):tof(r.get("sp_dec")) for r in runners}

        # Score all runners using card data
        rprs  = [tof(r.get("rpr")) for r in runners]
        ors   = [tof(r.get("ofr") or r.get("or")) for r in runners]
        tsrs  = [tof(r.get("ts") or r.get("tsr")) for r in runners]

        runner_scores = {}
        for r in runners:
            hid = r.get("horse_id","")
            runner_scores[hid] = {
                "stats":  stats_score_card(r, rprs, ors, tsrs),
                "norm":   norm_score(r),
                "card_score": r.get("score",0) or 0,
            }

        # Get ranked P1 at default mw=0.1 (current live setting)
        ranked = rank_card_runners(runners, going, mw=0.1)
        p1_card = ranked[0]
        p2_card = ranked[1] if len(ranked) > 1 else None

        p1_hid   = p1_card.get("horse_id","")
        p1_res   = rmap.get(p1_hid,{})
        p1_pos   = p1_res.get("pos")
        p1_bsp   = p1_res.get("bsp")
        p1_morn  = morning_price.get(p1_hid)
        p1_stats = runner_scores.get(p1_hid,{}).get("stats",0)
        p1_norm  = runner_scores.get(p1_hid,{}).get("norm",0)

        if p1_pos is None or not p1_morn: continue

        # Score margin vs rest of field
        all_stats = sorted([v["stats"] for v in runner_scores.values()], reverse=True)
        margin    = round(all_stats[0] - all_stats[1], 1) if len(all_stats)>1 else 0

        # Market rank of our stats P1 (is the market agreeing?)
        mkt_sorted = sorted(runners, key=lambda r: tof(r.get("sp_dec")) or 999)
        mkt_rank   = next((i+1 for i,r in enumerate(mkt_sorted) if r.get("horse_id","")==p1_hid), n)
        stats_rank = next((i+1 for i,(_,r) in enumerate(
            sorted([(v["stats"],r) for r in runners for hid2,v in [("",runner_scores.get(r.get("horse_id",""),{}))]], key=lambda x:-x[0])
        ) if r.get("horse_id","")==p1_hid), n)

        # Use tier from blended pick
        tc, _, _, _ = get_blended_picks(runners, mw_p1=0.1, mw_p2=0.4, raw_race=raw_meta)

        p1_won   = p1_pos == 1
        p1_plcd  = p1_pos <= ps

        # P2 info
        p2_hid  = p2_card.get("horse_id","") if p2_card else ""
        p2_res  = rmap.get(p2_hid,{})
        p2_pos  = p2_res.get("pos")
        p2_bsp  = p2_res.get("bsp")
        p2_morn = morning_price.get(p2_hid)
        p2_stats = runner_scores.get(p2_hid,{}).get("stats",0) if p2_hid else 0
        p2_norm  = runner_scores.get(p2_hid,{}).get("norm",0)  if p2_hid else 0
        p2_won   = p2_pos == 1   if p2_pos is not None else False
        p2_plcd  = p2_pos <= ps  if p2_pos is not None else False

        records.append({
            "date":      date_str,
            "tier":      tc,
            "n":         n,
            "ps":        ps,
            "div":       div,
            # P1
            "p1_hid":    p1_hid,
            "p1_morn":   p1_morn,
            "p1_bsp":    p1_bsp,
            "p1_stats":  p1_stats,
            "p1_norm":   p1_norm,
            "p1_won":    p1_won,
            "p1_plcd":   p1_plcd,
            "p1_pos":    p1_pos,
            "mkt_rank":  mkt_rank,
            "margin":    margin,
            # P2
            "p2_hid":    p2_hid,
            "p2_morn":   p2_morn,
            "p2_bsp":    p2_bsp,
            "p2_stats":  p2_stats,
            "p2_norm":   p2_norm,
            "p2_won":    p2_won,
            "p2_plcd":   p2_plcd,
            "score_gap": round(p1_stats - p2_stats, 1),
        })

print(f"Records: {len(records)} qualifying races across {len(card_dates)} days")
print()

def win_pnl(sp, stake, won):
    return round(stake*(sp-1)*(1-COMMISSION),2) if won else -stake

def place_pnl_est(sp, stake, placed, div):
    if not div: return 0.0
    ep = (sp-1)/div + 1
    return round(stake*(ep-1)*(1-COMMISSION),2) if placed else -stake

def losing_runs(seq):
    runs = []; cur = 0
    for w in seq:
        if not w: cur += 1
        else:
            if cur: runs.append(cur)
            cur = 0
    if cur: runs.append(cur)
    return runs

# ═════════════════════════════════════════════════════════════════════════════
# SECTION 1: SIGNAL AUDIT
# ═════════════════════════════════════════════════════════════════════════════

print("=" * 65)
print("1. SIGNAL AUDIT — coverage of key card fields")
print("=" * 65)
print()

total_r = 0
rpr_n = or_n = ts_n = odds_n = trainer_n = jockey_n = 0
for date_str in card_dates:
    with open(f"{CARDS_DIR}/{date_str}.json") as f: d = json.load(f)
    for race in (d.get("races") or []):
        for r in race.get("all_runners",[]):
            total_r += 1
            if tof(r.get("rpr")): rpr_n += 1
            if tof(r.get("ofr") or r.get("or")): or_n += 1
            if tof(r.get("ts") or r.get("tsr")): ts_n += 1
            if r.get("odds"): odds_n += 1
            t14 = r.get("trainer_14d") or {}
            j14 = r.get("jockey_14d") or {}
            if isinstance(t14, dict) and t14.get("runs",0): trainer_n += 1
            if isinstance(j14, dict) and j14.get("runs",0): jockey_n += 1

print(f"  Total card runners:  {total_r}")
print(f"  RPR coverage:        {rpr_n} ({pct(rpr_n, total_r)})")
print(f"  OR coverage:         {or_n} ({pct(or_n, total_r)})")
print(f"  TSR coverage:        {ts_n} ({pct(ts_n, total_r)})")
print(f"  Morning odds:        {odds_n} ({pct(odds_n, total_r)})")
print(f"  Trainer 14d stats:   {trainer_n} ({pct(trainer_n, total_r)})")
print(f"  Jockey 14d stats:    {jockey_n} ({pct(jockey_n, total_r)})")
print()

# ═════════════════════════════════════════════════════════════════════════════
# SECTION 2: SCORE ANALYSIS
# ═════════════════════════════════════════════════════════════════════════════

print("=" * 65)
print("2. SCORE ANALYSIS — stats score vs norm score, win rates")
print("=" * 65)
print()

# Win rate by stats score band
print("  P1 win rate by stats score band (clean, card data):")
print(f"  {'Score band':<18} {'N':>5} {'Win%':>7} {'MornP&L':>9} {'Per bet':>8}")
print(f"  {'-'*50}")
score_bands = [(0,5,"0-5"),(5,10,"5-10"),(10,15,"10-15"),(15,20,"15-20"),(20,99,"20+")]
for lo, hi, lbl in score_bands:
    b = [r for r in records if lo <= r["p1_stats"] < hi]
    if len(b) < 5: continue
    w = sum(1 for r in b if r["p1_won"])
    pnl = sum(win_pnl(r["p1_morn"],2,r["p1_won"]) for r in b)
    print(f"  {lbl:<18} {len(b):>5} {pct(w,len(b)):>7} {pnl:>+9.2f} {pnl/len(b):>+8.3f}")

print()

# Win rate by norm score (SP-free)
print("  P1 win rate by SP-free (norm) score band:")
print(f"  {'Score band':<18} {'N':>5} {'Win%':>7} {'MornP&L':>9} {'Per bet':>8}")
print(f"  {'-'*50}")
for lo, hi, lbl in [(0,3,"0-3"),(3,5,"3-5"),(5,7,"5-7"),(7,10,"7-10"),(10,99,"10+")]:
    b = [r for r in records if lo <= r["p1_norm"] < hi]
    if len(b) < 5: continue
    w = sum(1 for r in b if r["p1_won"])
    pnl = sum(win_pnl(r["p1_morn"],2,r["p1_won"]) for r in b)
    print(f"  {lbl:<18} {len(b):>5} {pct(w,len(b)):>7} {pnl:>+9.2f} {pnl/len(b):>+8.3f}")

print()

# Breakdown of why current bot picks P1 — which signals fire most
print("  Signal contribution to CURRENT score (from card pre-baked signals):")
sig_counts = defaultdict(int)
total_scored = 0
for date_str in card_dates:
    with open(f"{CARDS_DIR}/{date_str}.json") as f: d = json.load(f)
    for race in (d.get("races") or []):
        top1 = race.get("top1") or {}
        sigs = top1.get("signals") or {}
        if sigs:
            total_scored += 1
            for sig in sigs:
                sig_counts[sig] += 1

if total_scored:
    print(f"  (Based on {total_scored} card P1 picks)")
    for sig, cnt in sorted(sig_counts.items(), key=lambda x:-x[1])[:12]:
        is_sp = sig in SP_SIGNALS
        marker = " *** SP signal ***" if is_sp else ""
        print(f"    {sig:<25} {cnt:>4} ({pct(cnt,total_scored)}){marker}")

print()

# ═════════════════════════════════════════════════════════════════════════════
# SECTION 3: MARKET WEIGHT SWEEP
# ═════════════════════════════════════════════════════════════════════════════

print("=" * 65)
print("3. MARKET WEIGHT SWEEP — mw_p1 0.0 to 1.0")
print("   Using pre-race morning prices, outcomes from results")
print("=" * 65)
print()

print(f"  {'mw':>6} {'N':>5} {'Win%':>7} {'P&L (morn)':>12} {'Per bet':>8}")
print(f"  {'-'*45}")

for mw_int in range(0, 11):
    mw = mw_int / 10
    n = wins = 0; pnl = 0.0
    for date_str in card_dates:
        with open(f"{CARDS_DIR}/{date_str}.json") as f: d = json.load(f)
        for race in (d.get("races") or []):
            runners = race.get("all_runners",[])
            if not runners or not field_ok(runners, race): continue
            course = race.get("course","").lower().strip()
            off    = norm_time(race.get("off",""))
            rmap   = results_index.get((date_str, course, off),{})
            if not rmap or not any(v["pos"]==1 for v in rmap.values()): continue

            ranked = rank_card_runners(runners, race.get("going",""), mw)
            p1 = ranked[0]
            hid = p1.get("horse_id","")
            res = rmap.get(hid,{})
            if res.get("pos") is None: continue
            sp = tof(p1.get("sp_dec"))
            if not sp: continue
            won = res["pos"] == 1
            n += 1
            pnl += win_pnl(sp, 2, won)
            wins += int(won)

    lbl = f"{mw:.1f}"
    if mw == 0.0: lbl = "0.0 (pure stats)"
    if mw == 1.0: lbl = "1.0 (pure mkt)"
    print(f"  {lbl:>16} {n:>5} {pct(wins,n):>7} {pnl:>+12.2f} {pnl/n:>+8.3f}")

print()

# ═════════════════════════════════════════════════════════════════════════════
# SECTION 4: SCORE MARGIN
# ═════════════════════════════════════════════════════════════════════════════

print("=" * 65)
print("4. SCORE MARGIN — P1 stats score gap vs rest of field")
print("=" * 65)
print()

print(f"  {'Margin':<20} {'N':>5} {'Win%':>7} {'MornP&L':>9} {'Per bet':>8}")
print(f"  {'-'*52}")
margin_buckets = [
    ("0 (tied)",      0.0,  0.1),
    ("0.1-2",         0.1,  2.1),
    ("2-4",           2.1,  4.1),
    ("4-6",           4.1,  6.1),
    ("6+ (dominant)", 6.1, 99.0),
]
for lbl, lo, hi in margin_buckets:
    b = [r for r in records if lo <= r["margin"] < hi]
    if not b: continue
    w = sum(1 for r in b if r["p1_won"])
    pnl = sum(win_pnl(r["p1_morn"],2,r["p1_won"]) for r in b)
    print(f"  {lbl:<20} {len(b):>5} {pct(w,len(b)):>7} {pnl:>+9.2f} {pnl/len(b):>+8.3f}")

print()

# Margin as filter — cumulative effect
print("  Margin as filter (only bet when margin >= threshold):")
print(f"  {'Min margin':<15} {'N':>5} {'Win%':>7} {'P&L':>9} {'Per bet':>8}")
print(f"  {'-'*47}")
for min_m in [0, 1, 2, 3, 4, 5]:
    b = [r for r in records if r["margin"] >= min_m]
    if not b: continue
    w = sum(1 for r in b if r["p1_won"])
    pnl = sum(win_pnl(r["p1_morn"],2,r["p1_won"]) for r in b)
    print(f"  {min_m:<15} {len(b):>5} {pct(w,len(b)):>7} {pnl:>+9.2f} {pnl/len(b):>+8.3f}")

print()

# Market rank as filter — only bet if stats P1 is top-N in market
print("  Market rank filter (bet only if our P1 is top-N favourite):")
print(f"  {'Max mkt rank':<15} {'N':>5} {'Win%':>7} {'P&L':>9} {'Per bet':>8}")
print(f"  {'-'*47}")
for cap in [1, 2, 3, 4, 5, None]:
    b = [r for r in records if cap is None or r["mkt_rank"] <= cap]
    if not b: continue
    w = sum(1 for r in b if r["p1_won"])
    pnl = sum(win_pnl(r["p1_morn"],2,r["p1_won"]) for r in b)
    lbl = f"top-{cap}" if cap else "no cap"
    print(f"  {lbl:<15} {len(b):>5} {pct(w,len(b)):>7} {pnl:>+9.2f} {pnl/len(b):>+8.3f}")

print()

# ═════════════════════════════════════════════════════════════════════════════
# SECTION 5: SP FLOOR
# ═════════════════════════════════════════════════════════════════════════════

print("=" * 65)
print("5. SP FLOOR — morning price threshold effects")
print("=" * 65)
print()

print(f"  {'Min SP floor':<18} {'N':>5} {'Win%':>7} {'P&L':>9} {'Per bet':>8} {'Breakeven':>10}")
print(f"  {'-'*58}")
for min_sp in [1.0, 1.1, 1.25, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0]:
    b = [r for r in records if r["p1_morn"] and r["p1_morn"] >= min_sp]
    if not b: continue
    w = sum(1 for r in b if r["p1_won"])
    pnl = sum(win_pnl(r["p1_morn"],2,r["p1_won"]) for r in b)
    avg_sp = sum(r["p1_morn"] for r in b)/len(b)
    be = round(100/(avg_sp*(1-COMMISSION)),1)
    print(f"  {min_sp:<18.2f} {len(b):>5} {pct(w,len(b)):>7} {pnl:>+9.2f} {pnl/len(b):>+8.3f} {be:>9.1f}%")

print()

# Odds-on races specifically
odds_on = [r for r in records if r["p1_morn"] and r["p1_morn"] < 2.0]
if odds_on:
    w = sum(1 for r in odds_on if r["p1_won"])
    pnl = sum(win_pnl(r["p1_morn"],2,r["p1_won"]) for r in odds_on)
    print(f"  Odds-on breakdown ({len(odds_on)} races currently skipped):")
    for lo, hi, lbl in [(1.0,1.25,"<5/4"),(1.25,1.5,"5/4-6/4"),(1.5,2.0,"6/4-2/1")]:
        b = [r for r in odds_on if lo<=r["p1_morn"]<hi]
        if not b: continue
        bw = sum(1 for r in b if r["p1_won"])
        bp = sum(win_pnl(r["p1_morn"],2,r["p1_won"]) for r in b)
        be = round(100/((sum(r["p1_morn"] for r in b)/len(b))*(1-COMMISSION)),1)
        print(f"    {lbl}: n={len(b):>3}  win={pct(bw,len(b)):>6}  P&L={bp:>+8.2f}  "
              f"per bet={bp/len(b):>+7.3f}  breakeven={be:.0f}%")

print()

# ═════════════════════════════════════════════════════════════════════════════
# SECTION 6: SP DRIFT — morning price vs settled BSP
# ═════════════════════════════════════════════════════════════════════════════

print("=" * 65)
print("6. SP DRIFT — morning price vs settled BSP")
print("   Shows how much morning price changes by race time")
print("=" * 65)
print()

drifts = [(r["p1_morn"], r["p1_bsp"]) for r in records
          if r["p1_morn"] and r["p1_bsp"]]

if drifts:
    ratios = [bsp/morn for morn,bsp in drifts]
    print(f"  Races with both morning price and BSP: {len(drifts)}")
    print(f"  Avg BSP/morning ratio: {sum(ratios)/len(ratios):.3f}")
    print(f"    <1 = BSP shortened from morning price")
    print(f"    >1 = BSP drifted out from morning price")
    print()

    # By morning price band
    print(f"  {'Morn band':<14} {'N':>5} {'Avg drift':>10} {'Shortened%':>11} {'Drifted%':>9} {'Win% morn':>10} {'Win% BSP':>9}")
    print(f"  {'-'*65}")
    for lo, hi, lbl in [(1.0,2.0,"<2/1"),(2.0,3.0,"2-3/1"),(3.0,5.0,"3-5/1"),
                        (5.0,8.0,"5-8/1"),(8.0,99,"8/1+")]:
        b = [(m,bsp,r) for m,bsp,r in
             [(r["p1_morn"],r["p1_bsp"],r) for r in records
              if r["p1_morn"] and r["p1_bsp"] and lo<=r["p1_morn"]<hi]]
        if len(b) < 3: continue
        avg_r  = sum(bsp/m for m,bsp,_ in b)/len(b)
        short  = sum(1 for m,bsp,_ in b if bsp < m)
        drift  = sum(1 for m,bsp,_ in b if bsp > m)
        w_morn = sum(1 for _,_,r in b if r["p1_won"])
        # Win rate using BSP as settlement
        w_bsp  = sum(1 for _,bsp,r in b if r["p1_won"])  # same wins, just different price used
        pnl_morn = sum(win_pnl(m,2,r["p1_won"]) for m,_,r in b)
        pnl_bsp  = sum(win_pnl(bsp,2,r["p1_won"]) for _,bsp,r in b)
        print(f"  {lbl:<14} {len(b):>5} {avg_r:>10.3f} {pct(short,len(b)):>11} {pct(drift,len(b)):>9} "
              f"{pct(w_morn,len(b)):>10} {pct(w_bsp,len(b)):>9}")
    print()

    # P&L comparison: morning price vs BSP settlement
    all_morn_pnl = sum(win_pnl(r["p1_morn"],2,r["p1_won"]) for r in records if r["p1_morn"])
    all_bsp_pnl  = sum(win_pnl(r["p1_bsp"],2,r["p1_won"])  for r in records if r["p1_bsp"])
    nr = len([r for r in records if r["p1_morn"] and r["p1_bsp"]])
    print(f"  P&L comparison (£2 flat, {nr} races with both prices):")
    print(f"    Using morning price: {sgn(all_morn_pnl)} ({sgn(all_morn_pnl/nr)} per bet)")
    print(f"    Using settled BSP:   {sgn(all_bsp_pnl)} ({sgn(all_bsp_pnl/nr)} per bet)")
    print(f"    Difference:          {sgn(all_bsp_pnl-all_morn_pnl)}")

print()

# ═════════════════════════════════════════════════════════════════════════════
# SECTION 7: STAKING
# ═════════════════════════════════════════════════════════════════════════════

print("=" * 65)
print("7. STAKING — flat vs variable by SP band")
print("   All using morning price as proxy")
print("=" * 65)
print()

def var_stake(sp):
    if not sp or sp < 2.0: return 0
    if sp < 6.0: return 2
    if sp < 10.0: return 4
    return 6

strategies = [
    ("Flat £2 — all races",
        lambda r: 2 if r["p1_morn"] else 0),
    ("Flat £2 — SP>=2 only (current floor)",
        lambda r: 2 if (r["p1_morn"] or 0) >= 2.0 else 0),
    ("Flat £2 — SP>=3 only",
        lambda r: 2 if (r["p1_morn"] or 0) >= 3.0 else 0),
    ("Variable £2/£4/£6 by SP — floor SP>=2",
        lambda r: var_stake(r["p1_morn"])),
    ("Variable £2/£4/£6 by SP — floor SP>=1.1",
        lambda r: var_stake(r["p1_morn"]) if (r["p1_morn"] or 0)>=1.1 else 2 if (r["p1_morn"] or 0)>=1.1 else 0),
    ("Variable + norm score>=3 filter",
        lambda r: var_stake(r["p1_morn"]) if r["p1_norm"]>=3 else 0),
    ("Variable + norm score>=3 + SP>=1.1",
        lambda r: (var_stake(r["p1_morn"]) if (r["p1_morn"] or 0)>=2.0 else 2) if (r["p1_norm"]>=3 and (r["p1_morn"] or 0)>=1.1) else 0),
    ("Variable + margin>=2 filter",
        lambda r: var_stake(r["p1_morn"]) if r["margin"]>=2 else 0),
    ("Variable + mkt rank<=3 filter",
        lambda r: var_stake(r["p1_morn"]) if r["mkt_rank"]<=3 else 0),
]

print(f"  {'Strategy':<45} {'N':>5} {'Win%':>7} {'P&L':>10} {'Per bet':>8} {'ROI':>7}")
print(f"  {'-'*85}")
for name, stake_fn in strategies:
    n = wins = 0; pnl = staked = 0.0; seq = []
    for r in records:
        s = stake_fn(r)
        if not s or not r["p1_morn"]: continue
        n += 1; staked += s; seq.append(r["p1_won"])
        p = win_pnl(r["p1_morn"], s, r["p1_won"])
        pnl += p; wins += int(r["p1_won"])
    if not n: continue
    runs = losing_runs(seq)
    roi  = pnl/staked*100 if staked else 0
    print(f"  {name:<45} {n:>5} {pct(wins,n):>7} {pnl:>+10.2f} {pnl/n:>+8.3f} {roi:>6.1f}%")

print()

# ═════════════════════════════════════════════════════════════════════════════
# SECTION 8: PLACE BETS
# ═════════════════════════════════════════════════════════════════════════════

print("=" * 65)
print("8. PLACE BETS — P2 place bet filters using score gap and SP")
print("   P&L estimated using 1/4 odds model")
print("=" * 65)
print()

def est_place(sp, placed, stake, div):
    if not div or not sp: return 0.0
    ep = (sp-1)/div + 1
    return round(stake*(ep-1)*(1-COMMISSION),2) if placed else -stake

print("  P2 place performance by score gap (P1 stats minus P2 stats):")
print(f"  {'Score gap':<25} {'N':>5} {'Place%':>8} {'Win%':>7} {'Per bet':>8}")
print(f"  {'-'*55}")
gap_buckets = [
    ("P2 stronger (gap<=-2)",  lambda r: r["score_gap"] <= -2),
    ("P2 slightly stronger (-1)", lambda r: r["score_gap"] == -1),
    ("Equal (0)",              lambda r: r["score_gap"] == 0),
    ("P1 slightly stronger (1)", lambda r: r["score_gap"] == 1),
    ("P1 stronger (2)",        lambda r: r["score_gap"] == 2),
    ("P1 much stronger (3+)",  lambda r: r["score_gap"] >= 3),
]
for lbl, fn in gap_buckets:
    b = [r for r in records if fn(r) and r["p2_morn"] and r["div"]]
    if len(b) < 5: continue
    placed = sum(1 for r in b if r["p2_plcd"])
    won    = sum(1 for r in b if r["p2_won"])
    pnl    = sum(est_place(r["p2_morn"],r["p2_plcd"],2,r["div"]) for r in b)
    print(f"  {lbl:<25} {len(b):>5} {pct(placed,len(b)):>8} {pct(won,len(b)):>7} {pnl/len(b):>+8.3f}")

print()

print("  P2 place performance by morning SP band:")
print(f"  {'SP band':<14} {'N':>5} {'Place%':>8} {'Win%':>7} {'P&L':>9} {'Per bet':>8} {'Breakeven':>10}")
print(f"  {'-'*62}")
for lo,hi,lbl in [(1,2,"<2/1"),(2,3,"2-3/1"),(3,5,"3-5/1"),(5,8,"5-8/1"),(8,99,"8/1+")]:
    b = [r for r in records if r["p2_morn"] and lo<=r["p2_morn"]<hi and r["div"]]
    if not b: continue
    placed = sum(1 for r in b if r["p2_plcd"])
    won    = sum(1 for r in b if r["p2_won"])
    pnl    = sum(est_place(r["p2_morn"],r["p2_plcd"],2,r["div"]) for r in b)
    avg_sp = sum(r["p2_morn"] for r in b)/len(b)
    be     = round(100/((avg_sp/4+1)*(1-COMMISSION)),1)  # place breakeven
    print(f"  {lbl:<14} {len(b):>5} {pct(placed,len(b)):>8} {pct(won,len(b)):>7} "
          f"{pnl:>+9.2f} {pnl/len(b):>+8.3f} {be:>9.1f}%")

print()

print("  Combined place bet rules:")
print(f"  {'Rule':<45} {'N':>5} {'Place%':>8} {'P&L':>9} {'Per bet':>8}")
print(f"  {'-'*70}")
place_rules = [
    ("All P2 (no filter)",
        lambda r: True),
    ("P2 norm_score >= 3",
        lambda r: r["p2_norm"] >= 3),
    ("P2 norm_score >= 4",
        lambda r: r["p2_norm"] >= 4),
    ("P2 SP >= 3.0",
        lambda r: (r["p2_morn"] or 0) >= 3.0),
    ("P2 SP >= 5.0",
        lambda r: (r["p2_morn"] or 0) >= 5.0),
    ("norm_score>=4 AND SP>=2 (current rule)",
        lambda r: r["p2_norm"]>=4 and (r["p2_morn"] or 0)>=2.0),
    ("norm_score>=3 AND SP>=5",
        lambda r: r["p2_norm"]>=3 and (r["p2_morn"] or 0)>=5.0),
    ("norm_score>=4 OR (norm_score>=3 AND SP>=5) — deployed",
        lambda r: r["p2_norm"]>=4 or (r["p2_norm"]>=3 and (r["p2_morn"] or 0)>=5.0)),
    ("score_gap <= 0 (P2 stats equal/stronger)",
        lambda r: r["score_gap"] <= 0),
    ("score_gap <= 0 AND SP >= 3",
        lambda r: r["score_gap"]<=0 and (r["p2_morn"] or 0)>=3.0),
    ("score_gap <= -1 AND SP >= 2",
        lambda r: r["score_gap"]<=-1 and (r["p2_morn"] or 0)>=2.0),
]
for lbl, fn in place_rules:
    b = [r for r in records if fn(r) and r["p2_morn"] and r["div"]]
    if len(b) < 5: continue
    placed = sum(1 for r in b if r["p2_plcd"])
    pnl    = sum(est_place(r["p2_morn"],r["p2_plcd"],2,r["div"]) for r in b)
    print(f"  {lbl:<45} {len(b):>5} {pct(placed,len(b)):>8} {pnl:>+9.2f} {pnl/len(b):>+8.3f}")

print()

# ═════════════════════════════════════════════════════════════════════════════
# SECTION 9: TIER DEFINITIONS
# ═════════════════════════════════════════════════════════════════════════════

print("=" * 65)
print("9. TIER DEFINITIONS — current vs market-relative")
print("=" * 65)
print()

def tier_mkt_relative(runners, going):
    """Market-relative tier: how much do stats and market agree on P1."""
    n = len(runners)
    if n < 2: return 1
    rprs = [tof(r.get("rpr")) for r in runners]
    ors  = [tof(r.get("ofr") or r.get("or")) for r in runners]
    tsrs = [tof(r.get("ts") or r.get("tsr")) for r in runners]
    stats = sorted([(stats_score_card(r,rprs,ors,tsrs), r) for r in runners], key=lambda x:-x[0])
    stats_rank = {r.get("horse_id",""):i+1 for i,(_,r) in enumerate(stats)}
    mkt = sorted([(tof(r.get("sp_dec")) or 999, r) for r in runners], key=lambda x:x[0])
    mkt_rank = {r.get("horse_id",""):i+1 for i,(_,r) in enumerate(mkt)}
    p1 = stats[0][1]
    hid = p1.get("horse_id","")
    sr = stats_rank.get(hid, n); mr = mkt_rank.get(hid, n)
    sc_free = norm_score(p1)
    rd = abs(sr - mr)
    if sr==1 and mr==1 and sc_free>=3: return 4
    if sr==1 and mr==1:                return 3
    if rd<=1 and sc_free>=3:           return 3
    if rd<=1:                          return 2
    if rd<=2 and sc_free>=3:           return 2
    if rd<=3:                          return 1
    return 0

tier_results = {"current": defaultdict(lambda:{"n":0,"w":0,"pnl":0.0}),
                "mkt_rel": defaultdict(lambda:{"n":0,"w":0,"pnl":0.0})}

for date_str in card_dates:
    with open(f"{CARDS_DIR}/{date_str}.json") as f: d = json.load(f)
    for race in (d.get("races") or []):
        runners = race.get("all_runners",[])
        if not runners or not field_ok(runners, race): continue
        course = race.get("course","").lower().strip()
        off    = norm_time(race.get("off",""))
        rmap   = results_index.get((date_str, course, off),{})
        if not rmap or not any(v["pos"]==1 for v in rmap.values()): continue

        raw_meta = {
            "class":   str(race.get("race_class","") or race.get("class","") or ""),
            "surface": race.get("surface","Turf") or "Turf",
            "type":    race.get("type","") or "",
        }

        # Current tier
        tc_cur, _, _, _ = get_blended_picks(runners, mw_p1=0.1, mw_p2=0.4, raw_race=raw_meta)
        ranked = rank_card_runners(runners, race.get("going",""), 0.1)
        p1 = ranked[0]; hid = p1.get("horse_id","")
        res = rmap.get(hid,{})
        sp  = tof(p1.get("sp_dec"))
        if res.get("pos") is None or not sp: continue
        won = res["pos"] == 1
        pnl = win_pnl(sp, 2, won)
        tn  = TIER_NAMES.get(tc_cur, str(tc_cur))
        tier_results["current"][tn]["n"] += 1
        tier_results["current"][tn]["w"] += int(won)
        tier_results["current"][tn]["pnl"] += pnl

        # Market-relative tier
        tc_mr = tier_mkt_relative(runners, race.get("going",""))
        tier_results["mkt_rel"][tc_mr]["n"] += 1
        tier_results["mkt_rel"][tc_mr]["w"] += int(won)
        tier_results["mkt_rel"][tc_mr]["pnl"] += pnl

print(f"  {'System':<22} {'Tier':<10} {'N':>5} {'Win%':>7} {'P&L':>9} {'Per bet':>8}")
print(f"  {'-'*65}")
for sys_name, tmap in [("Current (mw=0.1)", "current"), ("Mkt-relative", "mkt_rel")]:
    for tier in (["ELITE","STRONG","GOOD","STD"] if sys_name.startswith("C") else [4,3,2,1,0]):
        s = tier_results[tmap].get(tier)
        if not s or not s["n"]: continue
        lbl = tier if isinstance(tier,str) else {4:"ELITE",3:"STRONG",2:"GOOD",1:"STD",0:"WEAK"}.get(tier,"?")
        print(f"  {sys_name:<22} {lbl:<10} {s['n']:>5} {pct(s['w'],s['n']):>7} "
              f"{s['pnl']:>+9.2f} {s['pnl']/s['n']:>+8.3f}")
    print()

# ═════════════════════════════════════════════════════════════════════════════
# SECTION 10: DAILY P&L
# ═════════════════════════════════════════════════════════════════════════════

print("=" * 65)
print("10. DAILY P&L — variance across clean card dates")
print("    Flat £2, morning price, all qualifying races")
print("=" * 65)
print()

by_date = defaultdict(lambda: {"n":0,"w":0,"pnl":0.0})
for r in records:
    by_date[r["date"]]["n"] += 1
    by_date[r["date"]]["w"] += int(r["p1_won"])
    by_date[r["date"]]["pnl"] += win_pnl(r["p1_morn"],2,r["p1_won"])

print(f"  {'Date':<12} {'N':>4} {'Win%':>7} {'Daily P&L':>10} {'Cumulative':>11}")
print(f"  {'-'*48}")
cum = 0.0
daily_vals = []
for date_str in sorted(by_date.keys()):
    s = by_date[date_str]
    cum += s["pnl"]
    daily_vals.append(s["pnl"])
    print(f"  {date_str:<12} {s['n']:>4} {pct(s['w'],s['n']):>7} {s['pnl']:>+10.2f} {cum:>+11.2f}")

neg = [d for d in daily_vals if d < 0]
print(f"  {'-'*48}")
print(f"  {'TOTAL':<12} {sum(s['n'] for s in by_date.values()):>4} "
      f"{pct(sum(s['w'] for s in by_date.values()), sum(s['n'] for s in by_date.values())):>7} "
      f"{'':>10} {cum:>+11.2f}")
print()
print(f"  Summary: {len(daily_vals)} days | Loss days: {len(neg)} ({pct(len(neg),len(daily_vals))})")
if daily_vals:
    print(f"  Best: {sgn(max(daily_vals))} | Worst: {sgn(min(daily_vals))} | Avg: {sgn(sum(daily_vals)/len(daily_vals))}")
print()

# ═════════════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ═════════════════════════════════════════════════════════════════════════════

print("=" * 65)
print("FINAL SUMMARY — clean backtest vs contaminated backtest")
print("=" * 65)
print()

n   = len(records)
w   = sum(1 for r in records if r["p1_won"])
pnl = sum(win_pnl(r["p1_morn"],2,r["p1_won"]) for r in records)
bsp_pnl = sum(win_pnl(r["p1_bsp"],2,r["p1_won"]) for r in records if r["p1_bsp"])
bsp_n   = sum(1 for r in records if r["p1_bsp"])
runs    = losing_runs([r["p1_won"] for r in records])

print(f"  Clean backtest (pre-race card inputs, morning price):")
print(f"    Races:       {n}")
print(f"    Win rate:    {pct(w,n)}")
print(f"    P&L:         {sgn(pnl)}")
print(f"    Per bet:     {sgn(pnl/n)}")
print(f"    Max losing run: {max(runs) if runs else 0}")
print(f"    Loss days:   {pct(len(neg), len(daily_vals))}")
print()
if bsp_n:
    print(f"  Using settled BSP instead of morning price ({bsp_n} races):")
    print(f"    P&L:     {sgn(bsp_pnl)}")
    print(f"    Per bet: {sgn(bsp_pnl/bsp_n)}")
    print()
print(f"  Contaminated backtest figures (for reference):")
print(f"    Previously showed: ~43-47% win rate, +£9-13/bet, 284-376% ROI")
print(f"    These used post-race RPR which inflated scores of horses")
print(f"    that ran well. The clean figures above are the truth.")
print()
print("Done.")
