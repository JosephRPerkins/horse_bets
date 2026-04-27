"""
scratch_final_test.py  —  NOT part of the main bot, do not commit
==================================================================
Final combined test of the revised system against 17-day history.

Parameters locked:
  - System C tiers (market-relative ranking)
  - P1 mw=0.60, P2 mw=0.40 (split market weights)
  - RPR coverage filter: >=60% of field must have valid RPR
  - Field size cap: max 12 runners
  - Bet tiers: ELITE (4), STRONG (3), GOOD (2)
  - Skip: STANDARD (1), WEAK (0), HARD SKIP (-1/-2)
  - Place bets: ELITE+STRONG only, 8+ runners
  - Stake cascade per tier (independent profit tracking):
      ELITE:  £2 -> £4 at £30 profit, £4 -> £6 at £60 profit
      STRONG: £2 -> £4 at £50 profit, £4 -> £6 at £100 profit
      GOOD:   £2 -> £4 at £75 profit, £4 -> £6 at £150 profit
  - Compared against current model throughout

Run from ~/horse_bets_v3:
  python3 scratch_final_test.py 2>&1 | tee final_test_output.txt
"""

import json, glob, os, sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from predict import score_runner, SIGNAL_WEIGHTS
from predict_v2 import race_confidence, TIER_LABELS

# ═══════════════════════════════════════════════════════════════════════════════
# DATA
# ═══════════════════════════════════════════════════════════════════════════════

history = {}
for fp in sorted(glob.glob("data/history/*.json")):
    with open(fp) as f: d = json.load(f)
    history[d["date"]] = d

raw_index = {}
for fp in sorted(glob.glob("data/raw/*.json")):
    with open(fp) as f: d = json.load(f)
    for race in (d.get("results") or d.get("races") or []):
        raw_index[race["race_id"]] = race

print(f"Loaded {len(history)} days | {len(raw_index)} raw races")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

SP_SIGNALS = {"sp_odds_on", "sp_2_to_4", "sp_4_to_6"}

def tof(v):
    try:
        f = float(str(v).strip())
        return f if f > 0 else None
    except: return None

def place_spots(n): return 1 if n<=4 else (2 if n<=7 else 3)
def place_div(n):   return None if n<=4 else (4.0 if n<=7 else (5.0 if n<=11 else 6.0))

def norm(val, vals, sc=10.0):
    v = [x for x in vals if x is not None]
    if not v or len(v) < 2: return sc/2
    lo, hi = min(v), max(v)
    return sc/2 if hi==lo else ((val-lo)/(hi-lo))*sc

def sp_free(runner):
    sc, sigs = score_runner(runner)
    return sc - sum(SIGNAL_WEIGHTS.get(s,0) for s in sigs if s in SP_SIGNALS)

def stats_score_raw(r, rprs, ors, tsrs):
    s = 0.0
    rpr = tof(r.get("rpr")); or_ = tof(r.get("or")); tsr = tof(r.get("tsr"))
    if rpr: s += norm(rpr, rprs, 10.0)
    if or_: s += norm(or_,  ors,  10.0)
    if tsr: s += norm(tsr,  tsrs,  5.0)
    if rpr and or_ and rpr > or_: s += 2.0
    fd = r.get("form_detail") or {}
    if isinstance(fd, dict):
        plc4 = fd.get("placed_last_4", 0) or 0
        bad  = fd.get("bad_recent",    0) or 0
        if plc4 >= 3:   s += 2.0
        elif plc4 >= 2: s += 1.0
        if bad == 0 and r.get("form",""): s += 1.0
    for f14 in [r.get("trainer_14d"), r.get("jockey_14d")]:
        if not isinstance(f14, dict): continue
        ae = f14.get("ae",0) or 0; runs = f14.get("runs",0) or 0
        if runs >= 3:
            if   ae >= 2.0 and runs >= 5: s += 3
            elif ae >= 1.5 and runs >= 5: s += 2
            elif ae >= 1.0 and runs >= 5: s += 1
    return s

def rpr_coverage(runners):
    """Fraction of runners with a valid RPR value."""
    if not runners: return 0.0
    return sum(1 for r in runners if tof(r.get("rpr"))) / len(runners)

def get_blended_picks(runners, raw, mw_p1=0.6, mw_p2=0.4):
    """
    Returns (tier, p1, p2) using System C tiers.
    P1 ranked by mw=mw_p1, P2 ranked by mw=mw_p2 from remaining runners.
    """
    n   = len(runners)
    cls = str(raw.get("class","") or "").replace("Class ","").strip()

    # Hard skip conditions
    if cls in ("1","2"): return -2, None, None
    if n > 12:           return -2, None, None
    if n < 2:            return  1, runners[0], None

    rprs = [tof(r.get("rpr")) for r in runners]
    ors  = [tof(r.get("or"))  for r in runners]
    tsrs = [tof(r.get("tsr")) for r in runners]

    # Stats ranks
    stats_s = sorted(
        [(stats_score_raw(r,rprs,ors,tsrs), i, r) for i,r in enumerate(runners)],
        key=lambda x: -x[0]
    )
    stats_rank = {r.get("horse_id","_"+str(i)): rank+1
                  for rank,(_,i,r) in enumerate(stats_s)}

    # Market ranks
    mkt_s = sorted(
        [(tof(r.get("sp_dec")) or 999, i, r) for i,r in enumerate(runners)],
        key=lambda x: x[0]
    )
    mkt_rank = {r.get("horse_id","_"+str(i)): rank+1
                for rank,(_,i,r) in enumerate(mkt_s)}

    def blend(mw):
        b = []
        for ss,_i,r in stats_s:
            hid = r.get("horse_id","")
            sr  = stats_rank.get(hid, n)
            mr  = mkt_rank.get(hid, n)
            cs  = (1-mw)*(sr-1)/max(n-1,1) + mw*(mr-1)/max(n-1,1)
            b.append((cs, sr, mr, ss, r))
        b.sort(key=lambda x: x[0])
        return b

    # P1 from mw_p1 blend
    b1 = blend(mw_p1)
    _,sr1,mr1,ss1,p1 = b1[0]

    # P2 from mw_p2 blend — best runner excluding P1
    p1_hid = p1.get("horse_id","")
    b2 = blend(mw_p2)
    p2 = next((r for _,_,_,_,r in b2 if r.get("horse_id","") != p1_hid), None)

    # Tier from P1 characteristics
    sc1f    = sp_free(p1)
    rd      = abs(sr1 - mr1)
    ba      = sr1==1 and mr1==1

    if ba and sc1f >= 3:         tc = 4   # ELITE
    elif ba:                     tc = 3   # STRONG
    elif rd <= 1 and sc1f >= 3:  tc = 3   # STRONG
    elif rd <= 1:                tc = 2   # GOOD
    elif rd <= 2 and sc1f >= 3:  tc = 2   # GOOD
    elif rd <= 3 and sc1f >= 2:  tc = 1   # STANDARD
    elif sc1f >= 1:              tc = 1   # STANDARD
    else:                        tc = 0   # WEAK

    return tc, p1, p2

# Stake cascade
THRESHOLDS = {
    4: [(0,2.0),(30,4.0),(60,6.0)],
    3: [(0,2.0),(50,4.0),(100,6.0)],
    2: [(0,2.0),(75,4.0),(150,6.0)],
}
TIER_NAMES = {4:"ELITE", 3:"STRONG", 2:"GOOD", 1:"STANDARD", 0:"WEAK"}

def get_stake(profit, tier):
    stake = THRESHOLDS[tier][0][1]
    for min_p, s in THRESHOLDS[tier]:
        if profit >= min_p: stake = s
    return stake

# ═══════════════════════════════════════════════════════════════════════════════
# SIMULATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

def simulate(use_rpr_filter=True, use_field_cap=True):
    """Run full simulation. Returns per-tier stats dict."""

    tier_profit = {4:0.0, 3:0.0, 2:0.0}
    tier_peak   = {4:0.0, 3:0.0, 2:0.0}

    stats = defaultdict(lambda: {
        "n":0, "p1w":0, "p2w":0, "p1p":0, "p2p":0,
        "either_w":0, "either_p":0, "both_p":0, "neither_p":0,
        "p1_win_pnl":0.0, "p2_win_pnl":0.0,
        "p1_plc_pnl":0.0, "p2_plc_pnl":0.0,
        "races_skipped_rpr":0, "races_skipped_field":0,
    })

    day_pnl_track = defaultdict(lambda: defaultdict(float))
    drawdown_track = {4:0.0, 3:0.0, 2:0.0}
    streak_track   = {4:0, 3:0, 2:0}
    max_streak     = {4:0, 3:0, 2:0}
    max_drawdown   = {4:0.0, 3:0.0, 2:0.0}

    for date, day in sorted(history.items()):
        for rec in day.get("races",[]):
            raw = raw_index.get(rec["race_id"])
            if not raw: continue
            runners = raw.get("runners",[])
            if len(runners) < 2: continue

            # ── Filters ───────────────────────────────────────────────────────
            # Field cap
            if use_field_cap and len(runners) > 12:
                stats["FILTERED"]["races_skipped_field"] += 1
                continue

            # RPR coverage
            if use_rpr_filter and rpr_coverage(runners) < 0.6:
                stats["FILTERED"]["races_skipped_rpr"] += 1
                continue

            # ── Get picks and tier ────────────────────────────────────────────
            tc, p1, p2 = get_blended_picks(runners, raw, mw_p1=0.6, mw_p2=0.4)

            # Only bet ELITE, STRONG, GOOD
            if tc not in (4,3,2): continue
            if p1 is None:        continue

            n   = len(runners)
            ps  = place_spots(n)
            div = place_div(n)

            # ── Outcomes ──────────────────────────────────────────────────────
            pos_by = {
                r.get("horse","").split(" (")[0].strip().lower(): r
                for r in runners
            }
            def outcome(horse):
                r = pos_by.get(horse.get("horse","").split(" (")[0].strip().lower())
                if not r: return None, None
                try:    p = int(str(r.get("position","")).strip())
                except: return None, None
                return p, tof(r.get("sp_dec")) or tof(r.get("bsp"))

            p1_pos, p1_sp = outcome(p1)
            p2_pos, p2_sp = outcome(p2) if p2 else (None, None)
            if p1_pos is None or not p1_sp: continue

            p1_won  = p1_pos == 1
            p1_plcd = p1_pos <= ps
            p2_won  = p2_pos == 1   if p2_pos is not None else False
            p2_plcd = p2_pos <= ps  if p2_pos is not None else False

            # ── Stakes ────────────────────────────────────────────────────────
            stake = get_stake(tier_profit[tc], tc)

            # Win bets: P1 + P2 both backed
            p1_wpnl = (p1_sp-1)*stake if p1_won else -stake
            p2_wpnl = 0.0
            if p2_sp:
                p2_wpnl = (p2_sp-1)*stake if p2_won else -stake

            # Place bets: ELITE+STRONG, 8+ runners only
            p1_ppnl = p2_ppnl = 0.0
            if tc in (4,3) and div and n >= 8:
                ep1 = (p1_sp-1)/div + 1
                p1_ppnl = (ep1-1)*stake if p1_plcd else -stake
                if p2_sp:
                    ep2 = (p2_sp-1)/div + 1
                    p2_ppnl = (ep2-1)*stake if p2_plcd else -stake

            race_pnl = p1_wpnl + p2_wpnl + p1_ppnl + p2_ppnl
            tier_profit[tc] += race_pnl
            day_pnl_track[date][tc] += race_pnl

            # Peak / drawdown tracking
            if tier_profit[tc] > tier_peak[tc]:
                tier_peak[tc] = tier_profit[tc]
            dd = tier_peak[tc] - tier_profit[tc]
            if dd > max_drawdown[tc]:
                max_drawdown[tc] = dd

            # Losing streak
            if race_pnl < 0:
                streak_track[tc] += 1
                if streak_track[tc] > max_streak[tc]:
                    max_streak[tc] = streak_track[tc]
            else:
                streak_track[tc] = 0

            # ── Accumulate stats ──────────────────────────────────────────────
            tl = TIER_NAMES[tc]
            s  = stats[tl]
            s["n"]        += 1
            s["p1w"]      += p1_won
            s["p2w"]      += p2_won
            s["p1p"]      += p1_plcd
            s["p2p"]      += p2_plcd
            s["either_w"] += p1_won or p2_won
            s["either_p"] += p1_plcd or p2_plcd
            s["both_p"]   += p1_plcd and p2_plcd
            s["neither_p"]+= not (p1_plcd or p2_plcd)
            s["p1_win_pnl"] += p1_wpnl
            s["p2_win_pnl"] += p2_wpnl
            s["p1_plc_pnl"] += p1_ppnl
            s["p2_plc_pnl"] += p2_ppnl

    return stats, tier_profit, max_drawdown, max_streak, day_pnl_track

# ═══════════════════════════════════════════════════════════════════════════════
# CURRENT MODEL SIMULATION (for comparison)
# ═══════════════════════════════════════════════════════════════════════════════

def simulate_current():
    """Replay history using current model picks and tiers, flat £2 stake."""
    stats = defaultdict(lambda: {
        "n":0,"p1w":0,"p2w":0,"p1p":0,"p2p":0,
        "either_w":0,"neither_p":0,
        "p1_win_pnl":0.0,"p2_win_pnl":0.0,
        "p1_plc_pnl":0.0,"p2_plc_pnl":0.0,
    })
    total_pnl = 0.0

    for date, day in sorted(history.items()):
        for rec in day.get("races",[]):
            raw = raw_index.get(rec["race_id"])
            if not raw: continue
            runners = raw.get("runners",[])
            if len(runners) < 2: continue

            n   = len(runners)
            ps  = place_spots(n)
            div = place_div(n)

            cur_scored = sorted(
                [{**r, "_s": score_runner(r)[0]} for r in runners],
                key=lambda x: -x["_s"]
            )
            raw2 = {**raw, "runners": cur_scored}
            cur_tier, _ = race_confidence(raw2, cur_scored[0]["_s"] if cur_scored else 0)
            tl = TIER_LABELS.get(cur_tier,"?").split()[0]

            p1_sp = tof(cur_scored[0].get("sp_dec")) or tof(cur_scored[0].get("bsp")) if cur_scored else None
            p2_sp = tof(cur_scored[1].get("sp_dec")) or tof(cur_scored[1].get("bsp")) if len(cur_scored)>1 else None

            p1_won  = rec.get("a_pos") == 1
            p1_plcd = bool(rec.get("std_a") or rec.get("cons_a"))
            p2_won  = rec.get("b_pos") == 1
            p2_plcd = bool(rec.get("std_b") or rec.get("cons_b"))

            stake = 2.0
            p1_wpnl = (p1_sp-1)*stake if (p1_won and p1_sp) else (-stake if p1_sp else 0)
            p2_wpnl = (p2_sp-1)*stake if (p2_won and p2_sp) else (-stake if p2_sp else 0)
            p1_ppnl = p2_ppnl = 0.0
            if p1_sp and div:
                ep=(p1_sp-1)/div+1
                p1_ppnl=(ep-1)*stake if p1_plcd else -stake
            if p2_sp and div:
                ep=(p2_sp-1)/div+1
                p2_ppnl=(ep-1)*stake if p2_won else -stake

            s = stats[tl]
            s["n"]         += 1
            s["p1w"]       += p1_won
            s["p2w"]       += p2_won
            s["p1p"]       += p1_plcd
            s["p2p"]       += p2_plcd
            s["either_w"]  += p1_won or p2_won
            s["neither_p"] += not (p1_plcd or p2_plcd)
            s["p1_win_pnl"]+= p1_wpnl
            s["p2_win_pnl"]+= p2_wpnl
            s["p1_plc_pnl"]+= p1_ppnl
            s["p2_plc_pnl"]+= p2_ppnl
            total_pnl += p1_wpnl+p2_wpnl+p1_ppnl+p2_ppnl

    return stats, total_pnl

# ═══════════════════════════════════════════════════════════════════════════════
# RUN SIMULATIONS
# ═══════════════════════════════════════════════════════════════════════════════

print("Running revised system (all filters)...")
rev_stats, rev_profit, rev_dd, rev_streak, rev_days = simulate(use_rpr_filter=True, use_field_cap=True)

print("Running revised system (no RPR filter, for comparison)...")
nofilter_stats, nofilter_profit, _, _, _ = simulate(use_rpr_filter=False, use_field_cap=True)

print("Running current model...")
cur_stats, cur_total = simulate_current()
print()

# ═══════════════════════════════════════════════════════════════════════════════
# OUTPUT
# ═══════════════════════════════════════════════════════════════════════════════

print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  REVISED SYSTEM — FULL RESULTS (with RPR filter + field cap ≤12)       ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()

# Filter stats
filt = rev_stats.get("FILTERED",{})
print(f"  Races filtered out:")
print(f"    RPR coverage <60%:  {filt.get('races_skipped_rpr',0)}")
print(f"    Field size >12:     {filt.get('races_skipped_field',0)}")
print()

# Per-tier results
print(f"  {'Tier':<10} {'N':>5} {'P1win%':>8} {'P2win%':>8} {'P1plc%':>8} "
      f"{'P2plc%':>8} {'EitherW%':>9} {'NeitherP%':>10} "
      f"{'WinP&L':>9} {'PlcP&L':>9} {'Combined':>10} {'FinalPot':>10}")
print(f"  {'-'*108}")

total_combined = 0.0
for tl in ["ELITE","STRONG","GOOD"]:
    s = rev_stats.get(tl,{})
    n = s.get("n",0)
    if not n: continue
    wpnl = s["p1_win_pnl"]+s["p2_win_pnl"]
    ppnl = s["p1_plc_pnl"]+s["p2_plc_pnl"]
    tc   = {v:k for k,v in TIER_NAMES.items()}.get(tl,0)
    pot  = rev_profit.get(tc,0)
    total_combined += wpnl+ppnl
    print(f"  {tl:<10} {n:>5} "
          f"{s['p1w']/n*100:>7.1f}%  {s['p2w']/n*100:>7.1f}%  "
          f"{s['p1p']/n*100:>7.1f}%  {s['p2p']/n*100:>7.1f}%  "
          f"{s['either_w']/n*100:>8.1f}%  {s['neither_p']/n*100:>9.1f}%  "
          f"{wpnl:>+9.2f} {ppnl:>+9.2f} {wpnl+ppnl:>+10.2f} {pot:>+10.2f}")

# Totals
all_n   = sum(rev_stats.get(tl,{}).get("n",0) for tl in ["ELITE","STRONG","GOOD"])
all_p1w = sum(rev_stats.get(tl,{}).get("p1w",0) for tl in ["ELITE","STRONG","GOOD"])
all_ew  = sum(rev_stats.get(tl,{}).get("either_w",0) for tl in ["ELITE","STRONG","GOOD"])
all_np  = sum(rev_stats.get(tl,{}).get("neither_p",0) for tl in ["ELITE","STRONG","GOOD"])
all_wp  = sum(rev_stats.get(tl,{}).get("p1_win_pnl",0)+rev_stats.get(tl,{}).get("p2_win_pnl",0) for tl in ["ELITE","STRONG","GOOD"])
all_pp  = sum(rev_stats.get(tl,{}).get("p1_plc_pnl",0)+rev_stats.get(tl,{}).get("p2_plc_pnl",0) for tl in ["ELITE","STRONG","GOOD"])
total_pot = sum(rev_profit.get(tc,0) for tc in (4,3,2))

print(f"  {'─'*108}")
print(f"  {'TOTAL':<10} {all_n:>5} "
      f"{all_p1w/all_n*100:>7.1f}%  {'':>8}  {'':>8}  {'':>8}  "
      f"{all_ew/all_n*100:>8.1f}%  {all_np/all_n*100:>9.1f}%  "
      f"{all_wp:>+9.2f} {all_pp:>+9.2f} {all_wp+all_pp:>+10.2f} {total_pot:>+10.2f}")
print()

# Drawdown and streak summary
print(f"  {'Tier':<10} {'MaxDrawdown':>13} {'MaxStreak':>11} {'FinalProfit':>13}")
print(f"  {'-'*50}")
for tc,tl in [(4,"ELITE"),(3,"STRONG"),(2,"GOOD")]:
    print(f"  {tl:<10} {rev_dd.get(tc,0):>+13.2f} {rev_streak.get(tc,0):>11}  {rev_profit.get(tc,0):>+13.2f}")
print()

# ── Effect of RPR filter ──────────────────────────────────────────────────────
print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  EFFECT OF RPR FILTER (with vs without)                                 ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()
print(f"  {'Tier':<10} {'N(filt)':>9} {'N(nofilt)':>11} {'P&L(filt)':>11} {'P&L(nofilt)':>13} {'Delta':>8}")
print(f"  {'-'*58}")
for tl in ["ELITE","STRONG","GOOD"]:
    sf = rev_stats.get(tl,{})
    sn = nofilter_stats.get(tl,{})
    nf = sf.get("n",0); nn = sn.get("n",0)
    pf = sf.get("p1_win_pnl",0)+sf.get("p2_win_pnl",0)+sf.get("p1_plc_pnl",0)+sf.get("p2_plc_pnl",0)
    pn = sn.get("p1_win_pnl",0)+sn.get("p2_win_pnl",0)+sn.get("p1_plc_pnl",0)+sn.get("p2_plc_pnl",0)
    print(f"  {tl:<10} {nf:>9} {nn:>11} {pf:>+11.2f} {pn:>+13.2f} {pf-pn:>+8.2f}")
print()

# ── vs Current model ──────────────────────────────────────────────────────────
print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  COMPARISON VS CURRENT MODEL (flat £2, all races)                       ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()
print(f"  {'Model':<35} {'N':>5} {'P1win%':>8} {'EitherW%':>10} {'NeitherP%':>11} {'TotalP&L':>10}")
print(f"  {'-'*82}")

# Current model totals
cur_n   = sum(s.get("n",0) for s in cur_stats.values())
cur_p1w = sum(s.get("p1w",0) for s in cur_stats.values())
cur_ew  = sum(s.get("either_w",0) for s in cur_stats.values())
cur_np  = sum(s.get("neither_p",0) for s in cur_stats.values())
print(f"  {'Current model (flat £2, all tiers)':<35} {cur_n:>5} "
      f"{cur_p1w/cur_n*100:>7.1f}%  {cur_ew/cur_n*100:>9.1f}%  "
      f"{cur_np/cur_n*100:>10.1f}%  {cur_total:>+10.2f}")

print(f"  {'Revised (flat £2 equiv, bet tiers)':<35} {all_n:>5} "
      f"{all_p1w/all_n*100:>7.1f}%  {all_ew/all_n*100:>9.1f}%  "
      f"{all_np/all_n*100:>10.1f}%  {all_wp+all_pp:>+10.2f}")

print(f"  {'Revised (dynamic stakes, bet tiers)':<35} {all_n:>5} "
      f"{'':>8}  {'':>10}  {'':>11}  {total_pot:>+10.2f}")
print()

# ── Daily P&L distribution ────────────────────────────────────────────────────
print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  DAILY P&L SUMMARY (revised system, dynamic stakes)                     ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()
print(f"  {'Date':12} {'ELITE':>9} {'STRONG':>9} {'GOOD':>9} {'Combined':>10}")
print(f"  {'-'*52}")
grand_total = 0.0
for date in sorted(set(list(rev_days.keys()))):
    e = rev_days[date].get(4,0)
    s = rev_days[date].get(3,0)
    g = rev_days[date].get(2,0)
    comb = e+s+g
    grand_total += comb
    print(f"  {date:12} {e:>+9.2f} {s:>+9.2f} {g:>+9.2f} {comb:>+10.2f}")
print(f"  {'─'*52}")
print(f"  {'TOTAL':12} {sum(rev_days[d].get(4,0) for d in rev_days):>+9.2f} "
      f"{sum(rev_days[d].get(3,0) for d in rev_days):>+9.2f} "
      f"{sum(rev_days[d].get(2,0) for d in rev_days):>+9.2f} "
      f"{grand_total:>+10.2f}")
print()
print("Done.")
