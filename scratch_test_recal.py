"""
scratch_tier_recal.py  —  NOT part of the main bot, do not commit
==================================================================
Tests alternative tier assignment logic against card data outcomes.

Problem identified: existing tier system is anti-correlated with
outcomes — higher scores produce lower win rates because SP-based
signals (sp_odds_on=3, sp_2_to_4=2, sp_4_to_6=1) inflate scores
for short-priced horses, creating a false confidence signal.

This script tests four alternative tier approaches:
  A) Current system (baseline)
  B) SP-free score tiers — score only non-SP signals, retier
  C) Market-relative tiers — tier by stats rank vs market rank gap
  D) Combined: SP-free score + score gap + market agreement

All tested on card dates Apr 22-25 (94 matched races, full RPR).

Run from ~/horse_bets_v3:
  python3 scratch_tier_recal.py 2>&1 | tee tier_recal_output.txt
"""

import json, glob, os, sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from predict    import score_runner, SIGNAL_WEIGHTS
from predict_v2 import race_confidence, TIER_LABELS, TIER_SUPREME, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP

# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════════

card_races = {}
for fp in sorted(glob.glob("data/cards/2026-04-2*.json")):
    with open(fp) as f: d = json.load(f)
    for race in d.get("races", []):
        key = (race.get("course",""), race.get("off_dt", race.get("off","")))
        card_races[key] = race

raw_by_key = {}
for fp in sorted(glob.glob("data/raw/2026-04-2*.json")):
    with open(fp) as f: d = json.load(f)
    for race in (d.get("results") or d.get("races") or []):
        key = (race.get("course",""), race.get("off_dt", race.get("off","")))
        raw_by_key[key] = race

print(f"Loaded {len(card_races)} card races, {len(raw_by_key)} raw races")
print(f"Matched: {sum(1 for k in card_races if k in raw_by_key)} races")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

SP_SIGNALS = {"sp_odds_on", "sp_2_to_4", "sp_4_to_6"}

def strip_country(name):
    return (name or "").split(" (")[0].strip().lower()

def to_float(v):
    try:
        f = float(str(v).strip())
        return f if f > 0 else None
    except: return None

def place_spots(n):
    if n <= 4: return 1
    if n <= 7: return 2
    return 3

def place_divisor(n):
    if n <= 4:  return None
    if n <= 7:  return 4.0
    if n <= 11: return 5.0
    return 6.0

def sp_free_score(runner):
    """Score runner using only non-SP signals."""
    sc, signals = score_runner(runner)
    sp_contribution = sum(
        SIGNAL_WEIGHTS.get(sig, 0)
        for sig in signals
        if sig in SP_SIGNALS
    )
    return sc - sp_contribution, {k:v for k,v in signals.items() if k not in SP_SIGNALS}

def normalise(val, vals, scale=10.0):
    valid = [v for v in vals if v is not None]
    if not valid or len(valid) < 2: return scale/2
    lo, hi = min(valid), max(valid)
    if hi == lo: return scale/2
    return ((val-lo)/(hi-lo))*scale

def stats_score_card(runner, field_rprs, field_ors, field_tsrs, going):
    """Pure stats score from card data (used for market-relative tier)."""
    score = 0.0
    rpr = to_float(runner.get("rpr"))
    or_ = to_float(runner.get("ofr") or runner.get("or"))
    ts  = to_float(runner.get("ts") or runner.get("tsr"))
    if rpr: score += normalise(rpr, field_rprs, 10.0)
    if or_: score += normalise(or_, field_ors,  10.0)
    if ts:  score += normalise(ts,  field_tsrs,  5.0)
    if rpr and or_ and rpr > or_: score += 2.0
    fd = runner.get("form_detail") or {}
    if isinstance(fd, dict):
        plc4 = fd.get("placed_last_4", 0) or 0
        bad  = fd.get("bad_recent",    0) or 0
        if plc4 >= 3:   score += 2.0
        elif plc4 >= 2: score += 1.0
        if bad == 0 and runner.get("form",""): score += 1.0
    t14 = runner.get("trainer_14d") or {}
    j14 = runner.get("jockey_14d")  or {}
    for f14 in [t14, j14]:
        if not isinstance(f14, dict): continue
        ae = f14.get("ae",0) or 0
        runs = f14.get("runs",0) or 0
        wp = f14.get("win_pct",0) or 0
        if runs >= 3:
            if   ae >= 2.0 and runs >= 5: score += 3
            elif ae >= 1.5 and runs >= 5: score += 2
            elif ae >= 1.0 and runs >= 5: score += 1
    return score

# ═══════════════════════════════════════════════════════════════════════════════
# TIER SYSTEMS
# ═══════════════════════════════════════════════════════════════════════════════

# Tier labels for new systems
TIER_NAMES = {4:"ELITE", 3:"STRONG", 2:"GOOD", 1:"STANDARD", 0:"WEAK", -1:"SKIP"}

def tier_A_current(card_race):
    """Current system — pre-baked tier_label from card."""
    tier_label = card_race.get("tier_label","?")
    # Map to numeric
    mapping = {
        "🔥🔥🔥 SUPREME": 4,
        "🔥🔥  STRONG ":  3,
        "🔥    GOOD   ":  2,
        "·     STANDARD": 1,
        "✗     SKIP   ":  -1,
    }
    for k,v in mapping.items():
        if k.strip() in tier_label.strip() or tier_label.strip() in k.strip():
            return v
    return 1

def tier_B_spfree(card_race):
    """
    SP-free score tiers.
    Score P1 using only non-SP signals, then tier by that score + gap.
    Thresholds derived from score distribution analysis:
      score 4+ performs well, so we tier from there upward.
    """
    top1     = card_race.get("top1") or {}
    top2     = card_race.get("top2") or {}
    runners  = card_race.get("all_runners") or []
    going    = card_race.get("going","")
    surface  = "AW" if "standard" in going.lower() or "aw" in (card_race.get("surface","") or "").lower() else "Turf"
    cls      = card_race.get("race_class") or card_race.get("class","")
    n        = len(runners)

    # Skip conditions (these don't depend on score)
    cls_12 = cls in ("Class 1","Class 2","1","2")
    if cls_12:          return -1
    if n >= 13:         return -1

    sc1_free, sig1 = sp_free_score(top1)
    sc2_free, sig2 = sp_free_score(top2) if top2 else (0, {})
    gap = sc1_free - sc2_free

    # ELITE: strong SP-free score + clear gap + market agrees (P1 shorter than P2)
    p1_sp = to_float(top1.get("sp_dec"))
    p2_sp = to_float(top2.get("sp_dec")) if top2 else None
    mkt_agrees = p1_sp and p2_sp and p1_sp < p2_sp

    if sc1_free >= 4 and gap >= 2 and mkt_agrees:    return 4  # ELITE
    if sc1_free >= 4 and gap >= 2:                    return 3  # STRONG
    if sc1_free >= 3 and gap >= 1 and mkt_agrees:     return 3  # STRONG
    if sc1_free >= 3 and mkt_agrees:                  return 2  # GOOD
    if sc1_free >= 2 and gap >= 1:                    return 2  # GOOD
    if surface == "AW" and sc1_free < 2:              return -1 # SKIP AW low score
    if sc1_free >= 2:                                 return 1  # STANDARD
    return 0  # WEAK

def tier_C_market_relative(card_race):
    """
    Market-relative tier: tier by how much stats rank agrees with market rank.
    Uses the mw=0.60 insight — market signal is strong, agreement = confidence.
    """
    runners = card_race.get("all_runners") or []
    going   = card_race.get("going","")
    cls     = card_race.get("race_class") or card_race.get("class","")
    n       = len(runners)

    if cls in ("Class 1","Class 2","1","2"): return -1
    if n >= 13:                              return -1

    if len(runners) < 2: return 1

    field_rprs = [to_float(r.get("rpr")) for r in runners]
    field_ors  = [to_float(r.get("ofr") or r.get("or")) for r in runners]
    field_tsrs = [to_float(r.get("ts") or r.get("tsr")) for r in runners]

    # Stats scores and ranks
    stats_scored = sorted(
        [(stats_score_card(r, field_rprs, field_ors, field_tsrs, going), i, r)
         for i,r in enumerate(runners)],
        reverse=True
    )
    stats_rank = {r.get("horse_id","_"+str(i)): rank+1
                  for rank,(sc,i,r) in enumerate(stats_scored)}

    # Market ranks
    mkt_scored = sorted(
        [(to_float(r.get("sp_dec")) or 999, i, r)
         for i,r in enumerate(runners)]
    )
    mkt_rank = {r.get("horse_id","_"+str(i)): rank+1
                for rank,(sp,i,r) in enumerate(mkt_scored)}

    # P1 by stats
    p1_stats = stats_scored[0][2]
    hid      = p1_stats.get("horse_id","_0")
    sr       = stats_rank.get(hid, n)
    mr       = mkt_rank.get(hid, n)
    rank_diff = abs(sr - mr)

    # SP-free score of stats P1
    sc1_free, _ = sp_free_score(p1_stats)

    # Tier by agreement + SP-free score
    if sr == 1 and mr == 1 and sc1_free >= 3:    return 4   # ELITE: full agreement, strong score
    if sr == 1 and mr == 1:                       return 3   # STRONG: full agreement
    if rank_diff <= 1 and sc1_free >= 3:          return 3   # STRONG: near agreement, strong score
    if rank_diff <= 1:                            return 2   # GOOD: near agreement
    if rank_diff <= 3 and sc1_free >= 3:          return 2   # GOOD: moderate disagreement, strong score
    if rank_diff <= 3:                            return 1   # STANDARD
    return 0                                                  # WEAK: large disagreement

def tier_D_combined(card_race):
    """
    Combined: SP-free score + market agreement + score gap.
    Best of both B and C.
    """
    runners = card_race.get("all_runners") or []
    going   = card_race.get("going","")
    cls     = card_race.get("race_class") or card_race.get("class","")
    n       = len(runners)

    if cls in ("Class 1","Class 2","1","2"): return -1
    if n >= 13:                              return -1
    if len(runners) < 2:                     return 1

    field_rprs = [to_float(r.get("rpr")) for r in runners]
    field_ors  = [to_float(r.get("ofr") or r.get("or")) for r in runners]
    field_tsrs = [to_float(r.get("ts") or r.get("tsr")) for r in runners]

    # Stats ranking (mw=0.6 blended)
    stats_scores = [(stats_score_card(r, field_rprs, field_ors, field_tsrs, going), r)
                    for r in runners]
    stats_scores.sort(key=lambda x: -x[0])
    stats_rank = {r.get("horse_id",""):i+1 for i,(_,r) in enumerate(stats_scores)}

    mkt_scores = [(to_float(r.get("sp_dec")) or 999, r) for r in runners]
    mkt_scores.sort(key=lambda x: x[0])
    mkt_rank = {r.get("horse_id",""):i+1 for i,(_,r) in enumerate(mkt_scores)}

    # Blended rank (mw=0.6)
    blended = []
    for ss, r in stats_scores:
        hid = r.get("horse_id","")
        sr  = stats_rank.get(hid, n)
        mr  = mkt_rank.get(hid, n)
        sr_norm = (sr-1)/max(n-1,1)
        mr_norm = (mr-1)/max(n-1,1)
        cs = 0.4*sr_norm + 0.6*mr_norm
        blended.append((cs, sr, mr, ss, r))
    blended.sort(key=lambda x: x[0])

    p1 = blended[0]
    p2 = blended[1]
    cs1, sr1, mr1, ss1, r1 = p1
    cs2, sr2, mr2, ss2, r2 = p2

    sc1_free, _ = sp_free_score(r1)
    sc2_free, _ = sp_free_score(r2)
    gap = sc1_free - sc2_free

    rank_diff  = abs(sr1 - mr1)   # how much stats and market disagree on P1
    both_agree = sr1 == 1 and mr1 == 1

    # Surface
    going_low = going.lower()
    is_aw = "standard" in going_low or "aw" in (card_race.get("surface","") or "").lower()

    if both_agree and sc1_free >= 4 and gap >= 2:           return 4  # ELITE
    if both_agree and sc1_free >= 3:                        return 3  # STRONG
    if rank_diff <= 1 and sc1_free >= 4 and gap >= 1:       return 3  # STRONG
    if rank_diff <= 1 and sc1_free >= 3:                    return 2  # GOOD
    if rank_diff <= 2 and sc1_free >= 3 and not is_aw:      return 2  # GOOD
    if rank_diff <= 3 and sc1_free >= 2:                    return 1  # STANDARD
    if is_aw and sc1_free < 2:                              return -1 # SKIP
    if sc1_free >= 1:                                       return 1  # STANDARD
    return 0                                                           # WEAK

# ═══════════════════════════════════════════════════════════════════════════════
# EVALUATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

def evaluate_tier_system(tier_fn, system_name):
    """
    For each card race, assign a tier using tier_fn, look up actual
    outcomes from raw results, accumulate stats by tier.
    """
    tier_stats = defaultdict(lambda: {
        "n":0,"p1w":0,"p1p":0,"p2w":0,"p2p":0,
        "either_w":0,"either_p":0,"both_p":0,"neither_p":0,
        "p1_win_pnl":0.0,"p2_win_pnl":0.0,
        "p1_plc_pnl":0.0,"p2_plc_pnl":0.0,
    })

    for key, card_race in card_races.items():
        raw = raw_by_key.get(key)
        if not raw: continue
        raw_runners = raw.get("runners",[])
        n   = len(raw_runners)
        ps  = place_spots(n)
        div = place_divisor(n)
        pos_by = {strip_country(r.get("horse","")): r for r in raw_runners}

        def outcome(name):
            r = pos_by.get(strip_country(name))
            if not r: return None, None
            try:    p = int(str(r.get("position","")).strip())
            except: return None, None
            return p, to_float(r.get("sp_dec"))

        # Get P1/P2 from card
        top1 = card_race.get("top1") or {}
        top2 = card_race.get("top2") or {}
        p1_pos, p1_sp = outcome(top1.get("horse",""))
        p2_pos, p2_sp = outcome(top2.get("horse",""))
        if p1_pos is None: continue

        # Assign tier
        tier = tier_fn(card_race)

        p1_won  = p1_pos == 1
        p1_plcd = p1_pos <= ps
        p2_won  = p2_pos == 1   if p2_pos is not None else False
        p2_plcd = p2_pos <= ps  if p2_pos is not None else False

        s = tier_stats[tier]
        s["n"]        += 1
        s["p1w"]      += p1_won
        s["p1p"]      += p1_plcd
        s["p2w"]      += p2_won
        s["p2p"]      += p2_plcd
        s["either_w"] += p1_won or p2_won
        s["either_p"] += p1_plcd or p2_plcd
        s["both_p"]   += p1_plcd and p2_plcd
        s["neither_p"]+= not (p1_plcd or p2_plcd)

        if p1_sp:
            s["p1_win_pnl"] += (p1_sp-1)*2 if p1_won else -2
            if div: s["p1_plc_pnl"] += ((p1_sp-1)/div)*2 if p1_plcd else -2
        if p2_sp:
            s["p2_win_pnl"] += (p2_sp-1)*2 if p2_won else -2
            if div: s["p2_plc_pnl"] += ((p2_sp-1)/div)*2 if p2_plcd else -2

    return tier_stats

# ═══════════════════════════════════════════════════════════════════════════════
# PRINT HELPER
# ═══════════════════════════════════════════════════════════════════════════════

def print_tier_results(system_name, tier_stats, tier_order, tier_labels):
    total_n = sum(s["n"] for s in tier_stats.values())
    print(f"  {'Tier':<12} {'N':>5} {'%races':>7} {'P1win%':>8} {'P1plc%':>8} "
          f"{'P2win%':>8} {'P2plc%':>8} {'EitherW%':>9} {'NeitherP%':>10} "
          f"{'WinP&L':>8} {'PlcP&L':>8} {'Combined':>10}")
    print(f"  {'-'*108}")

    grand = defaultdict(float)
    for tier in tier_order:
        s = tier_stats.get(tier)
        if not s or s["n"] == 0: continue
        n    = s["n"]
        lbl  = tier_labels.get(tier, str(tier))
        wpnl = s["p1_win_pnl"] + s["p2_win_pnl"]
        ppnl = s["p1_plc_pnl"] + s["p2_plc_pnl"]
        print(f"  {lbl:<12} {n:>5} {n/total_n*100:>6.0f}%  "
              f"{s['p1w']/n*100:>7.1f}%  {s['p1p']/n*100:>7.1f}%  "
              f"{s['p2w']/n*100:>7.1f}%  {s['p2p']/n*100:>7.1f}%  "
              f"{s['either_w']/n*100:>8.1f}%  {s['neither_p']/n*100:>9.1f}%  "
              f"{wpnl:>+8.2f} {ppnl:>+8.2f} {wpnl+ppnl:>+10.2f}")
        for k,v in s.items():
            if isinstance(v, (int,float)):
                grand[k] += v

    # Totals
    n = int(grand["n"])
    if n:
        wpnl = grand["p1_win_pnl"] + grand["p2_win_pnl"]
        ppnl = grand["p1_plc_pnl"] + grand["p2_plc_pnl"]
        print(f"  {'TOTAL':<12} {n:>5} {'100%':>7}  "
              f"{grand['p1w']/n*100:>7.1f}%  {grand['p1p']/n*100:>7.1f}%  "
              f"{grand['p2w']/n*100:>7.1f}%  {grand['p2p']/n*100:>7.1f}%  "
              f"{grand['either_w']/n*100:>8.1f}%  {grand['neither_p']/n*100:>9.1f}%  "
              f"{wpnl:>+8.2f} {ppnl:>+8.2f} {wpnl+ppnl:>+10.2f}")

    # Bet-only rows (tier >= 2)
    bet_tiers = [t for t in tier_order if t >= 2 and tier_stats.get(t,{}).get("n",0) > 0]
    if bet_tiers:
        bn = sum(tier_stats[t]["n"] for t in bet_tiers)
        bw = sum(tier_stats[t]["p1w"] for t in bet_tiers)
        bp = sum(tier_stats[t]["p1p"] for t in bet_tiers)
        bew= sum(tier_stats[t]["either_w"] for t in bet_tiers)
        bnp= sum(tier_stats[t]["neither_p"] for t in bet_tiers)
        bwpl = sum(tier_stats[t]["p1_win_pnl"]+tier_stats[t]["p2_win_pnl"] for t in bet_tiers)
        bppl = sum(tier_stats[t]["p1_plc_pnl"]+tier_stats[t]["p2_plc_pnl"] for t in bet_tiers)
        print(f"  {'-'*108}")
        print(f"  {'BET ONLY':<12} {bn:>5} {bn/total_n*100:>6.0f}%  "
              f"{bw/bn*100:>7.1f}%  {bp/bn*100:>7.1f}%  "
              f"{'':>8}  {'':>8}  "
              f"{bew/bn*100:>8.1f}%  {bnp/bn*100:>9.1f}%  "
              f"{bwpl:>+8.2f} {bppl:>+8.2f} {bwpl+bppl:>+10.2f}")
    print()

# ═══════════════════════════════════════════════════════════════════════════════
# RUN ALL FOUR SYSTEMS
# ═══════════════════════════════════════════════════════════════════════════════

# Current system tier order/labels
cur_order  = [TIER_SUPREME, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP]
cur_labels = {
    TIER_SUPREME: "SUPREME",
    TIER_STRONG:  "STRONG",
    TIER_GOOD:    "GOOD",
    TIER_STD:     "STANDARD",
    TIER_SKIP:    "SKIP",
}

# New systems tier order/labels
new_order  = [4, 3, 2, 1, 0, -1]
new_labels = {4:"ELITE", 3:"STRONG", 2:"GOOD", 1:"STANDARD", 0:"WEAK", -1:"SKIP"}

systems = [
    ("A: CURRENT SYSTEM (baseline)",           tier_A_current,      cur_order, cur_labels),
    ("B: SP-FREE SCORE TIERS",                 tier_B_spfree,       new_order, new_labels),
    ("C: MARKET-RELATIVE TIERS",               tier_C_market_relative, new_order, new_labels),
    ("D: COMBINED (SP-free + mkt agreement)",  tier_D_combined,     new_order, new_labels),
]

for sys_name, tier_fn, t_order, t_labels in systems:
    print("╔══════════════════════════════════════════════════════════════════════════╗")
    print(f"║  {sys_name:<72}║")
    print("╚══════════════════════════════════════════════════════════════════════════╝")
    print()
    stats = evaluate_tier_system(tier_fn, sys_name)
    print_tier_results(sys_name, stats, t_order, t_labels)

# ═══════════════════════════════════════════════════════════════════════════════
# DIRECT COMPARISON: bet-only races across all systems
# ═══════════════════════════════════════════════════════════════════════════════

print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  SUMMARY: BET-ONLY RACES COMPARISON (tier >= GOOD/2)                   ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()
print(f"  {'System':<35} {'N':>5} {'P1win%':>8} {'EitherW%':>10} {'NeitherP%':>11} {'Combined':>10}")
print(f"  {'-'*82}")

for sys_name, tier_fn, t_order, t_labels in systems:
    stats = evaluate_tier_system(tier_fn, sys_name)
    bet_tiers = [t for t in t_order if t >= 2]
    bn = sum(stats.get(t,{}).get("n",0) for t in bet_tiers)
    if not bn: continue
    bw  = sum(stats.get(t,{}).get("p1w",0) for t in bet_tiers)
    bew = sum(stats.get(t,{}).get("either_w",0) for t in bet_tiers)
    bnp = sum(stats.get(t,{}).get("neither_p",0) for t in bet_tiers)
    bwpl= sum(stats.get(t,{}).get("p1_win_pnl",0)+stats.get(t,{}).get("p2_win_pnl",0) for t in bet_tiers)
    bppl= sum(stats.get(t,{}).get("p1_plc_pnl",0)+stats.get(t,{}).get("p2_plc_pnl",0) for t in bet_tiers)
    print(f"  {sys_name:<35} {bn:>5} "
          f"{bw/bn*100:>7.1f}%  {bew/bn*100:>9.1f}%  {bnp/bn*100:>10.1f}%  "
          f"{bwpl+bppl:>+10.2f}")

print()

# ═══════════════════════════════════════════════════════════════════════════════
# DEEP DIVE: what does System D's ELITE/STRONG look like?
# ═══════════════════════════════════════════════════════════════════════════════

print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  SYSTEM D DEEP DIVE — ELITE and STRONG race details                    ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()
print(f"  {'Course':20} {'Off':6} {'P1':20} {'SP':>6} {'Pos':>5} {'P2':20} {'SP':>6} {'Pos':>5} {'Tier':8}")
print(f"  {'-'*98}")

for key, card_race in sorted(card_races.items()):
    raw = raw_by_key.get(key)
    if not raw: continue
    tier = tier_D_combined(card_race)
    if tier < 3: continue

    raw_runners = raw.get("runners",[])
    n   = len(raw_runners)
    pos_by = {strip_country(r.get("horse","")): r for r in raw_runners}

    def o(name):
        r = pos_by.get(strip_country(name))
        if not r: return "?", "?"
        try:    p = int(str(r.get("position","")).strip())
        except: p = "?"
        sp = to_float(r.get("sp_dec"))
        return p, f"{sp:.1f}" if sp else "?"

    top1 = card_race.get("top1") or {}
    top2 = card_race.get("top2") or {}
    p1_pos, p1_sp = o(top1.get("horse",""))
    p2_pos, p2_sp = o(top2.get("horse",""))
    t_lbl = new_labels.get(tier,"?")

    win_mark = " W" if p1_pos == 1 else ""
    print(f"  {card_race.get('course','')[:18]:20} {card_race.get('off','')[:5]:6} "
          f"{top1.get('horse','')[:18]:20} {p1_sp:>6} {str(p1_pos)+win_mark:>7} "
          f"{top2.get('horse','')[:18]:20} {p2_sp:>6} {str(p2_pos):>5} {t_lbl:8}")
print()
print("Done.")
