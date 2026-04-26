"""
scratch_bet_logic.py  —  NOT part of the main bot, do not commit
=================================================================
Diagnostic: for each history race, assigns a System C tier then
shows what the current model picked vs mw=0.6 picked, whether
each was correct, and what the current betting logic would have
done (staking, redirects, skips).

Identifies:
  - Which tier x horse-ranking combinations produce the best outcomes
  - Which betting logic decisions are costing money
  - What a revised betting logic should look like

Run from ~/horse_bets_v3:
  python3 scratch_bet_logic.py 2>&1 | tee bet_logic_output.txt
"""

import json, glob, os, sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from predict    import score_runner, SIGNAL_WEIGHTS
from predict_v2 import TIER_LABELS, TIER_SUPREME, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP
from betfair.strategy import (
    pick_stakes, qualifies, should_back_pick1, should_back_pick2,
    get_stake, get_tsr_stake, get_redirect_stake,
    MIN_PICK1_PRICE, MIN_PICK2_PRICE, STAKE_TIERS,
)

# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
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
# HELPERS  (shared with scratch_tier_recal.py)
# ═══════════════════════════════════════════════════════════════════════════════

SP_SIGNALS = {"sp_odds_on", "sp_2_to_4", "sp_4_to_6"}

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
    sc, signals = score_runner(runner)
    sp_pts = sum(SIGNAL_WEIGHTS.get(s,0) for s in signals if s in SP_SIGNALS)
    return sc - sp_pts

def normalise(val, vals, scale=10.0):
    valid = [v for v in vals if v is not None]
    if not valid or len(valid) < 2: return scale/2
    lo, hi = min(valid), max(valid)
    if hi == lo: return scale/2
    return ((val-lo)/(hi-lo))*scale

def stats_score_raw(runner, field_rprs, field_ors, field_tsrs):
    score = 0.0
    rpr = to_float(runner.get("rpr"))
    or_ = to_float(runner.get("or"))
    tsr = to_float(runner.get("tsr"))
    if rpr: score += normalise(rpr, field_rprs, 10.0)
    if or_: score += normalise(or_, field_ors,  10.0)
    if tsr: score += normalise(tsr, field_tsrs,  5.0)
    if rpr and or_ and rpr > or_: score += 2.0
    fd = runner.get("form_detail") or {}
    if isinstance(fd, dict):
        plc4 = fd.get("placed_last_4",0) or 0
        bad  = fd.get("bad_recent",0) or 0
        if plc4 >= 3:   score += 2.0
        elif plc4 >= 2: score += 1.0
        if bad == 0 and runner.get("form",""): score += 1.0
    for f14 in [runner.get("trainer_14d"), runner.get("jockey_14d")]:
        if not isinstance(f14,dict): continue
        ae = f14.get("ae",0) or 0
        runs = f14.get("runs",0) or 0
        if runs >= 3:
            if   ae >= 2.0 and runs >= 5: score += 3
            elif ae >= 1.5 and runs >= 5: score += 2
            elif ae >= 1.0 and runs >= 5: score += 1
    return score

def tier_C_raw(runners, raw):
    """System C tier from raw runners. Returns (tier_int, p1, p2)."""
    n   = len(runners)
    cls = str(raw.get("class","") or "").replace("Class ","").strip()
    if cls in ("1","2"): return -2, None, None   # -2 = hard skip
    if n >= 13:          return -2, None, None
    if n < 2:            return  1, runners[0] if runners else None, None

    field_rprs = [to_float(r.get("rpr")) for r in runners]
    field_ors  = [to_float(r.get("or"))  for r in runners]
    field_tsrs = [to_float(r.get("tsr")) for r in runners]

    stats_scored = sorted(
        [(stats_score_raw(r,field_rprs,field_ors,field_tsrs), i, r)
         for i,r in enumerate(runners)],
        key=lambda x: -x[0]
    )
    stats_rank = {r.get("horse_id","_"+str(i)): rank+1
                  for rank,(_,i,r) in enumerate(stats_scored)}

    mkt_scored = sorted(
        [(to_float(r.get("sp_dec")) or 999, i, r) for i,r in enumerate(runners)],
        key=lambda x: x[0]
    )
    mkt_rank = {r.get("horse_id","_"+str(i)): rank+1
                for rank,(_,i,r) in enumerate(mkt_scored)}

    blended = []
    for ss,_i,r in stats_scored:
        hid = r.get("horse_id","")
        sr  = stats_rank.get(hid, n)
        mr  = mkt_rank.get(hid, n)
        sr_norm = (sr-1)/max(n-1,1)
        mr_norm = (mr-1)/max(n-1,1)
        cs = 0.4*sr_norm + 0.6*mr_norm
        blended.append((cs, sr, mr, ss, r))
    blended.sort(key=lambda x: x[0])

    _,sr1,mr1,ss1,p1 = blended[0]
    _,sr2,mr2,ss2,p2 = blended[1]

    sc1_free = sp_free_score(p1)
    rank_diff  = abs(sr1-mr1)
    both_agree = sr1==1 and mr1==1

    TIER_NAMES = {4:"ELITE",3:"STRONG",2:"GOOD",1:"STANDARD",0:"WEAK",-1:"SKIP",-2:"HARD_SKIP"}

    if both_agree and sc1_free >= 3:        return 4, p1, p2
    if both_agree:                          return 3, p1, p2
    if rank_diff <= 1 and sc1_free >= 3:    return 3, p1, p2
    if rank_diff <= 1:                      return 2, p1, p2
    if rank_diff <= 2 and sc1_free >= 3:    return 2, p1, p2
    if rank_diff <= 3 and sc1_free >= 2:    return 1, p1, p2
    if sc1_free >= 1:                       return 1, p1, p2
    return 0, p1, p2

TIER_C_LABELS = {4:"ELITE",3:"STRONG",2:"GOOD",1:"STANDARD",0:"WEAK",-1:"SKIP",-2:"HARD_SKIP"}

# ═══════════════════════════════════════════════════════════════════════════════
# BUILD RACE RECORDS
# Each record captures: System C tier, current pick, mw0.6 pick, outcomes,
# what the current betting logic would do, actual P&L under current logic.
# ═══════════════════════════════════════════════════════════════════════════════

PROFIT_SNAPSHOT = 56.24   # paper mode starting balance used as proxy profit

records = []

for date, day in sorted(history.items()):
    for rec in day.get("races", []):
        raw = raw_index.get(rec["race_id"])
        if not raw: continue
        runners = raw.get("runners", [])
        if len(runners) < 2: continue

        n   = len(runners)
        ps  = place_spots(n)
        div = place_divisor(n)

        # ── System C tier + mw=0.6 picks ─────────────────────────────────────
        tier_c, ms_p1, ms_p2 = tier_C_raw(runners, raw)

        # ── Current model picks (from history rec, backed by score_runner) ────
        cur_scored = sorted(
            [{**r, "_s": score_runner(r)[0]} for r in runners],
            key=lambda x: -x["_s"]
        )
        cur_p1 = cur_scored[0] if cur_scored else None
        cur_p2 = cur_scored[1] if len(cur_scored) > 1 else None

        # ── Outcome lookup ────────────────────────────────────────────────────
        pos_by_id = {r.get("horse_id",""): r for r in runners}

        def get_pos(r):
            if not r: return None
            try: return int(str(r.get("position","")).strip())
            except: return None

        def get_sp(r):
            if not r: return None
            return to_float(r.get("sp_dec")) or to_float(r.get("bsp"))

        # Current model outcomes (from history rec — ground truth)
        cur_p1_won  = rec.get("a_pos") == 1
        cur_p1_plcd = bool(rec.get("std_a") or rec.get("cons_a"))
        cur_p2_won  = rec.get("b_pos") == 1
        cur_p2_plcd = bool(rec.get("std_b") or rec.get("cons_b"))

        cur_p1_sp = get_sp(cur_p1)
        cur_p2_sp = get_sp(cur_p2)
        cur_score_gap = (cur_p1.get("_s",0) - cur_p2.get("_s",0)) if cur_p2 else 0

        # mw=0.6 outcomes
        ms_p1_pos = get_pos(ms_p1)
        ms_p2_pos = get_pos(ms_p2)
        ms_p1_sp  = get_sp(ms_p1)
        ms_p2_sp  = get_sp(ms_p2)
        ms_p1_won  = ms_p1_pos == 1
        ms_p1_plcd = ms_p1_pos is not None and ms_p1_pos <= ps
        ms_p2_won  = ms_p2_pos == 1 if ms_p2_pos is not None else False
        ms_p2_plcd = ms_p2_pos <= ps if ms_p2_pos is not None else False

        # ── Current betting logic simulation ──────────────────────────────────
        # Reconstruct what the bot would have done using current strategy.py
        # Use current model's tier (from predict_v2 on scored runners)
        from predict_v2 import race_confidence, TIER_LABELS
        raw2 = {**raw, "runners": cur_scored}
        cur_tier, _ = race_confidence(raw2, cur_scored[0]["_s"] if cur_scored else 0)

        # Parse dist_f to float for attrition check
        raw_dist_f = raw.get("dist_f","") or ""
        try:
            dist_f_float = float(str(raw_dist_f).replace("f","").strip())
        except:
            dist_f_float = 0.0

        race_dict = {
            **raw,
            "tier":        cur_tier,
            "tier_label":  TIER_LABELS.get(cur_tier,""),
            "top1":        cur_p1,
            "top2":        cur_p2,
            "all_runners": runners,
            "dist_f":      dist_f_float,
        }

        qualifies_flag = qualifies(race_dict)

        # Get P3 price for two-horse-race check
        cur_p3_sp = get_sp(cur_scored[2]) if len(cur_scored) > 2 else None
        cur_tsr   = cur_tier == TIER_SUPREME

        if qualifies_flag:
            s1, s2 = pick_stakes(
                profit     = PROFIT_SNAPSHOT,
                tsr        = cur_tsr,
                pick1_price = cur_p1_sp,
                pick2_price = cur_p2_sp,
                tier        = cur_tier,
                pick2_score = int(cur_p2.get("_s",0)) if cur_p2 else 0,
                pick3_price = cur_p3_sp,
                score_gap   = int(cur_score_gap),
            )
        else:
            s1, s2 = 0.0, 0.0

        # Actual P&L under current logic
        def win_pnl(sp, stake, won):
            if not sp or stake == 0: return 0.0
            return (sp-1)*stake if won else -stake

        def plc_pnl(sp, stake, plcd, d):
            if not sp or stake == 0 or not d: return 0.0
            ep = (sp-1)/d + 1
            return (ep-1)*stake if plcd else -stake

        cur_p1_win_pnl  = win_pnl(cur_p1_sp, s1, cur_p1_won)
        cur_p2_win_pnl  = win_pnl(cur_p2_sp, s2, cur_p2_won)
        cur_p1_plc_pnl  = plc_pnl(cur_p1_sp, s1, cur_p1_plcd, div)
        cur_p2_plc_pnl  = plc_pnl(cur_p2_sp, s2, cur_p2_won, div)  # using win outcome for P2 place proxy

        records.append({
            "date":         date,
            "race_id":      rec["race_id"],
            "race":         raw,
            "n":            n,
            "ps":           ps,
            "div":          div,
            # System C
            "tier_c":       tier_c,
            "ms_p1":        ms_p1,
            "ms_p2":        ms_p2,
            "ms_p1_sp":     ms_p1_sp,
            "ms_p2_sp":     ms_p2_sp,
            "ms_p1_won":    ms_p1_won,
            "ms_p1_plcd":   ms_p1_plcd,
            "ms_p2_won":    ms_p2_won,
            "ms_p2_plcd":   ms_p2_plcd,
            # Current model
            "cur_tier":     cur_tier,
            "cur_p1":       cur_p1,
            "cur_p2":       cur_p2,
            "cur_p1_sp":    cur_p1_sp,
            "cur_p2_sp":    cur_p2_sp,
            "cur_p1_won":   cur_p1_won,
            "cur_p1_plcd":  cur_p1_plcd,
            "cur_p2_won":   cur_p2_won,
            "cur_p2_plcd":  cur_p2_plcd,
            "cur_score_gap":int(cur_score_gap),
            # Betting logic
            "qualifies":    qualifies_flag,
            "s1":           s1,
            "s2":           s2,
            "cur_tier_lbl": TIER_LABELS.get(cur_tier,""),
            # P&L
            "cur_p1_win_pnl": cur_p1_win_pnl,
            "cur_p2_win_pnl": cur_p2_win_pnl,
            "cur_p1_plc_pnl": cur_p1_plc_pnl,
            "cur_p2_plc_pnl": cur_p2_plc_pnl,
            "cur_combined":   cur_p1_win_pnl+cur_p2_win_pnl+cur_p1_plc_pnl+cur_p2_plc_pnl,
        })

print(f"Built {len(records)} race records")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1: Pick agreement — does current model agree with mw=0.6?
# By System C tier — where do they diverge and who is right?
# ═══════════════════════════════════════════════════════════════════════════════

print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  SECTION 1: PICK AGREEMENT — current vs mw=0.6 by System C tier       ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()

agree_stats = defaultdict(lambda: {
    "n":0,
    "agree":0,"disagree":0,
    "agree_p1w":0,"agree_p1p":0,
    "dis_cur_w":0,"dis_ms_w":0,
    "dis_cur_p":0,"dis_ms_p":0,
})

for r in records:
    tc = r["tier_c"]
    if tc <= -1: continue
    s = agree_stats[tc]
    s["n"] += 1
    cur_name = (r["cur_p1"] or {}).get("horse","")
    ms_name  = (r["ms_p1"]  or {}).get("horse","")
    # Strip country suffix for comparison
    cur_h = cur_name.split(" (")[0].strip().lower()
    ms_h  = ms_name.split(" (")[0].strip().lower()
    if cur_h == ms_h:
        s["agree"] += 1
        s["agree_p1w"] += r["cur_p1_won"]
        s["agree_p1p"] += r["cur_p1_plcd"]
    else:
        s["disagree"] += 1
        s["dis_cur_w"] += r["cur_p1_won"]
        s["dis_ms_w"]  += r["ms_p1_won"]
        s["dis_cur_p"] += r["cur_p1_plcd"]
        s["dis_ms_p"]  += r["ms_p1_plcd"]

print(f"  {'Tier':<10} {'N':>5} {'Agree':>7} {'AgreeW%':>9} {'AgreeP%':>9} "
      f"{'Dis':>5} {'CurW%':>8} {'MSW%':>8} {'CurP%':>8} {'MSP%':>8}")
print(f"  {'-'*82}")
for tc in [4,3,2,1,0]:
    s = agree_stats.get(tc)
    if not s or s["n"]==0: continue
    lbl  = TIER_C_LABELS.get(tc,"?")
    ag   = s["agree"]
    dis  = s["disagree"]
    agw  = s["agree_p1w"]/ag*100 if ag else 0
    agp  = s["agree_p1p"]/ag*100 if ag else 0
    cw   = s["dis_cur_w"]/dis*100 if dis else 0
    mw   = s["dis_ms_w"]/dis*100  if dis else 0
    cp   = s["dis_cur_p"]/dis*100 if dis else 0
    mp   = s["dis_ms_p"]/dis*100  if dis else 0
    print(f"  {lbl:<10} {s['n']:>5} {ag:>7} {agw:>8.1f}%  {agp:>8.1f}%  "
          f"{dis:>5} {cw:>7.1f}%  {mw:>7.1f}%  {cp:>7.1f}%  {mp:>7.1f}%")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2: Betting logic audit — what does the current strategy do by tier?
# ═══════════════════════════════════════════════════════════════════════════════

print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  SECTION 2: CURRENT BETTING LOGIC BY SYSTEM C TIER                     ║")
print("║  What does the bot actually do in each System C tier?                  ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()

logic_stats = defaultdict(lambda: defaultdict(lambda: {
    "n":0,"p1w":0,"p1p":0,"p2w":0,"p2p":0,
    "win_pnl":0.0,"plc_pnl":0.0,
}))

# Decision categories
def decision_label(r):
    if not r["qualifies"]:              return "SKIP (qual)"
    if r["s1"]==0 and r["s2"]==0:       return "SKIP (stakes)"
    if r["s1"]>0  and r["s2"]==0:       return "P1 solo"
    if r["s1"]==0 and r["s2"]>0:        return "P2 redirect"
    return "P1+P2"

for r in records:
    tc  = r["tier_c"]
    dec = decision_label(r)
    s   = logic_stats[tc][dec]
    s["n"]       += 1
    s["p1w"]     += r["cur_p1_won"]
    s["p1p"]     += r["cur_p1_plcd"]
    s["p2w"]     += r["cur_p2_won"]
    s["p2p"]     += r["cur_p2_plcd"]
    s["win_pnl"] += r["cur_p1_win_pnl"]+r["cur_p2_win_pnl"]
    s["plc_pnl"] += r["cur_p1_plc_pnl"]+r["cur_p2_plc_pnl"]

dec_order = ["P1+P2","P1 solo","P2 redirect","SKIP (stakes)","SKIP (qual)"]

for tc in [4,3,2,1,0,-1]:
    lbl = TIER_C_LABELS.get(tc,"?")
    tier_dec = logic_stats.get(tc,{})
    if not tier_dec: continue
    total = sum(s["n"] for s in tier_dec.values())
    print(f"  System C {lbl} ({total} races):")
    print(f"  {'Decision':<16} {'N':>5} {'%':>5} {'P1win%':>8} {'P1plc%':>8} "
          f"{'WinP&L':>9} {'PlcP&L':>9} {'Combined':>10}")
    print(f"  {'-'*74}")
    for dec in dec_order:
        s = tier_dec.get(dec)
        if not s or s["n"]==0: continue
        n = s["n"]
        comb = s["win_pnl"]+s["plc_pnl"]
        print(f"  {dec:<16} {n:>5} {n/total*100:>4.0f}%  "
              f"{s['p1w']/n*100:>7.1f}%  {s['p1p']/n*100:>7.1f}%  "
              f"{s['win_pnl']:>+9.2f} {s['plc_pnl']:>+9.2f} {comb:>+10.2f}")
    print()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3: Expensive decisions — which logic choices are costing the most?
# ═══════════════════════════════════════════════════════════════════════════════

print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  SECTION 3: EXPENSIVE LOGIC DECISIONS — ranked by P&L cost             ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()

# Specific logic failures:
expensive = []

for r in records:
    tc  = r["tier_c"]
    dec = decision_label(r)
    # Case 1: ELITE/STRONG skipped or redirected when P1 would have won
    if tc in (4,3) and r["cur_p1_won"] and r["s1"] == 0:
        expensive.append({
            "type": f"SysC {TIER_C_LABELS[tc]}: P1 skipped/redirected but P1 WON",
            "tc": tc, "rec": r, "cost": -(r["cur_p1_sp"]-1)*2 if r["cur_p1_sp"] else 0
        })
    # Case 2: SUPREME elevated stake on a loss
    if r["cur_tier"] == TIER_SUPREME and not r["cur_p1_won"] and r["s1"] > 2:
        expensive.append({
            "type": "SUPREME elevated stake LOSS",
            "tc": tc, "rec": r, "cost": -r["s1"]
        })
    # Case 3: P2 redirect when P1 would have won at better price
    if dec == "P2 redirect" and r["cur_p1_won"]:
        cost = -(r["cur_p1_sp"]-1)*2 if r["cur_p1_sp"] else 0
        expensive.append({
            "type": f"P2 redirect but P1 WON ({r['cur_tier_lbl'].strip()})",
            "tc": tc, "rec": r, "cost": cost
        })
    # Case 4: GOOD tier capped at £2 when P1 won at good price
    if tc == 2 and r["cur_p1_won"] and r["cur_p1_sp"] and r["cur_p1_sp"] > 4:
        potential = (r["cur_p1_sp"]-1)*4 - (r["cur_p1_sp"]-1)*2
        expensive.append({
            "type": "GOOD tier cap missed big winner",
            "tc": tc, "rec": r, "cost": -potential
        })

# Summarise by type
by_type = defaultdict(lambda: {"n":0,"total_cost":0.0,"examples":[]})
for e in expensive:
    s = by_type[e["type"]]
    s["n"]          += 1
    s["total_cost"] += e["cost"]
    if len(s["examples"]) < 3:
        r = e["rec"]
        p1 = (r["cur_p1"] or {}).get("horse","?")
        s["examples"].append(
            f"{r['date']} {r['race'].get('course','?')} {r['race'].get('off','')} "
            f"P1={p1[:16]} SP={r['cur_p1_sp']:.1f if r['cur_p1_sp'] else '?'} "
            f"tier_c={TIER_C_LABELS.get(r['tier_c'],'?')}"
        )

print(f"  {'Decision type':<45} {'N':>5} {'Total cost':>12}")
print(f"  {'-'*65}")
for typ, s in sorted(by_type.items(), key=lambda x: x[1]["total_cost"]):
    print(f"  {typ:<45} {s['n']:>5} {s['total_cost']:>+12.2f}")
    for ex in s["examples"]:
        print(f"    → {ex}")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4: Odds-on P1 analysis by System C tier
# Current logic: SUPREME backs odds-on, STRONG skips, GOOD redirects to P2
# Is this correct?
# ═══════════════════════════════════════════════════════════════════════════════

print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  SECTION 4: ODDS-ON P1 ANALYSIS BY SYSTEM C TIER                      ║")
print("║  Current: SUPREME backs, STRONG skips, GOOD redirects if P2 strong    ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()

odds_on_stats = defaultdict(lambda: {"n":0,"p1w":0,"p1p":0,"p2w":0,"p2p":0,
                                       "p1_pnl":0.0,"p2_pnl":0.0})
not_odds_on   = defaultdict(lambda: {"n":0,"p1w":0,"p1p":0,"p1_pnl":0.0})

for r in records:
    tc = r["tier_c"]
    if tc <= 0: continue
    sp = r["cur_p1_sp"]
    if not sp: continue
    div = r["div"]
    if sp < 2.0:
        s = odds_on_stats[tc]
        s["n"]     += 1
        s["p1w"]   += r["cur_p1_won"]
        s["p1p"]   += r["cur_p1_plcd"]
        s["p2w"]   += r["cur_p2_won"]
        s["p2p"]   += r["cur_p2_plcd"]
        s["p1_pnl"]+= (sp-1)*2 if r["cur_p1_won"] else -2
        if r["cur_p2_sp"] and div:
            s["p2_pnl"] += (r["cur_p2_sp"]-1)*2 if r["cur_p2_won"] else -2
    else:
        s = not_odds_on[tc]
        s["n"]     += 1
        s["p1w"]   += r["cur_p1_won"]
        s["p1p"]   += r["cur_p1_plcd"]
        s["p1_pnl"]+= (sp-1)*2 if r["cur_p1_won"] else -2

print(f"  {'Tier':<10} {'Sub':>12} {'N':>5} {'P1win%':>8} {'P1plc%':>8} "
      f"{'P1 P&L':>9} {'P2win%':>8} {'P2plc%':>8} {'Verdict':>14}")
print(f"  {'-'*82}")
for tc in [4,3,2,1]:
    lbl = TIER_C_LABELS.get(tc,"?")
    oo  = odds_on_stats.get(tc)
    noo = not_odds_on.get(tc)
    for sub, s in [("Odds-on P1", oo), ("Not odds-on", noo)]:
        if not s or s["n"]==0: continue
        n = s["n"]
        p1w = s["p1w"]/n*100
        p1p = s["p1p"]/n*100
        p1pnl = s.get("p1_pnl",0)
        p2w = s.get("p2w",0)/n*100
        p2p = s.get("p2p",0)/n*100
        # Verdict: should we back odds-on P1?
        verdict = ""
        if sub == "Odds-on P1":
            if p1w >= 50 and p1pnl >= 0:   verdict = "BACK ✓"
            elif p1w >= 50 and p1pnl < 0:  verdict = "BACK (neg ROI)"
            elif p1w >= 40:                 verdict = "MARGINAL"
            else:                           verdict = "SKIP ✗"
        print(f"  {lbl:<10} {sub:>12} {n:>5} {p1w:>7.1f}%  {p1p:>7.1f}%  "
              f"{p1pnl:>+9.2f} {p2w:>7.1f}%  {p2p:>7.1f}%  {verdict:>14}")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5: Score gap + market disagreement redirect — is it working?
# Current: gap<3 AND P2 shorter than P1 → redirect to P2
# ═══════════════════════════════════════════════════════════════════════════════

print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  SECTION 5: SCORE GAP + MARKET REDIRECT — is the rule working?         ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()

redirect_stats = defaultdict(lambda: {"n":0,"p1w":0,"p2w":0,"p1p":0,"p2p":0,
                                        "redirect_pnl":0.0,"p1_pnl":0.0})

for r in records:
    tc  = r["tier_c"]
    gap = r["cur_score_gap"]
    p1sp = r["cur_p1_sp"]
    p2sp = r["cur_p2_sp"]
    if not p1sp or not p2sp: continue
    # Would the redirect fire?
    would_redirect = gap < 3 and p2sp < p1sp
    key = (tc, "redirect" if would_redirect else "normal")
    s = redirect_stats[key]
    s["n"]    += 1
    s["p1w"]  += r["cur_p1_won"]
    s["p2w"]  += r["cur_p2_won"]
    s["p1p"]  += r["cur_p1_plcd"]
    s["p2p"]  += r["cur_p2_plcd"]
    s["redirect_pnl"] += (p2sp-1)*2 if r["cur_p2_won"] else -2  # P2 win pnl
    s["p1_pnl"]       += (p1sp-1)*2 if r["cur_p1_won"] else -2  # P1 win pnl (counterfactual)

print(f"  {'Tier':<10} {'Type':<12} {'N':>5} {'P1win%':>8} {'P2win%':>8} "
      f"{'P2(redirect)P&L':>17} {'P1(counterfact)P&L':>20} {'Better?':>9}")
print(f"  {'-'*86}")
for tc in [4,3,2,1,0]:
    lbl = TIER_C_LABELS.get(tc,"?")
    for rtype in ["redirect","normal"]:
        s = redirect_stats.get((tc,rtype))
        if not s or s["n"]==0: continue
        n = s["n"]
        better = "P2 ✓" if s["redirect_pnl"] > s["p1_pnl"] else "P1 ✓"
        print(f"  {lbl:<10} {rtype:<12} {n:>5} "
              f"{s['p1w']/n*100:>7.1f}%  {s['p2w']/n*100:>7.1f}%  "
              f"{s['redirect_pnl']:>+17.2f} {s['p1_pnl']:>+20.2f} {better:>9}")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6: Revised betting logic proposal
# Based on what System C + data tells us should change
# ═══════════════════════════════════════════════════════════════════════════════

print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  SECTION 6: REVISED BETTING LOGIC PROPOSAL                             ║")
print("║  What the data suggests the logic should look like                     ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()

# Simulate revised logic: flat £2 each on P1+P2 win, no tier-based skips except
# SKIP/HARD_SKIP. Use System C tier to decide whether to add place bets.
FLAT_STAKE = 2.0
revised_stats = defaultdict(lambda: {
    "n":0,"p1w":0,"p2w":0,"p1p":0,"p2p":0,
    "either_w":0,"neither_p":0,
    "win_pnl":0.0,"plc_pnl":0.0,
})

for r in records:
    tc = r["tier_c"]
    if tc <= -1: continue   # hard skip (Class 1/2, 13+ runners)
    if tc == 0:  continue   # WEAK — skip

    div = r["div"]
    s   = revised_stats[tc]
    s["n"]       += 1
    s["p1w"]     += r["ms_p1_won"]
    s["p2w"]     += r["ms_p2_won"]
    s["p1p"]     += r["ms_p1_plcd"]
    s["p2p"]     += r["ms_p2_plcd"]
    s["either_w"]+= r["ms_p1_won"] or r["ms_p2_won"]
    s["neither_p"]+= not (r["ms_p1_plcd"] or r["ms_p2_plcd"])

    # Win bets: P1+P2 flat
    s["win_pnl"] += (r["ms_p1_sp"]-1)*FLAT_STAKE if (r["ms_p1_won"] and r["ms_p1_sp"]) else (-FLAT_STAKE if r["ms_p1_sp"] else 0)
    s["win_pnl"] += (r["ms_p2_sp"]-1)*FLAT_STAKE if (r["ms_p2_won"] and r["ms_p2_sp"]) else (-FLAT_STAKE if r["ms_p2_sp"] else 0)

    # Place bets: only on ELITE/STRONG (highest confidence tiers)
    if tc >= 3 and div and r["ms_p1_sp"]:
        ep = (r["ms_p1_sp"]-1)/div + 1
        s["plc_pnl"] += (ep-1)*FLAT_STAKE if r["ms_p1_plcd"] else -FLAT_STAKE
    if tc >= 3 and div and r["ms_p2_sp"]:
        ep = (r["ms_p2_sp"]-1)/div + 1
        s["plc_pnl"] += (ep-1)*FLAT_STAKE if r["ms_p2_plcd"] else -FLAT_STAKE

print(f"  Revised logic: mw=0.6 picks, flat £2 each, place bets ELITE+STRONG only")
print(f"  {'Tier':<10} {'N':>5} {'P1win%':>8} {'P2win%':>8} {'EitherW%':>10} "
      f"{'NeitherP%':>11} {'WinP&L':>9} {'PlcP&L':>9} {'Combined':>10}")
print(f"  {'-'*88}")
for tc in [4,3,2,1]:
    s = revised_stats.get(tc)
    if not s or s["n"]==0: continue
    n = s["n"]
    lbl = TIER_C_LABELS.get(tc,"?")
    comb = s["win_pnl"]+s["plc_pnl"]
    print(f"  {lbl:<10} {n:>5} "
          f"{s['p1w']/n*100:>7.1f}%  {s['p2w']/n*100:>7.1f}%  "
          f"{s['either_w']/n*100:>9.1f}%  {s['neither_p']/n*100:>10.1f}%  "
          f"{s['win_pnl']:>+9.2f} {s['plc_pnl']:>+9.2f} {comb:>+10.2f}")

total_n   = sum(s["n"]       for s in revised_stats.values())
total_wp  = sum(s["win_pnl"] for s in revised_stats.values())
total_pp  = sum(s["plc_pnl"] for s in revised_stats.values())
total_p1w = sum(s["p1w"]     for s in revised_stats.values())
total_ew  = sum(s["either_w"]for s in revised_stats.values())
total_np  = sum(s["neither_p"]for s in revised_stats.values())
print(f"  {'TOTAL':<10} {total_n:>5} "
      f"{total_p1w/total_n*100:>7.1f}%  {'':>8}  "
      f"{total_ew/total_n*100:>9.1f}%  {total_np/total_n*100:>10.1f}%  "
      f"{total_wp:>+9.2f} {total_pp:>+9.2f} {total_wp+total_pp:>+10.2f}")

print()
print("Done.")
