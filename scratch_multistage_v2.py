"""
scratch_multistage_v2.py  —  NOT part of the main bot, do not commit
=====================================================================
Card-sourced multistage ranking test. Four sections:

  1. CARD MULTISTAGE  — score from card all_runners (rpr/ofr/ts/form),
     join to raw results for outcomes. Sweep market_weight 0.0-1.0.
     Includes place bet analysis using standard Betfair place terms.

  2. VS CURRENT MODEL  — same card races, compare multistage P1 vs
     card top1 (current model pick). Win rate, place rate, flat P&L.
     Includes place bets for both.

  3. RPR SUBSET  — split full history (17 days) into races where RPR
     was available in the raw vs absent. Compare ranking success rate,
     winner-in-top-2, and place rates. Shows what RPR adds.

  4. PLACE BET DEEP DIVE  — dedicated analysis of place bet EV across
     all three datasets (card multistage, current model, history full).
     Betfair standard place terms: <=4 runners = no place, 5-7 = 2 places,
     8-11 = 3 places, 12-15 = 4 places, 16+ = 4 places (handicaps).

Run from ~/horse_bets_v3:
  python3 scratch_multistage_v2.py 2>&1 | tee multistage_v2_output.txt
"""

import json, glob, os, sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from predict    import score_runner
from predict_v2 import race_confidence, TIER_LABELS

# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════════

history = {}
for fp in sorted(glob.glob("data/history/*.json")):
    with open(fp) as f: d = json.load(f)
    history[d["date"]] = d

raw_index = {}        # race_id -> race
raw_by_key = {}       # (course, off_dt) -> race
for fp in sorted(glob.glob("data/raw/*.json")):
    with open(fp) as f: d = json.load(f)
    date_str = os.path.basename(fp).replace(".json","")
    for race in (d.get("results") or d.get("races") or []):
        race["_date"] = date_str
        raw_index[race["race_id"]] = race
        key = (race.get("course",""), race.get("off_dt", race.get("off","")))
        raw_by_key[key] = race

cards = {}            # date -> list of card races
for fp in sorted(glob.glob("data/cards/2026-*.json")):
    date_str = os.path.basename(fp).replace(".json","")
    with open(fp) as f: d = json.load(f)
    cards[date_str] = d.get("races") or []

print(f"Loaded {len(history)} days of history | "
      f"{len(raw_index)} raw races | "
      f"cards for {sorted(cards.keys())}")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

NULL_VALS = {"", "-", "--", "None", "none"}

def to_float(v):
    if v is None: return None
    sv = str(v).strip()
    if sv in NULL_VALS: return None
    try:
        f = float(sv)
        return f if f > 0 else None
    except: return None

def place_spots_betfair(n, is_handicap=False):
    """Standard Betfair place terms (not conservative)."""
    if n <= 4:  return 1   # win only (no place market)
    if n <= 7:  return 2
    if n <= 11: return 3
    return 4               # 12+ runners = 4 places

def place_divisor(n):
    if n <= 4:  return None
    if n <= 7:  return 4.0
    if n <= 11: return 5.0
    return 6.0

def going_family(going):
    g = (going or "").lower()
    if "heavy" in g:           return "heavy"
    if "soft" in g:            return "soft"
    if "good to soft" in g:    return "gd_soft"
    if "good to firm" in g:    return "gd_firm"
    if "good" in g:            return "good"
    if "firm" in g:            return "firm"
    if "standard" in g:        return "aw"
    return "other"

def get_form(d):
    if not isinstance(d, dict): return 0.0, 0.0, 0
    return d.get("win_pct",0) or 0, d.get("ae",0) or 0, d.get("runs",0) or 0

def strip_country(name):
    return (name or "").split(" (")[0].strip().lower()

AE_BINS = [(2.0,5,3),(1.5,5,2),(1.0,5,1)]

def ae_pts(win_pct, ae, runs):
    if runs < 3: return 0
    pts = 0
    for min_ae, min_runs, p in AE_BINS:
        if ae >= min_ae and runs >= min_runs:
            pts = p; break
    if win_pct >= 0.30: pts += 1
    return pts

def normalise(val, vals, scale=10.0):
    valid = [v for v in vals if v is not None]
    if not valid or len(valid) < 2: return scale / 2
    lo, hi = min(valid), max(valid)
    if hi == lo: return scale / 2
    return ((val - lo) / (hi - lo)) * scale

# ═══════════════════════════════════════════════════════════════════════════════
# STATS SCORER  (card runner format — uses rpr/ofr/ts/placed_last_4/bad_recent)
# ═══════════════════════════════════════════════════════════════════════════════

def stats_score_card(runner, field_rprs, field_ors, field_tsrs, going):
    score = 0.0
    rpr = to_float(runner.get("rpr"))
    or_ = to_float(runner.get("ofr") or runner.get("or"))
    ts  = to_float(runner.get("ts")  or runner.get("tsr"))

    if rpr: score += normalise(rpr, field_rprs, 10.0)
    if or_: score += normalise(or_, field_ors,  10.0)
    if ts:  score += normalise(ts,  field_tsrs,  5.0)
    if rpr and or_ and rpr > or_: score += 2.0

    # Use pre-computed form flags from card
    fd = runner.get("form_detail") or {}
    if isinstance(fd, dict):
        plc4 = fd.get("placed_last_4", 0) or 0
        bad  = fd.get("bad_recent",    0) or 0
        if plc4 >= 3:   score += 2.0
        elif plc4 >= 2: score += 1.0
        if bad == 0 and runner.get("form",""):
            score += 1.0

    # Trainer / jockey
    t_wp, t_ae, t_runs = get_form(runner.get("trainer_14d"))
    j_wp, j_ae, j_runs = get_form(runner.get("jockey_14d"))
    score += ae_pts(t_wp, t_ae, t_runs)
    score += ae_pts(j_wp, j_ae, j_runs)

    # Going
    gf = going_family(going)
    if gf in ("good","aw"): score += 1.0
    if gf == "heavy":       score -= 2.0

    return score

def rank_card_runners(runners, going, market_weight):
    """Rank card runners by blended stats+market score."""
    n = len(runners)
    if n < 2: return runners

    field_rprs = [to_float(r.get("rpr")) for r in runners]
    field_ors  = [to_float(r.get("ofr") or r.get("or")) for r in runners]
    field_tsrs = [to_float(r.get("ts")  or r.get("tsr")) for r in runners]

    # Stats scores and ranks
    stats = [(stats_score_card(r, field_rprs, field_ors, field_tsrs, going), r)
             for r in runners]
    stats.sort(key=lambda x: -x[0])
    stats_rank = {r.get("horse_id",""):i+1 for i,(_,r) in enumerate(stats)}

    # Market ranks from sp_dec (pre-race odds from card)
    mkt = [(to_float(r.get("sp_dec")) or 999, r) for r in runners]
    mkt.sort(key=lambda x: x[0])
    mkt_rank = {r.get("horse_id",""):i+1 for i,(_,r) in enumerate(mkt)}

    # Combined
    combined = []
    for s_score, r in stats:
        hid = r.get("horse_id","")
        sr  = stats_rank.get(hid, n)
        mr  = mkt_rank.get(hid, n)
        sr_norm = (sr-1) / max(n-1,1)
        mr_norm = (mr-1) / max(n-1,1)
        cs = (1-market_weight)*sr_norm + market_weight*mr_norm
        combined.append((cs, sr, mr, s_score, r))

    combined.sort(key=lambda x: x[0])
    result = []
    for rank,(cs,sr,mr,ss,r) in enumerate(combined):
        result.append({**r,
            "_combined_rank": rank+1,
            "_stats_rank":    sr,
            "_market_rank":   mr,
            "_stats_score":   ss,
        })
    return result

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1: CARD MULTISTAGE — market weight sweep + place bets
# ═══════════════════════════════════════════════════════════════════════════════

print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  SECTION 1: CARD MULTISTAGE — market weight sweep                       ║")
print("║  Score from card all_runners (rpr/ofr/ts/form), outcomes from raw       ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()

def evaluate_card(market_weight, agree_filter=False, max_mkt_rank=None):
    total=p1w=p2w=p1p=p2p=skipped=0
    p1_win_pnl=p2_win_pnl=p1_plc_pnl=p2_plc_pnl=0.0
    gap_stats = defaultdict(lambda:{"n":0,"p1w":0,"p1p":0,"p1_plc_ev":0.0})

    for date, card_races in sorted(cards.items()):
        for card_race in card_races:
            # Join card to raw via (course, off_dt)
            key = (card_race.get("course",""),
                   card_race.get("off_dt", card_race.get("off","")))
            raw = raw_by_key.get(key)
            if not raw: continue

            runners = card_race.get("all_runners", [])
            if len(runners) < 2: continue

            ranked = rank_card_runners(runners, card_race.get("going",""), market_weight)
            p1 = ranked[0]
            p2 = ranked[1]

            if agree_filter and p1["_stats_rank"] != 1:
                skipped += 1; continue
            if max_mkt_rank and p1["_market_rank"] > max_mkt_rank:
                skipped += 1; continue

            # Outcomes from raw results
            raw_runners = raw.get("runners", [])
            n = len(raw_runners)
            ps  = place_spots_betfair(n)
            div = place_divisor(n)

            pos_by_horse = {strip_country(r.get("horse","")): r
                            for r in raw_runners}

            def outcome(horse_name):
                r = pos_by_horse.get(strip_country(horse_name))
                if not r: return None, None
                try: p = int(str(r.get("position","")).strip())
                except: return None, None
                sp = to_float(r.get("sp_dec"))
                return p, sp

            p1_pos, p1_sp = outcome(p1.get("horse",""))
            p2_pos, p2_sp = outcome(p2.get("horse",""))

            if p1_pos is None: continue

            p1_won  = p1_pos == 1
            p2_won  = p2_pos == 1 if p2_pos else False
            p1_plcd = p1_pos <= ps
            p2_plcd = (p2_pos <= ps) if p2_pos else False

            # Rank agreement bucket
            rd = abs(p1["_stats_rank"] - p1["_market_rank"])
            gb = "agree" if rd==0 else ("differ_1" if rd==1 else
                 ("differ_2-3" if rd<=3 else "differ_4+"))
            gap_stats[gb]["n"]   += 1
            gap_stats[gb]["p1w"] += p1_won
            gap_stats[gb]["p1p"] += p1_plcd

            # Win P&L (£2 flat)
            if p1_sp:
                p1_win_pnl += (p1_sp-1)*2 if p1_won else -2
            if p2_sp:
                p2_win_pnl += (p2_sp-1)*2 if p2_won else -2

            # Place P&L (£2 flat, estimated Betfair place odds)
            if p1_sp and div:
                est_plc = (p1_sp-1)/div + 1
                p1_plc_pnl += (est_plc-1)*2 if p1_plcd else -2
            if p2_sp and div:
                est_plc = (p2_sp-1)/div + 1
                p2_plc_pnl += (est_plc-1)*2 if p2_plcd else -2

            total  += 1
            p1w    += p1_won
            p2w    += p2_won
            p1p    += p1_plcd
            p2p    += p2_plcd

    return dict(total=total, skipped=skipped,
                p1w=p1w, p2w=p2w, p1p=p1p, p2p=p2p,
                p1_win_pnl=p1_win_pnl, p2_win_pnl=p2_win_pnl,
                p1_plc_pnl=p1_plc_pnl, p2_plc_pnl=p2_plc_pnl,
                gap_stats=gap_stats)

# Market weight sweep
print(f"  {'MktWt':>6} {'N':>5} {'P1win%':>8} {'P1plc%':>8} "
      f"{'P1WinP&L':>10} {'P1PlcP&L':>10} {'P2win%':>8} {'P2plc%':>8}")
print(f"  {'-'*74}")

sweep = {}
for mw_int in range(0,11):
    mw = mw_int/10
    r  = evaluate_card(mw)
    sweep[mw] = r
    n = r["total"]
    if n == 0: continue
    print(f"  {mw:>6.1f} {n:>5} "
          f"{r['p1w']/n*100:>7.1f}% "
          f"{r['p1p']/n*100:>7.1f}% "
          f"{r['p1_win_pnl']:>+10.2f} "
          f"{r['p1_plc_pnl']:>+10.2f} "
          f"{r['p2w']/n*100:>7.1f}% "
          f"{r['p2p']/n*100:>7.1f}%")
print()

# Rank agreement at mw=0.5
print(f"  Rank agreement breakdown (mw=0.5):")
print(f"  {'Agreement':14} {'N':>5} {'P1win%':>8} {'P1plc%':>8}")
r5 = sweep[0.5]
for gb in ["agree","differ_1","differ_2-3","differ_4+"]:
    s = r5["gap_stats"].get(gb)
    if s and s["n"]:
        print(f"  {gb:14} {s['n']:>5} "
              f"{s['p1w']/s['n']*100:>7.1f}%  "
              f"{s['p1p']/s['n']*100:>7.1f}%")
print()

# Agree filter sweep
print(f"  Agree filter (stats P1 == market P1):")
print(f"  {'MktWt':>6} {'N':>5} {'Skip':>5} {'P1win%':>8} {'P1plc%':>8} "
      f"{'WinP&L':>10} {'PlcP&L':>10}")
print(f"  {'-'*60}")
for mw_int in [0,3,5,7,10]:
    mw = mw_int/10
    r  = evaluate_card(mw, agree_filter=True)
    n  = r["total"]
    if n == 0: continue
    print(f"  {mw:>6.1f} {n:>5} {r['skipped']:>5} "
          f"{r['p1w']/n*100:>7.1f}%  "
          f"{r['p1p']/n*100:>7.1f}%  "
          f"{r['p1_win_pnl']:>+10.2f} "
          f"{r['p1_plc_pnl']:>+10.2f}")
print()

# Market rank cap (pure stats)
print(f"  Market rank cap (mw=0.0, only bet if stats P1 is top-N in market):")
print(f"  {'Cap':>6} {'N':>5} {'Skip':>5} {'P1win%':>8} {'P1plc%':>8} "
      f"{'WinP&L':>10} {'PlcP&L':>10}")
print(f"  {'-'*60}")
for cap in [1,2,3,4,5,None]:
    r = evaluate_card(0.0, max_mkt_rank=cap)
    n = r["total"]
    lbl = str(cap) if cap else "all"
    if n == 0: continue
    print(f"  {lbl:>6} {n:>5} {r['skipped']:>5} "
          f"{r['p1w']/n*100:>7.1f}%  "
          f"{r['p1p']/n*100:>7.1f}%  "
          f"{r['p1_win_pnl']:>+10.2f} "
          f"{r['p1_plc_pnl']:>+10.2f}")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1B: FINE-GRAINED SWEEP — 0.05 steps from 0.30 to 0.70
# Combined P1+P2 outcomes compared against current model baseline
# ═══════════════════════════════════════════════════════════════════════════════

print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  SECTION 1B: FINE-GRAINED SWEEP — 0.05 steps, mw 0.30 to 0.70         ║")
print("║  Combined P1+P2: £2 win + £2 place per pick. Current model as baseline ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()

# Current model baseline — replay over same matched races
cur_base = {
    "n":0,
    "p1w":0,"p2w":0,"p1p":0,"p2p":0,
    "p1_win_pnl":0.0,"p2_win_pnl":0.0,
    "p1_plc_pnl":0.0,"p2_plc_pnl":0.0,
    "either_won":0,"either_plcd":0,"both_plcd":0,"neither_plcd":0,
}

for date, card_races in sorted(cards.items()):
    for card_race in card_races:
        key = (card_race.get("course",""),
               card_race.get("off_dt", card_race.get("off","")))
        raw = raw_by_key.get(key)
        if not raw: continue
        runners     = card_race.get("all_runners",[])
        raw_runners = raw.get("runners",[])
        if len(runners) < 2 or len(raw_runners) < 2: continue

        n   = len(raw_runners)
        ps  = place_spots_betfair(n)
        div = place_divisor(n)
        pos_by_horse = {strip_country(r.get("horse","")): r for r in raw_runners}

        def _out(name):
            r = pos_by_horse.get(strip_country(name))
            if not r: return None, None
            try:    pos = int(str(r.get("position","")).strip())
            except: return None, None
            return pos, to_float(r.get("sp_dec"))

        top1 = card_race.get("top1") or {}
        top2 = card_race.get("top2") or {}
        cp1_pos, cp1_sp = _out(top1.get("horse",""))
        cp2_pos, cp2_sp = _out(top2.get("horse",""))
        if cp1_pos is None: continue

        cp1_won  = cp1_pos == 1
        cp1_plcd = cp1_pos <= ps
        cp2_won  = cp2_pos == 1  if cp2_pos is not None else False
        cp2_plcd = cp2_pos <= ps if cp2_pos is not None else False

        cur_base["n"]           += 1
        cur_base["p1w"]         += cp1_won
        cur_base["p2w"]         += cp2_won
        cur_base["p1p"]         += cp1_plcd
        cur_base["p2p"]         += cp2_plcd
        cur_base["either_won"]  += cp1_won or cp2_won
        cur_base["either_plcd"] += cp1_plcd or cp2_plcd
        cur_base["both_plcd"]   += cp1_plcd and cp2_plcd
        cur_base["neither_plcd"]+= not (cp1_plcd or cp2_plcd)
        if cp1_sp:
            cur_base["p1_win_pnl"] += (cp1_sp-1)*2 if cp1_won else -2
            if div: cur_base["p1_plc_pnl"] += ((cp1_sp-1)/div)*2 if cp1_plcd else -2
        if cp2_sp:
            cur_base["p2_win_pnl"] += (cp2_sp-1)*2 if cp2_won else -2
            if div: cur_base["p2_plc_pnl"] += ((cp2_sp-1)/div)*2 if cp2_plcd else -2

# Fine sweep function — returns combined stats per race for a given mw
def fine_sweep(market_weight):
    res = {
        "n":0,
        "p1w":0,"p2w":0,"p1p":0,"p2p":0,
        "p1_win_pnl":0.0,"p2_win_pnl":0.0,
        "p1_plc_pnl":0.0,"p2_plc_pnl":0.0,
        "either_won":0,"either_plcd":0,"both_plcd":0,"neither_plcd":0,
    }
    for date, card_races in sorted(cards.items()):
        for card_race in card_races:
            key = (card_race.get("course",""),
                   card_race.get("off_dt", card_race.get("off","")))
            raw = raw_by_key.get(key)
            if not raw: continue
            runners     = card_race.get("all_runners",[])
            raw_runners = raw.get("runners",[])
            if len(runners) < 2 or len(raw_runners) < 2: continue

            n   = len(raw_runners)
            ps  = place_spots_betfair(n)
            div = place_divisor(n)
            pos_by_horse = {strip_country(r.get("horse","")): r for r in raw_runners}

            def _o(name):
                r = pos_by_horse.get(strip_country(name))
                if not r: return None, None
                try:    pos = int(str(r.get("position","")).strip())
                except: return None, None
                return pos, to_float(r.get("sp_dec"))

            ranked = rank_card_runners(runners, card_race.get("going",""), market_weight)
            p1, p2 = ranked[0], ranked[1]
            p1_pos, p1_sp = _o(p1.get("horse",""))
            p2_pos, p2_sp = _o(p2.get("horse",""))
            if p1_pos is None: continue

            p1_won  = p1_pos == 1
            p1_plcd = p1_pos <= ps
            p2_won  = p2_pos == 1  if p2_pos is not None else False
            p2_plcd = p2_pos <= ps if p2_pos is not None else False

            res["n"]            += 1
            res["p1w"]          += p1_won
            res["p2w"]          += p2_won
            res["p1p"]          += p1_plcd
            res["p2p"]          += p2_plcd
            res["either_won"]   += p1_won or p2_won
            res["either_plcd"]  += p1_plcd or p2_plcd
            res["both_plcd"]    += p1_plcd and p2_plcd
            res["neither_plcd"] += not (p1_plcd or p2_plcd)
            if p1_sp:
                res["p1_win_pnl"] += (p1_sp-1)*2 if p1_won else -2
                if div: res["p1_plc_pnl"] += ((p1_sp-1)/div)*2 if p1_plcd else -2
            if p2_sp:
                res["p2_win_pnl"] += (p2_sp-1)*2 if p2_won else -2
                if div: res["p2_plc_pnl"] += ((p2_sp-1)/div)*2 if p2_plcd else -2
    return res

# Print header
print(f"  {'MktWt':>7} {'N':>5} {'P1win%':>8} {'P2win%':>8} "
      f"{'P1plc%':>8} {'P2plc%':>8} "
      f"{'EitherWon%':>11} {'EitherPlcd%':>12} {'NeitherPlcd%':>13} "
      f"{'WinP&L':>9} {'PlcP&L':>9} {'Combined':>10}")
print(f"  {'-'*112}")

# Current model baseline row
cb = cur_base
cn = cb["n"]
if cn:
    comb_win = cb["p1_win_pnl"] + cb["p2_win_pnl"]
    comb_plc = cb["p1_plc_pnl"] + cb["p2_plc_pnl"]
    print(f"  {'CURRENT':>7} {cn:>5} "
          f"{cb['p1w']/cn*100:>7.1f}%  {cb['p2w']/cn*100:>7.1f}%  "
          f"{cb['p1p']/cn*100:>7.1f}%  {cb['p2p']/cn*100:>7.1f}%  "
          f"{cb['either_won']/cn*100:>10.1f}%  {cb['either_plcd']/cn*100:>11.1f}%  "
          f"{cb['neither_plcd']/cn*100:>12.1f}%  "
          f"{comb_win:>+9.2f} {comb_plc:>+9.2f} {comb_win+comb_plc:>+10.2f}")
    print(f"  {'-'*112}")

# Fine sweep 0.30 to 0.70 in 0.05 steps
best_combined = None
best_mw = None
fine_results = {}

for mw_step in range(30, 75, 5):
    mw = mw_step / 100
    r  = fine_sweep(mw)
    fine_results[mw] = r
    n  = r["n"]
    if not n: continue
    comb_win = r["p1_win_pnl"] + r["p2_win_pnl"]
    comb_plc = r["p1_plc_pnl"] + r["p2_plc_pnl"]
    combined = comb_win + comb_plc
    if best_combined is None or combined > best_combined:
        best_combined = combined
        best_mw = mw

    # Mark rows that beat current on combined P&L
    beat = " *" if cn and (comb_win+comb_plc) > (cur_base["p1_win_pnl"]+cur_base["p2_win_pnl"]+cur_base["p1_plc_pnl"]+cur_base["p2_plc_pnl"]) else "  "
    print(f"  {mw:>7.2f} {n:>5} "
          f"{r['p1w']/n*100:>7.1f}%  {r['p2w']/n*100:>7.1f}%  "
          f"{r['p1p']/n*100:>7.1f}%  {r['p2p']/n*100:>7.1f}%  "
          f"{r['either_won']/n*100:>10.1f}%  {r['either_plcd']/n*100:>11.1f}%  "
          f"{r['neither_plcd']/n*100:>12.1f}%  "
          f"{comb_win:>+9.2f} {comb_plc:>+9.2f} {comb_win+comb_plc:>+10.2f}{beat}")

print(f"  {'-'*112}")
print(f"  * = beats current model on combined P&L")
print()

# Best mw summary
if best_mw is not None:
    r  = fine_results[best_mw]
    n  = r["n"]
    cw = r["p1_win_pnl"] + r["p2_win_pnl"]
    cp = r["p1_plc_pnl"] + r["p2_plc_pnl"]
    print(f"  Best mw by combined P&L: {best_mw:.2f}  (combined = {cw+cp:+.2f})")
    print()

    # Head-to-head vs current on every metric
    print(f"  Head-to-head: mw={best_mw:.2f} vs current model")
    print(f"  {'Metric':<28} {'Current':>12} {'mw='+str(best_mw):>12} {'Delta':>10}")
    print(f"  {'-'*64}")
    metrics = [
        ("P1 win%",          cb["p1w"]/cn*100,           r["p1w"]/n*100),
        ("P2 win%",          cb["p2w"]/cn*100,           r["p2w"]/n*100),
        ("Either won%",      cb["either_won"]/cn*100,    r["either_won"]/n*100),
        ("P1 place%",        cb["p1p"]/cn*100,           r["p1p"]/n*100),
        ("P2 place%",        cb["p2p"]/cn*100,           r["p2p"]/n*100),
        ("Either placed%",   cb["either_plcd"]/cn*100,   r["either_plcd"]/n*100),
        ("Both placed%",     cb["both_plcd"]/cn*100,     r["both_plcd"]/n*100),
        ("Neither placed%",  cb["neither_plcd"]/cn*100,  r["neither_plcd"]/n*100),
        ("P1 win P&L",       cb["p1_win_pnl"],           r["p1_win_pnl"]),
        ("P2 win P&L",       cb["p2_win_pnl"],           r["p2_win_pnl"]),
        ("P1 place P&L",     cb["p1_plc_pnl"],           r["p1_plc_pnl"]),
        ("P2 place P&L",     cb["p2_plc_pnl"],           r["p2_plc_pnl"]),
        ("Total win P&L",    cb["p1_win_pnl"]+cb["p2_win_pnl"], cw),
        ("Total place P&L",  cb["p1_plc_pnl"]+cb["p2_plc_pnl"], cp),
        ("Combined P&L",     cb["p1_win_pnl"]+cb["p2_win_pnl"]+cb["p1_plc_pnl"]+cb["p2_plc_pnl"], cw+cp),
    ]
    for label, cur_val, ms_val in metrics:
        delta = ms_val - cur_val
        is_pct = "%" in label
        fmt = lambda v: f"{v:+.1f}%" if is_pct else f"{v:+.2f}"
        win_marker = " +" if delta > 0 else (" -" if delta < 0 else "  ")
        print(f"  {label:<28} {cur_val:>+12.2f}  {ms_val:>+12.2f}  {fmt(delta):>10}{win_marker}")
    print()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2: VS CURRENT MODEL (card top1/top2 vs multistage P1/P2)
# ═══════════════════════════════════════════════════════════════════════════════

print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  SECTION 2: MULTISTAGE vs CURRENT MODEL — full P1 + P2 comparison      ║")
print("║  Current model = card top1/top2.  Multistage = mw=0.0 and mw=0.6       ║")
print("║  Win bets: £2 flat each. Place bets: £2 flat each (standard terms).    ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()

# ── Collect per-race results for both models ──────────────────────────────────
# Each entry: dict with all P1/P2 win/place outcomes and P&L for both models
race_results = []

for date, card_races in sorted(cards.items()):
    for card_race in card_races:
        key = (card_race.get("course",""),
               card_race.get("off_dt", card_race.get("off","")))
        raw = raw_by_key.get(key)
        if not raw: continue

        runners     = card_race.get("all_runners",[])
        raw_runners = raw.get("runners",[])
        if len(runners) < 2 or len(raw_runners) < 2: continue

        n   = len(raw_runners)
        ps  = place_spots_betfair(n)
        div = place_divisor(n)

        pos_by_horse = {strip_country(r.get("horse","")): r for r in raw_runners}

        def get_outcome(name):
            r = pos_by_horse.get(strip_country(name))
            if not r: return None, None
            try:    pos = int(str(r.get("position","")).strip())
            except: return None, None
            return pos, to_float(r.get("sp_dec"))

        def win_pnl(sp, won):
            if not sp: return 0.0
            return (sp-1)*2 if won else -2.0

        def plc_pnl(sp, placed, d):
            if not sp or not d: return 0.0
            ep = (sp-1)/d + 1
            return (ep-1)*2 if placed else -2.0

        # ── Current model ─────────────────────────────────────────────────────
        top1 = card_race.get("top1") or {}
        top2 = card_race.get("top2") or {}
        cp1_pos, cp1_sp = get_outcome(top1.get("horse",""))
        cp2_pos, cp2_sp = get_outcome(top2.get("horse",""))
        if cp1_pos is None: continue

        cp1_won  = cp1_pos == 1
        cp1_plcd = cp1_pos <= ps
        cp2_won  = cp2_pos == 1   if cp2_pos is not None else False
        cp2_plcd = cp2_pos <= ps  if cp2_pos is not None else False

        cur = dict(
            p1_won=cp1_won, p1_plcd=cp1_plcd,
            p2_won=cp2_won, p2_plcd=cp2_plcd,
            p1_win_pnl  = win_pnl(cp1_sp, cp1_won),
            p1_plc_pnl  = plc_pnl(cp1_sp, cp1_plcd, div),
            p2_win_pnl  = win_pnl(cp2_sp, cp2_won),
            p2_plc_pnl  = plc_pnl(cp2_sp, cp2_plcd, div),
            either_won  = cp1_won or cp2_won,
            either_plcd = cp1_plcd or cp2_plcd,
            both_plcd   = cp1_plcd and cp2_plcd,
        )

        # ── Multistage mw=0.0 ─────────────────────────────────────────────────
        ranked_0 = rank_card_runners(runners, card_race.get("going",""), 0.0)
        mp1_0, mp2_0 = ranked_0[0], ranked_0[1]
        mp1_0_pos, mp1_0_sp = get_outcome(mp1_0.get("horse",""))
        mp2_0_pos, mp2_0_sp = get_outcome(mp2_0.get("horse",""))
        if mp1_0_pos is None: continue

        mp1_0_won  = mp1_0_pos == 1
        mp1_0_plcd = mp1_0_pos <= ps
        mp2_0_won  = mp2_0_pos == 1   if mp2_0_pos is not None else False
        mp2_0_plcd = mp2_0_pos <= ps  if mp2_0_pos is not None else False

        ms0 = dict(
            p1_won=mp1_0_won, p1_plcd=mp1_0_plcd,
            p2_won=mp2_0_won, p2_plcd=mp2_0_plcd,
            p1_win_pnl  = win_pnl(mp1_0_sp, mp1_0_won),
            p1_plc_pnl  = plc_pnl(mp1_0_sp, mp1_0_plcd, div),
            p2_win_pnl  = win_pnl(mp2_0_sp, mp2_0_won),
            p2_plc_pnl  = plc_pnl(mp2_0_sp, mp2_0_plcd, div),
            either_won  = mp1_0_won or mp2_0_won,
            either_plcd = mp1_0_plcd or mp2_0_plcd,
            both_plcd   = mp1_0_plcd and mp2_0_plcd,
        )

        # ── Multistage mw=0.6 ─────────────────────────────────────────────────
        ranked_6 = rank_card_runners(runners, card_race.get("going",""), 0.6)
        mp1_6, mp2_6 = ranked_6[0], ranked_6[1]
        mp1_6_pos, mp1_6_sp = get_outcome(mp1_6.get("horse",""))
        mp2_6_pos, mp2_6_sp = get_outcome(mp2_6.get("horse",""))

        if mp1_6_pos is None:
            ms6 = ms0   # fallback
        else:
            mp1_6_won  = mp1_6_pos == 1
            mp1_6_plcd = mp1_6_pos <= ps
            mp2_6_won  = mp2_6_pos == 1   if mp2_6_pos is not None else False
            mp2_6_plcd = mp2_6_pos <= ps  if mp2_6_pos is not None else False
            ms6 = dict(
                p1_won=mp1_6_won, p1_plcd=mp1_6_plcd,
                p2_won=mp2_6_won, p2_plcd=mp2_6_plcd,
                p1_win_pnl  = win_pnl(mp1_6_sp, mp1_6_won),
                p1_plc_pnl  = plc_pnl(mp1_6_sp, mp1_6_plcd, div),
                p2_win_pnl  = win_pnl(mp2_6_sp, mp2_6_won),
                p2_plc_pnl  = plc_pnl(mp2_6_sp, mp2_6_plcd, div),
                either_won  = mp1_6_won or mp2_6_won,
                either_plcd = mp1_6_plcd or mp2_6_plcd,
                both_plcd   = mp1_6_plcd and mp2_6_plcd,
            )

        race_results.append(dict(
            n=n, ps=ps, div=div, cur=cur, ms0=ms0, ms6=ms6,
            cur_p1_name = top1.get("horse",""),
            ms0_p1_name = mp1_0.get("horse",""),
            ms6_p1_name = mp1_6.get("horse",""),
            winner = next((r.get("horse","") for r in raw_runners
                           if str(r.get("position",""))=="1"), "?"),
        ))

N = len(race_results)
print(f"  Matched card races: {N}")
print()

# ── 2A: Individual pick analysis ──────────────────────────────────────────────
print(f"  {'─'*80}")
print(f"  2A: INDIVIDUAL PICK ANALYSIS — P1 and P2 separately")
print(f"  {'─'*80}")
print(f"  {'Model':<28} {'N':>5} {'P1win%':>8} {'P2win%':>8} "
      f"{'P1plc%':>8} {'P2plc%':>8} {'P1WinP&L':>10} {'P2WinP&L':>10} "
      f"{'P1PlcP&L':>10} {'P2PlcP&L':>10}")
print(f"  {'-'*100}")

def summarise(results, key):
    r = [rec[key] for rec in results]
    n = len(r)
    if not n: return
    p1w   = sum(1 for x in r if x["p1_won"])
    p2w   = sum(1 for x in r if x["p2_won"])
    p1p   = sum(1 for x in r if x["p1_plcd"])
    p2p   = sum(1 for x in r if x["p2_plcd"])
    p1wpl = sum(x["p1_win_pnl"] for x in r)
    p2wpl = sum(x["p2_win_pnl"] for x in r)
    p1ppl = sum(x["p1_plc_pnl"] for x in r)
    p2ppl = sum(x["p2_plc_pnl"] for x in r)
    return n,p1w,p2w,p1p,p2p,p1wpl,p2wpl,p1ppl,p2ppl

def print_individual(label, results, key):
    s = summarise(results, key)
    if not s: return
    n,p1w,p2w,p1p,p2p,p1wpl,p2wpl,p1ppl,p2ppl = s
    print(f"  {label:<28} {n:>5} "
          f"{p1w/n*100:>7.1f}%  {p2w/n*100:>7.1f}%  "
          f"{p1p/n*100:>7.1f}%  {p2p/n*100:>7.1f}%  "
          f"{p1wpl:>+10.2f} {p2wpl:>+10.2f} "
          f"{p1ppl:>+10.2f} {p2ppl:>+10.2f}")

print_individual("Current model (top1/top2)", race_results, "cur")
print_individual("Multistage mw=0.0",         race_results, "ms0")
print_individual("Multistage mw=0.6",         race_results, "ms6")
print()

# ── 2B: P2-to-P2 direct comparison ───────────────────────────────────────────
print(f"  {'─'*80}")
print(f"  2B: P2-TO-P2 COMPARISON — where models disagree on P2")
print(f"  {'─'*80}")

p2_agree = [r for r in race_results
            if strip_country(r["cur_p1_name"]) == strip_country(r["ms0_p1_name"])]
p2_disagree = [r for r in race_results
               if strip_country(r["cur_p1_name"]) != strip_country(r["ms0_p1_name"])]

print(f"  Same P1 pick: {len(p2_agree)} races  |  Different P1 pick: {len(p2_disagree)} races")
print()

for subset_label, subset in [("All races", race_results),
                               ("Same P1 pick", p2_agree),
                               ("Different P1 pick", p2_disagree)]:
    if not subset: continue
    n = len(subset)
    print(f"  {subset_label} ({n} races):")
    print(f"  {'Pick':<22} {'Win%':>8} {'Place%':>8} {'WinP&L':>10} {'PlcP&L':>10}")
    print(f"  {'-'*52}")
    for model_key, pick_key, label in [
        ("cur","p2","  Current P2"),
        ("ms0","p2","  Multistage P2 (mw=0)"),
        ("ms6","p2","  Multistage P2 (mw=0.6)"),
    ]:
        wins   = sum(1 for r in subset if r[model_key]["p2_won"])
        placed = sum(1 for r in subset if r[model_key]["p2_plcd"])
        wpnl   = sum(r[model_key]["p2_win_pnl"] for r in subset)
        ppnl   = sum(r[model_key]["p2_plc_pnl"] for r in subset)
        print(f"  {label:<22} {wins/n*100:>7.1f}%  {placed/n*100:>7.1f}%  "
              f"{wpnl:>+10.2f} {ppnl:>+10.2f}")
    print()

# ── 2C: Combined P1+P2 outcomes ───────────────────────────────────────────────
print(f"  {'─'*80}")
print(f"  2C: COMBINED P1+P2 OUTCOMES — the actual unit of betting value")
print(f"  Staking £2 win + £2 place on each pick = £8 per race total (if place mkt exists)")
print(f"  {'─'*80}")
print(f"  {'Model':<28} {'N':>5} {'EitherWon%':>11} {'EitherPlcd%':>12} "
      f"{'BothPlcd%':>10} {'TotalWinP&L':>12} {'TotalPlcP&L':>12} {'Combined':>10}")
print(f"  {'-'*98}")

def print_combined(label, results, key):
    r = [rec[key] for rec in results]
    n = len(r)
    if not n: return
    ew   = sum(1 for x in r if x["either_won"])
    ep   = sum(1 for x in r if x["either_plcd"])
    bp   = sum(1 for x in r if x["both_plcd"])
    wpnl = sum(x["p1_win_pnl"]+x["p2_win_pnl"] for x in r)
    ppnl = sum(x["p1_plc_pnl"]+x["p2_plc_pnl"] for x in r)
    print(f"  {label:<28} {n:>5} "
          f"{ew/n*100:>10.1f}%  {ep/n*100:>11.1f}%  {bp/n*100:>9.1f}%  "
          f"{wpnl:>+12.2f} {ppnl:>+12.2f} {wpnl+ppnl:>+10.2f}")

print_combined("Current model (top1/top2)", race_results, "cur")
print_combined("Multistage mw=0.0",         race_results, "ms0")
print_combined("Multistage mw=0.6",         race_results, "ms6")
print()

# ── 2D: Race outcome categories ──────────────────────────────────────────────
print(f"  {'─'*80}")
print(f"  2D: RACE OUTCOME BREAKDOWN — what actually happens each race")
print(f"  {'─'*80}")
print(f"  {'Outcome':<28} {'Current':>10} {'MS mw=0':>10} {'MS mw=0.6':>12}")
print(f"  {'-'*64}")

outcomes = [
    ("P1 wins",           lambda r,k: r[k]["p1_won"]),
    ("P2 wins",           lambda r,k: r[k]["p2_won"]),
    ("Either wins",       lambda r,k: r[k]["either_won"]),
    ("P1 places",         lambda r,k: r[k]["p1_plcd"]),
    ("P2 places",         lambda r,k: r[k]["p2_plcd"]),
    ("Either places",     lambda r,k: r[k]["either_plcd"]),
    ("Both place",        lambda r,k: r[k]["both_plcd"]),
    ("Neither places",    lambda r,k: not r[k]["either_plcd"]),
]
n = len(race_results)
for label, fn in outcomes:
    c  = sum(1 for r in race_results if fn(r, "cur"))
    m0 = sum(1 for r in race_results if fn(r, "ms0"))
    m6 = sum(1 for r in race_results if fn(r, "ms6"))
    print(f"  {label:<28} {c/n*100:>9.1f}%  {m0/n*100:>9.1f}%  {m6/n*100:>11.1f}%")
print()

# ── 2E: Disagreement head-to-head (P1 only) ───────────────────────────────────
disagree = [(r["cur_p1_name"], r["ms0_p1_name"], r["winner"],
             r["cur"]["p1_won"], r["ms0"]["p1_won"],
             r["cur"]["p1_plcd"], r["ms0"]["p1_plcd"])
            for r in race_results
            if strip_country(r["cur_p1_name"]) != strip_country(r["ms0_p1_name"])]

if disagree:
    nd     = len(disagree)
    dw_cur = sum(1 for d in disagree if d[3])
    dw_ms  = sum(1 for d in disagree if d[4])
    dp_cur = sum(1 for d in disagree if d[5])
    dp_ms  = sum(1 for d in disagree if d[6])
    print(f"  {'─'*80}")
    print(f"  2E: P1 DISAGREEMENT HEAD-TO-HEAD ({nd} races)")
    print(f"  {'─'*80}")
    print(f"  Current right (P1 won):     {dw_cur}/{nd} ({dw_cur/nd*100:.1f}%)")
    print(f"  Multistage right (P1 won):  {dw_ms}/{nd} ({dw_ms/nd*100:.1f}%)")
    print(f"  Current P1 placed:          {dp_cur}/{nd} ({dp_cur/nd*100:.1f}%)")
    print(f"  Multistage P1 placed:       {dp_ms}/{nd} ({dp_ms/nd*100:.1f}%)")
    print()
    print(f"  {'Cur P1':22} {'MS P1':22} {'Winner':22} {'C':>3} {'M':>3}")
    print(f"  {'-'*74}")
    for d in disagree[:20]:
        mc = "W" if d[3] else ("P" if d[5] else " ")
        mm = "W" if d[4] else ("P" if d[6] else " ")
        print(f"  {d[0][:20]:22} {d[1][:20]:22} {d[2][:20]:22} {mc:>3} {mm:>3}")
    print(f"  (W=won, P=placed, blank=neither)")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3: RPR SUBSET — full history split by RPR availability
# ═══════════════════════════════════════════════════════════════════════════════

print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  SECTION 3: RPR AVAILABILITY SUBSET (full 17-day history)               ║")
print("║  Splits history races into RPR-present vs RPR-absent in raw results     ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()

rpr_yes = {"n":0,"cur_p1w":0,"cur_p1p":0,"cur_top2":0,
           "ms_p1w":0,"ms_p1p":0,"ms_top2":0}
rpr_no  = {"n":0,"cur_p1w":0,"cur_p1p":0,"cur_top2":0,
           "ms_p1w":0,"ms_p1p":0,"ms_top2":0}

for date, day in sorted(history.items()):
    for rec in day.get("races",[]):
        raw = raw_index.get(rec["race_id"])
        if not raw: continue
        runners = raw.get("runners",[])
        if len(runners) < 2: continue

        n  = len(runners)
        ps = place_spots_betfair(n)

        # Check RPR availability in this race
        field_rprs = [to_float(r.get("rpr")) for r in runners]
        rpr_avail  = sum(1 for v in field_rprs if v) / len(runners) >= 0.5

        # Current model outcome (from history rec)
        cur_p1_won  = rec.get("a_pos") == 1
        cur_p1_plcd = bool(rec.get("std_a") or rec.get("cons_a"))

        # Check if winner is in current model top-2
        cur_top2 = rec.get("a_pos") in (1,2) or rec.get("b_pos") in (1,2)
        # More precisely — winner rank in current model scoring
        scored_cur = sorted(
            [{**r, "_s": score_runner(r)[0]} for r in runners],
            key=lambda x: -x["_s"]
        )
        winner_horse = next((r.get("horse","") for r in runners
                             if str(r.get("position",""))=="1"), None)
        cur_winner_rank = next((i+1 for i,r in enumerate(scored_cur)
                                if r.get("horse","") == winner_horse), None)

        # Multistage (stats-only, no SP from raw)
        field_ors  = [to_float(r.get("or")) for r in runners]
        field_tsrs = [to_float(r.get("tsr")) for r in runners]
        going = raw.get("going","")
        ms_scored = []
        for r in runners:
            rpr = to_float(r.get("rpr"))
            or_ = to_float(r.get("or"))
            ts  = to_float(r.get("tsr"))
            s   = 0.0
            if rpr: s += normalise(rpr, field_rprs, 10)
            if or_: s += normalise(or_, field_ors,  10)
            if ts:  s += normalise(ts,  field_tsrs,  5)
            if rpr and or_ and rpr > or_: s += 2
            t_wp,t_ae,t_runs = get_form(r.get("trainer_14d"))
            j_wp,j_ae,j_runs = get_form(r.get("jockey_14d"))
            s += ae_pts(t_wp,t_ae,t_runs)
            s += ae_pts(j_wp,j_ae,j_runs)
            ms_scored.append((s, r))
        ms_scored.sort(key=lambda x: -x[0])
        ms_p1      = ms_scored[0][1]
        ms_p1_pos  = ms_p1.get("position")
        try: ms_p1_pos = int(str(ms_p1_pos).strip())
        except: ms_p1_pos = None
        ms_p1_won  = ms_p1_pos == 1
        ms_p1_plcd = ms_p1_pos is not None and ms_p1_pos <= ps
        ms_winner_rank = next((i+1 for i,(s,r) in enumerate(ms_scored)
                               if r.get("horse","") == winner_horse), None)

        bucket = rpr_yes if rpr_avail else rpr_no
        bucket["n"]        += 1
        bucket["cur_p1w"]  += cur_p1_won
        bucket["cur_p1p"]  += cur_p1_plcd
        bucket["cur_top2"] += (cur_winner_rank or 99) <= 2
        bucket["ms_p1w"]   += ms_p1_won
        bucket["ms_p1p"]   += ms_p1_plcd
        bucket["ms_top2"]  += (ms_winner_rank or 99) <= 2

print(f"  {'Metric':<28} {'RPR available':>15} {'RPR absent':>14}")
print(f"  {'-'*58}")
ra, rn = rpr_yes, rpr_no
na, nn = ra["n"], rn["n"]

def pct(num, den): return f"{num/den*100:.1f}%" if den else "n/a"

rows = [
    ("Races",                       na,              nn,              False),
    ("Current P1 win%",             ra["cur_p1w"],   rn["cur_p1w"],   True),
    ("Current P1 place%",           ra["cur_p1p"],   rn["cur_p1p"],   True),
    ("Current winner in top-2%",    ra["cur_top2"],  rn["cur_top2"],  True),
    ("Multistage P1 win%",          ra["ms_p1w"],    rn["ms_p1w"],    True),
    ("Multistage P1 place%",        ra["ms_p1p"],    rn["ms_p1p"],    True),
    ("Multistage winner in top-2%", ra["ms_top2"],   rn["ms_top2"],   True),
]
for label, va, vn, as_pct in rows:
    if as_pct:
        print(f"  {label:<28} {pct(va,na):>15} {pct(vn,nn):>14}")
    else:
        print(f"  {label:<28} {va:>15} {vn:>14}")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4: PLACE BET DEEP DIVE
# ═══════════════════════════════════════════════════════════════════════════════

print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  SECTION 4: PLACE BET DEEP DIVE                                         ║")
print("║  Betfair standard: <=4=win only, 5-7=2plc, 8-11=3plc, 12+=4plc         ║")
print("║  Est. place odds = (win_sp - 1) / divisor + 1                           ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()

# Place EV by field size band — three datasets
def place_ev_by_field(races_iter):
    """
    races_iter yields (p1_sp, p1_placed, n_runners) tuples.
    Returns dict: field_band -> {n, plc_rate, avg_win_ev, avg_plc_ev}
    """
    bands = {}
    for p1_sp, p1_placed, p1_won, n in races_iter:
        if n <= 4: continue   # no place market
        div = place_divisor(n)
        if not div or not p1_sp: continue
        if n <= 7:    fb = "5-7 (2 places)"
        elif n <= 11: fb = "8-11 (3 places)"
        else:         fb = "12+ (4 places)"
        if fb not in bands:
            bands[fb] = {"n":0,"plc":0,"win_ev":0.0,"plc_ev":0.0}
        b = bands[fb]
        b["n"]      += 1
        b["plc"]    += p1_placed
        b["win_ev"] += (p1_sp-1)*2 if p1_won else -2
        b["plc_ev"] += ((p1_sp-1)/div)*2 if p1_placed else -2
    return bands

def print_place_table(title, bands):
    print(f"  {title}")
    print(f"  {'Band':<18} {'N':>5} {'PlcRate%':>9} {'AvgWinEV':>10} {'AvgPlcEV':>10} {'PlcBetter?':>11}")
    print(f"  {'-'*66}")
    for fb in ["5-7 (2 places)","8-11 (3 places)","12+ (4 places)"]:
        b = bands.get(fb)
        if not b or b["n"]==0: continue
        avg_win = b["win_ev"] / b["n"]
        avg_plc = b["plc_ev"] / b["n"]
        plc_rt  = b["plc"]   / b["n"] * 100
        better  = "YES" if avg_plc > avg_win else "no"
        print(f"  {fb:<18} {b['n']:>5} {plc_rt:>8.1f}%  {avg_win:>10.3f}  {avg_plc:>10.3f}  {better:>11}")
    print()

# Dataset A: card multistage P1 (mw=0.0)
def iter_card_ms():
    for date, card_races in sorted(cards.items()):
        for card_race in card_races:
            key = (card_race.get("course",""),
                   card_race.get("off_dt", card_race.get("off","")))
            raw = raw_by_key.get(key)
            if not raw: continue
            runners = card_race.get("all_runners",[])
            if len(runners) < 2: continue
            ranked = rank_card_runners(runners, card_race.get("going",""), 0.0)
            p1 = ranked[0]
            raw_runners = raw.get("runners",[])
            n = len(raw_runners)
            pos_by_horse = {strip_country(r.get("horse","")): r for r in raw_runners}
            r = pos_by_horse.get(strip_country(p1.get("horse","")))
            if not r: continue
            try: pos = int(str(r.get("position","")).strip())
            except: continue
            sp = to_float(r.get("sp_dec"))
            if not sp: continue
            ps = place_spots_betfair(n)
            yield sp, pos <= ps, pos == 1, n

# Dataset B: card current model (top1)
def iter_card_cur():
    for date, card_races in sorted(cards.items()):
        for card_race in card_races:
            key = (card_race.get("course",""),
                   card_race.get("off_dt", card_race.get("off","")))
            raw = raw_by_key.get(key)
            if not raw: continue
            top1 = card_race.get("top1") or {}
            if not top1: continue
            raw_runners = raw.get("runners",[])
            n = len(raw_runners)
            pos_by_horse = {strip_country(r.get("horse","")): r for r in raw_runners}
            r = pos_by_horse.get(strip_country(top1.get("horse","")))
            if not r: continue
            try: pos = int(str(r.get("position","")).strip())
            except: continue
            sp = to_float(r.get("sp_dec"))
            if not sp: continue
            ps = place_spots_betfair(n)
            yield sp, pos <= ps, pos == 1, n

# Dataset C: full history current model (uses BSP from raw)
def iter_history_cur():
    for date, day in sorted(history.items()):
        for rec in day.get("races",[]):
            raw = raw_index.get(rec["race_id"])
            if not raw: continue
            runners = raw.get("runners",[])
            n = len(runners)
            # Find P1 runner (current model top pick = highest score_runner)
            if not runners: continue
            scored = sorted([{**r,"_s":score_runner(r)[0]} for r in runners],
                            key=lambda x:-x["_s"])
            p1 = scored[0]
            sp = to_float(p1.get("sp_dec")) or to_float(p1.get("bsp"))
            if not sp: continue
            try: pos = int(str(p1.get("position","")).strip())
            except: continue
            ps = place_spots_betfair(n)
            p1_plcd = bool(rec.get("std_a") or rec.get("cons_a"))
            yield sp, p1_plcd, pos==1, n

print_place_table("A: Card multistage P1 (mw=0.0) — 5 card dates",
                  place_ev_by_field(iter_card_ms()))
print_place_table("B: Card current model (top1) — 5 card dates",
                  place_ev_by_field(iter_card_cur()))
print_place_table("C: Full history current model — 17 days (uses BSP)",
                  place_ev_by_field(iter_history_cur()))

# Place EV by tier — full history
print("  Place EV by tier (full history, current model, uses BSP):")
print(f"  {'Tier':<10} {'N':>5} {'PlcRate%':>9} {'AvgWinEV':>10} {'AvgPlcEV':>10} {'PlcBetter?':>11}")
print(f"  {'-'*58}")

tier_plc = defaultdict(lambda:{"n":0,"plc":0,"win_ev":0.0,"plc_ev":0.0})
for date, day in sorted(history.items()):
    for rec in day.get("races",[]):
        raw = raw_index.get(rec["race_id"])
        if not raw: continue
        runners = raw.get("runners",[])
        if len(runners) < 2: continue
        n   = len(runners)
        div = place_divisor(n)
        if not div: continue
        scored = sorted([{**r,"_s":score_runner(r)[0]} for r in runners],
                        key=lambda x:-x["_s"])
        raw2  = {**raw,"runners":scored}
        tier,_ = race_confidence(raw2, scored[0]["_s"])
        label  = TIER_LABELS.get(tier,"?").split()[0]
        sp = to_float(scored[0].get("sp_dec")) or to_float(scored[0].get("bsp"))
        if not sp: continue
        p1_won  = rec.get("a_pos") == 1
        p1_plcd = bool(rec.get("std_a") or rec.get("cons_a"))
        b = tier_plc[label]
        b["n"]      += 1
        b["plc"]    += p1_plcd
        b["win_ev"] += (sp-1)*2 if p1_won else -2
        b["plc_ev"] += ((sp-1)/div)*2 if p1_plcd else -2

for label in ["🔥🔥🔥","🔥🔥","🔥","·","✗"]:
    b = tier_plc.get(label)
    if not b or b["n"]==0: continue
    avg_win = b["win_ev"]/b["n"]
    avg_plc = b["plc_ev"]/b["n"]
    plc_rt  = b["plc"]/b["n"]*100
    better  = "YES" if avg_plc > avg_win else "no"
    print(f"  {label:<10} {b['n']:>5} {plc_rt:>8.1f}%  {avg_win:>10.3f}  {avg_plc:>10.3f}  {better:>11}")

print()
print("Done.")
