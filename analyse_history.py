"""
analyse_history.py
Cross-references paper trading history with raw results to show
tier-level performance, pick hit rates, going/field/time/odds patterns.

Also analyses the full raw results dataset (independent of paper history)
for market-level baselines: favourite rates, price band win rates, draw
bias, trainer form, jockey form, course patterns, and daily summaries.

Run on VM:
  cd ~/horse_bets_v3
  python3 analyse_history.py
"""

import json
import glob
import sys
import os
import statistics
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))

from predict    import score_runner, place_terms
from predict_v2 import race_confidence, TIER_LABELS, TIER_SUPREME, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP

# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════════

# ── Paper history (existing) ──────────────────────────────────────────────────
history = {}
for fp in sorted(glob.glob("data/history/*.json")):
    with open(fp) as fh:
        d = json.load(fh)
    history[d["date"]] = d

# ── Raw results index by race_id ──────────────────────────────────────────────
raw_index = {}
races_all = []          # flat list for raw-only analysis
for fp in sorted(glob.glob("data/raw/*.json")):
    with open(fp) as fh:
        d = json.load(fh)
    date_str = os.path.basename(fp).replace(".json", "")
    raw_races = d.get("results") or d.get("races") or []
    for race in raw_races:
        race["_date"] = date_str
        raw_index[race["race_id"]] = race
    races_all.extend(raw_races)

# ── Cards (pre-race model data, available from midnight job) ──────────────────
cards_by_date = {}
for fp in sorted(glob.glob("data/cards/2026-*.json")):
    date_str = os.path.basename(fp).replace(".json", "")
    try:
        with open(fp) as fh:
            d = json.load(fh)
        racecards = d.get("racecards") or d.get("races") or []
        idx = {}
        for rc in racecards:
            key = (rc.get("course", ""), rc.get("off", ""))
            idx[key] = rc
        cards_by_date[date_str] = idx
    except Exception:
        pass

print(f"Loaded {len(history)} days of history | "
      f"{len(raw_index)} raw races ({len(set(r['_date'] for r in races_all))} days) | "
      f"cards for {len(cards_by_date)} days")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def to_float(v):
    try: return float(str(v).strip())
    except: return None

def to_int(v):
    try: return int(str(v).strip())
    except: return None

def place_spots_for(n):
    """Betfair place spots by field size."""
    if n <= 4:  return 1
    if n <= 7:  return 2
    return 3

def place_divisor(n):
    """Betfair place dividend divisor."""
    if n <= 4:  return None
    if n <= 7:  return 4.0
    if n <= 11: return 5.0
    return 6.0

def time_bucket(off_dt):
    try:
        from datetime import datetime
        dt = datetime.fromisoformat(str(off_dt))
        total = dt.hour * 60 + dt.minute
        if total < 13 * 60:   return "before 1pm"
        elif total < 14 * 60: return "1pm-2pm"
        elif total < 15 * 60: return "2pm-3pm"
        elif total < 16 * 60: return "3pm-4pm"
        elif total < 17 * 60: return "4pm-5pm"
        elif total < 18 * 60: return "5pm-6pm"
        else:                  return "after 6pm"
    except:
        return "unknown"

def odds_bucket(sp_dec):
    if sp_dec is None:  return "unknown"
    if sp_dec < 2.0:    return "odds-on  <2.0"
    if sp_dec < 3.0:    return "2.0-2.9"
    if sp_dec < 5.0:    return "3.0-4.9"
    if sp_dec < 8.0:    return "5.0-7.9"
    if sp_dec < 13.0:   return "8.0-12.9"
    return               "13.0+"

def going_family(going):
    """Broad going category."""
    g = (going or "").lower()
    if "heavy" in g:                        return "heavy"
    if "soft" in g:                         return "soft"
    if "good to soft" in g or "gts" in g:  return "gd/sft"
    if "good to firm" in g or "gtf" in g:  return "gd/frm"
    if "good" in g:                         return "good"
    if "firm" in g:                         return "firm"
    if "standard" in g:                     return "standard (AW)"
    if "fast" in g:                         return "fast (AW)"
    return "other"

def empty_stat():
    return {
        "total": 0, "std_win": 0, "cons_win": 0,
        "a_placed": 0, "b_placed": 0,
        "a_win": 0, "b_win": 0,
        "sp_sum": 0.0, "sp_n": 0,
    }

def accumulate(d_dict, key, rec, p1_sp=None):
    if key not in d_dict:
        d_dict[key] = empty_stat()
    t = d_dict[key]
    t["total"] += 1
    if rec.get("std_win"):                    t["std_win"]  += 1
    if rec.get("cons_win"):                   t["cons_win"] += 1
    if rec.get("std_a") or rec.get("cons_a"): t["a_placed"] += 1
    if rec.get("std_b") or rec.get("cons_b"): t["b_placed"] += 1
    if rec.get("a_pos") == 1:                 t["a_win"]    += 1
    if rec.get("b_pos") == 1:                 t["b_win"]    += 1
    if p1_sp is not None:
        t["sp_sum"] += p1_sp
        t["sp_n"]   += 1

def print_table(title, stats, order=None, sort_by="total", top=None):
    print(f"{'='*76}")
    print(f"  {title}")
    print(f"{'='*76}")
    print(f"  {'Key':<18} {'N':>5} {'P1win%':>7} {'P2win%':>7} "
          f"{'P1plc%':>7} {'P2plc%':>7} {'Both%':>7} {'Cons%':>7} {'AvgSP':>6}")
    print(f"  {'-'*72}")
    if order:
        items = [(k, stats[k]) for k in order if k in stats]
    else:
        items = sorted(stats.items(), key=lambda x: -x[1][sort_by])
    if top:
        items = items[:top]
    for key, t in items:
        n = t["total"]
        if n == 0:
            continue
        avg_sp = t["sp_sum"] / t["sp_n"] if t["sp_n"] else 0
        print(f"  {key:<18} {n:>5} "
              f"{t['a_win']/n*100:>6.1f}% "
              f"{t['b_win']/n*100:>6.1f}% "
              f"{t['a_placed']/n*100:>6.1f}% "
              f"{t['b_placed']/n*100:>6.1f}% "
              f"{t['std_win']/n*100:>6.1f}% "
              f"{t['cons_win']/n*100:>6.1f}% "
              f"{avg_sp:>6.2f}")
    print()

# ═══════════════════════════════════════════════════════════════════════════════
# PART A: PAPER HISTORY ANALYSIS (existing sections 1–14)
# ═══════════════════════════════════════════════════════════════════════════════

print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  PART A — PAPER HISTORY ANALYSIS (model picks vs actual results)        ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()

# ── Cross-reference ───────────────────────────────────────────────────────────

tier_stats   = {}
going_stats  = {}
field_stats  = {}
time_stats   = {}
odds_stats   = {}
tier_sp_list = {}
matched      = 0
unmatched    = 0

# Collect per-race detail for later sections
p1p2_data  = []   # (p1_sp, p2_sp, gap, label, p1_won, p2_won, p1_plc, p2_plc, n_runners)
race_cats  = []   # (category, tier, gap, n, p1_sp, p2_sp, market_agrees)
winner_rank = {}

for date, day in sorted(history.items()):
    for rec in day.get("races", []):
        rid = rec.get("race_id")
        raw = raw_index.get(rid)
        if not raw:
            unmatched += 1
            continue
        matched += 1

        runners = raw.get("runners", [])
        if len(runners) < 2:
            continue

        scored = sorted(
            [{**r, "score": score_runner(r)[0]} for r in runners],
            key=lambda x: -x["score"]
        )

        raw2      = {**raw, "runners": scored}
        win_score = scored[0]["score"] if scored else 0
        tier, _   = race_confidence(raw2, win_score)
        label     = TIER_LABELS.get(tier, "?").split()[0]

        going    = (raw.get("going") or "unknown").lower()
        n        = len(runners)
        off      = raw.get("off_dt", "unknown")
        p1_sp    = to_float(scored[0].get("sp_dec")) if scored else None
        p2_sp    = to_float(scored[1].get("sp_dec")) if len(scored) > 1 else None
        gap      = scored[0].get("score", 0) - scored[1].get("score", 0) if len(scored) > 1 else 0

        if n <= 5:    field = "2-5"
        elif n <= 8:  field = "6-8"
        elif n <= 12: field = "9-12"
        else:         field = "13+"

        tbkt = time_bucket(off)
        obkt = odds_bucket(p1_sp)

        accumulate(tier_stats,  label, rec, p1_sp)
        accumulate(going_stats, going, rec, p1_sp)
        accumulate(field_stats, field, rec, p1_sp)
        accumulate(time_stats,  tbkt,  rec, p1_sp)
        accumulate(odds_stats,  obkt,  rec, p1_sp)

        if label not in tier_sp_list:
            tier_sp_list[label] = []
        if p1_sp:
            tier_sp_list[label].append(p1_sp)

        # Winner rank
        winner = next((r for r in runners if str(r.get("position", "")) == "1"), None)
        rank = None
        if winner:
            for i, r in enumerate(scored):
                if r.get("horse") == winner.get("horse"):
                    rank = i + 1
                    break
        winner_rank[rank] = winner_rank.get(rank, 0) + 1

        p1_won = rec.get("a_pos") == 1
        p2_won = rec.get("b_pos") == 1
        p1_plc = bool(rec.get("std_a") or rec.get("cons_a"))
        p2_plc = bool(rec.get("std_b") or rec.get("cons_b"))
        mkt_ok = bool(p1_sp and p2_sp and p1_sp <= p2_sp)

        p1p2_data.append((p1_sp, p2_sp, gap, label, p1_won, p2_won, p1_plc, p2_plc, n))

        if not p1_plc and not p2_plc:   cat = "both_loss"
        elif p1_won or p2_won:           cat = "both_win"
        else:                            cat = "place_only_win"
        race_cats.append((cat, label, gap, n, p1_sp, p2_sp, mkt_ok))

# ── A1: Tier ─────────────────────────────────────────────────────────────────
print(f"Matched: {matched} | Unmatched (no raw data): {unmatched}")
print()

tier_order = ["🔥🔥🔥", "🔥🔥", "🔥", "·", "✗"]
print_table("A1: BY TIER", tier_stats, order=tier_order)

# ── A2: Field size ────────────────────────────────────────────────────────────
field_order = ["2-5", "6-8", "9-12", "13+"]
print_table("A2: BY FIELD SIZE", field_stats, order=field_order)

# ── A3: Going ────────────────────────────────────────────────────────────────
print_table("A3: BY GOING (top 10 by volume)", going_stats, top=10)

# ── A4: Time of day ──────────────────────────────────────────────────────────
time_order = ["before 1pm","1pm-2pm","2pm-3pm","3pm-4pm","4pm-5pm","5pm-6pm","after 6pm"]
print_table("A4: BY TIME OF DAY", time_stats, order=time_order)

# ── A5: P1 SP odds band ──────────────────────────────────────────────────────
odds_order = ["odds-on  <2.0","2.0-2.9","3.0-4.9","5.0-7.9","8.0-12.9","13.0+"]
print_table("A5: BY P1 SP ODDS BAND", odds_stats, order=odds_order)

# ── A6: Average P1 SP by tier ────────────────────────────────────────────────
print(f"{'='*76}")
print(f"  A6: AVERAGE P1 SP BY TIER")
print(f"{'='*76}")
print(f"  {'Tier':<18} {'N':>5} {'AvgSP':>8} {'MedianSP':>10} {'Min':>7} {'Max':>7}")
print(f"  {'-'*58}")
for label in tier_order:
    sps = tier_sp_list.get(label, [])
    if not sps:
        continue
    sps_s = sorted(sps)
    avg   = sum(sps) / len(sps)
    mid   = len(sps_s) // 2
    med   = sps_s[mid] if len(sps_s) % 2 else (sps_s[mid-1] + sps_s[mid]) / 2
    print(f"  {label:<18} {len(sps):>5} {avg:>8.2f} {med:>10.2f} {sps_s[0]:>7.2f} {sps_s[-1]:>7.2f}")
print()

# ── A7: Summary totals ────────────────────────────────────────────────────────
all_total = sum(t["total"]    for t in tier_stats.values())
all_stdw  = sum(t["std_win"]  for t in tier_stats.values())
all_consw = sum(t["cons_win"] for t in tier_stats.values())
all_awin  = sum(t["a_win"]    for t in tier_stats.values())
all_bwin  = sum(t["b_win"]    for t in tier_stats.values())
all_aplc  = sum(t["a_placed"] for t in tier_stats.values())
all_bplc  = sum(t["b_placed"] for t in tier_stats.values())

print(f"{'='*76}")
print(f"  A7: TOTALS ({all_total} races across {len(history)} days)")
print(f"{'='*76}")
print(f"  Pick 1 win rate:    {all_awin/all_total*100:.1f}%  ({all_awin}/{all_total})")
print(f"  Pick 2 win rate:    {all_bwin/all_total*100:.1f}%  ({all_bwin}/{all_total})")
print(f"  Pick 1 place rate:  {all_aplc/all_total*100:.1f}%  ({all_aplc}/{all_total})")
print(f"  Pick 2 place rate:  {all_bplc/all_total*100:.1f}%  ({all_bplc}/{all_total})")
print(f"  Both placed (std):  {all_stdw/all_total*100:.1f}%  ({all_stdw}/{all_total})")
print(f"  Both placed (cons): {all_consw/all_total*100:.1f}%  ({all_consw}/{all_total})")
print()

# ── A8: Winner rank distribution ─────────────────────────────────────────────
print(f"{'='*76}")
print(f"  A8: WINNER RANK IN MODEL SCORING")
print(f"{'='*76}")
total_ranked = sum(winner_rank.values())
cumulative = 0
for rank in sorted(k for k in winner_rank if k is not None):
    n_r = winner_rank[rank]
    cumulative += n_r
    print(f"  Rank {rank:>2}: {n_r:>4} races  ({n_r/total_ranked*100:.1f}%)  "
          f"cumulative top-{rank}: {cumulative/total_ranked*100:.1f}%")
missed = winner_rank.get(None, 0)
if missed:
    print(f"  Winner not in scored runners: {missed}")
print()

# ── A9: P1/P2 price ratio ────────────────────────────────────────────────────
ratio_stats = {}
for p1_sp, p2_sp, gap, label, p1_won, p2_won, p1_plc, p2_plc, n in p1p2_data:
    if not p1_sp or not p2_sp:
        continue
    ratio = p2_sp / p1_sp
    if ratio < 1.0:    rbkt = "P2 shorter than P1"
    elif ratio < 1.5:  rbkt = "P2 1.0-1.5x P1"
    elif ratio < 2.5:  rbkt = "P2 1.5-2.5x P1"
    elif ratio < 4.0:  rbkt = "P2 2.5-4.0x P1"
    else:              rbkt = "P2 4x+ P1"
    if rbkt not in ratio_stats:
        ratio_stats[rbkt] = {"n":0,"p1w":0,"p2w":0,"p1p":0,"p2p":0}
    s = ratio_stats[rbkt]
    s["n"]  += 1
    if p1_won: s["p1w"] += 1
    if p2_won: s["p2w"] += 1
    if p1_plc: s["p1p"] += 1
    if p2_plc: s["p2p"] += 1

print(f"{'='*76}")
print(f"  A9: P1/P2 PRICE RATIO")
print(f"{'='*76}")
print(f"  {'Ratio band':<22} {'N':>5} {'P1win%':>8} {'P2win%':>8} {'P1plc%':>8} {'P2plc%':>8}")
print(f"  {'-'*62}")
for rbkt in ["P2 shorter than P1","P2 1.0-1.5x P1","P2 1.5-2.5x P1","P2 2.5-4.0x P1","P2 4x+ P1"]:
    s = ratio_stats.get(rbkt)
    if not s or s["n"] == 0:
        continue
    print(f"  {rbkt:<22} {s['n']:>5} "
          f"{s['p1w']/s['n']*100:>7.1f}% "
          f"{s['p2w']/s['n']*100:>7.1f}% "
          f"{s['p1p']/s['n']*100:>7.1f}% "
          f"{s['p2p']/s['n']*100:>7.1f}%")
print()

# ── A10: Score gap vs win rate ────────────────────────────────────────────────
gap_stats = {}
for p1_sp, p2_sp, gap, label, p1_won, p2_won, p1_plc, p2_plc, n in p1p2_data:
    if gap <= 0:      gbkt = "gap 0 (tied)"
    elif gap == 1:    gbkt = "gap 1"
    elif gap == 2:    gbkt = "gap 2"
    elif gap <= 4:    gbkt = "gap 3-4"
    else:             gbkt = "gap 5+"
    if gbkt not in gap_stats:
        gap_stats[gbkt] = {"n":0,"p1w":0,"p2w":0,"p1p":0,"p2p":0}
    s = gap_stats[gbkt]
    s["n"]  += 1
    if p1_won: s["p1w"] += 1
    if p2_won: s["p2w"] += 1
    if p1_plc: s["p1p"] += 1
    if p2_plc: s["p2p"] += 1

print(f"{'='*76}")
print(f"  A10: P1-P2 SCORE GAP vs WIN RATE")
print(f"{'='*76}")
print(f"  {'Score gap':<22} {'N':>5} {'P1win%':>8} {'P2win%':>8} {'P1plc%':>8} {'P2plc%':>8}")
print(f"  {'-'*62}")
for gbkt in ["gap 0 (tied)","gap 1","gap 2","gap 3-4","gap 5+"]:
    s = gap_stats.get(gbkt)
    if not s or s["n"] == 0:
        continue
    print(f"  {gbkt:<22} {s['n']:>5} "
          f"{s['p1w']/s['n']*100:>7.1f}% "
          f"{s['p2w']/s['n']*100:>7.1f}% "
          f"{s['p1p']/s['n']*100:>7.1f}% "
          f"{s['p2p']/s['n']*100:>7.1f}%")
print()

# ── A11: Combined signal ──────────────────────────────────────────────────────
combined_stats = {}
for p1_sp, p2_sp, gap, label, p1_won, p2_won, p1_plc, p2_plc, n in p1p2_data:
    if not p1_sp or not p2_sp:
        continue
    strong_gap    = gap >= 3
    market_agrees = p1_sp <= p2_sp
    if strong_gap and market_agrees:       key = "Strong gap + market agrees"
    elif strong_gap and not market_agrees: key = "Strong gap + mkt disagrees"
    elif not strong_gap and market_agrees: key = "Weak gap + market agrees"
    else:                                  key = "Weak gap + mkt disagrees"
    if key not in combined_stats:
        combined_stats[key] = {"n":0,"p1w":0,"p2w":0,"p1p":0}
    s = combined_stats[key]
    s["n"]  += 1
    if p1_won: s["p1w"] += 1
    if p2_won: s["p2w"] += 1
    if p1_plc: s["p1p"] += 1

print(f"{'='*76}")
print(f"  A11: COMBINED SIGNAL — score gap + market agreement")
print(f"  'market agrees' = P1 SP <= P2 SP  |  'strong gap' = score gap >= 3")
print(f"{'='*76}")
print(f"  {'Condition':<35} {'N':>5} {'P1win%':>8} {'P2win%':>8} {'P1plc%':>8}")
print(f"  {'-'*65}")
for key in ["Strong gap + market agrees","Strong gap + mkt disagrees",
            "Weak gap + market agrees","Weak gap + mkt disagrees"]:
    s = combined_stats.get(key)
    if not s or s["n"] == 0:
        continue
    print(f"  {key:<35} {s['n']:>5} "
          f"{s['p1w']/s['n']*100:>7.1f}% "
          f"{s['p2w']/s['n']*100:>7.1f}% "
          f"{s['p1p']/s['n']*100:>7.1f}%")
print()

# ── A12: Race category profiles ───────────────────────────────────────────────
def profile_cat(races, name):
    if not races:
        return
    n = len(races)
    tiers = {}
    for _, t, *_ in races:
        tiers[t] = tiers.get(t, 0) + 1
    gaps   = [g for _, _, g, *_ in races]
    fields = [fld for _, _, _, fld, *_ in races]
    sps    = [p for _, _, _, _, p, *_ in races if p]
    agrees = sum(1 for *_, m in races if m)
    print(f"  {name} ({n} races)")
    print(f"    Tiers:          {dict(sorted(tiers.items(), key=lambda x: -x[1]))}")
    print(f"    Avg score gap:  {sum(gaps)/len(gaps):.2f}  "
          f"(gap>=3: {sum(1 for g in gaps if g>=3)}/{n} = {sum(1 for g in gaps if g>=3)/n*100:.0f}%)")
    print(f"    Avg field size: {sum(fields)/len(fields):.1f}")
    print(f"    Avg P1 SP:      {sum(sps)/len(sps):.2f}" if sps else "    Avg P1 SP: n/a")
    print(f"    Market agrees:  {agrees}/{n} ({agrees/n*100:.0f}%)")
    print()

print(f"{'='*76}")
print(f"  A12: RACE OUTCOME CATEGORY PROFILES")
print(f"{'='*76}")
print()
both_loss = [r for r in race_cats if r[0] == "both_loss"]
place_win = [r for r in race_cats if r[0] == "place_only_win"]
both_win  = [r for r in race_cats if r[0] == "both_win"]
profile_cat(both_loss, "BOTH LOSS — neither pick placed")
profile_cat(place_win, "PLACE-WIN ONLY — win bet lost, place saved it")
profile_cat(both_win,  "BOTH WIN — win + place both returned")

# ── A13: Both-loss predictors ─────────────────────────────────────────────────
print(f"{'='*76}")
print(f"  A13: BOTH-LOSS RATE BY KEY VARIABLES")
print(f"{'='*76}")
all_races = race_cats

print()
print("  By score gap:")
print(f"  {'Gap':<12} {'Total':>6} {'BothLoss':>10} {'Rate':>8}")
for thresh, lbl in [(0,"gap=0"),(1,"gap=1"),(2,"gap=2")]:
    grp = [c for c, _, g, *_ in all_races if g == thresh]
    bl  = sum(1 for c in grp if c == "both_loss")
    if grp:
        print(f"  {lbl:<12} {len(grp):>6} {bl:>10} {bl/len(grp)*100:>7.1f}%")
grp = [c for c, _, g, *_ in all_races if g >= 3]
bl  = sum(1 for c in grp if c == "both_loss")
if grp:
    print(f"  {'gap>=3':<12} {len(grp):>6} {bl:>10} {bl/len(grp)*100:>7.1f}%")

print()
print("  By field size:")
print(f"  {'Field':>10} {'Total':>6} {'BothLoss':>10} {'Rate':>8}")
for lo, hi, lbl in [(2,5,"2-5"),(6,8,"6-8"),(9,12,"9-12"),(13,99,"13+")]:
    grp = [c for c, _, _, n, *_ in all_races if lo <= n <= hi]
    bl  = sum(1 for c in grp if c == "both_loss")
    if grp:
        print(f"  {lbl:>10} {len(grp):>6} {bl:>10} {bl/len(grp)*100:>7.1f}%")

print()
print("  By P1 SP band:")
print(f"  {'SP band':<16} {'Total':>6} {'BothLoss':>10} {'Rate':>8}")
for lo, hi, lbl in [(0,2,"odds-on <2"),(2,3,"2.0-2.9"),(3,5,"3.0-4.9"),
                    (5,8,"5.0-7.9"),(8,13,"8.0-12.9"),(13,999,"13.0+")]:
    grp = [c for c, _, _, _, sp, *_ in all_races if sp and lo <= sp < hi]
    bl  = sum(1 for c in grp if c == "both_loss")
    if grp:
        print(f"  {lbl:<16} {len(grp):>6} {bl:>10} {bl/len(grp)*100:>7.1f}%")

print()
print("  By market agreement:")
for agrees, lbl in [(True,"Mkt agrees"),(False,"Mkt disagrees")]:
    grp = [c for c, _, _, _, _, _, m in all_races if m == agrees]
    bl  = sum(1 for c in grp if c == "both_loss")
    if grp:
        print(f"  {lbl:<16} total={len(grp)}  both_loss={bl} ({bl/len(grp)*100:.1f}%)")
print()

# ── A14: Place bet EV summary ─────────────────────────────────────────────────
print(f"{'='*76}")
print(f"  A14: PLACE BET VALUE — estimated EV by field size")
print(f"  Place odds = (win_bsp-1)/divisor + 1  |  divisors: 5-7=4, 8-11=5, 12+=6")
print(f"{'='*76}")
print()

ev_stats = {}
for p1_sp, p2_sp, gap, label, p1_won, p2_won, p1_plc, p2_plc, n in p1p2_data:
    div = place_divisor(n)
    if not div or not p1_sp:
        continue
    est_place_odds = (p1_sp - 1.0) / div + 1.0
    win_ev  = (p1_sp - 1.0) if p1_won else -1.0
    plc_ev  = (est_place_odds - 1.0) if p1_plc else -1.0

    if n <= 7:    fbkt = "5-7 runners (2 places)"
    elif n <= 11: fbkt = "8-11 runners (3 places)"
    else:         fbkt = "12+ runners (4 places)"

    if fbkt not in ev_stats:
        ev_stats[fbkt] = {"n":0,"win_ev":0.0,"plc_ev":0.0,"p1_plc":0}
    s = ev_stats[fbkt]
    s["n"]      += 1
    s["win_ev"] += win_ev
    s["plc_ev"] += plc_ev
    if p1_plc: s["p1_plc"] += 1

print(f"  {'Field band':<22} {'N':>5} {'PlcRate%':>9} {'AvgWinEV':>10} {'AvgPlcEV':>10} {'PlcBetter?':>11}")
print(f"  {'-'*68}")
for fbkt in ["5-7 runners (2 places)","8-11 runners (3 places)","12+ runners (4 places)"]:
    s = ev_stats.get(fbkt)
    if not s or s["n"] == 0:
        continue
    avg_win  = s["win_ev"] / s["n"]
    avg_plc  = s["plc_ev"] / s["n"]
    plc_rate = s["p1_plc"] / s["n"] * 100
    better   = "YES" if avg_plc > avg_win else "no"
    print(f"  {fbkt:<22} {s['n']:>5} {plc_rate:>8.1f}% {avg_win:>10.3f} {avg_plc:>10.3f} {better:>11}")
print()
print(f"  Note: EV per £1 staked. Positive = profitable, negative = losing.")
print()

# ── A15: Place EV by tier (new) ───────────────────────────────────────────────
print(f"{'='*76}")
print(f"  A15: PLACE BET EV BY TIER")
print(f"{'='*76}")
print()

tier_plc_ev = {}
for p1_sp, p2_sp, gap, label, p1_won, p2_won, p1_plc, p2_plc, n in p1p2_data:
    div = place_divisor(n)
    if not div or not p1_sp:
        continue
    est_place_odds = (p1_sp - 1.0) / div + 1.0
    win_ev = (p1_sp - 1.0) if p1_won else -1.0
    plc_ev = (est_place_odds - 1.0) if p1_plc else -1.0
    if label not in tier_plc_ev:
        tier_plc_ev[label] = {"n":0,"win_ev":0.0,"plc_ev":0.0,"plc_n":0}
    s = tier_plc_ev[label]
    s["n"]      += 1
    s["win_ev"] += win_ev
    s["plc_ev"] += plc_ev
    if p1_plc: s["plc_n"] += 1

print(f"  {'Tier':<18} {'N':>5} {'PlcRate%':>9} {'AvgWinEV':>10} {'AvgPlcEV':>10} {'PlcBetter?':>11}")
print(f"  {'-'*68}")
for label in tier_order:
    s = tier_plc_ev.get(label)
    if not s or s["n"] == 0:
        continue
    avg_win  = s["win_ev"] / s["n"]
    avg_plc  = s["plc_ev"] / s["n"]
    plc_rate = s["plc_n"] / s["n"] * 100
    better   = "YES" if avg_plc > avg_win else "no"
    print(f"  {label:<18} {s['n']:>5} {plc_rate:>8.1f}% {avg_win:>10.3f} {avg_plc:>10.3f} {better:>11}")
print()

# ── A16: P2 place EV by tier (new) ───────────────────────────────────────────
print(f"{'='*76}")
print(f"  A16: P2 PLACE BET EV BY TIER")
print(f"{'='*76}")
print()

tier_p2_ev = {}
for p1_sp, p2_sp, gap, label, p1_won, p2_won, p1_plc, p2_plc, n in p1p2_data:
    div = place_divisor(n)
    if not div or not p2_sp:
        continue
    est_place_odds = (p2_sp - 1.0) / div + 1.0
    win_ev = (p2_sp - 1.0) if p2_won else -1.0
    plc_ev = (est_place_odds - 1.0) if p2_plc else -1.0
    if label not in tier_p2_ev:
        tier_p2_ev[label] = {"n":0,"win_ev":0.0,"plc_ev":0.0,"plc_n":0}
    s = tier_p2_ev[label]
    s["n"]      += 1
    s["win_ev"] += win_ev
    s["plc_ev"] += plc_ev
    if p2_plc: s["plc_n"] += 1

print(f"  {'Tier':<18} {'N':>5} {'PlcRate%':>9} {'AvgWinEV':>10} {'AvgPlcEV':>10} {'PlcBetter?':>11}")
print(f"  {'-'*68}")
for label in tier_order:
    s = tier_p2_ev.get(label)
    if not s or s["n"] == 0:
        continue
    avg_win  = s["win_ev"] / s["n"]
    avg_plc  = s["plc_ev"] / s["n"]
    plc_rate = s["plc_n"] / s["n"] * 100
    better   = "YES" if avg_plc > avg_win else "no"
    print(f"  {label:<18} {s['n']:>5} {plc_rate:>8.1f}% {avg_win:>10.3f} {avg_plc:>10.3f} {better:>11}")
print()

# ── A17: Flat stake P&L simulation (new) ─────────────────────────────────────
print(f"{'='*76}")
print(f"  A17: FLAT £2 STAKE P&L SIMULATION — win bets only, by tier")
print(f"  Uses SP decimal. Shows what the model would have made at flat stakes.")
print(f"{'='*76}")
print()

stake = 2.0
tier_flat = {}
for p1_sp, p2_sp, gap, label, p1_won, p2_won, p1_plc, p2_plc, n in p1p2_data:
    if not p1_sp:
        continue
    pnl_p1 = (p1_sp - 1) * stake if p1_won else -stake
    pnl_p2 = (p2_sp - 1) * stake if (p2_won and p2_sp) else (-stake if p2_sp else 0)
    if label not in tier_flat:
        tier_flat[label] = {"n":0,"p1_pnl":0.0,"p2_pnl":0.0,"p1w":0,"p2w":0}
    s = tier_flat[label]
    s["n"]     += 1
    s["p1_pnl"] += pnl_p1
    s["p2_pnl"] += pnl_p2
    if p1_won: s["p1w"] += 1
    if p2_won: s["p2w"] += 1

print(f"  {'Tier':<18} {'N':>5} {'P1 P&L':>9} {'P2 P&L':>9} {'Total P&L':>10} {'P1 ROI%':>9} {'P2 ROI%':>9}")
print(f"  {'-'*72}")
grand_p1 = grand_p2 = 0.0
for label in tier_order:
    s = tier_flat.get(label)
    if not s or s["n"] == 0:
        continue
    total   = s["p1_pnl"] + s["p2_pnl"]
    roi_p1  = s["p1_pnl"] / (s["n"] * stake) * 100
    roi_p2  = s["p2_pnl"] / (s["n"] * stake) * 100
    grand_p1 += s["p1_pnl"]
    grand_p2 += s["p2_pnl"]
    print(f"  {label:<18} {s['n']:>5} "
          f"{s['p1_pnl']:>+9.2f} {s['p2_pnl']:>+9.2f} {total:>+10.2f} "
          f"{roi_p1:>+8.1f}% {roi_p2:>+8.1f}%")
n_all = sum(s["n"] for s in tier_flat.values())
print(f"  {'TOTAL':<18} {n_all:>5} "
      f"{grand_p1:>+9.2f} {grand_p2:>+9.2f} {grand_p1+grand_p2:>+10.2f}")
print()

# ── A18: Going family performance (new) ──────────────────────────────────────
print(f"{'='*76}")
print(f"  A18: PICK PERFORMANCE BY GOING FAMILY")
print(f"{'='*76}")
print()

going_fam_stats = {}
for date, day in sorted(history.items()):
    for rec in day.get("races", []):
        rid = rec.get("race_id")
        raw = raw_index.get(rid)
        if not raw:
            continue
        runners = raw.get("runners", [])
        if len(runners) < 2:
            continue
        scored = sorted(
            [{**r, "score": score_runner(r)[0]} for r in runners],
            key=lambda x: -x["score"]
        )
        gf = going_family(raw.get("going", ""))
        p1_sp = to_float(scored[0].get("sp_dec"))
        accumulate(going_fam_stats, gf, rec, p1_sp)

going_fam_order = ["firm","gd/frm","good","gd/sft","soft","heavy","standard (AW)","fast (AW)","other"]
print_table("A18: BY GOING FAMILY", going_fam_stats, order=going_fam_order)


# ═══════════════════════════════════════════════════════════════════════════════
# PART B: RAW DATA ANALYSIS (market baselines, independent of paper history)
# ═══════════════════════════════════════════════════════════════════════════════

print("╔══════════════════════════════════════════════════════════════════════════╗")
print("║  PART B — RAW DATA ANALYSIS (full dataset, market-level baselines)      ║")
print("╚══════════════════════════════════════════════════════════════════════════╝")
print()
print(f"  Dataset: {len(races_all)} races across {len(set(r['_date'] for r in races_all))} days\n")

# ── Data quality audit ────────────────────────────────────────────────────────
NULL_VALS = {"", "-", "--", "None", "none"}
NUMERIC_FIELDS = {"rpr", "tsr", "sp_dec", "bsp"}

dq_fields = ["rpr", "or", "tsr", "sp_dec", "bsp", "trainer_14d", "jockey_14d"]
dq_by_date = {}
for race in races_all:
    date = race.get("_date", "?")
    if date not in dq_by_date:
        dq_by_date[date] = {fi: [0, 0] for fi in dq_fields}
    for r in race.get("runners", []):
        for field in dq_fields:
            dq_by_date[date][field][1] += 1
            v = r.get(field)
            if v is None:
                continue
            if isinstance(v, dict):
                if v.get("runs", 0) > 0:
                    dq_by_date[date][field][0] += 1
            else:
                sv = str(v).strip()
                if sv in NULL_VALS:
                    continue
                if field in NUMERIC_FIELDS:
                    try:
                        if float(sv) > 0:
                            dq_by_date[date][field][0] += 1
                    except:
                        pass
                else:
                    dq_by_date[date][field][0] += 1

warnings = []
for date in sorted(dq_by_date.keys()):
    for field in dq_fields:
        present, total = dq_by_date[date][field]
        pct = present / total * 100 if total else 0
        if pct < 80 and field != "bsp":
            warnings.append((date, field, pct, total))

if warnings:
    print("  DATA QUALITY WARNINGS (fields <80% coverage):")
    print(f"  {'Date':12} {'Field':14} {'Coverage':>10} {'Runners':>8}")
    print(f"  {'-'*48}")
    for date, field, pct, total in warnings:
        print(f"  {date:12} {field:14} {pct:>9.0f}%  {total:>8}")
    print()
    print("  NOTE: RPR/TSR drop to 0% from 2026-04-13 onwards in the raw results")
    print("  endpoint. OR, trainer_14d, jockey_14d, sp_dec remain reliable throughout.")
    print("  BSP is absent for recent dates (not yet settled). Sections using RPR")
    print("  (B10 RPR simulation) are only reliable for pre-Apr-13 card dates.")
    print()
else:
    print("  Data quality: all key fields >80% coverage across all dates.\n")

# ── B1: Dataset overview ──────────────────────────────────────────────────────
print(f"{'='*76}")
print(f"  B1: DATASET OVERVIEW")
print(f"{'='*76}")

going_counts   = defaultdict(int)
class_counts   = defaultdict(int)
surface_counts = defaultdict(int)
field_sizes    = []
rpr_vals = []
or_vals  = []

for race in races_all:
    runners = race.get("runners", [])
    field_sizes.append(len(runners))
    going_counts[race.get("going","?")] += 1
    class_counts[str(race.get("class","?"))] += 1
    surface_counts[race.get("surface","?")] += 1
    for r in runners:
        v = to_float(r.get("rpr"))
        if v and v > 0: rpr_vals.append(v)
        v = to_float(r.get("or"))
        if v and v > 0: or_vals.append(v)

print(f"  Avg field size:  {sum(field_sizes)/len(field_sizes):.1f} runners")
if rpr_vals:
    print(f"  RPR available:   {len(rpr_vals)} runner-records  avg={sum(rpr_vals)/len(rpr_vals):.1f}")
if or_vals:
    print(f"  OR available:    {len(or_vals)} runner-records   avg={sum(or_vals)/len(or_vals):.1f}")
print(f"\n  Surface breakdown:")
for k,v in sorted(surface_counts.items(), key=lambda x:-x[1]):
    print(f"    {k:20} {v:>4} races ({v/len(races_all)*100:.1f}%)")
print(f"\n  Going breakdown (top 10):")
for k,v in sorted(going_counts.items(), key=lambda x:-x[1])[:10]:
    print(f"    {k:30} {v:>4} races")
print(f"\n  Class breakdown:")
for k,v in sorted(class_counts.items(), key=lambda x: (x[0]=='?', x[0])):
    print(f"    Class {k:5} {v:>4} races ({v/len(races_all)*100:.1f}%)")
print()

# ── B2: Favourite performance ─────────────────────────────────────────────────
print(f"{'='*76}")
print(f"  B2: FAVOURITE PERFORMANCE (market baseline)")
print(f"{'='*76}")

fav_win = fav_place = fav_total = 0
fav_by_surface  = defaultdict(lambda: [0,0,0])
fav_by_class    = defaultdict(lambda: [0,0,0])
fav_by_going    = defaultdict(lambda: [0,0,0])
fav_by_field    = defaultdict(lambda: [0,0,0])

for race in races_all:
    runners  = race.get("runners", [])
    nrunners = len(runners)
    ps       = place_spots_for(nrunners)
    valid    = []
    for r in runners:
        sp = to_float(r.get("sp_dec")) or to_float(r.get("bsp"))
        p  = to_int(r.get("position"))
        if sp and sp > 0:
            valid.append((sp, p, r))
    if not valid:
        continue
    valid.sort(key=lambda x: x[0])
    fav_sp, fav_pos, _ = valid[0]
    surf = race.get("surface", "?")
    cls  = str(race.get("class", "?"))
    gf   = going_family(race.get("going", ""))
    if nrunners <= 5:    fb = "2-5"
    elif nrunners <= 8:  fb = "6-8"
    elif nrunners <= 12: fb = "9-12"
    else:                fb = "13+"

    fav_total += 1
    fav_by_surface[surf][2] += 1
    fav_by_class[cls][2]    += 1
    fav_by_going[gf][2]     += 1
    fav_by_field[fb][2]     += 1

    if fav_pos == 1:
        fav_win += 1
        fav_by_surface[surf][0] += 1
        fav_by_class[cls][0]    += 1
        fav_by_going[gf][0]     += 1
        fav_by_field[fb][0]     += 1
    if fav_pos and fav_pos <= ps:
        fav_place += 1
        fav_by_surface[surf][1] += 1
        fav_by_class[cls][1]    += 1
        fav_by_going[gf][1]     += 1
        fav_by_field[fb][1]     += 1

print(f"  Overall:  win={fav_win}/{fav_total} ({fav_win/fav_total*100:.1f}%)  "
      f"place={fav_place}/{fav_total} ({fav_place/fav_total*100:.1f}%)\n")

def print_fav_breakdown(title, d, order=None):
    print(f"  {title}:")
    print(f"    {'Key':<20} {'N':>5} {'Win%':>8} {'Place%':>9}")
    items = [(k, d[k]) for k in order if k in d] if order else sorted(d.items(), key=lambda x: -x[1][2])
    for k, (w, pl, t) in items:
        if t:
            print(f"    {k:<20} {t:>5} {w/t*100:>7.1f}%  {pl/t*100:>8.1f}%")
    print()

print_fav_breakdown("By surface", fav_by_surface)
print_fav_breakdown("By going family", fav_by_going,
    order=["firm","gd/frm","good","gd/sft","soft","heavy","standard (AW)","fast (AW)","other"])
print_fav_breakdown("By field size", fav_by_field, order=["2-5","6-8","9-12","13+"])
print_fav_breakdown("By class", fav_by_class,
    order=[str(i) for i in range(1,8)] + ["?"]) 

# ── B3: Price band win/place rates (all runners) ──────────────────────────────
print(f"{'='*76}")
print(f"  B3: WIN/PLACE RATES BY SP PRICE BAND (all runners)")
print(f"{'='*76}")

sp_bands = [
    ("1.0–1.5",  1.0,  1.5),
    ("1.5–2.0",  1.5,  2.0),
    ("2.0–3.0",  2.0,  3.0),
    ("3.0–5.0",  3.0,  5.0),
    ("5.0–8.0",  5.0,  8.0),
    ("8.0–15.0", 8.0, 15.0),
    ("15.0+",   15.0, 9999),
]
band_stats = {b[0]: [0,0,0] for b in sp_bands}

for race in races_all:
    runners  = race.get("runners", [])
    nrunners = len(runners)
    ps       = place_spots_for(nrunners)
    for r in runners:
        sp = to_float(r.get("sp_dec")) or to_float(r.get("bsp"))
        p  = to_int(r.get("position"))
        if not sp or sp <= 0:
            continue
        for label, lo, hi in sp_bands:
            if lo <= sp < hi:
                band_stats[label][2] += 1
                if p == 1:          band_stats[label][0] += 1
                if p and p <= ps:   band_stats[label][1] += 1
                break

print(f"\n  {'Band':12} {'Runners':>8} {'Win%':>8} {'Place%':>9} {'Implied%':>10} {'Edge':>8}")
print(f"  {'-'*58}")
for label, lo, hi in sp_bands:
    w, pl, t = band_stats[label]
    if not t:
        continue
    mid_sp   = (lo + min(hi, lo*2)) / 2      # rough midpoint
    implied  = 1.0 / mid_sp * 100
    actual   = w / t * 100
    edge     = actual - implied
    print(f"  {label:12} {t:>8d} {actual:>7.1f}%  {pl/t*100:>8.1f}%  {implied:>9.1f}%  {edge:>+7.1f}%")
print()

# ── B4: Draw bias ─────────────────────────────────────────────────────────────
print(f"{'='*76}")
print(f"  B4: DRAW BIAS (flat/AW races only)")
print(f"{'='*76}")
print()

draw_stats     = defaultdict(lambda: [0,0,0])
draw_by_surf   = defaultdict(lambda: defaultdict(lambda: [0,0,0]))

for race in races_all:
    if race.get("jumps"):
        continue
    runners  = race.get("runners", [])
    nrunners = len(runners)
    ps       = place_spots_for(nrunners)
    surf     = race.get("surface", "?")
    for r in runners:
        draw = to_int(r.get("draw"))
        p    = to_int(r.get("position"))
        if draw is None or draw < 1 or draw > 20:
            continue
        draw_stats[draw][2] += 1
        draw_by_surf[surf][draw][2] += 1
        if p == 1:
            draw_stats[draw][0] += 1
            draw_by_surf[surf][draw][0] += 1
        if p and p <= ps:
            draw_stats[draw][1] += 1
            draw_by_surf[surf][draw][1] += 1

# Grouped
groups = [("Stalls 1-4",1,4),("Stalls 5-8",5,8),("Stalls 9+",9,99)]
print(f"  {'Group':<14} {'Runners':>8} {'Win%':>8} {'Place%':>9}")
print(f"  {'-'*42}")
for glabel, lo, hi in groups:
    gw = gpl = gt = 0
    for stall, (w,pl,t) in draw_stats.items():
        if lo <= stall <= hi:
            gw+=w; gpl+=pl; gt+=t
    if gt:
        print(f"  {glabel:<14} {gt:>8} {gw/gt*100:>7.1f}%  {gpl/gt*100:>8.1f}%")

print()
print(f"  By surface (grouped):")
for surf in sorted(draw_by_surf.keys()):
    gw = gpl = gt = 0
    for stall, (w,pl,t) in draw_by_surf[surf].items():
        gw+=w; gpl+=pl; gt+=t
    if gt:
        low_w=low_pl=low_t=high_w=high_pl=high_t=0
        for stall, (w,pl,t) in draw_by_surf[surf].items():
            if stall <= 4: low_w+=w; low_pl+=pl; low_t+=t
            else:          high_w+=w; high_pl+=pl; high_t+=t
        print(f"    {surf}: low stalls win={low_w/low_t*100:.0f}% (n={low_t})  "
              f"high stalls win={high_w/high_t*100:.0f}% (n={high_t})" if low_t and high_t else f"    {surf}: n={gt}")
print()

# ── B5: Trainer 14-day form ───────────────────────────────────────────────────
print(f"{'='*76}")
print(f"  B5: TRAINER 14-DAY STRIKE RATE vs ACTUAL WIN RATE")
print(f"{'='*76}")
print()

def parse_form_dict(v):
    """Extract (win_pct, ae) from a trainer/jockey 14d dict or return (None, None)."""
    if not v:
        return None, None
    if isinstance(v, dict):
        runs = v.get("runs", 0)
        if not runs:
            return None, None
        return v.get("win_pct", 0), v.get("ae")
    # Fallback: old "wins/runs" string format
    try:
        wins, runs = map(int, str(v).split("/"))
        return (wins / runs if runs else 0), None
    except:
        return None, None

form_bands = [
    ("0%  (0/n)",  0,     0.001),
    ("1–10%",      0.001, 0.10),
    ("10–20%",     0.10,  0.20),
    ("20–30%",     0.20,  0.30),
    ("30%+",       0.30,  1.01),
]

# t14_stats: [wins, places, total, ae_sum, ae_n]
t14_stats = {b[0]: [0, 0, 0, 0.0, 0] for b in form_bands}

for race in races_all:
    runners  = race.get("runners", [])
    nrunners = len(runners)
    ps       = place_spots_for(nrunners)
    for r in runners:
        rate, ae = parse_form_dict(r.get("trainer_14d"))
        if rate is None:
            continue
        p = to_int(r.get("position"))
        for label, lo, hi in form_bands:
            if lo <= rate < hi:
                t14_stats[label][2] += 1
                if p == 1:        t14_stats[label][0] += 1
                if p and p <= ps: t14_stats[label][1] += 1
                if ae is not None:
                    t14_stats[label][3] += ae
                    t14_stats[label][4] += 1
                break

print(f"  {'Trainer 14d':16} {'Runners':>8} {'Win%':>8} {'Place%':>9} {'AvgA/E':>8}")
print(f"  {'-'*52}")
for label, lo, hi in form_bands:
    w, pl, t, ae_sum, ae_n = t14_stats[label]
    if t:
        avg_ae = ae_sum / ae_n if ae_n else 0
        print(f"  {label:16} {t:>8} {w/t*100:>7.1f}%  {pl/t*100:>8.1f}%  {avg_ae:>7.2f}")
print()

# ── B6: Jockey 14-day form ────────────────────────────────────────────────────
print(f"{'='*76}")
print(f"  B6: JOCKEY 14-DAY STRIKE RATE vs ACTUAL WIN RATE")
print(f"{'='*76}")
print()

j14_stats = {b[0]: [0, 0, 0, 0.0, 0] for b in form_bands}

for race in races_all:
    runners  = race.get("runners", [])
    nrunners = len(runners)
    ps       = place_spots_for(nrunners)
    for r in runners:
        rate, ae = parse_form_dict(r.get("jockey_14d"))
        if rate is None:
            continue
        p = to_int(r.get("position"))
        for label, lo, hi in form_bands:
            if lo <= rate < hi:
                j14_stats[label][2] += 1
                if p == 1:        j14_stats[label][0] += 1
                if p and p <= ps: j14_stats[label][1] += 1
                if ae is not None:
                    j14_stats[label][3] += ae
                    j14_stats[label][4] += 1
                break

print(f"  {'Jockey 14d':16} {'Runners':>8} {'Win%':>8} {'Place%':>9} {'AvgA/E':>8}")
print(f"  {'-'*52}")
for label, lo, hi in form_bands:
    w, pl, t, ae_sum, ae_n = j14_stats[label]
    if t:
        avg_ae = ae_sum / ae_n if ae_n else 0
        print(f"  {label:16} {t:>8} {w/t*100:>7.1f}%  {pl/t*100:>8.1f}%  {avg_ae:>7.2f}")
print()

# ── B7: Course-level patterns ─────────────────────────────────────────────────
print(f"{'='*76}")
print(f"  B7: COURSE-LEVEL PATTERNS (top 15 by volume)")
print(f"{'='*76}")
print()

course_stats = defaultdict(lambda: {"n":0,"fav_win":0,"fav_plc":0,"avg_field":[],"avg_sp":[]})

for race in races_all:
    runners  = race.get("runners", [])
    nrunners = len(runners)
    ps       = place_spots_for(nrunners)
    course   = race.get("course", "?")
    cs       = course_stats[course]
    cs["n"] += 1
    cs["avg_field"].append(nrunners)

    valid = []
    for r in runners:
        sp = to_float(r.get("sp_dec")) or to_float(r.get("bsp"))
        p  = to_int(r.get("position"))
        if sp and sp > 0:
            valid.append((sp, p))
            cs["avg_sp"].append(sp)
    if not valid:
        continue
    valid.sort(key=lambda x: x[0])
    fav_sp, fav_pos = valid[0]
    if fav_pos == 1:              cs["fav_win"] += 1
    if fav_pos and fav_pos <= ps: cs["fav_plc"] += 1

top_courses = sorted(course_stats.items(), key=lambda x: -x[1]["n"])[:15]
print(f"  {'Course':<22} {'Races':>6} {'AvgField':>9} {'AvgSP':>7} {'FavWin%':>9} {'FavPlc%':>9}")
print(f"  {'-'*65}")
for course, cs in top_courses:
    n      = cs["n"]
    fw_pct = cs["fav_win"] / n * 100 if n else 0
    fp_pct = cs["fav_plc"] / n * 100 if n else 0
    af     = sum(cs["avg_field"])/len(cs["avg_field"]) if cs["avg_field"] else 0
    asp    = sum(cs["avg_sp"])/len(cs["avg_sp"]) if cs["avg_sp"] else 0
    print(f"  {course:<22} {n:>6} {af:>9.1f} {asp:>7.2f} {fw_pct:>8.1f}%  {fp_pct:>8.1f}%")
print()

# ── B8: Race class patterns ───────────────────────────────────────────────────
print(f"{'='*76}")
print(f"  B8: RACE CLASS — fav win rate and field size")
print(f"{'='*76}")
print()

class_stats = defaultdict(lambda: {"n":0,"fav_win":0,"fav_plc":0,"fields":[],"sps":[]})

for race in races_all:
    runners  = race.get("runners", [])
    nrunners = len(runners)
    ps       = place_spots_for(nrunners)
    cls      = str(race.get("class", "?"))
    cs       = class_stats[cls]
    cs["n"] += 1
    cs["fields"].append(nrunners)

    valid = [(to_float(r.get("sp_dec")) or to_float(r.get("bsp")),
              to_int(r.get("position"))) for r in runners]
    valid = [(sp,p) for sp,p in valid if sp and sp>0]
    if not valid:
        continue
    cs["sps"].extend(sp for sp,_ in valid)
    valid.sort(key=lambda x: x[0])
    fsp, fp = valid[0]
    if fp == 1:         cs["fav_win"] += 1
    if fp and fp <= ps: cs["fav_plc"] += 1

print(f"  {'Class':<8} {'Races':>6} {'AvgField':>9} {'AvgSP':>7} {'FavWin%':>9} {'FavPlc%':>9}")
print(f"  {'-'*55}")
for cls in [str(i) for i in range(1,8)] + ["?"]:
    cs = class_stats.get(cls)
    if not cs or cs["n"] == 0:
        continue
    af  = sum(cs["fields"])/len(cs["fields"])
    asp = sum(cs["sps"])/len(cs["sps"]) if cs["sps"] else 0
    fw  = cs["fav_win"] / cs["n"] * 100
    fp  = cs["fav_plc"] / cs["n"] * 100
    print(f"  {cls:<8} {cs['n']:>6} {af:>9.1f} {asp:>7.2f} {fw:>8.1f}%  {fp:>8.1f}%")
print()

# ── B9: Daily summary ─────────────────────────────────────────────────────────
print(f"{'='*76}")
print(f"  B9: DAILY SUMMARY")
print(f"{'='*76}")
print()
print(f"  {'Date':12} {'Races':>6} {'AvgField':>9} {'AvgSP':>7} {'FavWin%':>9} {'FavPlc%':>9}")
print(f"  {'-'*58}")

for date in sorted(set(r["_date"] for r in races_all)):
    day_races = [r for r in races_all if r["_date"] == date]
    fields    = [len(r.get("runners",[])) for r in day_races]
    fw = fpl = ft = 0
    all_sps = []
    for race in day_races:
        runners  = race.get("runners", [])
        nrunners = len(runners)
        ps       = place_spots_for(nrunners)
        valid    = [(to_float(r.get("sp_dec")) or to_float(r.get("bsp")),
                     to_int(r.get("position"))) for r in runners]
        valid    = [(sp,p) for sp,p in valid if sp and sp>0]
        if not valid:
            continue
        all_sps.extend(sp for sp,_ in valid)
        valid.sort(key=lambda x: x[0])
        fsp, fpos = valid[0]
        ft += 1
        if fpos == 1:         fw  += 1
        if fpos and fpos <= ps: fpl += 1
    avg_f   = sum(fields)/len(fields) if fields else 0
    avg_sp  = sum(all_sps)/len(all_sps) if all_sps else 0
    fw_pct  = fw/ft*100 if ft else 0
    fpl_pct = fpl/ft*100 if ft else 0
    print(f"  {date:12} {len(day_races):>6} {avg_f:>9.1f} {avg_sp:>7.2f} {fw_pct:>8.1f}%  {fpl_pct:>8.1f}%")

# ── B10: Card-based RPR pick simulation ───────────────────────────────────────
print()
print(f"{'='*76}")
print(f"  B10: RPR-RANKED PICK SIMULATION (card dates: {sorted(cards_by_date.keys())})")
print(f"  Ranks runners by pre-race RPR, checks top-2 vs actual results.")
print(f"  NOTE: RPR is 0% in raw results from Apr 13+, so card dates Apr 22-26")
print(f"  will show empty results here. Only Apr 10 card has reliable RPR coverage.")
print(f"{'='*76}")
print()

if not cards_by_date:
    print("  No cards available — skipping")
else:
    sim_stats = defaultdict(lambda: {"n":0,"p1w":0,"p1p":0,"p2w":0,"p2p":0,"both_p":0})

    for date, card_idx in sorted(cards_by_date.items()):
        raw_path = os.path.join("data/raw", f"{date}.json")
        if not os.path.exists(raw_path):
            continue
        with open(raw_path) as fh:
            rd = json.load(fh)
        raw_races = rd.get("results") or rd.get("races") or []
        raw_idx   = {(r.get("course",""), r.get("off","")): r for r in raw_races}

        for key, card_race in card_idx.items():
            raw_race = raw_idx.get(key)
            if not raw_race:
                continue
            card_runners = card_race.get("runners") or card_race.get("all_runners") or []
            raw_runners  = raw_race.get("runners", [])
            nrunners     = len(raw_runners)
            ps           = place_spots_for(nrunners)
            res_by_horse = {r.get("horse","").lower(): r for r in raw_runners}

            ranked = sorted(
                [(to_float(r.get("rpr")), r.get("horse","").lower()) for r in card_runners
                 if to_float(r.get("rpr")) and to_float(r.get("rpr")) > 0],
                reverse=True
            )
            if len(ranked) < 2:
                continue

            surf = raw_race.get("surface", "?")
            gf   = going_family(raw_race.get("going", ""))

            for bkt in ["all", surf, gf]:
                s  = sim_stats[bkt]
                s["n"] += 1
                res1 = res_by_horse.get(ranked[0][1])
                res2 = res_by_horse.get(ranked[1][1])
                p1_won = p1_plc = p2_won = p2_plc = False
                if res1:
                    p = to_int(res1.get("position"))
                    if p == 1:        p1_won = True
                    if p and p <= ps: p1_plc = True
                if res2:
                    p = to_int(res2.get("position"))
                    if p == 1:        p2_won = True
                    if p and p <= ps: p2_plc = True
                if p1_won: s["p1w"] += 1
                if p1_plc: s["p1p"] += 1
                if p2_won: s["p2w"] += 1
                if p2_plc: s["p2p"] += 1
                if p1_plc and p2_plc: s["both_p"] += 1

    print(f"  {'Bucket':<20} {'N':>5} {'P1win%':>8} {'P1plc%':>8} {'P2win%':>8} {'P2plc%':>8} {'BothPlc%':>9}")
    print(f"  {'-'*68}")
    for bkt in ["all"] + [k for k in sim_stats if k != "all"]:
        s = sim_stats.get(bkt)
        if not s or s["n"] == 0:
            continue
        n = s["n"]
        print(f"  {bkt:<20} {n:>5} "
              f"{s['p1w']/n*100:>7.1f}%  "
              f"{s['p1p']/n*100:>7.1f}%  "
              f"{s['p2w']/n*100:>7.1f}%  "
              f"{s['p2p']/n*100:>7.1f}%  "
              f"{s['both_p']/n*100:>8.1f}%")
    print()

print("═"*76)
print("  Done.")
print("═"*76)
