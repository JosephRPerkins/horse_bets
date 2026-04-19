"""
analyse_history.py
Cross-references paper trading history with raw results to show
tier-level performance, pick hit rates, going/field/time/odds patterns.
"""
import json
import glob
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from predict    import score_runner, place_terms
from predict_v2 import race_confidence, TIER_LABELS, TIER_SUPREME, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP

# ── Load data ─────────────────────────────────────────────────────────────────

history = {}
for f in sorted(glob.glob("data/history/*.json")):
    with open(f) as fp:
        d = json.load(fp)
    history[d["date"]] = d

raw_index = {}
for f in sorted(glob.glob("data/raw/*.json")):
    with open(f) as fp:
        d = json.load(fp)
    for race in d.get("results", []):
        raw_index[race["race_id"]] = race

print(f"Loaded {len(history)} days of history, {len(raw_index)} raw races")
print()

# ── Helpers ───────────────────────────────────────────────────────────────────

def to_float(v):
    try: return float(str(v).strip())
    except: return None

def time_bucket(off_dt):
    # Parse from ISO timestamp (e.g. "2026-04-18T19:45:00+01:00") — reliable local time
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
    if rec.get("std_win"):              t["std_win"]  += 1
    if rec.get("cons_win"):             t["cons_win"] += 1
    if rec.get("std_a") or rec.get("cons_a"): t["a_placed"] += 1
    if rec.get("std_b") or rec.get("cons_b"): t["b_placed"] += 1
    if rec.get("a_pos") == 1:           t["a_win"]    += 1
    if rec.get("b_pos") == 1:           t["b_win"]    += 1
    if p1_sp is not None:
        t["sp_sum"] += p1_sp
        t["sp_n"]   += 1

# ── Cross-reference ───────────────────────────────────────────────────────────

tier_stats   = {}
going_stats  = {}
field_stats  = {}
time_stats   = {}
odds_stats   = {}
tier_sp_list = {}   # tier label -> list of P1 SP dec values
matched      = 0
unmatched    = 0

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
            key=lambda x: -x["score"], reverse=False
        )
        scored.sort(key=lambda x: x["score"], reverse=True)

        raw2      = {**raw, "runners": scored}
        win_score = scored[0]["score"] if scored else 0
        tier, _   = race_confidence(raw2, win_score)
        label     = TIER_LABELS.get(tier, "?").split()[0]

        going     = (raw.get("going") or "unknown").lower()
        n_runners = len(runners)
        off       = raw.get("off_dt", "unknown")
        p1_sp     = to_float(scored[0].get("sp_dec")) if scored else None

        # Field size bucket
        if n_runners <= 5:    field = "2-5"
        elif n_runners <= 8:  field = "6-8"
        elif n_runners <= 12: field = "9-12"
        else:                 field = "13+"

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

# ── Output helpers ────────────────────────────────────────────────────────────

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

# ── Section 1: Tier ───────────────────────────────────────────────────────────
print(f"Matched: {matched} | Unmatched (no raw data): {unmatched}")
print()

tier_order = ["🔥🔥🔥", "🔥🔥", "🔥", "·", "✗"]
print_table("BY TIER", tier_stats, order=tier_order)

# ── Section 2: Field size ─────────────────────────────────────────────────────
field_order = ["2-5", "6-8", "9-12", "13+"]
print_table("BY FIELD SIZE", field_stats, order=field_order)

# ── Section 3: Going (top 10 by volume) ──────────────────────────────────────
print_table("BY GOING (top 10 by volume)", going_stats, top=10)

# ── Section 4: Time of day ────────────────────────────────────────────────────
time_order = ["before 1pm","1pm-2pm","2pm-3pm","3pm-4pm","4pm-5pm","5pm-6pm","after 6pm"]
print_table("BY TIME OF DAY", time_stats, order=time_order)

# ── Section 5: P1 SP odds band ────────────────────────────────────────────────
odds_order = ["odds-on  <2.0","2.0-2.9","3.0-4.9","5.0-7.9","8.0-12.9","13.0+"]
print_table("BY P1 SP ODDS BAND", odds_stats, order=odds_order)

# ── Section 6: Average P1 SP by tier ─────────────────────────────────────────
print(f"{'='*76}")
print(f"  AVERAGE P1 SP BY TIER")
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

# ── Section 7: Summary totals ─────────────────────────────────────────────────
all_total = sum(t["total"]    for t in tier_stats.values())
all_stdw  = sum(t["std_win"]  for t in tier_stats.values())
all_consw = sum(t["cons_win"] for t in tier_stats.values())
all_awin  = sum(t["a_win"]    for t in tier_stats.values())
all_bwin  = sum(t["b_win"]    for t in tier_stats.values())
all_aplc  = sum(t["a_placed"] for t in tier_stats.values())
all_bplc  = sum(t["b_placed"] for t in tier_stats.values())

print(f"{'='*76}")
print(f"  TOTALS ({all_total} races across {len(history)} days)")
print(f"{'='*76}")
print(f"  Pick 1 win rate:    {all_awin/all_total*100:.1f}%  ({all_awin}/{all_total})")
print(f"  Pick 2 win rate:    {all_bwin/all_total*100:.1f}%  ({all_bwin}/{all_total})")
print(f"  Pick 1 place rate:  {all_aplc/all_total*100:.1f}%  ({all_aplc}/{all_total})")
print(f"  Pick 2 place rate:  {all_bplc/all_total*100:.1f}%  ({all_bplc}/{all_total})")
print(f"  Both placed (std):  {all_stdw/all_total*100:.1f}%  ({all_stdw}/{all_total})")
print(f"  Both placed (cons): {all_consw/all_total*100:.1f}%  ({all_consw}/{all_total})")
print()

# ── Section 8: Winner rank distribution ───────────────────────────────────────

winner_rank  = {}
p1p2_data    = []   # (p1_sp, p2_sp, gap, tier_label, p1_won, p2_won, p1_plc, p2_plc, n_runners)
race_cats    = []   # (category, tier, gap, n, p1_sp, p2_sp, market_agrees)

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
        raw2     = {**raw, "runners": scored}
        tier, _  = race_confidence(raw2, scored[0]["score"])
        label    = TIER_LABELS.get(tier, "?").split()[0]
        p1       = scored[0]
        p2       = scored[1]
        p1_sp    = to_float(p1.get("sp_dec"))
        p2_sp    = to_float(p2.get("sp_dec"))
        gap      = p1.get("score", 0) - p2.get("score", 0)
        n        = len(runners)
        p1_won   = rec.get("a_pos") == 1
        p2_won   = rec.get("b_pos") == 1
        p1_plc   = bool(rec.get("std_a") or rec.get("cons_a"))
        p2_plc   = bool(rec.get("std_b") or rec.get("cons_b"))
        mkt_ok   = bool(p1_sp and p2_sp and p1_sp <= p2_sp)

        # Winner rank
        winner = next((r for r in runners if str(r.get("position", "")) == "1"), None)
        rank = None
        if winner:
            for i, r in enumerate(scored):
                if r.get("horse") == winner.get("horse"):
                    rank = i + 1
                    break
        winner_rank[rank] = winner_rank.get(rank, 0) + 1

        p1p2_data.append((p1_sp, p2_sp, gap, label, p1_won, p2_won, p1_plc, p2_plc, n))

        # Race category
        if not p1_plc and not p2_plc:
            cat = "both_loss"
        elif p1_won or p2_won:
            cat = "both_win"
        else:
            cat = "place_only_win"
        race_cats.append((cat, label, gap, n, p1_sp, p2_sp, mkt_ok))

print(f"{'='*76}")
print(f"  WINNER RANK IN MODEL SCORING")
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

# ── Section 9: P1/P2 price ratio ─────────────────────────────────────────────

ratio_stats = {}
for p1_sp, p2_sp, gap, label, p1_won, p2_won, p1_plc, p2_plc, n in p1p2_data:
    if not p1_sp or not p2_sp:
        continue
    ratio = p2_sp / p1_sp
    if ratio < 1.0:      rbkt = "P2 shorter than P1"
    elif ratio < 1.5:    rbkt = "P2 1.0-1.5x P1"
    elif ratio < 2.5:    rbkt = "P2 1.5-2.5x P1"
    elif ratio < 4.0:    rbkt = "P2 2.5-4.0x P1"
    else:                rbkt = "P2 4x+ P1"
    if rbkt not in ratio_stats:
        ratio_stats[rbkt] = {"n":0,"p1w":0,"p2w":0,"p1p":0,"p2p":0}
    s = ratio_stats[rbkt]
    s["n"]  += 1
    if p1_won: s["p1w"] += 1
    if p2_won: s["p2w"] += 1
    if p1_plc: s["p1p"] += 1
    if p2_plc: s["p2p"] += 1

print(f"{'='*76}")
print(f"  P1/P2 PRICE RATIO — does relative price predict which pick wins?")
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

# ── Section 10: Score gap vs win rate ─────────────────────────────────────────

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
print(f"  P1-P2 SCORE GAP — does a bigger gap mean P1 is more reliable?")
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

# ── Section 11: Combined signal ───────────────────────────────────────────────

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
print(f"  COMBINED SIGNAL: score gap + market agreement")
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

# ── Section 12: Race category profiles ───────────────────────────────────────

def profile_cat(races, name):
    if not races:
        return
    n = len(races)
    tiers = {}
    for _, t, *_ in races:
        tiers[t] = tiers.get(t, 0) + 1
    gaps      = [g for _, _, g, *_ in races]
    fields    = [f for _, _, _, f, *_ in races]
    sps       = [p for _, _, _, _, p, *_ in races if p]
    agrees    = sum(1 for *_, m in races if m)
    print(f"  {name} ({n} races)")
    print(f"    Tiers:          {dict(sorted(tiers.items(), key=lambda x: -x[1]))}")
    print(f"    Avg score gap:  {sum(gaps)/len(gaps):.2f}  "
          f"(gap>=3: {sum(1 for g in gaps if g>=3)}/{n} = {sum(1 for g in gaps if g>=3)/n*100:.0f}%)")
    print(f"    Avg field size: {sum(fields)/len(fields):.1f}")
    print(f"    Avg P1 SP:      {sum(sps)/len(sps):.2f}" if sps else "    Avg P1 SP: n/a")
    print(f"    Market agrees:  {agrees}/{n} ({agrees/n*100:.0f}%)")
    print()

print(f"{'='*76}")
print(f"  RACE OUTCOME CATEGORY PROFILES")
print(f"{'='*76}")
print()
both_loss  = [r for r in race_cats if r[0] == "both_loss"]
place_win  = [r for r in race_cats if r[0] == "place_only_win"]
both_win   = [r for r in race_cats if r[0] == "both_win"]
profile_cat(both_loss, "BOTH LOSS — neither pick placed")
profile_cat(place_win, "PLACE-WIN ONLY — win bet lost, place saved it")
profile_cat(both_win,  "BOTH WIN — win + place both returned")

# ── Section 13: Both-loss predictors ─────────────────────────────────────────

print(f"{'='*76}")
print(f"  BOTH-LOSS RATE BY KEY VARIABLES")
print(f"{'='*76}")
all_races = race_cats

print()
print("  By score gap:")
print(f"  {'Gap':<12} {'Total':>6} {'BothLoss':>10} {'Rate':>8}")
for thresh, label in [(0,"gap=0"),(1,"gap=1"),(2,"gap=2")]:
    grp = [c for c, _, g, *_ in all_races if g == thresh]
    bl  = sum(1 for c in grp if c == "both_loss")
    if grp:
        print(f"  {label:<12} {len(grp):>6} {bl:>10} {bl/len(grp)*100:>7.1f}%")
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

# ── Section 14: Place bet EV summary ─────────────────────────────────────────

print(f"{'='*76}")
print(f"  PLACE BET VALUE — estimated EV by race conditions")
print(f"  Using BSP to estimate place return: place_odds = (win_bsp-1)/divisor + 1")
print(f"  Betfair place divisors: 5-7 runners=4, 8-11=5, 12+=6")
print(f"{'='*76}")
print()

def place_divisor(n):
    if n <= 4:  return None   # win only
    if n <= 7:  return 4.0
    if n <= 11: return 5.0
    return 6.0

ev_stats = {}
for p1_sp, p2_sp, gap, label, p1_won, p2_won, p1_plc, p2_plc, n in p1p2_data:
    # Use BSP from raw data if available, fall back to sp_dec
    div = place_divisor(n)
    if not div or not p1_sp:
        continue
    est_place_odds = (p1_sp - 1.0) / div + 1.0

    # EV of £1 place bet = P(place) * (est_place_odds - 1) - P(not place)
    p_place = 1.0 if p1_plc else 0.0   # actual outcome
    win_ev  = (p1_sp - 1.0) if p1_won else -1.0
    plc_ev  = (est_place_odds - 1.0) if p1_plc else -1.0

    # Bucket by field size
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
    avg_win = s["win_ev"] / s["n"]
    avg_plc = s["plc_ev"] / s["n"]
    plc_rate = s["p1_plc"] / s["n"] * 100
    better = "YES" if avg_plc > avg_win else "no"
    print(f"  {fbkt:<22} {s['n']:>5} {plc_rate:>8.1f}% {avg_win:>10.3f} {avg_plc:>10.3f} {better:>11}")
print()
print(f"  Note: EV per £1 staked. Positive = profitable long-run, negative = losing.")
print(f"  Place EV uses estimated Betfair place odds from win BSP and standard divisors.")
print()
