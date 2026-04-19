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
