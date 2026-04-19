"""
analyse_history.py
Cross-references paper trading history with raw results to show
tier-level performance, pick hit rates, and course/going patterns.
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

# ── Cross-reference ───────────────────────────────────────────────────────────

tier_stats  = {}
going_stats = {}
course_stats = {}
field_stats = {}   # by field size bucket
matched     = 0
unmatched   = 0

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

        # Score runners to find top picks
        scored = []
        for r in runners:
            sc, _ = score_runner(r)
            scored.append({**r, "score": sc})
        scored.sort(key=lambda x: x["score"], reverse=True)

        raw2      = {**raw, "runners": scored}
        win_score = scored[0]["score"] if scored else 0
        tier, _   = race_confidence(raw2, win_score)
        label     = TIER_LABELS.get(tier, "?").split()[0]

        going      = (raw.get("going") or "unknown").lower()
        course     = raw.get("course", "unknown")
        n_runners  = len(runners)

        # Field size bucket
        if n_runners <= 5:
            field = "2-5"
        elif n_runners <= 8:
            field = "6-8"
        elif n_runners <= 12:
            field = "9-12"
        else:
            field = "13+"

        # Init stats dicts
        for d_dict, key in [(tier_stats, label), (going_stats, going),
                            (field_stats, field)]:
            if key not in d_dict:
                d_dict[key] = {
                    "total": 0, "std_win": 0, "cons_win": 0,
                    "a_placed": 0, "b_placed": 0,
                    "a_win": 0, "b_win": 0,
                }

        for d_dict, key in [(tier_stats, label), (going_stats, going),
                            (field_stats, field)]:
            t = d_dict[key]
            t["total"]    += 1
            if rec.get("std_win"):   t["std_win"]   += 1
            if rec.get("cons_win"):  t["cons_win"]  += 1
            if rec.get("std_a") or rec.get("cons_a"): t["a_placed"] += 1
            if rec.get("std_b") or rec.get("cons_b"): t["b_placed"] += 1
            # Win = finished 1st
            if rec.get("a_pos") == 1: t["a_win"] += 1
            if rec.get("b_pos") == 1: t["b_win"] += 1

# ── Output ────────────────────────────────────────────────────────────────────

print(f"Matched: {matched} | Unmatched (no raw data): {unmatched}")
print()

def print_table(title, stats, sort_by="total"):
    print(f"{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")
    print(f"  {'Key':<16} {'N':>5} {'P1win%':>7} {'P2win%':>7} "
          f"{'P1plc%':>7} {'P2plc%':>7} {'Both%':>7} {'Cons%':>7}")
    print(f"  {'-'*64}")
    for key, t in sorted(stats.items(), key=lambda x: -x[1][sort_by]):
        n = t["total"]
        if n == 0:
            continue
        print(f"  {key:<16} {n:>5} "
              f"{t['a_win']/n*100:>6.1f}% "
              f"{t['b_win']/n*100:>6.1f}% "
              f"{t['a_placed']/n*100:>6.1f}% "
              f"{t['b_placed']/n*100:>6.1f}% "
              f"{t['std_win']/n*100:>6.1f}% "
              f"{t['cons_win']/n*100:>6.1f}%")
    print()

print_table("BY TIER", tier_stats)
print_table("BY FIELD SIZE", field_stats)
print_table("BY GOING (top 10)", dict(sorted(going_stats.items(),
            key=lambda x: -x[1]["total"])[:10]))

# ── Summary totals ────────────────────────────────────────────────────────────
all_total = sum(t["total"]   for t in tier_stats.values())
all_stdw  = sum(t["std_win"] for t in tier_stats.values())
all_consw = sum(t["cons_win"]for t in tier_stats.values())
all_awin  = sum(t["a_win"]   for t in tier_stats.values())
all_bwin  = sum(t["b_win"]   for t in tier_stats.values())
all_aplc  = sum(t["a_placed"]for t in tier_stats.values())
all_bplc  = sum(t["b_placed"]for t in tier_stats.values())

print(f"{'='*70}")
print(f"  TOTALS ({all_total} races across {len(history)} days)")
print(f"{'='*70}")
print(f"  Pick 1 win rate:    {all_awin/all_total*100:.1f}%  ({all_awin}/{all_total})")
print(f"  Pick 2 win rate:    {all_bwin/all_total*100:.1f}%  ({all_bwin}/{all_total})")
print(f"  Pick 1 place rate:  {all_aplc/all_total*100:.1f}%  ({all_aplc}/{all_total})")
print(f"  Pick 2 place rate:  {all_bplc/all_total*100:.1f}%  ({all_bplc}/{all_total})")
print(f"  Both placed (std):  {all_stdw/all_total*100:.1f}%  ({all_stdw}/{all_total})")
print(f"  Both placed (cons): {all_consw/all_total*100:.1f}%  ({all_consw}/{all_total})")
print()
