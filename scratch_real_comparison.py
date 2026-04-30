"""
scratch_real_comparison.py
==========================
Compares actual betfair bot picks (from tier tracker logs) against
what the old score_runner system would have picked on the same races.
Uses only real settled data — no backtest contamination.

Run from ~/horse_bets_v3:
  python3 scratch_real_comparison.py 2>&1 | tee real_comparison_output.txt
"""

import json, os, sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from predict import score_runner
from predict_v2 import TIER_LABELS

def tof(v):
    try:
        f = float(str(v).strip())
        return f if f > 0 else None
    except:
        return None

def get_pos(r):
    try:
        return int(str(r.get("position", "")).strip())
    except:
        return None

# ── Load tier tracker (real settled bets) ─────────────────────────────────────

with open("data/logs/tier_performance.json") as f:
    tracker = json.load(f)

print(f"Tier tracker entries: {len(tracker)}")
print()

# ── Compare per race ──────────────────────────────────────────────────────────

hdr = "{:<12} {:5} {:<20} {:<24} {:>6} {:<24} {:>7} {:>6}"
print(hdr.format("Date","Off","Course","Betfair P1","BF win","OldSys P1","Old win","Same?"))
print("-" * 110)

same_pick = diff_pick = 0
bf_wins = old_wins = 0
bf_wins_diff = old_wins_diff = 0
matched = unmatched = 0

# Accumulate by tier
tier_stats = defaultdict(lambda: {
    "n":0,"bf_w":0,"old_w":0,"same":0,"diff":0,
    "bf_w_diff":0,"old_w_diff":0
})

for entry in tracker:
    date_str = entry.get("date", "")
    off      = entry.get("off", "")
    course   = entry.get("course", "")
    bf_p1    = entry.get("pick1", "")
    bf_win1  = bool(entry.get("win1", False))
    tier     = entry.get("tier", 0)
    tier_lbl = TIER_LABELS.get(tier, "?").strip()

    raw_path = f"data/raw/{date_str}.json"
    if not os.path.exists(raw_path):
        unmatched += 1
        continue

    with open(raw_path) as f:
        d = json.load(f)
    races = d.get("results") or d.get("races") or []

    matched_race = None
    for race in races:
        rc  = (race.get("course") or "").lower().replace(" ", "").replace("-", "")
        tc  = course.lower().replace(" ", "").replace("-", "").replace("(aw)", "").replace("(ire)", "")
        off_raw = str(race.get("off") or race.get("off_time") or "").strip()
        # Raw results are UTC — convert to BST (+1h) and normalise to HH:MM
        if off_raw and ":" in off_raw:
            try:
                parts  = off_raw.split(":")
                h_bst  = (int(parts[0]) + 1) % 24
                m      = int(parts[1])
                off_bst = f"{h_bst:02d}:{m:02d}"
            except:
                off_bst = off_raw
        else:
            off_bst = off_raw
        if tc in rc and off_bst == off:
            matched_race = race
            break

    if not matched_race:
        unmatched += 1
        continue

    matched += 1
    runners = matched_race.get("runners", [])
    if len(runners) < 2:
        continue

    # Old system P1 — pure score_runner
    scored = sorted(
        runners,
        key=lambda r: (-score_runner(r)[0], tof(r.get("sp_dec")) or 999)
    )
    old_p1      = scored[0] if scored else None
    old_p1_name = (old_p1.get("horse", "?") or "")[:24] if old_p1 else "?"
    old_p1_pos  = get_pos(old_p1) if old_p1 else None
    old_win1    = old_p1_pos == 1

    # Are they the same pick?
    bf_short  = bf_p1.split(" (")[0].strip().lower()[:12]
    old_short = old_p1_name.lower()[:12]
    same = bf_short == old_short
    flag = "" if same else "DIFF"

    # Accumulate totals
    if same:
        same_pick += 1
    else:
        diff_pick += 1
        if bf_win1:  bf_wins_diff  += 1
        if old_win1: old_wins_diff += 1

    bf_wins  += int(bf_win1)
    old_wins += int(old_win1)

    # Per tier
    ts = tier_stats[tier_lbl]
    ts["n"]    += 1
    ts["bf_w"] += int(bf_win1)
    ts["old_w"]+= int(old_win1)
    if same:
        ts["same"] += 1
    else:
        ts["diff"]     += 1
        ts["bf_w_diff"] += int(bf_win1)
        ts["old_w_diff"]+= int(old_win1)

    bf_display  = bf_p1[:24]
    old_display = old_p1_name[:24]
    bf_res  = "WIN" if bf_win1  else "loss"
    old_res = "WIN" if old_win1 else "loss"

    print(hdr.format(
        date_str, off, course[:20],
        bf_display, bf_res,
        old_display, old_res,
        flag
    ))

total = same_pick + diff_pick
print()
print(f"Matched races:   {matched}")
print(f"Unmatched:       {unmatched}")
print()

# ── Summary ───────────────────────────────────────────────────────────────────

print("=" * 60)
print("OVERALL SUMMARY")
print("=" * 60)
print(f"Total races:         {total}")
print(f"Same P1 pick:        {same_pick}/{total} ({same_pick/total*100:.0f}%)")
print(f"Different P1 pick:   {diff_pick}/{total} ({diff_pick/total*100:.0f}%)")
print()
print(f"Betfair P1 win rate: {bf_wins}/{total} ({bf_wins/total*100:.0f}%)")
print(f"OldSys P1 win rate:  {old_wins}/{total} ({old_wins/total*100:.0f}%)")
print()
if diff_pick:
    print(f"When picks DIFFER ({diff_pick} races):")
    print(f"  Betfair P1 won:  {bf_wins_diff}/{diff_pick} ({bf_wins_diff/diff_pick*100:.0f}%)")
    print(f"  OldSys P1 won:   {old_wins_diff}/{diff_pick} ({old_wins_diff/diff_pick*100:.0f}%)")
print()

# ── Per tier ──────────────────────────────────────────────────────────────────

print("=" * 60)
print("BY TIER")
print("=" * 60)
for tlbl in sorted(tier_stats.keys()):
    ts = tier_stats[tlbl]
    n  = ts["n"]
    if not n:
        continue
    print(f"\n  {tlbl}")
    print(f"    Races:           {n}")
    print(f"    Betfair P1 win:  {ts['bf_w']}/{n} ({ts['bf_w']/n*100:.0f}%)")
    print(f"    OldSys P1 win:   {ts['old_w']}/{n} ({ts['old_w']/n*100:.0f}%)")
    print(f"    Same pick:       {ts['same']}/{n} ({ts['same']/n*100:.0f}%)")
    print(f"    Differ:          {ts['diff']}/{n} ({ts['diff']/n*100:.0f}%)")
    if ts["diff"]:
        print(f"      BF wins when diff:  {ts['bf_w_diff']}/{ts['diff']} ({ts['bf_w_diff']/ts['diff']*100:.0f}%)")
        print(f"      Old wins when diff: {ts['old_w_diff']}/{ts['diff']} ({ts['old_w_diff']/ts['diff']*100:.0f}%)")

print()
print("Done.")
