"""
scratch_today_analysis.py
=========================
Shows today's races: betfair bot P1, old system P1, actual winner.
Run from ~/horse_bets_v3:
  python3 scratch_today_analysis.py
"""
import json, os, sys
from datetime import date
sys.path.insert(0, os.path.dirname(__file__))
from predict import score_runner
from predict_v2 import TIER_LABELS

def tof(v):
    try:
        f = float(str(v).strip())
        return f if f > 0 else None
    except:
        return None

def conv_off(off_raw):
    """Convert raw results time (12-hour no AM/PM) to HH:MM BST."""
    if not off_raw or ":" not in off_raw:
        return off_raw
    try:
        parts = off_raw.strip().split(":")
        h = int(parts[0])
        m = int(parts[1])
        if h < 9:
            h += 12
        return f"{h:02d}:{m:02d}"
    except:
        return off_raw

def norm_course(c):
    return (c or "").lower().replace(" ", "").replace("-", "").replace("(aw)", "").replace("(ire)", "")

# ── Load data ─────────────────────────────────────────────────────────────────

today_str = date.today().strftime("%Y-%m-%d")

with open("data/logs/tier_performance.json") as f:
    tracker = json.load(f)

# Deduplicate
seen = set()
deduped = []
for e in tracker:
    key = (e.get("date",""), e.get("off",""), e.get("course",""))
    if key not in seen:
        seen.add(key)
        deduped.append(e)
tracker = deduped

today = [r for r in tracker if r.get("date") == today_str]
print(f"Today ({today_str}): {len(today)} races in tier tracker")
print()

raw_path = f"data/raw/{today_str}.json"
if not os.path.exists(raw_path):
    print(f"No raw results file yet for {today_str}")
    sys.exit(0)

with open(raw_path) as f:
    d = json.load(f)
raw_races = d.get("results") or d.get("races") or []
print(f"Raw results: {len(raw_races)} races")
print()

# ── Print header ──────────────────────────────────────────────────────────────

print(f"{'Race':<26} {'Tier':<9} {'BF P1':<22} {'BF':>5} {'Old P1':<22} {'Old':>5} {'Winner':<22} {'WinSP':>6}")
print("-" * 120)

bf_wins = old_wins = same = diff = total = 0
bf_wins_diff = old_wins_diff = 0

for entry in today:
    off    = entry.get("off", "")
    course = entry.get("course", "")
    bf_p1  = entry.get("pick1", "")
    bf_win = bool(entry.get("win1", False))
    tier   = entry.get("tier", 0)
    tlbl   = TIER_LABELS.get(tier, "?").strip()[:8]

    # Match raw race
    matched = None
    tc = norm_course(course)
    for race in raw_races:
        rc = norm_course(race.get("course", ""))
        off_conv = conv_off(str(race.get("off") or ""))
        if tc in rc and off_conv == off:
            matched = race
            break

    if not matched:
        print(f"{off:5} {course[:18]:<20} {tlbl:<9} {bf_p1[:20]:<22} {'?':>5} {'no match':<22} {'':>5} {'':22} {'':>6}")
        continue

    runners = matched.get("runners", [])
    if not runners:
        continue

    # Old system P1
    scored = sorted(runners, key=lambda r: (-score_runner(r)[0], tof(r.get("sp_dec")) or 999))
    op1 = scored[0] if scored else None
    op1_name = (op1.get("horse", "?") or "")[:20] if op1 else "?"
    try:
        op1_pos = int(str(op1.get("position", "")).strip())
    except:
        op1_pos = None
    old_win = op1_pos == 1

    # Actual winner
    winner = next((r for r in runners if str(r.get("position", "")).strip() == "1"), None)
    win_name = (winner.get("horse", "?") or "")[:20] if winner else "?"
    win_sp   = tof(winner.get("sp_dec")) if winner else None

    # Same pick?
    import re
    def norm_name(n):
        return re.sub(r"\s*\([A-Z]{2,3}\)\s*$", "", n).strip().lower()[:18]
    is_same = norm_name(bf_p1) == norm_name(op1_name)

    total += 1
    bf_wins  += int(bf_win)
    old_wins += int(old_win)
    if is_same:
        same += 1
    else:
        diff += 1
        bf_wins_diff  += int(bf_win)
        old_wins_diff += int(old_win)

    bf_r  = "WIN" if bf_win  else "loss"
    old_r = "WIN" if old_win else "loss"
    flag  = "" if is_same else "*"

    label = f"{off:5} {course[:18]:<18}"
    print(f"{label:<26} {tlbl:<9} {bf_p1[:20]:<22} {bf_r:>5} {op1_name:<22} {old_r:>5} {win_name:<22} {win_sp or 0:>6.2f} {flag}")

# ── Summary ───────────────────────────────────────────────────────────────────

print()
print("=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"Total races:          {total}")
print(f"Betfair P1 wins:      {bf_wins}/{total} ({bf_wins/total*100:.0f}%)" if total else "")
print(f"Old system P1 wins:   {old_wins}/{total} ({old_wins/total*100:.0f}%)" if total else "")
print(f"Same pick:            {same}/{total} ({same/total*100:.0f}%)" if total else "")
print(f"Different pick:       {diff}/{total} ({diff/total*100:.0f}%)" if total else "")
if diff:
    print(f"  BF wins when diff:  {bf_wins_diff}/{diff} ({bf_wins_diff/diff*100:.0f}%)")
    print(f"  Old wins when diff: {old_wins_diff}/{diff} ({old_wins_diff/diff*100:.0f}%)")
print()

# ── Price analysis ────────────────────────────────────────────────────────────

print("=" * 60)
print("WINNER SP DISTRIBUTION (races we lost)")
print("=" * 60)
win_sps = []
for entry in today:
    off    = entry.get("off", "")
    course = entry.get("course", "")
    bf_win = bool(entry.get("win1", False))
    if bf_win:
        continue
    tc = norm_course(course)
    for race in raw_races:
        rc = norm_course(race.get("course", ""))
        off_conv = conv_off(str(race.get("off") or ""))
        if tc in rc and off_conv == off:
            runners = race.get("runners", [])
            winner = next((r for r in runners if str(r.get("position","")).strip() == "1"), None)
            if winner:
                sp = tof(winner.get("sp_dec"))
                if sp:
                    win_sps.append(sp)
            break

if win_sps:
    avg = sum(win_sps) / len(win_sps)
    print(f"Average winner SP when we lost: {avg:.2f}")
    bands = [
        (1.0, 2.0,  "odds-on  (<2/1)"),
        (2.0, 4.0,  "2/1-4/1        "),
        (4.0, 8.0,  "4/1-8/1        "),
        (8.0, 999,  "8/1+           "),
    ]
    for lo, hi, lbl in bands:
        n = sum(1 for s in win_sps if lo <= s < hi)
        if n:
            print(f"  {lbl}: {n} races")

print()
print("Done.")
