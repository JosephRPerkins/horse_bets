"""
scratch_real_comparison.py
"""
import json, os, sys, re
from collections import defaultdict
sys.path.insert(0, os.path.dirname(__file__))
from predict import score_runner
from predict_v2 import TIER_LABELS

def tof(v):
    try:
        f = float(str(v).strip())
        return f if f > 0 else None
    except: return None

def get_pos(r):
    try: return int(str(r.get("position","")).strip())
    except: return None

def conv_off(off_raw):
    if not off_raw or ":" not in off_raw: return off_raw
    try:
        parts = str(off_raw).strip().split(":")
        h = int(parts[0]); m = int(parts[1])
        if h < 9: h += 12
        return f"{h:02d}:{m:02d}"
    except: return off_raw

def norm_course(c):
    return (c or "").lower().replace(" ","").replace("-","").replace("(aw)","").replace("(ire)","")

def norm_name(n):
    return re.sub(r"\s*\([A-Z]{2,3}\)\s*$","",n).strip().lower()[:20]

with open("data/logs/tier_performance.json") as f: tracker = json.load(f)
seen = set(); deduped = []
for e in tracker:
    key = (e.get("date",""), e.get("off",""), e.get("course",""))
    if key not in seen: seen.add(key); deduped.append(e)
tracker = deduped
print(f"Tier tracker entries: {len(tracker)} (after dedup)\n")

same_pick = diff_pick = bf_wins = old_wins = bf_wins_diff = old_wins_diff = matched = unmatched = 0
tier_stats = defaultdict(lambda: {"n":0,"bf_w":0,"old_w":0,"same":0,"diff":0,"bf_w_diff":0,"old_w_diff":0})
rows = []

for entry in tracker:
    date_str = entry.get("date",""); off = entry.get("off",""); course = entry.get("course","")
    bf_p1 = entry.get("pick1",""); bf_win1 = bool(entry.get("win1",False))
    tier = entry.get("tier",0); tier_lbl = TIER_LABELS.get(tier,"?").strip()

    raw_path = f"data/raw/{date_str}.json"
    if not os.path.exists(raw_path): unmatched += 1; continue

    with open(raw_path) as f: d = json.load(f)
    races = d.get("results") or d.get("races") or []

    matched_race = None; tc = norm_course(course)
    for race in races:
        rc = norm_course(race.get("course",""))
        off_bst = conv_off(str(race.get("off") or ""))
        if tc in rc and off_bst == off: matched_race = race; break

    if not matched_race: unmatched += 1; continue
    matched += 1
    runners = matched_race.get("runners",[])
    if len(runners) < 2: continue

    scored = sorted(runners, key=lambda r: (-score_runner(r)[0], tof(r.get("sp_dec")) or 999))
    old_p1 = scored[0] if scored else None
    old_p1_name = (old_p1.get("horse","?") or "")[:24] if old_p1 else "?"
    old_p1_pos = get_pos(old_p1) if old_p1 else None
    old_win1 = old_p1_pos == 1

    same = norm_name(bf_p1) == norm_name(old_p1_name)
    if same: same_pick += 1
    else:
        diff_pick += 1
        if bf_win1: bf_wins_diff += 1
        if old_win1: old_wins_diff += 1

    bf_wins += int(bf_win1); old_wins += int(old_win1)
    ts = tier_stats[tier_lbl]
    ts["n"] += 1; ts["bf_w"] += int(bf_win1); ts["old_w"] += int(old_win1)
    if same: ts["same"] += 1
    else:
        ts["diff"] += 1; ts["bf_w_diff"] += int(bf_win1); ts["old_w_diff"] += int(old_win1)

    rows.append((date_str, off, course[:20], bf_p1[:24], "WIN" if bf_win1 else "loss",
                 old_p1_name[:24], "WIN" if old_win1 else "loss", "" if same else "DIFF"))

hdr = "{:<12} {:5} {:<20} {:<24} {:>6} {:<24} {:>7} {:>6}"
print(hdr.format("Date","Off","Course","Betfair P1","BF win","OldSys P1","Old win","Same?"))
print("-"*110)
for r in rows: print(hdr.format(*r))

total = same_pick + diff_pick
print(f"\nMatched: {matched}  Unmatched: {unmatched}")
if total == 0: print("No races matched."); sys.exit(1)

print("\n" + "="*60 + "\nOVERALL SUMMARY\n" + "="*60)
print(f"Total races:         {total}")
print(f"Same P1 pick:        {same_pick}/{total} ({same_pick/total*100:.0f}%)")
print(f"Different P1 pick:   {diff_pick}/{total} ({diff_pick/total*100:.0f}%)")
print(f"\nBetfair P1 win rate: {bf_wins}/{total} ({bf_wins/total*100:.0f}%)")
print(f"OldSys P1 win rate:  {old_wins}/{total} ({old_wins/total*100:.0f}%)")
if diff_pick:
    print(f"\nWhen picks DIFFER ({diff_pick} races):")
    print(f"  Betfair P1 won:  {bf_wins_diff}/{diff_pick} ({bf_wins_diff/diff_pick*100:.0f}%)")
    print(f"  OldSys P1 won:   {old_wins_diff}/{diff_pick} ({old_wins_diff/diff_pick*100:.0f}%)")

print("\n" + "="*60 + "\nBY TIER\n" + "="*60)
for tlbl in sorted(tier_stats.keys()):
    ts = tier_stats[tlbl]; n = ts["n"]
    if not n: continue
    print(f"\n  {tlbl}")
    print(f"    Races:           {n}")
    print(f"    Betfair P1 win:  {ts['bf_w']}/{n} ({ts['bf_w']/n*100:.0f}%)")
    print(f"    OldSys P1 win:   {ts['old_w']}/{n} ({ts['old_w']/n*100:.0f}%)")
    print(f"    Same pick:       {ts['same']}/{n} ({ts['same']/n*100:.0f}%)")
    print(f"    Differ:          {ts['diff']}/{n} ({ts['diff']/n*100:.0f}%)")
    if ts["diff"]:
        print(f"      BF wins when diff:  {ts['bf_w_diff']}/{ts['diff']} ({ts['bf_w_diff']/ts['diff']*100:.0f}%)")
        print(f"      Old wins when diff: {ts['old_w_diff']}/{ts['diff']} ({ts['old_w_diff']/ts['diff']*100:.0f}%)")
print("\nDone.")
