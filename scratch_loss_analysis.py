"""
scratch_loss_analysis.py
========================
Analyses losing patterns across all historical data using the live staking rules.
Looks at stake band performance, consecutive loss days, and score distribution.

Run from ~/horse_bets_v3:
  python3 scratch_loss_analysis.py
"""

import json, os, sys
from collections import defaultdict
sys.path.insert(0, os.path.dirname(__file__))
from predict_v2 import get_blended_picks, TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD, _sp_free_score

BET_TIERS  = {TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD}
COMMISSION = 0.05

def tof(v):
    try:
        f = float(str(v).strip())
        return f if f > 0 else None
    except: return None

def get_pos(r):
    try: return int(str(r.get("position", "")).strip())
    except: return None

def field_ok(runners, race):
    n = len(runners)
    if n < 2: return False
    rt = str(race.get("type", "") or "").lower()
    is_jump = any(t in rt for t in ("chase", "hurdle", "nh flat", "national hunt"))
    cls = str(race.get("class", "") or "").replace("Class", "").strip()
    if cls in ("1", "2") and is_jump: return False
    if is_jump and n > 12: return False
    if not is_jump and n > 20: return False
    return True

def band_stake(sp):
    if not sp or sp < 2.0: return 0
    if sp < 6.0: return 2
    if sp < 10.0: return 4
    return 6

# ── Load records ──────────────────────────────────────────────────────────────

print("Loading races...")
records = []
for fp in sorted(os.listdir("data/raw")):
    if not fp.endswith(".json"): continue
    date = fp.replace(".json", "")
    try:
        with open(f"data/raw/{fp}") as f: d = json.load(f)
    except: continue
    for race in (d.get("results") or d.get("races") or []):
        runners = race.get("runners", [])
        if not field_ok(runners, race): continue
        if not any(get_pos(r) == 1 for r in runners): continue
        raw_meta = {
            "class":   str(race.get("class", "") or ""),
            "surface": race.get("surface", "Turf") or "Turf",
            "type":    race.get("type", "") or "",
        }
        tc, p1, p2, _ = get_blended_picks(runners, mw_p1=0.1, mw_p2=0.4, raw_race=raw_meta)
        if not p1 or tc not in BET_TIERS: continue
        p1sp  = tof(p1.get("sp_dec"))
        p1pos = get_pos(p1)
        if not p1sp or p1pos is None: continue
        score = _sp_free_score(p1)
        stake = band_stake(p1sp)
        if stake == 0 or score < 3: continue
        won  = p1pos == 1
        pnl  = round(stake * (p1sp - 1) * (1 - COMMISSION), 2) if won else -stake
        records.append({
            "date":  date,
            "tier":  tc,
            "sp":    p1sp,
            "score": score,
            "stake": stake,
            "won":   won,
            "pnl":   pnl,
        })

print(f"Records: {len(records)} qualifying bets across {len(set(r['date'] for r in records))} days")
print()

winners = [r for r in records if r["won"]]
losers  = [r for r in records if not r["won"]]

# ── 1. By stake band ──────────────────────────────────────────────────────────

print("=" * 60)
print("1. PERFORMANCE BY STAKE BAND")
print("=" * 60)
print(f"  {'Band':<20} {'N':>5} {'Wins':>5} {'Win%':>6} {'P&L':>10} {'Per bet':>8}")
print(f"  {'-'*57}")
for stake, label in [(2, "£2  (2-6/1)"), (4, "£4  (6-10/1)"), (6, "£6  (10/1+)")]:
    band = [r for r in records if r["stake"] == stake]
    if not band: continue
    bw   = sum(1 for r in band if r["won"])
    bp   = sum(r["pnl"] for r in band)
    print(f"  {label:<20} {len(band):>5} {bw:>5} {bw/len(band)*100:>5.0f}% {bp:>+10.2f} {bp/len(band):>+8.3f}")

# ── 2. Daily P&L distribution ─────────────────────────────────────────────────

print()
print("=" * 60)
print("2. DAILY P&L DISTRIBUTION")
print("=" * 60)

by_date = defaultdict(float)
for r in records:
    by_date[r["date"]] += r["pnl"]

daily_vals  = [by_date[d] for d in sorted(by_date.keys())]
neg_days    = [d for d in daily_vals if d < 0]
pos_days    = [d for d in daily_vals if d >= 0]

print(f"  Total days:      {len(daily_vals)}")
print(f"  Profit days:     {len(pos_days)} ({len(pos_days)/len(daily_vals)*100:.0f}%)")
print(f"  Loss days:       {len(neg_days)} ({len(neg_days)/len(daily_vals)*100:.0f}%)")
print(f"  Avg profit day:  +£{sum(pos_days)/len(pos_days):.2f}")
print(f"  Avg loss day:    £{sum(neg_days)/len(neg_days):.2f}")
print(f"  Best day:        +£{max(daily_vals):.2f}")
print(f"  Worst day:       £{min(daily_vals):.2f}")
print()

# Consecutive loss days
daily_seq = [(d, by_date[d] >= 0) for d in sorted(by_date.keys())]
max_consec = cur = 0
cur_streak_dates = []
worst_streak = []
for date, profitable in daily_seq:
    if not profitable:
        cur += 1
        cur_streak_dates.append(date)
        if cur > max_consec:
            max_consec = cur
            worst_streak = cur_streak_dates[:]
    else:
        cur = 0
        cur_streak_dates = []

print(f"  Max consecutive loss days: {max_consec}")
if worst_streak:
    print(f"  Worst streak dates: {worst_streak[0]} to {worst_streak[-1]}")
    streak_cost = sum(by_date[d] for d in worst_streak)
    print(f"  Cost of that streak: £{streak_cost:.2f}")

# ── 3. Score distribution ─────────────────────────────────────────────────────

print()
print("=" * 60)
print("3. SCORE DISTRIBUTION — win rate and P&L per score")
print("=" * 60)
print(f"  {'Score':<8} {'N':>5} {'Wins':>5} {'Win%':>6} {'AvgSP':>7} {'P&L':>10} {'Per bet':>8}")
print(f"  {'-'*52}")
for score in range(3, 12):
    band = [r for r in records if r["score"] == score]
    if len(band) < 5: continue
    bw   = sum(1 for r in band if r["won"])
    bp   = sum(r["pnl"] for r in band)
    avg_sp = sum(r["sp"] for r in band) / len(band)
    print(f"  {score:<8} {len(band):>5} {bw:>5} {bw/len(band)*100:>5.0f}% {avg_sp:>7.2f} {bp:>+10.2f} {bp/len(band):>+8.3f}")

# ── 4. What does the actual winner look like when we lose? ────────────────────

print()
print("=" * 60)
print("4. WHEN WE LOSE — where does the actual winner rank?")
print("=" * 60)
print("   (How often is the winner P2 vs an unranked horse?)")
print()

# Reload to get P2 data
p2_was_winner = unranked_winner = 0
total_losses_with_data = 0

for fp in sorted(os.listdir("data/raw")):
    if not fp.endswith(".json"): continue
    try:
        with open(f"data/raw/{fp}") as f: d = json.load(f)
    except: continue
    for race in (d.get("results") or d.get("races") or []):
        runners = race.get("runners", [])
        if not field_ok(runners, race): continue
        if not any(get_pos(r) == 1 for r in runners): continue
        raw_meta = {
            "class":   str(race.get("class", "") or ""),
            "surface": race.get("surface", "Turf") or "Turf",
            "type":    race.get("type", "") or "",
        }
        tc, p1, p2, _ = get_blended_picks(runners, mw_p1=0.1, mw_p2=0.4, raw_race=raw_meta)
        if not p1 or tc not in BET_TIERS: continue
        p1sp  = tof(p1.get("sp_dec"))
        p1pos = get_pos(p1)
        if not p1sp or p1pos is None: continue
        score = _sp_free_score(p1)
        stake = band_stake(p1sp)
        if stake == 0 or score < 3: continue
        if p1pos == 1: continue  # only look at losses

        total_losses_with_data += 1
        p2_id   = p2.get("horse_id", "") if p2 else ""
        winner  = next((r for r in runners if get_pos(r) == 1), None)
        if not winner: continue
        w_id = winner.get("horse_id", "")
        if p2 and w_id == p2_id:
            p2_was_winner += 1
        else:
            unranked_winner += 1

n = total_losses_with_data
print(f"  Total losses analysed: {n}")
if n:
    print(f"  Winner was our P2:        {p2_was_winner:>4} ({p2_was_winner/n*100:.0f}%)")
    print(f"  Winner was unranked pick: {unranked_winner:>4} ({unranked_winner/n*100:.0f}%)")
    print()
    print("  When we lose, the winner is an unranked horse")
    print(f"  {unranked_winner/n*100:.0f}% of the time — these are races where")
    print("  neither our stats nor the model identified the winner.")

print()
print("NOTE: P&L inflated by backtest contamination.")
print("Done.")
