"""
scratch_model_test.py  —  NOT part of the main bot, do not commit
=================================================================
Replays all history races and compares:
  A) Current model    — binary trainer/jockey P&L flag (+1 each)
  B) Augmented model  — replaces binary flags with continuous
                        A/E and win_pct signals, weighted by
                        confidence in the form sample size

Run from ~/horse_bets_v3:
  python3 scratch_model_test.py
"""

import json, glob, os, sys, math
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from predict    import score_runner, SIGNAL_WEIGHTS
from predict_v2 import race_confidence, TIER_LABELS

# ── Load data ─────────────────────────────────────────────────────────────────

history = {}
for fp in sorted(glob.glob("data/history/*.json")):
    with open(fp) as f: d = json.load(f)
    history[d["date"]] = d

raw_index = {}
for fp in sorted(glob.glob("data/raw/*.json")):
    with open(fp) as f: d = json.load(f)
    for race in (d.get("results") or d.get("races") or []):
        raw_index[race["race_id"]] = race

print(f"Loaded {len(history)} days of history, {len(raw_index)} raw races")
print()

# ── Helpers ───────────────────────────────────────────────────────────────────

def to_float(v):
    try: return float(v)
    except: return None

def place_spots(n):
    if n <= 4: return 1
    if n <= 7: return 2
    return 3

def get_form(d):
    """Return (win_pct, ae, runs, pl) from a trainer/jockey dict."""
    if not isinstance(d, dict):
        return 0.0, 0.0, 0, 0.0
    return (
        d.get("win_pct", 0.0) or 0.0,
        d.get("ae",      0.0) or 0.0,
        d.get("runs",    0)   or 0,
        d.get("pl",      0.0) or 0.0,
    )

# ── Augmented scorer ──────────────────────────────────────────────────────────
# Strategy: replace the two binary flags (trainer_pos=1, jockey_pos=1)
# with continuous scores that reward:
#   - High A/E ratio (actual winners vs expected) — best predictor
#   - High win_pct  — corroborating signal
#   - Sample size confidence — dampen signals from tiny samples
#
# Weighting rationale:
#   ae >= 2.0 with 10+ runs  →  +3  (strong form, reliable sample)
#   ae >= 1.5 with 5+ runs   →  +2  (good form)
#   ae >= 1.0 with 5+ runs   →  +1  (market-rate form, replaces old binary)
#   ae <  1.0                →   0  (underperforming market)
#   win_pct >= 0.25           →  +1  (extra corroboration for high strike rate)
#   win_pct >= 0.30           →  +2  (exceptional strike rate)
#
# These are additive on top of the base score_runner() output,
# replacing the old trainer_pos and jockey_pos points.

AE_BINS = [
    (2.0, 10, 3),   # (min_ae, min_runs, points)
    (1.5,  5, 2),
    (1.0,  5, 1),
]
WINPCT_BINS = [
    (0.30, 2),
    (0.25, 1),
]
MIN_RUNS_FOR_ANY_SIGNAL = 3


def ae_score(win_pct, ae, runs):
    """Continuous score contribution from A/E and win_pct."""
    if runs < MIN_RUNS_FOR_ANY_SIGNAL:
        return 0
    pts = 0
    for min_ae, min_runs, points in AE_BINS:
        if ae >= min_ae and runs >= min_runs:
            pts = points
            break
    for min_wp, wp_pts in WINPCT_BINS:
        if win_pct >= min_wp:
            pts += wp_pts
            break
    return pts


def score_runner_augmented(runner):
    """
    Drop-in replacement for score_runner() with improved trainer/jockey signals.
    Returns (score, signals_dict).
    """
    base_score, base_signals = score_runner(runner)

    # Remove the old binary contributions
    old_trainer = base_signals.get("trainer_pos", 0)
    old_jockey  = base_signals.get("jockey_pos",  0)
    adjusted    = base_score - old_trainer - old_jockey

    t14 = runner.get("trainer_14d") or {}
    j14 = runner.get("jockey_14d")  or {}

    t_wp, t_ae, t_runs, t_pl = get_form(t14)
    j_wp, j_ae, j_runs, j_pl = get_form(j14)

    new_trainer = ae_score(t_wp, t_ae, t_runs)
    new_jockey  = ae_score(j_wp, j_ae, j_runs)

    new_score = adjusted + new_trainer + new_jockey

    new_signals = dict(base_signals)
    new_signals["trainer_pos"] = new_trainer
    new_signals["jockey_pos"]  = new_jockey

    return new_score, new_signals


# ── Replay engine ─────────────────────────────────────────────────────────────

def replay(scorer, label):
    """
    Replay all history races with a given scorer function.
    Returns per-tier stats dict.
    """
    tier_stats   = defaultdict(lambda: {"n":0,"p1w":0,"p2w":0,"p1p":0,"p2p":0,
                                         "p1_sp_sum":0.0,"p1_sp_n":0,
                                         "p1_pnl":0.0,"p2_pnl":0.0})
    gap_stats    = defaultdict(lambda: {"n":0,"p1w":0,"p2w":0,"p1p":0,"p2p":0})
    rank_changed = 0   # races where augmented model picks a different P1
    total        = 0
    winner_in_top2 = 0

    for date, day in sorted(history.items()):
        for rec in day.get("races", []):
            raw = raw_index.get(rec["race_id"])
            if not raw:
                continue
            runners = raw.get("runners", [])
            if len(runners) < 2:
                continue

            scored = sorted(
                [{**r, "_score": scorer(r)[0]} for r in runners],
                key=lambda x: -x["_score"]
            )

            raw2      = {**raw, "runners": scored}
            win_score = scored[0]["_score"]
            tier, _   = race_confidence(raw2, win_score)
            label_t   = TIER_LABELS.get(tier, "?").split()[0]

            p1     = scored[0]
            p2     = scored[1]
            p1_sp  = to_float(p1.get("sp_dec"))
            p2_sp  = to_float(p2.get("sp_dec"))
            gap    = p1["_score"] - p2["_score"]
            n      = len(runners)
            ps     = place_spots(n)

            p1_won = rec["a_pos"] == 1
            p2_won = rec["b_pos"] == 1
            p1_plc = bool(rec.get("std_a") or rec.get("cons_a"))
            p2_plc = bool(rec.get("std_b") or rec.get("cons_b"))

            # Check if the winner of the race is in our top-2
            winner_pos = None
            for i, r in enumerate(scored):
                if r.get("position") == 1 or str(r.get("position","")) == "1":
                    winner_pos = i + 1
                    break
            if winner_pos and winner_pos <= 2:
                winner_in_top2 += 1

            # Gap bucket
            if gap <= 0:     gb = "gap 0"
            elif gap == 1:   gb = "gap 1"
            elif gap == 2:   gb = "gap 2"
            elif gap <= 4:   gb = "gap 3-4"
            else:            gb = "gap 5+"

            s = tier_stats[label_t]
            s["n"]    += 1
            s["p1w"]  += p1_won
            s["p2w"]  += p2_won
            s["p1p"]  += p1_plc
            s["p2p"]  += p2_plc
            if p1_sp:
                s["p1_sp_sum"] += p1_sp
                s["p1_sp_n"]   += 1
                s["p1_pnl"] += (p1_sp - 1) * 2 if p1_won else -2
            if p2_sp:
                s["p2_pnl"] += (p2_sp - 1) * 2 if p2_won else -2

            g = gap_stats[gb]
            g["n"]   += 1
            g["p1w"] += p1_won
            g["p2w"] += p2_won
            g["p1p"] += p1_plc
            g["p2p"] += p2_plc

            total += 1

    return tier_stats, gap_stats, total, winner_in_top2


# ── Run both models ───────────────────────────────────────────────────────────

print("Running current model...")
cur_tier,  cur_gap,  cur_total,  cur_top2  = replay(score_runner,           "current")
print("Running augmented model...")
aug_tier,  aug_gap,  aug_total,  aug_top2  = replay(score_runner_augmented, "augmented")
print()

# ── Compare: where do the models disagree on P1? ─────────────────────────────
# Replay both simultaneously to find rank changes
disagreements = []
for date, day in sorted(history.items()):
    for rec in day.get("races", []):
        raw = raw_index.get(rec["race_id"])
        if not raw:
            continue
        runners = raw.get("runners", [])
        if len(runners) < 2:
            continue

        cur_scored = sorted([{**r, "_s": score_runner(r)[0]} for r in runners],           key=lambda x: -x["_s"])
        aug_scored = sorted([{**r, "_s": score_runner_augmented(r)[0]} for r in runners], key=lambda x: -x["_s"])

        cur_p1 = cur_scored[0].get("horse","")
        aug_p1 = aug_scored[0].get("horse","")

        if cur_p1 != aug_p1:
            # Which one was right?
            winner = next((r.get("horse","") for r in runners if str(r.get("position",""))=="1"), None)
            cur_right = (cur_p1 == winner)
            aug_right = (aug_p1 == winner)
            t_wp, t_ae, t_runs, _ = get_form(aug_scored[0].get("trainer_14d") or {})
            j_wp, j_ae, j_runs, _ = get_form(aug_scored[0].get("jockey_14d")  or {})
            disagreements.append({
                "date":      date,
                "race_id":   rec["race_id"],
                "cur_p1":    cur_p1,
                "aug_p1":    aug_p1,
                "winner":    winner,
                "cur_right": cur_right,
                "aug_right": aug_right,
                "t_ae":      t_ae,
                "t_runs":    t_runs,
                "j_ae":      j_ae,
                "j_runs":    j_runs,
                "cur_p1_pos":  rec["a_pos"],
            })

# ── Print comparison ──────────────────────────────────────────────────────────

TIER_ORDER = ["🔥🔥🔥", "🔥🔥", "🔥", "·", "✗"]

def print_tier_table(label, tier_stats, total, top2):
    print(f"{'='*72}")
    print(f"  {label}  ({total} races)")
    print(f"{'='*72}")
    print(f"  {'Tier':<8} {'N':>5} {'P1win%':>8} {'P2win%':>8} "
          f"{'P1plc%':>8} {'P2plc%':>8} {'P1 P&L':>8} {'P2 P&L':>8} {'AvgSP':>7}")
    print(f"  {'-'*70}")
    grand = {"n":0,"p1w":0,"p2w":0,"p1p":0,"p2p":0,"p1_pnl":0.0,"p2_pnl":0.0,"p1_sp_sum":0.0,"p1_sp_n":0}
    for t in TIER_ORDER:
        s = tier_stats.get(t)
        if not s or s["n"] == 0: continue
        n = s["n"]
        avg_sp = s["p1_sp_sum"] / s["p1_sp_n"] if s["p1_sp_n"] else 0
        print(f"  {t:<8} {n:>5} "
              f"{s['p1w']/n*100:>7.1f}% "
              f"{s['p2w']/n*100:>7.1f}% "
              f"{s['p1p']/n*100:>7.1f}% "
              f"{s['p2p']/n*100:>7.1f}% "
              f"{s['p1_pnl']:>+8.2f} "
              f"{s['p2_pnl']:>+8.2f} "
              f"{avg_sp:>7.2f}")
        for k in grand: grand[k] += s[k]
    n = grand["n"]
    print(f"  {'TOTAL':<8} {n:>5} "
          f"{grand['p1w']/n*100:>7.1f}% "
          f"{grand['p2w']/n*100:>7.1f}% "
          f"{grand['p1p']/n*100:>7.1f}% "
          f"{grand['p2p']/n*100:>7.1f}% "
          f"{grand['p1_pnl']:>+8.2f} "
          f"{grand['p2_pnl']:>+8.2f}")
    print(f"  Winner in top-2: {top2}/{total} ({top2/total*100:.1f}%)")
    print()

def print_gap_table(label, gap_stats):
    print(f"{'='*72}")
    print(f"  {label} — by score gap")
    print(f"{'='*72}")
    print(f"  {'Gap':<10} {'N':>5} {'P1win%':>8} {'P2win%':>8} {'P1plc%':>8} {'P2plc%':>8}")
    print(f"  {'-'*50}")
    for gb in ["gap 0","gap 1","gap 2","gap 3-4","gap 5+"]:
        s = gap_stats.get(gb)
        if not s or s["n"] == 0: continue
        n = s["n"]
        print(f"  {gb:<10} {n:>5} "
              f"{s['p1w']/n*100:>7.1f}% "
              f"{s['p2w']/n*100:>7.1f}% "
              f"{s['p1p']/n*100:>7.1f}% "
              f"{s['p2p']/n*100:>7.1f}%")
    print()

print_tier_table("CURRENT MODEL",   cur_tier, cur_total, cur_top2)
print_tier_table("AUGMENTED MODEL", aug_tier, aug_total, aug_top2)
print_gap_table("CURRENT MODEL",   cur_gap)
print_gap_table("AUGMENTED MODEL", aug_gap)

# ── Disagreement analysis ─────────────────────────────────────────────────────
print(f"{'='*72}")
print(f"  MODEL DISAGREEMENTS — races where P1 pick differs ({len(disagreements)} races)")
print(f"{'='*72}")
cur_wins = sum(1 for d in disagreements if d["cur_right"])
aug_wins = sum(1 for d in disagreements if d["aug_right"])
both_right  = sum(1 for d in disagreements if d["cur_right"] and d["aug_right"])
neither     = sum(1 for d in disagreements if not d["cur_right"] and not d["aug_right"])
aug_only    = sum(1 for d in disagreements if not d["cur_right"] and d["aug_right"])
cur_only    = sum(1 for d in disagreements if d["cur_right"] and not d["aug_right"])

print(f"  Total disagreements:     {len(disagreements)}")
print(f"  Augmented right, cur wrong:  {aug_only}  ({aug_only/len(disagreements)*100:.1f}%)")
print(f"  Current right, aug wrong:    {cur_only}  ({cur_only/len(disagreements)*100:.1f}%)")
print(f"  Both right:              {both_right}  ({both_right/len(disagreements)*100:.1f}%)")
print(f"  Neither right:           {neither}  ({neither/len(disagreements)*100:.1f}%)")
print()

# Cases where augmented model was right and current was wrong
print(f"  Cases where augmented model improved (aug right, cur wrong) — top 15:")
print(f"  {'Date':12} {'Cur P1':22} {'Aug P1':22} {'Winner':22} {'T_AE':>6} {'J_AE':>6}")
print(f"  {'-'*95}")
improved = [d for d in disagreements if d["aug_right"] and not d["cur_right"]]
for d in improved[:15]:
    print(f"  {d['date']:12} {d['cur_p1'][:20]:22} {d['aug_p1'][:20]:22} "
          f"{(d['winner'] or '')[:20]:22} {d['t_ae']:>6.2f} {d['j_ae']:>6.2f}")
print()

print(f"  Cases where current model was better (cur right, aug wrong) — top 15:")
print(f"  {'Date':12} {'Cur P1':22} {'Aug P1':22} {'Winner':22} {'T_AE':>6} {'J_AE':>6}")
print(f"  {'-'*95}")
regressed = [d for d in disagreements if d["cur_right"] and not d["aug_right"]]
for d in regressed[:15]:
    print(f"  {d['date']:12} {d['cur_p1'][:20]:22} {d['aug_p1'][:20]:22} "
          f"{(d['winner'] or '')[:20]:22} {d['t_ae']:>6.2f} {d['j_ae']:>6.2f}")
print()

# ── A/E threshold sensitivity ─────────────────────────────────────────────────
print(f"{'='*72}")
print(f"  A/E THRESHOLD SENSITIVITY — P1 win rate at different AE cutoffs")
print(f"  (all history races, filtering to those where aug P1 trainer AE >= threshold)")
print(f"{'='*72}")
print(f"  {'AE>=':>6} {'MinRuns':>8} {'Races':>7} {'P1win%':>8} {'P1plc%':>8}")
print(f"  {'-'*42}")

for min_ae, min_runs in [(0.0,0),(1.0,5),(1.5,5),(2.0,5),(2.0,10),(3.0,5)]:
    n = w = p = 0
    for date, day in sorted(history.items()):
        for rec in day.get("races", []):
            raw = raw_index.get(rec["race_id"])
            if not raw: continue
            runners = raw.get("runners", [])
            if len(runners) < 2: continue
            aug_scored = sorted([{**r, "_s": score_runner_augmented(r)[0]} for r in runners],
                                 key=lambda x: -x["_s"])
            t_wp, t_ae, t_runs, _ = get_form(aug_scored[0].get("trainer_14d") or {})
            if t_ae >= min_ae and t_runs >= min_runs:
                n += 1
                if rec["a_pos"] == 1: w += 1
                if rec.get("std_a") or rec.get("cons_a"): p += 1
    if n:
        print(f"  {min_ae:>6.1f} {min_runs:>8} {n:>7} {w/n*100:>7.1f}%  {p/n*100:>7.1f}%")
print()

# ── Combined filter — high AE trainer + gap >= 3 ─────────────────────────────
print(f"{'='*72}")
print(f"  COMBINED FILTER — score gap >= 3 AND trainer AE >= 1.5 (5+ runs)")
print(f"  The sweet spot: model confident + trainer in strong form")
print(f"{'='*72}")
n = w = p = 0
for date, day in sorted(history.items()):
    for rec in day.get("races", []):
        raw = raw_index.get(rec["race_id"])
        if not raw: continue
        runners = raw.get("runners", [])
        if len(runners) < 2: continue
        aug_scored = sorted([{**r, "_s": score_runner_augmented(r)[0]} for r in runners],
                             key=lambda x: -x["_s"])
        gap = aug_scored[0]["_s"] - aug_scored[1]["_s"]
        t_wp, t_ae, t_runs, _ = get_form(aug_scored[0].get("trainer_14d") or {})
        if gap >= 3 and t_ae >= 1.5 and t_runs >= 5:
            n += 1
            if rec["a_pos"] == 1: w += 1
            if rec.get("std_a") or rec.get("cons_a"): p += 1

print(f"  Races meeting filter: {n}")
if n:
    print(f"  P1 win rate:          {w}/{n} = {w/n*100:.1f}%")
    print(f"  P1 place rate:        {p}/{n} = {p/n*100:.1f}%")
print()
print("Done.")
