"""
scratch_multistage.py  —  NOT part of the main bot, do not commit
==================================================================
Tests a two-stage ranking approach:

  Stage 1 — Pure stats rank  (rpr, tsr, or, trainer/jockey form,
             form signals, going/dist signals — NO SP anywhere)

  Stage 2 — Market rank      (SP ascending, shortest = rank 1)

  Stage 3 — Combined rank    (weighted blend of stage 1 + stage 2)

Sweeps market_weight from 0.0 to 1.0 to find optimal blend.
Also tests rank-agreement filter: only bet when stats and market
agree on the top pick.

Run from ~/horse_bets_v3:
  python3 scratch_multistage.py 2>&1 | tee multistage_output.txt
"""

import json, glob, os, sys, math
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))

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

# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def to_float(v):
    try:
        f = float(str(v).strip())
        return f if f > 0 else None
    except:
        return None

def place_spots(n):
    if n <= 4: return 1
    if n <= 7: return 2
    return 3

def get_form_detail(runner):
    fd = runner.get("form_detail")
    if not isinstance(fd, dict):
        return [], ""
    return fd.get("recent_positions", []), fd.get("form_string", "")

def get_trainer(runner):
    t = runner.get("trainer_14d")
    if not isinstance(t, dict): return 0.0, 0.0, 0
    return t.get("win_pct", 0) or 0, t.get("ae", 0) or 0, t.get("runs", 0) or 0

def get_jockey(runner):
    j = runner.get("jockey_14d")
    if not isinstance(j, dict): return 0.0, 0.0, 0
    return j.get("win_pct", 0) or 0, j.get("ae", 0) or 0, j.get("runs", 0) or 0

def going_family(going):
    g = (going or "").lower()
    if "heavy" in g:                        return "heavy"
    if "soft" in g:                         return "soft"
    if "good to soft" in g:                 return "gd_soft"
    if "good to firm" in g:                 return "gd_firm"
    if "good" in g:                         return "good"
    if "firm" in g:                         return "firm"
    if "standard" in g:                     return "aw"
    return "other"

# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 1: PURE STATS SCORER  (zero SP involvement)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Signals and rationale:
#
# RATINGS (objective ability measures)
#   rpr_score      — RPR normalised within the race field (0-10).
#                    Higher RPR = faster horse on RPR scale.
#   or_score       — Official Rating normalised within field (0-10).
#   tsr_score      — Trainer/Stable Rating normalised within field (0-5).
#                    Lower weight than RPR/OR as it's less stable.
#   rpr_beats_or   — RPR > OR suggests horse running above its rating
#                    (in form). Binary +2.
#
# FORM
#   form_3_of_4    — Placed in 3 of last 4 runs. Strong consistency. +2
#   form_2_of_4    — Placed in 2 of last 4 runs. Moderate. +1
#   no_bad_recent  — No finish outside top half of field in last 2. +1
#
# TRAINER/JOCKEY (continuous, not binary)
#   trainer_score  — Stepped A/E bins: AE>=2 → +3, >=1.5 → +2, >=1 → +1
#                    with win_pct corroboration bonus.
#   jockey_score   — Same bins as trainer.
#
# GOING / DISTANCE (race-context signals)
#   going_bonus    — "good" and "standard(AW)" get +1 (historically
#                    more predictable going; model performs better there).
#   going_penalty  — "heavy" gets -2 (unpredictable, form unreliable).
#   dist_penalty   — Significant distance change from horse's form
#                    distance gets -1 (unproven at trip).
#
# Normalisation: rpr/or/tsr are normalised within the field so that
# a horse with RPR 110 in a field of RPR 80-115 gets a different score
# than RPR 110 in an RPR 105-115 field. This avoids rewarding high-class
# horses just for being high-class without consideration of their rivals.

AE_BINS = [(2.0, 5, 3), (1.5, 5, 2), (1.0, 5, 1)]

def form_score_from_ae(win_pct, ae, runs):
    if runs < 3: return 0
    pts = 0
    for min_ae, min_runs, p in AE_BINS:
        if ae >= min_ae and runs >= min_runs:
            pts = p
            break
    if win_pct >= 0.30: pts += 2
    elif win_pct >= 0.20: pts += 1
    return pts

def normalise(val, field_vals, scale=10.0):
    """Linear normalise val within field_vals to [0, scale]."""
    valid = [v for v in field_vals if v is not None]
    if not valid or len(valid) < 2:
        return scale / 2
    lo, hi = min(valid), max(valid)
    if hi == lo:
        return scale / 2
    return ((val - lo) / (hi - lo)) * scale

def stats_score(runner, field_rprs, field_ors, field_tsrs, race_going):
    score = 0.0

    # ── Ratings ──────────────────────────────────────────────────────────────
    rpr = to_float(runner.get("rpr"))
    or_ = to_float(runner.get("or"))
    tsr = to_float(runner.get("tsr"))

    if rpr: score += normalise(rpr, field_rprs, scale=10.0)
    if or_: score += normalise(or_, field_ors,  scale=10.0)
    if tsr: score += normalise(tsr, field_tsrs, scale=5.0)

    # RPR beating OR — in-form signal
    if rpr and or_ and rpr > or_:
        score += 2.0

    # ── Form ─────────────────────────────────────────────────────────────────
    recent, form_str = get_form_detail(runner)
    if recent:
        placed = sum(1 for p in recent[:4] if isinstance(p, int) and p <= 3)
        if placed >= 3:   score += 2.0
        elif placed >= 2: score += 1.0

        # No bad recent — no finish in bottom half of field in last 2
        # We don't have field size per form run, use position > 6 as proxy
        bad = sum(1 for p in recent[:2] if isinstance(p, int) and p > 6)
        if bad == 0 and len(recent) >= 2:
            score += 1.0

    # ── Trainer / jockey ─────────────────────────────────────────────────────
    t_wp, t_ae, t_runs = get_trainer(runner)
    j_wp, j_ae, j_runs = get_jockey(runner)
    score += form_score_from_ae(t_wp, t_ae, t_runs)
    score += form_score_from_ae(j_wp, j_ae, j_runs)

    # ── Going ─────────────────────────────────────────────────────────────────
    gf = going_family(race_going)
    if gf in ("good", "aw"):    score += 1.0
    if gf == "heavy":           score -= 2.0

    return score


# ═══════════════════════════════════════════════════════════════════════════════
# RANKING ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

def rank_runners(runners, race_going, market_weight):
    """
    Returns runners sorted by combined rank (best first).
    market_weight: 0.0 = pure stats, 1.0 = pure market SP.
    """
    n = len(runners)
    if n < 2:
        return runners

    # Collect field-level rating arrays for normalisation
    field_rprs = [to_float(r.get("rpr")) for r in runners]
    field_ors  = [to_float(r.get("or"))  for r in runners]
    field_tsrs = [to_float(r.get("tsr")) for r in runners]

    # Stage 1: stats score and rank (rank 1 = best)
    scored = []
    for r in runners:
        s = stats_score(r, field_rprs, field_ors, field_tsrs, race_going)
        scored.append((s, r))
    scored.sort(key=lambda x: -x[0])
    stats_rank = {r.get("horse_id", r.get("horse","")): i+1
                  for i, (_, r) in enumerate(scored)}

    # Stage 2: market rank by SP (rank 1 = shortest price)
    sp_scored = []
    for r in runners:
        sp = to_float(r.get("sp_dec"))
        sp_scored.append((sp if sp else 999, r))
    sp_scored.sort(key=lambda x: x[0])
    market_rank = {r.get("horse_id", r.get("horse","")): i+1
                   for i, (_, r) in enumerate(sp_scored)}

    # Stage 3: combined rank score (lower = better)
    # Blend: combined = (1 - mw) * stats_rank + mw * market_rank
    # Then re-sort ascending → best combined rank = P1
    combined = []
    for r in runners:
        hid = r.get("horse_id", r.get("horse",""))
        sr  = stats_rank.get(hid, n)
        mr  = market_rank.get(hid, n)
        # Normalise ranks to [0,1] so scale is comparable
        sr_norm = (sr - 1) / max(n - 1, 1)
        mr_norm = (mr - 1) / max(n - 1, 1)
        combined_score = (1 - market_weight) * sr_norm + market_weight * mr_norm
        combined.append((combined_score, stats_rank[hid], market_rank[hid],
                          scored[[r2 for _,r2 in scored].index(r)][0], r))

    combined.sort(key=lambda x: x[0])

    # Attach metadata for analysis
    result = []
    for rank, (cs, sr, mr, raw_score, r) in enumerate(combined):
        result.append({
            **r,
            "_combined_rank": rank + 1,
            "_stats_rank":    sr,
            "_market_rank":   mr,
            "_stats_score":   raw_score,
            "_combined_score": cs,
        })
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# EVALUATION
# ═══════════════════════════════════════════════════════════════════════════════

def evaluate(market_weight, agree_filter=False, max_market_rank=None):
    """
    Replay all history races with given market_weight.

    agree_filter: if True, only count races where stats P1 == market P1
    max_market_rank: if set, skip races where our stats P1 has market
                     rank > this value (e.g. 3 = only bet if stats P1
                     is top-3 in the market)

    Returns dict of stats.
    """
    total = p1w = p2w = p1p = p2p = skipped = 0
    p1_pnl = p2_pnl = 0.0
    gap_stats = defaultdict(lambda: {"n":0,"p1w":0,"p1p":0})

    for date, day in sorted(history.items()):
        for rec in day.get("races", []):
            raw = raw_index.get(rec["race_id"])
            if not raw:
                continue
            runners = raw.get("runners", [])
            if len(runners) < 2:
                continue

            going = raw.get("going", "")
            ranked = rank_runners(runners, going, market_weight)

            p1 = ranked[0]
            p2 = ranked[1]

            # agree_filter: skip if stats P1 != market P1
            if agree_filter and p1["_stats_rank"] != 1:
                skipped += 1
                continue

            # max_market_rank filter
            if max_market_rank and p1["_market_rank"] > max_market_rank:
                skipped += 1
                continue

            # Determine outcomes
            # History rec stores a_pos/b_pos for the CURRENT model's picks.
            # We need to look up actual finishing position from the raw runners.
            pos_by_horse = {r.get("horse",""): r.get("position") for r in runners}
            n = len(runners)
            ps = place_spots(n)

            def actual_pos(horse_name):
                p = pos_by_horse.get(horse_name)
                try: return int(str(p).strip())
                except: return None

            p1_pos = actual_pos(p1.get("horse",""))
            p2_pos = actual_pos(p2.get("horse",""))
            p1_sp  = to_float(p1.get("sp_dec"))
            p2_sp  = to_float(p2.get("sp_dec"))

            p1_won  = p1_pos == 1
            p2_won  = p2_pos == 1
            p1_plcd = p1_pos is not None and p1_pos <= ps
            p2_plcd = p2_pos is not None and p2_pos <= ps

            # Stats vs market rank gap (how much they agree on P1)
            rank_gap = abs(p1["_stats_rank"] - p1["_market_rank"])
            if rank_gap == 0:   gb = "agree"
            elif rank_gap == 1: gb = "differ_1"
            elif rank_gap <= 3: gb = "differ_2-3"
            else:               gb = "differ_4+"
            gap_stats[gb]["n"]   += 1
            gap_stats[gb]["p1w"] += p1_won
            gap_stats[gb]["p1p"] += p1_plcd

            total  += 1
            p1w    += p1_won
            p2w    += p2_won
            p1p    += p1_plcd
            p2p    += p2_plcd

            if p1_sp:
                p1_pnl += (p1_sp - 1) * 2 if p1_won else -2
            if p2_sp:
                p2_pnl += (p2_sp - 1) * 2 if p2_won else -2

    return {
        "total":    total,
        "skipped":  skipped,
        "p1w":      p1w,
        "p2w":      p2w,
        "p1p":      p1p,
        "p2p":      p2p,
        "p1_pnl":   p1_pnl,
        "p2_pnl":   p2_pnl,
        "gap_stats": gap_stats,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SWEEP: market_weight 0.0 → 1.0
# ═══════════════════════════════════════════════════════════════════════════════

print("="*80)
print("  MARKET WEIGHT SWEEP  (0.0 = pure stats  →  1.0 = pure market SP)")
print("  Flat £2 stake on P1 win. All history races.")
print("="*80)
print(f"  {'MktWt':>6} {'Races':>6} {'P1win%':>8} {'P1plc%':>8} "
      f"{'P2win%':>8} {'P2plc%':>8} {'P1 P&L':>9} {'P2 P&L':>9}")
print(f"  {'-'*72}")

sweep_results = {}
for mw_int in range(0, 11):
    mw = mw_int / 10
    r = evaluate(mw)
    sweep_results[mw] = r
    n = r["total"]
    print(f"  {mw:>6.1f} {n:>6} "
          f"{r['p1w']/n*100:>7.1f}% "
          f"{r['p1p']/n*100:>7.1f}% "
          f"{r['p2w']/n*100:>7.1f}% "
          f"{r['p2p']/n*100:>7.1f}% "
          f"{r['p1_pnl']:>+9.2f} "
          f"{r['p2_pnl']:>+9.2f}")

print()

# ═══════════════════════════════════════════════════════════════════════════════
# RANK AGREEMENT ANALYSIS  (at mw=0.5, best blend)
# ═══════════════════════════════════════════════════════════════════════════════

print("="*80)
print("  RANK AGREEMENT ANALYSIS  (market_weight=0.5)")
print("  How does P1 win rate vary by stats vs market rank agreement?")
print("="*80)

r = sweep_results[0.5]
gs = r["gap_stats"]
print(f"  {'Agreement':14} {'N':>6} {'P1win%':>8} {'P1plc%':>8}")
print(f"  {'-'*40}")
for gb in ["agree", "differ_1", "differ_2-3", "differ_4+"]:
    s = gs.get(gb)
    if s and s["n"]:
        print(f"  {gb:14} {s['n']:>6} "
              f"{s['p1w']/s['n']*100:>7.1f}%  "
              f"{s['p1p']/s['n']*100:>7.1f}%")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# AGREE FILTER: only bet when stats P1 == market P1
# ═══════════════════════════════════════════════════════════════════════════════

print("="*80)
print("  AGREE FILTER  — only bet races where stats rank 1 == market rank 1")
print("  Tests whether agreement between model and market is a quality filter")
print("="*80)
print(f"  {'MktWt':>6} {'Races':>6} {'Skipped':>8} {'P1win%':>8} "
      f"{'P1plc%':>8} {'P1 P&L':>9}")
print(f"  {'-'*58}")

for mw_int in [0, 3, 5, 7, 10]:
    mw = mw_int / 10
    r = evaluate(mw, agree_filter=True)
    n = r["total"]
    if n == 0:
        continue
    print(f"  {mw:>6.1f} {n:>6} {r['skipped']:>8} "
          f"{r['p1w']/n*100:>7.1f}%  "
          f"{r['p1p']/n*100:>7.1f}%  "
          f"{r['p1_pnl']:>+9.2f}")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# MARKET RANK CAP: only bet if stats P1 is top-N in market
# ═══════════════════════════════════════════════════════════════════════════════

print("="*80)
print("  MARKET RANK CAP  (pure stats mw=0.0)")
print("  Only bet if stats P1 is within top-N in the market SP ranking")
print("  Tests whether market acts as a useful sanity filter on stats picks")
print("="*80)
print(f"  {'MaxMktRk':>9} {'Races':>6} {'Skipped':>8} {'P1win%':>8} "
      f"{'P1plc%':>8} {'P1 P&L':>9}")
print(f"  {'-'*58}")

for cap in [1, 2, 3, 4, 5, None]:
    r = evaluate(0.0, max_market_rank=cap)
    n = r["total"]
    lbl = str(cap) if cap else "no cap"
    if n == 0:
        continue
    print(f"  {lbl:>9} {n:>6} {r['skipped']:>8} "
          f"{r['p1w']/n*100:>7.1f}%  "
          f"{r['p1p']/n*100:>7.1f}%  "
          f"{r['p1_pnl']:>+9.2f}")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# COMBINED: best weight + market rank cap
# ═══════════════════════════════════════════════════════════════════════════════

print("="*80)
print("  COMBINED FILTER GRID  — market_weight x market_rank_cap")
print("  P1 win% shown. Best cells highlighted by inspection.")
print("="*80)

caps = [1, 2, 3, 4, 5, None]
weights = [0.0, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 1.0]

# Header
print(f"  {'mw\\cap':>8}", end="")
for cap in caps:
    print(f"  {'top'+str(cap) if cap else 'no cap':>9}", end="")
print()
print(f"  {'-'*75}")

for mw in weights:
    print(f"  {mw:>8.1f}", end="")
    for cap in caps:
        r = evaluate(mw, max_market_rank=cap)
        n = r["total"]
        if n == 0:
            print(f"  {'–':>9}", end="")
        else:
            pct = r['p1w'] / n * 100
            marker = " *" if pct >= 35 else "  "
            print(f"  {pct:>7.1f}%{marker}", end="")
    print()
print()
print("  * = P1 win% >= 35%")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# DEEP DIVE: best combined setting
# ═══════════════════════════════════════════════════════════════════════════════
# Find the combination with highest P1 win% (min 50 races for reliability)

best_win = 0
best_cfg = None
best_result = None
for mw_int in range(0, 11):
    mw = mw_int / 10
    for cap in [1, 2, 3, 4, 5, None]:
        r = evaluate(mw, max_market_rank=cap)
        n = r["total"]
        if n >= 50:
            win_pct = r["p1w"] / n
            if win_pct > best_win:
                best_win = win_pct
                best_cfg = (mw, cap)
                best_result = r

if best_cfg:
    mw, cap = best_cfg
    r = best_result
    n = r["total"]
    print("="*80)
    print(f"  BEST CONFIGURATION (min 50 races)")
    print(f"  market_weight={mw}  market_rank_cap={cap}")
    print("="*80)
    print(f"  Races:        {n}  (skipped: {r['skipped']})")
    print(f"  P1 win rate:  {r['p1w']}/{n} = {r['p1w']/n*100:.1f}%")
    print(f"  P1 place rate:{r['p1p']}/{n} = {r['p1p']/n*100:.1f}%")
    print(f"  P2 win rate:  {r['p2w']}/{n} = {r['p2w']/n*100:.1f}%")
    print(f"  P2 place rate:{r['p2p']}/{n} = {r['p2p']/n*100:.1f}%")
    print(f"  P1 flat P&L:  {r['p1_pnl']:+.2f}")
    print(f"  P2 flat P&L:  {r['p2_pnl']:+.2f}")
    print()
    print("  Rank agreement breakdown at this setting:")
    print(f"  {'Agreement':14} {'N':>6} {'P1win%':>8} {'P1plc%':>8}")
    gs = r["gap_stats"]
    for gb in ["agree", "differ_1", "differ_2-3", "differ_4+"]:
        s = gs.get(gb)
        if s and s["n"]:
            print(f"  {gb:14} {s['n']:>6} "
                  f"{s['p1w']/s['n']*100:>7.1f}%  "
                  f"{s['p1p']/s['n']*100:>7.1f}%")
    print()

# ═══════════════════════════════════════════════════════════════════════════════
# COMPARE VS CURRENT MODEL
# ═══════════════════════════════════════════════════════════════════════════════

print("="*80)
print("  COMPARISON VS CURRENT MODEL")
print("  Current model results from analyse_history.py (A7 totals)")
print("="*80)
print(f"  {'Model':30} {'Races':>6} {'P1win%':>8} {'P1plc%':>8} {'P1 P&L':>9}")
print(f"  {'-'*60}")
print(f"  {'Current (SP-mixed)':30} {'647':>6} {'28.0%':>8} {'63.8%':>8} {'-13.98':>9}")

# Best no-filter multistage
r0 = sweep_results[0.0]
r5 = sweep_results[0.5]
r10 = sweep_results[1.0]
n = r0["total"]
print(f"  {'Multistage mw=0.0 (pure stats)':30} {n:>6} "
      f"{r0['p1w']/n*100:>7.1f}%  {r0['p1p']/n*100:>7.1f}%  {r0['p1_pnl']:>+9.2f}")
n = r5["total"]
print(f"  {'Multistage mw=0.5 (50/50 blend)':30} {n:>6} "
      f"{r5['p1w']/n*100:>7.1f}%  {r5['p1p']/n*100:>7.1f}%  {r5['p1_pnl']:>+9.2f}")
n = r10["total"]
print(f"  {'Multistage mw=1.0 (pure market)':30} {n:>6} "
      f"{r10['p1w']/n*100:>7.1f}%  {r10['p1p']/n*100:>7.1f}%  {r10['p1_pnl']:>+9.2f}")
if best_cfg:
    mw, cap = best_cfg
    r = best_result
    n = r["total"]
    lbl = f"Best: mw={mw} cap={cap}"
    print(f"  {lbl:30} {n:>6} "
          f"{r['p1w']/n*100:>7.1f}%  {r['p1p']/n*100:>7.1f}%  {r['p1_pnl']:>+9.2f}")
print()
print("Done.")
