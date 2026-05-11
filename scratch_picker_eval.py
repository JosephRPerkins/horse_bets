"""
scratch_picker_eval.py
======================
Fundamental picker evaluation using clean card data.
Answers: is the prediction system broken, and what should it look like?

Section A: Current picker — what we actually pick and how it performs
Section B: Naive baselines — market fav, 2nd fav, random, hindsight ceiling
Section C: Single-signal pickers — each signal in isolation, null-filtered
Section D: Signal combination grid — weighted blends of RPR/OR/TSR/SP/form/trainer
Section E: Agreement filter — only bet when N signals agree on same horse
Section F: Best combinations — top performers with context and losing run stats

Key principle: races where a signal is absent are EXCLUDED from that signal's
analysis. A signal's performance is only measured where it actually has data.
Cross-signal combinations require ALL constituent signals to be present.

Run from ~/horse_bets_v3:
    python3 scratch_picker_eval.py 2>&1 | tee picker_eval_output.txt
"""

import json, os, sys, itertools
from collections import defaultdict
sys.path.insert(0, os.path.dirname(__file__))

COMMISSION  = 0.05
FLAT_STAKE  = 2.0
CARDS_DIR   = "data/cards"
RAW_DIR     = "data/raw"

# ── Helpers ───────────────────────────────────────────────────────────────────

def tof(v):
    try:
        f = float(str(v).strip())
        return f if f > 0 else None
    except: return None

def get_pos(r):
    try: return int(str(r.get("position","")).strip())
    except: return None

def norm_time(t):
    if not t: return ""
    parts = str(t).strip().split(":")
    if len(parts) != 2: return str(t).strip()
    try:
        h, m = int(parts[0]), int(parts[1])
        if 1 <= h <= 9: h += 12
        return f"{h:02d}:{m:02d}"
    except: return str(t).strip()

def field_ok(runners, race):
    n = len(runners)
    if n < 3: return False
    rt = str(race.get("type","") or "").lower()
    is_jump = any(t in rt for t in ("chase","hurdle","nh flat","national hunt"))
    cls = str(race.get("race_class","") or race.get("class","") or "").replace("Class","").strip()
    if cls in ("1","2") and is_jump: return False
    if is_jump and n > 12: return False
    if not is_jump and n > 20: return False
    return True

def place_spots(n):
    if n <= 4: return 1
    if n <= 7: return 2
    if n <= 11: return 3
    return 4

def pct(a, b): return f"{a/b*100:.1f}%" if b else "—"
def sgn(v):    return f"+£{v:.2f}" if v >= 0 else f"-£{abs(v):.2f}"

def win_pnl(sp, won):
    return round(FLAT_STAKE*(sp-1)*(1-COMMISSION),2) if won else -FLAT_STAKE

def losing_runs(seq):
    runs = []; cur = 0
    for w in seq:
        if not w: cur += 1
        else:
            if cur: runs.append(cur)
            cur = 0
    if cur: runs.append(cur)
    return runs

# ── Signal extractors (return None if absent) ─────────────────────────────────

def sig_rpr(r):     return tof(r.get("rpr"))
def sig_or(r):      return tof(r.get("ofr") or r.get("or"))
def sig_tsr(r):     return tof(r.get("ts") or r.get("tsr"))
def sig_sp(r):      return tof(r.get("sp_dec"))   # lower = better (invert for ranking)
def sig_plc4(r):
    fd = r.get("form_detail") or {}
    v = fd.get("placed_last_4",0) if isinstance(fd,dict) else 0
    return float(v) if v is not None else None
def sig_trainer(r):
    t = r.get("trainer_14d") or {}
    if not isinstance(t, dict): return None
    runs = t.get("runs",0) or 0
    if runs < 3: return None
    ae = t.get("ae") or t.get("win_pct")  # try ae first, fallback to win_pct
    return tof(ae)
def sig_form_bad(r):
    """0 = no bad recent form (good), 1 = bad (penalise). Inverted for ranking."""
    fd = r.get("form_detail") or {}
    if not isinstance(fd, dict): return None
    bad = fd.get("bad_recent",0) or 0
    return float(-bad)  # negate so higher = better for ranking

# Score margin within field (how much does P1 stand out?)
def score_margin(scores):
    if len(scores) < 2: return 0
    s = sorted([v for v in scores if v is not None], reverse=True)
    if len(s) < 2: return 0
    return s[0] - s[1]

# ── Normalise a value within its field ────────────────────────────────────────

def normalise(val, field_vals, scale=10.0):
    if val is None: return None
    valid = [v for v in field_vals if v is not None]
    if not valid or len(valid) < 2: return scale/2
    lo, hi = min(valid), max(valid)
    if hi == lo: return scale/2
    return ((val-lo)/(hi-lo))*scale

# ── Build race dataset ────────────────────────────────────────────────────────

print("Loading data...")

# Results index: (date, course, off) -> {horse_id -> {pos, sp, bsp}}
results_index = {}
for fp in sorted(os.listdir(RAW_DIR)):
    if not fp.endswith(".json") or not fp[:4].isdigit(): continue
    date = fp.replace(".json","")
    with open(f"{RAW_DIR}/{fp}") as f: d = json.load(f)
    for race in (d.get("results") or d.get("races") or []):
        course = race.get("course","").lower().strip()
        off    = norm_time(race.get("off",""))
        key    = (date, course, off)
        hmap   = {}
        for r in race.get("runners",[]):
            hid = r.get("horse_id","")
            pos = get_pos(r)
            sp  = tof(r.get("sp_dec"))
            bsp = tof(r.get("bsp") or r.get("bsp_dec"))
            if hid and pos is not None:
                hmap[hid] = {"pos":pos, "sp":sp, "bsp":bsp or sp}
        results_index[key] = hmap

# Card races: build one record per race with all runners enriched
races = []
for fp in sorted(os.listdir(CARDS_DIR)):
    if not fp.endswith(".json") or not fp[:4].isdigit(): continue
    date = fp.replace(".json","")
    with open(f"{CARDS_DIR}/{fp}") as f: d = json.load(f)
    for race in (d.get("races") or []):
        runners = race.get("all_runners",[])
        if not runners: continue
        if not field_ok(runners, race): continue
        course = race.get("course","").lower().strip()
        off    = norm_time(race.get("off",""))
        rmap   = results_index.get((date, course, off),{})
        if not rmap: continue
        # Need at least a winner
        if not any(v["pos"]==1 for v in rmap.values()): continue

        n = len(runners)
        # Enrich each runner with result data
        enriched = []
        for r in runners:
            hid  = r.get("horse_id","")
            res  = rmap.get(hid,{})
            pos  = res.get("pos")
            sp_r = res.get("sp")    # settled SP
            bsp  = res.get("bsp")   # BSP
            enriched.append({
                **r,
                "_pos": pos,
                "_sp_result": sp_r,
                "_bsp": bsp,
                "_won": pos==1,
                "_placed": pos<=place_spots(n) if pos else False,
            })

        races.append({
            "date":     date,
            "course":   course,
            "off":      off,
            "n":        n,
            "type":     race.get("type","") or "",
            "class":    str(race.get("race_class","") or race.get("class","") or ""),
            "going":    race.get("going","") or "",
            "runners":  enriched,
        })

print(f"Races loaded: {len(races)} across {len(set(r['date'] for r in races))} dates")
print()

# ── Evaluation engine ─────────────────────────────────────────────────────────

def evaluate_picker(races, score_fn, label="", require_signals=None):
    """
    score_fn(runner, field_runners) -> float or None
    If None returned for P1 candidate, race is skipped.
    require_signals: list of signal extractor fns — race only included if ALL
                     return non-None for at least 50% of runners.
    Returns dict of stats.
    """
    n_total = n_skip_sig = n_skip_nores = 0
    wins = losses = 0
    pnl = staked = 0.0
    seq = []
    top2_wins = 0
    winner_sp_vals = []
    our_sp_vals    = []

    for race in races:
        runners = race["runners"]
        n_total += 1

        # Signal coverage check
        if require_signals:
            skip = False
            for sig_fn in require_signals:
                vals = [sig_fn(r) for r in runners]
                if sum(1 for v in vals if v is not None) / len(runners) < 0.5:
                    skip = True
                    break
            if skip:
                n_skip_sig += 1
                continue

        # Score all runners
        scores = [(score_fn(r, runners), r) for r in runners]
        # Filter out runners where score is None
        scored = [(s,r) for s,r in scores if s is not None]
        if len(scored) < 2: continue

        scored.sort(key=lambda x: -x[0])
        p1 = scored[0][1]
        p2 = scored[1][1] if len(scored) > 1 else None

        sp   = tof(p1.get("sp_dec"))  # morning price
        won  = p1.get("_won", False)
        pos  = p1.get("_pos")
        if pos is None or not sp: continue

        # winner SP for ceiling calculation
        winner = next((r for _,r in scores if r.get("_won")), None)
        if winner:
            wsp = tof(winner.get("sp_dec"))
            if wsp: winner_sp_vals.append(wsp)

        # top-2 check
        p2_won = p2.get("_won",False) if p2 else False
        if won or p2_won: top2_wins += 1

        our_sp_vals.append(sp)
        p = win_pnl(sp, won)
        pnl   += p
        staked += FLAT_STAKE
        seq.append(won)
        if won: wins += 1
        else:   losses += 1

    n = wins + losses
    runs = losing_runs(seq)
    return {
        "label":        label,
        "n":            n,
        "n_skip_sig":   n_skip_sig,
        "wins":         wins,
        "win_rate":     wins/n*100 if n else 0,
        "top2_rate":    top2_wins/n*100 if n else 0,
        "pnl":          pnl,
        "per_bet":      pnl/n if n else 0,
        "roi":          pnl/staked*100 if staked else 0,
        "avg_sp":       sum(our_sp_vals)/len(our_sp_vals) if our_sp_vals else 0,
        "avg_win_sp":   sum(winner_sp_vals)/len(winner_sp_vals) if winner_sp_vals else 0,
        "max_run":      max(runs) if runs else 0,
        "runs_gte_5":   sum(1 for r in runs if r>=5),
        "runs_gte_10":  sum(1 for r in runs if r>=10),
    }

def print_result(r, extra=""):
    print(f"  {r['label']:<45} {r['n']:>5} {r['win_rate']:>6.1f}% "
          f"{r['top2_rate']:>7.1f}% {r['pnl']:>+9.2f} {r['per_bet']:>+8.3f} "
          f"{r['roi']:>6.1f}% {r['avg_sp']:>6.2f} {r['max_run']:>7}{extra}")

HDR = (f"  {'Picker':<45} {'N':>5} {'Win%':>6} {'Top2%':>7} "
       f"{'P&L':>9} {'PerBet':>8} {'ROI':>6} {'AvgSP':>6} {'MaxRun':>7}")
SEP = "  " + "-"*102

# ── SECTION A: Current picker ─────────────────────────────────────────────────

print("=" * 75)
print("A. CURRENT PICKER — what the deployed model actually picks")
print("   Using card top1 (pre-baked by fetch_data) as the live pick")
print("=" * 75)
print()
print(HDR); print(SEP)

# A1: card pre-baked top1
def score_card_top1(r, field):
    """Uses the pre-baked top1 from card — rank 1 = P1, else 0."""
    # The card stores top1 at race level. We need to identify which runner
    # matches the card's top1 pick. Use the pre-baked score field.
    return tof(r.get("score")) or 0.0

# Better: use the card's pre-computed score field directly
def score_card_prebaked(r, field):
    return tof(r.get("score")) or 0.0

r_prebaked = evaluate_picker(races, score_card_prebaked,
    "Card pre-baked score (deployed)", require_signals=None)
print_result(r_prebaked)

# A2: SP-free score (remove SP signals)
from predict import score_runner, SIGNAL_WEIGHTS
SP_SIGNALS = {"sp_odds_on","sp_2_to_4","sp_4_to_6"}
def score_spfree(r, field):
    sc, sigs = score_runner(r)
    return sc - sum(SIGNAL_WEIGHTS.get(s,0) for s in sigs if s in SP_SIGNALS)

r_spfree = evaluate_picker(races, score_spfree, "SP-free score_runner")
print_result(r_spfree)

print()

# ── SECTION B: Naive baselines ────────────────────────────────────────────────

print("=" * 75)
print("B. NAIVE BASELINES — what simple strategies achieve")
print("=" * 75)
print()
print(HDR); print(SEP)

# B1: Market favourite (shortest morning price)
def score_mkt_fav(r, field):
    sp = tof(r.get("sp_dec"))
    return -sp if sp else None  # negate: shorter = higher score
r_fav = evaluate_picker(races, score_mkt_fav, "Market favourite (shortest SP)")
print_result(r_fav)

# B2: Second favourite
def score_second_fav(r, field):
    sps = sorted([tof(x.get("sp_dec")) for x in field if tof(x.get("sp_dec"))])
    if len(sps) < 2: return None
    second_sp = sps[1]
    my_sp = tof(r.get("sp_dec"))
    return 1.0 if my_sp == second_sp else 0.0
r_2nd = evaluate_picker(races, score_second_fav, "Second favourite")
print_result(r_2nd)

# B3: Highest RPR
def score_rpr_only(r, field):
    return tof(r.get("rpr"))
r_rpr = evaluate_picker(races, score_rpr_only, "Highest RPR only",
    require_signals=[sig_rpr])
print_result(r_rpr)

# B4: Longest SP (anti-strategy for reference)
def score_longest(r, field):
    sp = tof(r.get("sp_dec"))
    return sp  # longer = higher score
r_long = evaluate_picker(races, score_longest, "Longest SP (anti-strategy)")
print_result(r_long)

# B5: Perfect hindsight ceiling
# What if we always picked the winner? Average winner SP tells us theoretical max
winner_sps = []
for race in races:
    winner = next((r for r in race["runners"] if r.get("_won")), None)
    if winner:
        sp = tof(winner.get("sp_dec"))
        if sp: winner_sps.append(sp)

avg_winner_sp = sum(winner_sps)/len(winner_sps) if winner_sps else 0
ceiling_pnl   = sum((sp-1)*FLAT_STAKE*(1-COMMISSION) for sp in winner_sps)
print()
print(f"  PERFECT HINDSIGHT CEILING:")
print(f"    Races with known winner SP: {len(winner_sps)}")
print(f"    Avg winner SP:              {avg_winner_sp:.2f}")
print(f"    Perfect P&L (always right): {sgn(ceiling_pnl)}")
print(f"    Per bet if always correct:  {sgn(ceiling_pnl/len(winner_sps))}")
print(f"    (You need to win at ~{100/(avg_winner_sp*(1-COMMISSION)):.1f}% to break even at avg SP)")
print()

# ── SECTION C: Single-signal pickers ─────────────────────────────────────────

print("=" * 75)
print("C. SINGLE-SIGNAL PICKERS — each signal in isolation")
print("   Races excluded where signal has <50% field coverage")
print("=" * 75)
print()
print(HDR); print(SEP)

single_signals = [
    ("RPR (highest = pick)",        lambda r,f: tof(r.get("rpr")),                          [sig_rpr]),
    ("OR (highest = pick)",         lambda r,f: tof(r.get("ofr") or r.get("or")),           [sig_or]),
    ("TSR (highest = pick)",        lambda r,f: tof(r.get("ts") or r.get("tsr")),           [sig_tsr]),
    ("SP (shortest = pick)",        lambda r,f: -(tof(r.get("sp_dec")) or 999),             None),
    ("Placed last 4 (most = pick)", lambda r,f: sig_plc4(r),                               None),
    ("Trainer AE (highest = pick)", lambda r,f: sig_trainer(r),                            None),
    ("Form bad flag (fewest = pick)",lambda r,f: sig_form_bad(r),                          None),
    ("RPR normalised in field",
        lambda r,f: normalise(tof(r.get("rpr")),
                               [tof(x.get("rpr")) for x in f], 10),                        [sig_rpr]),
    ("OR normalised in field",
        lambda r,f: normalise(tof(r.get("ofr") or r.get("or")),
                               [tof(x.get("ofr") or x.get("or")) for x in f], 10),        [sig_or]),
    ("TSR normalised in field",
        lambda r,f: normalise(tof(r.get("ts") or r.get("tsr")),
                               [tof(x.get("ts") or x.get("tsr")) for x in f], 10),        [sig_tsr]),
    ("RPR > OR gap (rpr-or)",
        lambda r,f: (tof(r.get("rpr")) or 0) - (tof(r.get("ofr") or r.get("or")) or 0)
                    if tof(r.get("rpr")) and tof(r.get("ofr") or r.get("or")) else None,  [sig_rpr, sig_or]),
]

for label, score_fn, req in single_signals:
    result = evaluate_picker(races, score_fn, label, require_signals=req)
    print_result(result)

print()

# ── SECTION D: Signal combination grid ───────────────────────────────────────

print("=" * 75)
print("D. SIGNAL COMBINATION GRID")
print("   Weighted blends of RPR + OR + TSR + SP + Form + Trainer")
print("   All signals normalised within field. SP weight: 0=ignore, 1=include.")
print("   Only races where ALL included signals have >=50% field coverage.")
print("=" * 75)
print()

def combo_score(r, field, w_rpr, w_or, w_tsr, w_sp, w_plc, w_trainer):
    """
    Normalised weighted score. Returns None if any included signal
    (weight>0) is absent for this runner.
    """
    score = 0.0

    # Collect field values for normalisation
    f_rpr     = [tof(x.get("rpr")) for x in field]
    f_or      = [tof(x.get("ofr") or x.get("or")) for x in field]
    f_tsr     = [tof(x.get("ts") or x.get("tsr")) for x in field]
    f_sp      = [tof(x.get("sp_dec")) for x in field]
    f_plc     = [sig_plc4(x) for x in field]
    f_trainer = [sig_trainer(x) for x in field]

    if w_rpr > 0:
        v = tof(r.get("rpr"))
        if v is None: return None
        score += w_rpr * normalise(v, f_rpr, 10)

    if w_or > 0:
        v = tof(r.get("ofr") or r.get("or"))
        if v is None: return None
        score += w_or * normalise(v, f_or, 10)

    if w_tsr > 0:
        v = tof(r.get("ts") or r.get("tsr"))
        if v is None: return None
        score += w_tsr * normalise(v, f_tsr, 10)

    if w_sp > 0:
        v = tof(r.get("sp_dec"))
        if v is None: return None
        # invert SP: shorter price = higher normalised value
        inv_sps = [-x for x in f_sp if x is not None]
        score += w_sp * normalise(-v, inv_sps, 10)

    if w_plc > 0:
        v = sig_plc4(r)
        if v is None: return None
        score += w_plc * normalise(v, f_plc, 10)

    if w_trainer > 0:
        v = sig_trainer(r)
        if v is None: return None
        score += w_trainer * normalise(v, [sig_trainer(x) for x in field], 10)

    return score

# Grid: vary weights in steps
# Keep it tractable: 3 levels per signal (0=off, 1=half, 2=full)
# For each combo, require signals for any weight>0

grid_results = []

weight_options = [0, 1, 2]

print(f"  Running grid... (this may take a moment)")
print()

for w_rpr, w_or, w_tsr, w_sp, w_plc, w_trainer in itertools.product(
    weight_options, weight_options, weight_options,
    weight_options, weight_options, weight_options
):
    # Skip if all zero
    if w_rpr+w_or+w_tsr+w_sp+w_plc+w_trainer == 0: continue
    # Skip pure SP (that's already in baselines)
    if w_rpr+w_or+w_tsr+w_plc+w_trainer == 0: continue

    # Required signals for coverage check
    req = []
    if w_rpr > 0:     req.append(sig_rpr)
    if w_or > 0:      req.append(sig_or)
    if w_tsr > 0:     req.append(sig_tsr)

    label = (f"rpr={w_rpr} or={w_or} tsr={w_tsr} "
             f"sp={w_sp} plc={w_plc} tr={w_trainer}")

    def make_scorer(wr,wo,wt,ws,wp,wtr):
        return lambda r,f: combo_score(r,f,wr,wo,wt,ws,wp,wtr)

    result = evaluate_picker(
        races,
        make_scorer(w_rpr,w_or,w_tsr,w_sp,w_plc,w_trainer),
        label,
        require_signals=req if req else None
    )
    result["weights"] = (w_rpr,w_or,w_tsr,w_sp,w_plc,w_trainer)
    grid_results.append(result)

# Sort by per_bet descending, minimum 100 bets
qualified = [r for r in grid_results if r["n"] >= 100]
qualified.sort(key=lambda x: -x["per_bet"])

print(f"  Grid combinations run: {len(grid_results)}")
print(f"  Combinations with >=100 bets: {len(qualified)}")
print()

print(f"  TOP 20 BY PER-BET P&L (>= 100 bets):")
print(f"  {'Weights (rpr/or/tsr/sp/plc/tr)':<38} {'N':>5} {'Win%':>6} "
      f"{'Top2%':>7} {'P&L':>9} {'PerBet':>8} {'ROI':>6} {'AvgSP':>6} {'MaxRun':>7}")
print("  " + "-"*100)
for r in qualified[:20]:
    wr,wo,wt,ws,wp,wtr = r["weights"]
    wlbl = f"rpr={wr} or={wo} tsr={wt} sp={ws} plc={wp} tr={wtr}"
    print(f"  {wlbl:<38} {r['n']:>5} {r['win_rate']:>6.1f}% "
          f"{r['top2_rate']:>7.1f}% {r['pnl']:>+9.2f} {r['per_bet']:>+8.3f} "
          f"{r['roi']:>6.1f}% {r['avg_sp']:>6.2f} {r['max_run']:>7}")

print()
print(f"  BOTTOM 10 (worst performers, >= 100 bets):")
print("  " + "-"*100)
for r in qualified[-10:]:
    wr,wo,wt,ws,wp,wtr = r["weights"]
    wlbl = f"rpr={wr} or={wo} tsr={wt} sp={ws} plc={wp} tr={wtr}"
    print(f"  {wlbl:<38} {r['n']:>5} {r['win_rate']:>6.1f}% "
          f"{r['top2_rate']:>7.1f}% {r['pnl']:>+9.2f} {r['per_bet']:>+8.3f} "
          f"{r['roi']:>6.1f}% {r['avg_sp']:>6.2f} {r['max_run']:>7}")
print()

# ── SECTION E: Agreement filter ───────────────────────────────────────────────

print("=" * 75)
print("E. AGREEMENT FILTER — only bet when N signals point to same horse")
print("   Tests whether signal consensus is a quality filter")
print("=" * 75)
print()
print(f"  {'Filter':<45} {'N':>5} {'Win%':>6} {'Top2%':>7} "
      f"{'P&L':>9} {'PerBet':>8} {'ROI':>6} {'AvgSP':>6} {'MaxRun':>7}")
print("  " + "-"*100)

signal_fns = [
    ("rpr",     lambda r: tof(r.get("rpr"))),
    ("or",      lambda r: tof(r.get("ofr") or r.get("or"))),
    ("tsr",     lambda r: tof(r.get("ts") or r.get("tsr"))),
    ("sp",      lambda r: -(tof(r.get("sp_dec")) or 999)),
    ("plc4",    sig_plc4),
    ("trainer", sig_trainer),
]

for min_agree in [2, 3, 4, 5, 6]:
    n_bets = wins = 0
    pnl = 0.0; seq = []; top2 = 0

    for race in races:
        runners = race["runners"]

        # For each runner, count how many signals rank it #1
        votes = defaultdict(int)
        for sig_name, sig_fn in signal_fns:
            scored = [(sig_fn(r), r.get("horse_id","")) for r in runners]
            scored = [(v,hid) for v,hid in scored if v is not None]
            if not scored: continue
            scored.sort(key=lambda x: -x[0])
            top_hid = scored[0][1]
            votes[top_hid] += 1

        # Find horse with most votes
        if not votes: continue
        best_hid = max(votes, key=lambda h: votes[h])
        if votes[best_hid] < min_agree: continue

        p1 = next((r for r in runners if r.get("horse_id","")==best_hid), None)
        if not p1: continue
        sp  = tof(p1.get("sp_dec"))
        pos = p1.get("_pos")
        if not sp or pos is None: continue

        won = p1.get("_won",False)
        p2_won = any(r.get("_won",False) for r in runners
                     if r.get("horse_id","")!=best_hid and
                     votes.get(r.get("horse_id",""),0) >= 1)

        n_bets += 1
        seq.append(won)
        pnl += win_pnl(sp, won)
        if won: wins += 1
        if won or p2_won: top2 += 1

    if n_bets == 0: continue
    runs = losing_runs(seq)
    label = f"At least {min_agree} of {len(signal_fns)} signals agree"
    print(f"  {label:<45} {n_bets:>5} {wins/n_bets*100:>6.1f}% "
          f"{top2/n_bets*100:>7.1f}% {pnl:>+9.2f} {pnl/n_bets:>+8.3f} "
          f"{pnl/(n_bets*FLAT_STAKE)*100:>6.1f}% "
          f"{'—':>6} {max(runs) if runs else 0:>7}")
print()

# ── SECTION F: Best combinations deep dive ────────────────────────────────────

print("=" * 75)
print("F. BEST COMBINATIONS — deep dive on top performers")
print("   Score margin, SP band breakdown, and losing run analysis")
print("=" * 75)
print()

# Take top 5 from grid
top5 = qualified[:5] if len(qualified) >= 5 else qualified

for i, result in enumerate(top5):
    wr,wo,wt,ws,wp,wtr = result["weights"]
    print(f"  #{i+1}: rpr={wr} or={wo} tsr={wt} sp={ws} plc={wp} tr={wtr}")
    print(f"       {result['n']} bets | {result['win_rate']:.1f}% win | "
          f"{sgn(result['pnl'])} total | {sgn(result['per_bet'])} per bet | "
          f"{result['roi']:.1f}% ROI | max run {result['max_run']}")

    # Score margin analysis for this combination
    req = []
    if wr > 0: req.append(sig_rpr)
    if wo > 0: req.append(sig_or)
    if wt > 0: req.append(sig_tsr)

    margin_stats = defaultdict(lambda: {"n":0,"w":0,"pnl":0.0})

    for race in races:
        runners = race["runners"]
        if req:
            skip = False
            for sig_fn in req:
                vals = [sig_fn(r) for r in runners]
                if sum(1 for v in vals if v is not None)/len(runners) < 0.5:
                    skip = True; break
            if skip: continue

        scores = [(combo_score(r,runners,wr,wo,wt,ws,wp,wtr),r) for r in runners]
        scored = [(s,r) for s,r in scores if s is not None]
        if len(scored) < 2: continue
        scored.sort(key=lambda x: -x[0])
        p1 = scored[0][1]
        sp = tof(p1.get("sp_dec"))
        if not sp or p1.get("_pos") is None: continue

        margin = scored[0][0] - scored[1][0]
        won = p1.get("_won",False)
        p = win_pnl(sp, won)

        if margin < 1:       mb = "margin<1"
        elif margin < 5:     mb = "margin 1-5"
        elif margin < 10:    mb = "margin 5-10"
        else:                mb = "margin 10+"

        margin_stats[mb]["n"]   += 1
        margin_stats[mb]["w"]   += int(won)
        margin_stats[mb]["pnl"] += p

    print(f"       Score margin breakdown:")
    for mb in ["margin<1","margin 1-5","margin 5-10","margin 10+"]:
        s = margin_stats.get(mb)
        if not s or not s["n"]: continue
        print(f"         {mb:<15} n={s['n']:>4}  win={pct(s['w'],s['n']):>6}  "
              f"pnl={sgn(s['pnl']):>9}  per bet={sgn(s['pnl']/s['n'])}")
    print()

# ── SECTION G: Summary comparison ────────────────────────────────────────────

print("=" * 75)
print("G. SUMMARY — current picker vs best found vs baselines")
print("=" * 75)
print()
print(HDR); print(SEP)

# Current
print_result(r_prebaked, "  ← deployed")
print_result(r_spfree)

# Baselines
print_result(r_fav,  "  ← naive baseline")
print_result(r_rpr)

# Best from grid
if qualified:
    print()
    print(f"  Best grid results (top 3):")
    for r in qualified[:3]:
        print_result(r)

print()
print("Done.")
