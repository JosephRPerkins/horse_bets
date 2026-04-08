"""
predict.py

Interactive day predictor. Select a date from available data, and for every
race the script will:

  - Score every runner using the signal model derived from analysis.py
  - Pick the highest-scoring runner as the WIN selection
  - Pick the next two highest scorers as PLACE selections
  - Show the actual result and whether each pick landed

Place terms scale with field size (mirrors bookmaker each-way rules):
  ≤4 runners  → top 1 (win only)
  5–7         → top 2
  8–11        → top 3
  12–15       → top 4
  16–19       → top 5
  20+         → top 6 (capped at 10)

Usage:
    python predict.py
    python predict.py --date 2026-03-20
    python predict.py --date 2026-03-20 --scores   # show all runner scores
"""

import os
import sys
import json
import glob
import argparse
from collections import defaultdict

import config

# ── Helpers ───────────────────────────────────────────────────────────────────

def to_float(val, default=None):
    try:
        return float(str(val).replace("–", "").strip())
    except (ValueError, TypeError):
        return default

def to_int(val, default=None):
    try:
        return int(str(val).strip())
    except (ValueError, TypeError):
        return default

def sp_str(sp_dec):
    """Convert decimal SP to fractional-ish string for display."""
    if sp_dec is None:
        return "—"
    if sp_dec < 2.0:
        # odds-on: show as fraction
        num = sp_dec - 1
        return f"{num:.2g}/1"
    frac = sp_dec - 1
    # round to nearest common fraction
    return f"{frac:.1f}/1"

def finished_in(pos, n):
    try:
        return int(pos) <= n
    except (ValueError, TypeError):
        return False

def is_numeric(pos):
    try:
        int(pos)
        return True
    except (ValueError, TypeError):
        return False

def place_terms(n_runners):
    """How many places count as 'placed' given the field size."""
    if n_runners <= 4:  return 1
    if n_runners <= 7:  return 2
    if n_runners <= 11: return 3
    if n_runners <= 15: return 4
    if n_runners <= 19: return 5
    return min(n_runners // 4, 10)

def dist_furlongs(dist_f_str):
    try:
        return float(str(dist_f_str).replace("f", "").strip())
    except (ValueError, TypeError):
        return None


# ── Signal scoring ────────────────────────────────────────────────────────────

SIGNAL_WEIGHTS = {
    "sp_odds_on":    3,   # SP < 2.0
    "sp_2_to_4":     2,   # SP 2–4
    "sp_4_to_6":     1,   # SP 4–6
    "tsr_gte_or":    3,   # TSR > OR (strongest signal)
    "rpr_gte_or":    2,   # RPR >= OR
    "trainer_pos":   1,   # trainer 14d P&L > 0 (min 5 runs)
    "jockey_pos":    1,   # jockey 14d P&L > 0 (min 5 runs)
    "form_3_of_4":   2,   # placed 3+ of last 4
    "form_2_of_4":   1,   # placed 2+ of last 4
    "no_bad_recent": 1,   # no PU/F/UR in last 3 runs
}

def score_runner(runner):
    sp   = to_float(runner.get("sp_dec"))
    rpr  = to_int(runner.get("rpr"))
    or_  = to_int(runner.get("or"))
    tsr  = to_int(runner.get("tsr"))
    t14  = runner.get("trainer_14d") or {}
    j14  = runner.get("jockey_14d") or {}
    fd   = runner.get("form_detail") or {}

    signals = {}

    if sp is not None:
        if sp < 2.0:
            signals["sp_odds_on"] = SIGNAL_WEIGHTS["sp_odds_on"]
        elif sp < 4.0:
            signals["sp_2_to_4"]  = SIGNAL_WEIGHTS["sp_2_to_4"]
        elif sp < 6.0:
            signals["sp_4_to_6"]  = SIGNAL_WEIGHTS["sp_4_to_6"]

    if tsr is not None and or_ is not None and tsr > or_:
        signals["tsr_gte_or"] = SIGNAL_WEIGHTS["tsr_gte_or"]

    if rpr is not None and or_ is not None and rpr >= or_:
        signals["rpr_gte_or"] = SIGNAL_WEIGHTS["rpr_gte_or"]

    if (t14.get("runs") or 0) >= 5 and (t14.get("pl") or 0) > 0:
        signals["trainer_pos"] = SIGNAL_WEIGHTS["trainer_pos"]

    if (j14.get("runs") or 0) >= 5 and (j14.get("pl") or 0) > 0:
        signals["jockey_pos"] = SIGNAL_WEIGHTS["jockey_pos"]

    placed = fd.get("placed_last_4", 0) or 0
    if placed >= 3:
        signals["form_3_of_4"] = SIGNAL_WEIGHTS["form_3_of_4"]
    elif placed >= 2:
        signals["form_2_of_4"] = SIGNAL_WEIGHTS["form_2_of_4"]

    if (fd.get("bad_recent") or 0) == 0:
        signals["no_bad_recent"] = SIGNAL_WEIGHTS["no_bad_recent"]

    return sum(signals.values()), signals


# ── Display helpers ───────────────────────────────────────────────────────────

TICK  = "✓"
CROSS = "✗"
BOLD  = "\033[1m"
GREEN = "\033[92m"
RED   = "\033[91m"
DIM   = "\033[2m"
RESET = "\033[0m"

def green(s):  return f"{GREEN}{s}{RESET}"
def red(s):    return f"{RED}{s}{RESET}"
def bold(s):   return f"{BOLD}{s}{RESET}"
def dim(s):    return f"{DIM}{s}{RESET}"

def pos_display(pos):
    """Format the actual finishing position."""
    if is_numeric(pos):
        return f"Finished {pos}"
    return f"Did not finish ({pos})"

def result_line(label, horse, sp_dec, score, actual_pos, needed_top, show_score):
    sp_val = to_float(sp_dec)
    sp_s   = f"SP {sp_str(sp_val)}" if sp_val else "SP —"
    score_s = f"[score:{score}]" if show_score else ""

    landed = finished_in(actual_pos, needed_top)
    pos_s  = pos_display(actual_pos)

    if needed_top == 1:
        outcome = f"needed WIN" if not landed else "WIN"
    else:
        outcome = f"needed top {needed_top}" if not landed else f"top {needed_top}"

    result = f"{TICK} {green(outcome)}" if landed else f"{CROSS} {red(outcome)}"
    line = (
        f"  {bold(label):<10} {horse:<30} {sp_s:<12} "
        f"{dim(score_s):<15} → {pos_s:<20} {result}"
    )
    return line, landed


# ── Load / select data ────────────────────────────────────────────────────────

def available_dates():
    files = sorted(glob.glob(os.path.join(config.DIR_RAW, "*.json")))
    return [os.path.basename(f).replace(".json", "") for f in files]

def load_day(date_str):
    path = os.path.join(config.DIR_RAW, f"{date_str}.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)

def pick_date_interactive(dates):
    print()
    print(bold("Available dates:"))
    cols = 5
    for i, d in enumerate(dates, 1):
        print(f"  {i:>3}. {d}", end="\n" if i % cols == 0 else "")
    print()
    while True:
        raw = input("Select date (number or YYYY-MM-DD): ").strip()
        if raw.isdigit():
            idx = int(raw) - 1
            if 0 <= idx < len(dates):
                return dates[idx]
            print(f"  Out of range. Enter 1–{len(dates)}.")
        elif raw in dates:
            return raw
        else:
            print("  Not recognised. Try again.")


# ── Core predictor ────────────────────────────────────────────────────────────

def predict_race(race, show_scores=False):
    """
    Returns a dict with prediction details and outcome.
    """
    runners = race.get("runners", [])
    # Only score runners that have a valid numeric position (i.e. ran)
    # We still predict from all runners (pre-race we don't know who DNF)
    # but for result checking we need actual positions
    scored = []
    for r in runners:
        sc, signals = score_runner(r)
        sp = to_float(r.get("sp_dec"), 999)
        scored.append((sc, sp, r, signals))

    # Sort: highest score first, tiebreak by lowest SP
    scored.sort(key=lambda x: (-x[0], x[1]))

    n_runners = len(runners)
    places    = place_terms(n_runners)

    win_pick   = scored[0] if len(scored) >= 1 else None
    place_picks = scored[1:3] if len(scored) >= 3 else scored[1:2]

    return {
        "race":        race,
        "n_runners":   n_runners,
        "places":      places,
        "win_pick":    win_pick,
        "place_picks": place_picks,
        "all_scored":  scored,
        "show_scores": show_scores,
    }


def display_race(pred, race_num):
    race       = pred["race"]
    places     = pred["places"]
    win_pick   = pred["win_pick"]
    place_picks = pred["place_picks"]
    show_scores = pred["show_scores"]

    course   = race.get("course", "?")
    off      = race.get("off", "")
    rtype    = race.get("type", "")
    dist     = race.get("dist_f", "")
    going    = race.get("going", "")
    cls      = race.get("class", "")
    cls_s    = f" | {cls}" if cls and cls != "Unknown" else ""
    n        = pred["n_runners"]

    print()
    print(bold(f"RACE {race_num} — {off}  {course}  |  {rtype}  |  {dist}  |  {going}{cls_s}"))
    print(f"  {n} runners — places: top {places}")

    if show_scores:
        print(dim(f"  {'Horse':<30} {'SP':<8} {'OR':<5} {'RPR':<5} {'TSR':<5} {'Score':<7} Signals"))
        print(dim(f"  {'-'*30} {'-'*8} {'-'*5} {'-'*5} {'-'*5} {'-'*7} -------"))
        for sc, sp_raw, r, signals in pred["all_scored"]:
            sp_v = to_float(r.get("sp_dec"))
            sp_d = f"{sp_str(sp_v):<8}" if sp_v else f"{'—':<8}"
            sig_names = "+".join(k for k in signals)
            pos = r.get("position", "?")
            pos_s = f"[{pos}]"
            print(dim(f"  {r['horse']:<30} {sp_d} {str(r.get('or','—')):<5} {str(r.get('rpr','—')):<5} {str(r.get('tsr','—')):<5} {sc:<7} {sig_names}  {pos_s}"))
        print()

    correct = 0
    total   = 0

    if win_pick:
        sc, sp_raw, r, _ = win_pick
        line, landed = result_line(
            "WIN", r["horse"], r.get("sp_dec"), sc,
            r.get("position", "?"), 1, show_scores
        )
        print(line)
        total += 1
        correct += 1 if landed else 0

    for i, pick in enumerate(place_picks, 1):
        sc, sp_raw, r, _ = pick
        line, landed = result_line(
            f"PLACE {i}", r["horse"], r.get("sp_dec"), sc,
            r.get("position", "?"), places, show_scores
        )
        print(line)
        total += 1
        correct += 1 if landed else 0

    return correct, total


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Race day predictor")
    parser.add_argument("--date",   help="Date to analyse (YYYY-MM-DD)")
    parser.add_argument("--scores", action="store_true",
                        help="Show all runner scores for each race")
    args = parser.parse_args()

    dates = available_dates()
    if not dates:
        print("No data found in data/raw/. Run fetch_data.py first.")
        sys.exit(1)

    date_str = args.date if args.date else pick_date_interactive(dates)

    if date_str not in dates:
        print(f"No data for {date_str}. Available: {dates[0]} → {dates[-1]}")
        sys.exit(1)

    day = load_day(date_str)
    if not day:
        print(f"Could not load {date_str}")
        sys.exit(1)

    races = day.get("results", [])
    print()
    print("=" * 70)
    print(bold(f"  PREDICTIONS — {date_str}  ({len(races)} races)"))
    print("=" * 70)

    total_correct = 0
    total_picks   = 0
    win_correct   = 0
    win_total     = 0
    place_correct = 0
    place_total   = 0

    race_results = []

    for i, race in enumerate(races, 1):
        pred = predict_race(race, show_scores=args.scores)

        # Count separately
        n_runners = pred["n_runners"]
        places    = pred["places"]

        # WIN
        if pred["win_pick"]:
            sc, _, r, _ = pred["win_pick"]
            win_landed = finished_in(r.get("position", ""), 1)
            win_correct += 1 if win_landed else 0
            win_total   += 1

        # PLACE picks
        for pick in pred["place_picks"]:
            sc, _, r, _ = pick
            pl_landed = finished_in(r.get("position", ""), places)
            place_correct += 1 if pl_landed else 0
            place_total   += 1

        correct, total = display_race(pred, i)
        total_correct += correct
        total_picks   += total
        race_results.append((correct, total))

    # ── Summary ──────────────────────────────────────────────────────────────
    print()
    print("=" * 70)
    print(bold("  SUMMARY"))
    print("=" * 70)

    def pct(a, b):
        return f"{100*a/b:.1f}%" if b else "—"

    print(f"  Races analysed   : {len(races)}")
    print()
    print(f"  WIN picks        : {win_correct}/{win_total}  ({pct(win_correct, win_total)})")
    print(f"  PLACE picks      : {place_correct}/{place_total}  ({pct(place_correct, place_total)})")
    print(f"  OVERALL          : {total_correct}/{total_picks}  ({pct(total_correct, total_picks)})")
    print()

    # Score breakdown — how many races got all 3 right, 2/3, 1/3, 0/3
    breakdown = defaultdict(int)
    for c, t in race_results:
        breakdown[f"{c}/{t}"] += 1

    print("  Per-race accuracy:")
    for k in sorted(breakdown.keys(), reverse=True):
        bar = "█" * breakdown[k]
        print(f"    {k} correct : {breakdown[k]:>3} races  {bar}")
    print()


if __name__ == "__main__":
    main()
