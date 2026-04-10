"""
predict_v2.py  —  Race Day Predictor (backtest-informed)

Runs the same WIN/PLACE selection model as predict.py but layers on
confidence tiers derived from the 30-day backtest, so you can see
immediately which races are worth betting and which to skip.

CONFIDENCE TIERS (from backtest_report.txt):
  🔥🔥🔥  SUPREME  — TSR solo trigger fires (93% WIN rate across 57 races)
  🔥🔥    STRONG   — Score ≥7 + Chase/Hurdle/NH Flat / Turf  (65–67%)
             OR: Odds-on fav present + Turf + ≤7 runners (57–67%)
  🔥       GOOD    — Score ≥6 + Turf  (58%)
             OR: Class 3/4 + Chase/Hurdle  (55%)
             OR: Field ≤7 + Jump race  (54%)
  ·        STANDARD — everything else (45% avg, lower confidence)
  ✗        SKIP    — Flat/AW + score <6, or field 12+, or Class 1/2

BET RECOMMENDATIONS per tier:
  🔥🔥🔥  WIN only (93% — don't dilute into each-way)
  🔥🔥    WIN or each-way
  🔥       Each-way
  ·        Information only
  ✗        Skip / avoid

Races are displayed ordered by confidence tier (best first), then by
off time within each tier so you know when each runs.

Usage:
    python predict_v2.py
    python predict_v2.py --date 2026-03-20
    python predict_v2.py --date 2026-03-20 --scores    # show all runner detail
    python predict_v2.py --date 2026-03-20 --bet-only  # show 🔥 races only
"""

import os
import sys
import json
import glob
import argparse
from collections import defaultdict

import config
from predict import (
    score_runner, place_terms, dist_furlongs,
    to_float, to_int, finished_in, is_numeric,
    sp_str, pos_display,
    TICK, CROSS, BOLD, GREEN, RED, DIM, RESET,
    green, red, bold, dim,
)

# ── Confidence tier engine ────────────────────────────────────────────────────

TIER_SUPREME = 3   # 🔥🔥🔥
TIER_STRONG  = 2   # 🔥🔥
TIER_GOOD    = 1   # 🔥
TIER_STD     = 0   # ·
TIER_SKIP    = -1  # ✗

TIER_LABELS = {
    TIER_SUPREME: "🔥🔥🔥 SUPREME",
    TIER_STRONG:  "🔥🔥  STRONG ",
    TIER_GOOD:    "🔥    GOOD   ",
    TIER_STD:     "·     STANDARD",
    TIER_SKIP:    "✗     SKIP   ",
}

TIER_BET = {
    TIER_SUPREME: "WIN only",
    TIER_STRONG:  "WIN / each-way",
    TIER_GOOD:    "Each-way",
    TIER_STD:     "Info only",
    TIER_SKIP:    "Skip",
}

# Expected win rates per tier (from backtest)
TIER_WIN_PCT = {
    TIER_SUPREME: "~93%",
    TIER_STRONG:  "~65%",
    TIER_GOOD:    "~56%",
    TIER_STD:     "~43%",
    TIER_SKIP:    "~35%",
}


def has_tsr_solo(runners):
    """Exactly one runner has TSR strictly above their OR."""
    count = 0
    for r in runners:
        tsr = to_int(r.get("tsr"))
        or_ = to_int(r.get("or"))
        if tsr is not None and or_ is not None and tsr > or_:
            count += 1
    return count == 1


def ratings_coverage(runners: list) -> float:
    """Fraction of runners that have a valid TSR or RPR value."""
    if not runners:
        return 0.0
    rated = sum(
        1 for r in runners
        if (str(r.get("tsr") or "").strip() not in ("", "–", "-"))
        or (str(r.get("rpr") or "").strip() not in ("", "–", "-"))
    )
    return rated / len(runners)


def race_confidence(race, win_score):
    """
    Return (tier, reasons) for a race given its win-pick score and race metadata.
    Applies the exact filter hierarchy proved in the backtest.
    """
    rtype   = race.get("type", "Unknown")
    surface = race.get("surface") or "Turf"
    cls     = race.get("class", "Unknown") or "Unknown"
    runners = race.get("runners", [])
    n       = len(runners)
    dist_f  = dist_furlongs(race.get("dist_f", ""))

    # ── Ratings quality gate ──────────────────────────────────────────────────
    # The model's edge comes from RPR/TSR. Without them it's no better than SP.
    # If fewer than half the runners have ratings, cap at STANDARD and warn.
    cov = ratings_coverage(runners)
    ratings_blind = cov < 0.5

    is_jump   = rtype in ("Chase", "Hurdle", "NH Flat")
    is_chase  = rtype == "Chase"
    is_turf   = surface == "Turf"
    is_aw     = surface == "AW"
    is_flat   = rtype == "Flat"
    cls_34    = cls in ("Class 3", "Class 4")
    cls_12    = cls in ("Class 1", "Class 2")
    long_dist = (dist_f or 0) >= 14
    small_fld = n <= 7
    big_fld   = n >= 12

    # Determine if there's an odds-on favourite
    finishers = [r for r in runners if is_numeric(r.get("position", ""))]
    # For today's card (no result yet) fall back to all runners
    pool = finishers if finishers else runners
    if pool:
        pool_sorted = sorted(pool, key=lambda r: to_float(r.get("sp_dec"), 999))
        fav_sp = to_float(pool_sorted[0].get("sp_dec")) if pool_sorted else None
    else:
        fav_sp = None
    odds_on_fav = fav_sp is not None and fav_sp < 2.0

    tsr_solo = has_tsr_solo(runners)

    reasons = []

    # ── Ratings-blind override ────────────────────────────────────────────────
    # If <50% of runners have TSR/RPR, the model has no real edge.
    # TSR solo can still fire (it's a direct comparison), but score-based
    # tiers are unreliable. Cap at STANDARD with a clear warning.
    blind_note = f"⚠ ratings only {cov:.0%} — SP-based only"

    # ── SKIP conditions ───────────────────────────────────────────────────────
    if cls_12:
        return TIER_SKIP, ["Class 1/2 — highly competitive, model unreliable"]
    if big_fld and not is_jump:
        return TIER_SKIP, [f"Field of {n} — too many runners for reliable prediction"]
    if is_flat and is_aw and win_score < 6:
        return TIER_SKIP, ["Flat/AW + low score — historically weakest category"]

    # ── SUPREME ───────────────────────────────────────────────────────────────
    # TSR solo is a direct value comparison — valid even when coverage is low,
    # as long as the runners that DO have TSR/OR are the signal bearers.
    if tsr_solo:
        reasons.append("TSR solo trigger (93% win rate)")
        if is_turf:
            reasons.append("Turf surface ✓")
        if is_jump:
            reasons.append(f"{rtype} ✓")
        if ratings_blind:
            reasons.append(blind_note)
        return TIER_SUPREME, reasons

    # ── Ratings-blind cap: everything below SUPREME → STANDARD ───────────────
    if ratings_blind:
        return TIER_STD, [f"Score {win_score} / {rtype} / {surface}", blind_note]

    # ── STRONG ────────────────────────────────────────────────────────────────
    strong_reasons = []
    if win_score >= 7 and is_jump and is_turf:
        strong_reasons.append(f"Score {win_score} ≥ 7")
        strong_reasons.append(f"{rtype} / Turf")
        if long_dist:
            strong_reasons.append(f"{dist_f}f ≥ 14 ✓")
        return TIER_STRONG, strong_reasons

    if win_score >= 7 and is_turf:
        strong_reasons.append(f"Score {win_score} ≥ 7 / Turf")
        return TIER_STRONG, strong_reasons

    if win_score >= 7:
        strong_reasons.append(f"Score {win_score} ≥ 7")
        return TIER_STRONG, strong_reasons

    if odds_on_fav and is_turf and small_fld:
        strong_reasons.append(f"Odds-on fav ({sp_str(fav_sp)}) + Turf + {n} runners")
        return TIER_STRONG, strong_reasons

    if odds_on_fav and is_chase:
        strong_reasons.append(f"Odds-on fav ({sp_str(fav_sp)}) + Chase (67%)")
        return TIER_STRONG, strong_reasons

    # ── GOOD ──────────────────────────────────────────────────────────────────
    good_reasons = []
    if win_score >= 6 and is_turf:
        good_reasons.append(f"Score {win_score} ≥ 6 / Turf")
        if n <= 10:
            good_reasons.append(f"Field of {n} ≤ 10 ✓")
        return TIER_GOOD, good_reasons

    if win_score >= 6 and is_jump:
        good_reasons.append(f"Score {win_score} ≥ 6 / {rtype}")
        return TIER_GOOD, good_reasons

    if cls_34 and is_jump:
        good_reasons.append(f"Class 3/4 + {rtype}")
        return TIER_GOOD, good_reasons

    if small_fld and is_jump:
        good_reasons.append(f"Small field ({n}) + {rtype}")
        return TIER_GOOD, good_reasons

    if win_score >= 6:
        good_reasons.append(f"Score {win_score} ≥ 6")
        return TIER_GOOD, good_reasons

    # ── STANDARD ──────────────────────────────────────────────────────────────
    return TIER_STD, [f"Score {win_score} / {rtype} / {surface}"]


# ── Prediction builder ────────────────────────────────────────────────────────

def conservative_place_terms(n_runners):
    """Standard place terms + 1 position, capped at field size - 1."""
    std = place_terms(n_runners)
    return min(std + 1, max(n_runners - 1, 1))


def predict_race(race):
    runners = race.get("runners", [])
    scored = []
    for r in runners:
        # Pass race-level going down to runner so going_form signal can fire
        if "going" not in r and race.get("going"):
            r = {**r, "race_going": race.get("going")}
        sc, signals = score_runner(r)
        sp = to_float(r.get("sp_dec"), 999)
        scored.append((sc, sp, r, signals))
    scored.sort(key=lambda x: (-x[0], x[1]))

    n_runners  = len(runners)
    places     = place_terms(n_runners)
    cons_places = conservative_place_terms(n_runners)

    win_score   = scored[0][0] if scored else 0
    tier, reasons = race_confidence(race, win_score)

    return {
        "race":        race,
        "n_runners":   n_runners,
        "places":      places,
        "cons_places": cons_places,
        "win_pick":    scored[0] if scored else None,
        "place_picks": scored[1:3] if len(scored) >= 3 else scored[1:2],
        "all_scored":  scored,
        "tier":        tier,
        "reasons":     reasons,
        "win_score":   win_score,
        "tsr_solo":    has_tsr_solo(runners),
    }


# ── Display ───────────────────────────────────────────────────────────────────

YELLOW = "\033[93m"
CYAN   = "\033[96m"
MAGENTA = "\033[95m"

def tier_color(tier):
    if tier == TIER_SUPREME: return YELLOW
    if tier == TIER_STRONG:  return GREEN
    if tier == TIER_GOOD:    return CYAN
    if tier == TIER_SKIP:    return RED
    return ""

def colored_tier(tier):
    c = tier_color(tier)
    return f"{c}{BOLD}{TIER_LABELS[tier]}{RESET}"


def result_line(label, horse, sp_dec, score, actual_pos, needed_top,
                show_score, cons_top=None):
    """
    cons_top: if provided, show a second conservative result column.
    Only applies to PLACE picks (not WIN).
    """
    sp_val  = to_float(sp_dec)
    sp_s    = f"SP {sp_str(sp_val)}" if sp_val else "SP —"
    score_s = f"[{score}]" if show_score else ""

    landed  = finished_in(actual_pos, needed_top)
    pos_s   = pos_display(actual_pos)

    if needed_top == 1:
        outcome = "WIN" if landed else "needed WIN"
    else:
        outcome = f"top {needed_top}" if landed else f"needed top {needed_top}"

    result = f"{TICK} {green(outcome)}" if landed else f"{CROSS} {red(outcome)}"

    # Conservative column (only for place picks where cons_top > needed_top)
    cons_s = ""
    cons_landed = None
    if cons_top is not None and cons_top > needed_top:
        cons_landed = finished_in(actual_pos, cons_top)
        c_outcome = f"top {cons_top}" if cons_landed else f"needed top {cons_top}"
        c_result  = f"{TICK} {green(c_outcome)}" if cons_landed else f"{CROSS} {red(c_outcome)}"
        cons_s = f"  {dim('cons:')} {c_result}"

    line = (
        f"  {bold(label):<10} {horse:<30} {sp_s:<12}"
        + (f" {dim(score_s):<6}" if show_score else "")
        + f" → {pos_s:<22} {result}{cons_s}"
    )
    return line, landed, cons_landed


def display_race(pred, seq_num, show_scores=False):
    race        = pred["race"]
    tier        = pred["tier"]
    places      = pred["places"]
    cons_places = pred["cons_places"]

    course = race.get("course", "?")
    off    = race.get("off", "")
    rtype  = race.get("type", "")
    dist   = race.get("dist_f", "")
    going  = race.get("going", "")
    cls    = race.get("class", "") or ""
    n      = pred["n_runners"]

    cls_s  = f" | {cls}" if cls and cls not in ("Unknown", "") else ""

    tier_c  = tier_color(tier)
    bet_s   = TIER_BET[tier]
    exp_s   = TIER_WIN_PCT[tier]
    reasons = "  |  ".join(pred["reasons"])

    cons_note = f"  {dim(f'(conservative: top {cons_places})')}" if cons_places > places else ""

    print()
    print(f"{tier_c}{BOLD}{'─'*70}{RESET}")
    print(f"{tier_c}{BOLD}  {TIER_LABELS[tier]}   BET: {bet_s}   (hist. win {exp_s}){RESET}")
    print(
        f"  {bold(f'#{seq_num}  {off}  {course}')}"
        f"   {rtype}  |  {dist}  |  {going}{cls_s}  |  {n} runners"
    )
    print(f"  {dim(reasons)}")
    print(f"  Places: top {places}{cons_note}")

    if show_scores:
        print(dim(f"\n  {'Horse':<30} {'SP':<9} {'OR':<5} {'RPR':<5} {'TSR':<5} Sc  Signals"))
        print(dim(f"  {'-'*30} {'-'*9} {'-'*5} {'-'*5} {'-'*5} --  -------"))
        for sc, _, r, signals in pred["all_scored"]:
            sp_v  = to_float(r.get("sp_dec"))
            sp_d  = f"{sp_str(sp_v)}" if sp_v else "—"
            sigs  = "+".join(signals.keys())
            pos   = r.get("position", "?")
            print(dim(
                f"  {r['horse']:<30} {sp_d:<9} {str(r.get('or','—')):<5} "
                f"{str(r.get('rpr','—')):<5} {str(r.get('tsr','—')):<5} "
                f"{sc:<3} {sigs}  [{pos}]"
            ))
        print()

    correct       = 0
    total         = 0
    cons_correct  = 0
    cons_total    = 0

    if pred["win_pick"]:
        sc, _, r, _ = pred["win_pick"]
        line, landed, _ = result_line(
            "WIN", r["horse"], r.get("sp_dec"), sc,
            r.get("position", "?"), 1, show_scores
        )
        print(line)
        total   += 1
        correct += 1 if landed else 0

    for i, pick in enumerate(pred["place_picks"], 1):
        sc, _, r, _ = pick
        line, landed, cons_landed = result_line(
            f"PLACE {i}", r["horse"], r.get("sp_dec"), sc,
            r.get("position", "?"), places, show_scores,
            cons_top=cons_places
        )
        print(line)
        total   += 1
        correct += 1 if landed else 0
        if cons_landed is not None:
            cons_total   += 1
            cons_correct += 1 if cons_landed else 0

    return correct, total, cons_correct, cons_total


# ── Data helpers ──────────────────────────────────────────────────────────────

def available_dates():
    raw   = sorted(glob.glob(os.path.join(config.DIR_RAW,   "*.json")))
    cards = sorted(glob.glob(os.path.join(config.DIR_CARDS, "*.json")))
    dates = set(os.path.basename(f).replace(".json", "") for f in raw)
    dates |= set(os.path.basename(f).replace(".json", "") for f in cards)
    return sorted(dates)

def load_day(date_str):
    """
    Load race data for a date. For historical dates uses data/raw/ (results).
    For today, prefers data/cards/ (pro racecard with RPR/TSR pre-populated)
    falling back to data/raw/ if no card exists.
    """
    from datetime import date as _date
    today_str = _date.today().strftime("%Y-%m-%d")

    # Today: try card first (has ratings pre-race), then raw
    if date_str == today_str:
        card_path = os.path.join(config.DIR_CARDS, f"{date_str}.json")
        if os.path.exists(card_path):
            with open(card_path) as f:
                data = json.load(f)
            # Cards use "racecards" key; normalise to "results" for the rest of the pipeline
            if "racecards" in data and "results" not in data:
                data["results"] = data["racecards"]
            data["_source"] = "card"
            return data

    raw_path = os.path.join(config.DIR_RAW, f"{date_str}.json")
    if os.path.exists(raw_path):
        with open(raw_path) as f:
            data = json.load(f)
        data["_source"] = "results"
        return data

    return None

def pick_date_interactive(dates):
    print()
    print(bold("Available dates:"))
    for i, d in enumerate(dates, 1):
        print(f"  {i:>3}. {d}", end="\n" if i % 5 == 0 else "   ")
    print()
    while True:
        raw = input("\nSelect date (number or YYYY-MM-DD): ").strip()
        if raw.isdigit():
            idx = int(raw) - 1
            if 0 <= idx < len(dates):
                return dates[idx]
            print(f"  Out of range. Enter 1–{len(dates)}.")
        elif raw in dates:
            return raw
        else:
            print("  Not recognised. Try again.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Race day predictor v2")
    parser.add_argument("--date",     help="Date to analyse (YYYY-MM-DD)")
    parser.add_argument("--scores",   action="store_true",
                        help="Show all runner scores for each race")
    parser.add_argument("--bet-only", action="store_true",
                        help="Show only 🔥 races (GOOD tier and above)")
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
        print(f"No data for {date_str}. Run fetch_data.py first.")
        sys.exit(1)

    races  = day.get("results", [])
    source = day.get("_source", "results")

    # Overall ratings coverage for this day
    all_runners = [r for race in races for r in race.get("runners", [])]
    day_cov = ratings_coverage(all_runners) if all_runners else 0.0

    # Build all predictions
    preds = [predict_race(race) for race in races]

    # Sort by tier desc, then by off time asc within tier
    def sort_key(p):
        off = p["race"].get("off", "99:99")
        # normalise times like "1:30" vs "13:30" — treat as strings, prepend 0
        parts = off.split(":")
        h = int(parts[0]) if parts else 99
        m = int(parts[1]) if len(parts) > 1 else 99
        return (-p["tier"], h, m)

    preds_sorted = sorted(preds, key=sort_key)

    # Header
    tier_counts = defaultdict(int)
    for p in preds:
        tier_counts[p["tier"]] += 1

    print()
    print("=" * 70)
    print(bold(f"  PREDICTIONS v2 — {date_str}  ({len(races)} races)"))
    src_label = "today's racecard (pre-race)" if source == "card" else "historical results"
    cov_color = GREEN if day_cov >= 0.7 else (YELLOW if day_cov >= 0.4 else RED)
    print(f"  Source: {src_label}   |   "
          f"Ratings coverage: {cov_color}{BOLD}{day_cov:.0%}{RESET}")
    if day_cov < 0.5:
        print(f"  {RED}{BOLD}⚠ Low ratings coverage — TSR/RPR absent for most runners.{RESET}")
        print(f"  {RED}  Score-based tiers suppressed. Run: python fetch_data.py --refetch-recent{RESET}")
    print("=" * 70)
    print()
    for tier in (TIER_SUPREME, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP):
        n = tier_counts.get(tier, 0)
        if n:
            c = tier_color(tier)
            print(f"  {c}{BOLD}{TIER_LABELS[tier]}{RESET}  {n} race{'s' if n != 1 else ''}"
                  f"  —  {TIER_BET[tier]}  (hist. win {TIER_WIN_PCT[tier]})")
    print()

    # Per-tier tracking
    tier_stats = {t: {"win": 0, "win_n": 0,
                      "place": 0, "place_n": 0,
                      "cons_place": 0, "cons_place_n": 0,
                      "all3": 0, "cons_all3": 0, "races": 0}
                  for t in (TIER_SUPREME, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP)}

    race_results = []

    for seq, pred in enumerate(preds_sorted, 1):
        tier        = pred["tier"]
        places      = pred["places"]
        cons_places = pred["cons_places"]

        if args.bet_only and tier < TIER_GOOD:
            continue

        correct, total, cons_correct, cons_total = display_race(
            pred, seq, show_scores=args.scores
        )
        race_results.append((tier, correct, total, pred))

        # Accumulate stats
        ts = tier_stats[tier]
        ts["races"] += 1
        win_landed = False
        if pred["win_pick"]:
            r = pred["win_pick"][2]
            win_landed = finished_in(r.get("position", ""), 1)
            ts["win"]   += 1 if win_landed else 0
            ts["win_n"] += 1
        for pick in pred["place_picks"]:
            r = pick[2]
            pl      = finished_in(r.get("position", ""), places)
            cons_pl = finished_in(r.get("position", ""), cons_places)
            ts["place"]         += 1 if pl else 0
            ts["place_n"]       += 1
            ts["cons_place"]    += 1 if cons_pl else 0
            ts["cons_place_n"]  += 1
        all3 = win_landed and all(
            finished_in(p[2].get("position", ""), places)
            for p in pred["place_picks"]
        )
        cons_all3 = win_landed and all(
            finished_in(p[2].get("position", ""), cons_places)
            for p in pred["place_picks"]
        )
        ts["all3"]      += 1 if all3 else 0
        ts["cons_all3"] += 1 if cons_all3 else 0

    # ── Summary ──────────────────────────────────────────────────────────────
    def pct(a, b):
        return f"{100*a/b:.0f}%" if b else "—"

    print()
    print("=" * 70)
    print(bold("  RESULTS SUMMARY BY TIER"))
    print("=" * 70)
    print()

    total_win = total_win_n = total_place = total_place_n = 0
    total_cons_place = total_cons_place_n = 0
    total_all3 = total_cons_all3 = total_races = 0

    for tier in (TIER_SUPREME, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP):
        ts = tier_stats[tier]
        if ts["races"] == 0:
            continue
        c   = tier_color(tier)
        lbl = TIER_LABELS[tier]

        print(f"  {c}{BOLD}{lbl}{RESET}")
        print(
            f"    Races: {ts['races']}   "
            f"WIN: {ts['win']}/{ts['win_n']} ({pct(ts['win'], ts['win_n'])})"
        )
        print(
            f"    Place std:  {ts['place']}/{ts['place_n']} "
            f"({pct(ts['place'], ts['place_n'])})   "
            f"All 3 std: {ts['all3']}/{ts['races']} ({pct(ts['all3'], ts['races'])})"
        )
        print(
            f"    Place cons: {ts['cons_place']}/{ts['cons_place_n']} "
            f"({pct(ts['cons_place'], ts['cons_place_n'])})   "
            f"All 3 cons: {ts['cons_all3']}/{ts['races']} ({pct(ts['cons_all3'], ts['races'])})"
        )
        print()

        total_win           += ts["win"]
        total_win_n         += ts["win_n"]
        total_place         += ts["place"]
        total_place_n       += ts["place_n"]
        total_cons_place    += ts["cons_place"]
        total_cons_place_n  += ts["cons_place_n"]
        total_all3          += ts["all3"]
        total_cons_all3     += ts["cons_all3"]
        total_races         += ts["races"]

    print(f"  {bold('OVERALL')}")
    print(
        f"    Races: {total_races}   "
        f"WIN: {total_win}/{total_win_n} ({pct(total_win, total_win_n)})"
    )
    print(
        f"    Place std:  {total_place}/{total_place_n} "
        f"({pct(total_place, total_place_n)})   "
        f"All 3 std: {total_all3}/{total_races} ({pct(total_all3, total_races)})"
    )
    print(
        f"    Place cons: {total_cons_place}/{total_cons_place_n} "
        f"({pct(total_cons_place, total_cons_place_n)})   "
        f"All 3 cons: {total_cons_all3}/{total_races} ({pct(total_cons_all3, total_races)})"
    )
    print()

    # Flames-only summary
    bet_tiers = [t for t in (TIER_SUPREME, TIER_STRONG, TIER_GOOD)
                 if tier_stats[t]["races"] > 0]
    if bet_tiers:
        bw   = sum(tier_stats[t]["win"]           for t in bet_tiers)
        bn   = sum(tier_stats[t]["win_n"]         for t in bet_tiers)
        bp   = sum(tier_stats[t]["place"]         for t in bet_tiers)
        bpn  = sum(tier_stats[t]["place_n"]       for t in bet_tiers)
        bcp  = sum(tier_stats[t]["cons_place"]    for t in bet_tiers)
        bcpn = sum(tier_stats[t]["cons_place_n"]  for t in bet_tiers)
        br   = sum(tier_stats[t]["races"]         for t in bet_tiers)
        ba   = sum(tier_stats[t]["all3"]          for t in bet_tiers)
        bca  = sum(tier_stats[t]["cons_all3"]     for t in bet_tiers)
        print(f"  {YELLOW}{BOLD}🔥 BET RACES ONLY (GOOD tier and above){RESET}")
        print(f"    Races: {br}   WIN: {bw}/{bn} ({pct(bw, bn)})")
        print(
            f"    Place std:  {bp}/{bpn} ({pct(bp, bpn)})   "
            f"All 3 std: {ba}/{br} ({pct(ba, br)})"
        )
        print(
            f"    Place cons: {bcp}/{bcpn} ({pct(bcp, bcpn)})   "
            f"All 3 cons: {bca}/{br} ({pct(bca, br)})"
        )
        print()


if __name__ == "__main__":
    main()
