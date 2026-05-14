"""
predict_v2.py  —  Race Day Predictor (v3 internals — evidence-based scorer)

Replaces the System C market-relative tier approach with a clean stats-only
scorer validated against 19 days of uncontaminated card data.

KEY CHANGES FROM PREVIOUS VERSION:
  - Scorer: normalised RPR×2 + OR×2 + TSR×2 + placed_last_4×1 + trainer_wp×1 + dist_flag+2
  - Market weight: ZERO throughout — pure stats ranking
  - Jockey signal: REMOVED (0% coverage in pre-race card data)
  - SP signals: REMOVED (sp_odds_on, sp_2_to_4, sp_4_to_6 no longer used)
  - Tier system: based on score margin vs field, crossed with race type
  - Race filter: evidence-based per class/type analysis on clean backtest data

CONFIDENCE TIERS (v3 — based on clean backtest margin analysis):
  💎  ELITE    — margin 2-5, jump race OR Class 1/3 flat
  🔥  STRONG   — margin 2-5, other flat; OR margin 5-10, any jump
  ✓   GOOD     — margin 5-10 flat; OR margin 10+ any type (with caution)
  ·   STANDARD — margin <2 (information only, no bet)
  ✗   SKIP     — excluded race types/classes

RACE FILTER (evidence-based):
  EXCLUDED: Class 2 entirely (-£1.22/bet across all conditions)
  EXCLUDED: Class 5 flat (-£0.46/bet, 98 races)
  EXCLUDED: Class 6 flat below margin 10 (-£0.64/bet)
  EXCLUDED: fields < 5 runners (unreliable small samples)
  EXCLUDED: flat > 20 runners
  EXCLUDED: jumps > 16 runners
  INCLUDED: Irish racing (unclassed — treated as Class 4 equivalent)

All exported names and function signatures are identical to the previous
version so no downstream files need to change.
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
    sp_str, pos_display, SIGNAL_WEIGHTS,
    TICK, CROSS, BOLD, GREEN, RED, DIM, RESET,
    green, red, bold, dim,
)

# ── Tier constants (unchanged — downstream imports don't break) ───────────────

TIER_ELITE    =  4
TIER_STRONG   =  3
TIER_GOOD     =  2
TIER_STD      =  1
TIER_WEAK     =  0
TIER_SKIP     = -1

# Legacy alias
TIER_SUPREME  = TIER_ELITE

TIER_LABELS = {
    TIER_ELITE:  "💎  ELITE   ",
    TIER_STRONG: "🔥  STRONG  ",
    TIER_GOOD:   "✓   GOOD    ",
    TIER_STD:    "·   STANDARD",
    TIER_WEAK:   "~   WEAK    ",
    TIER_SKIP:   "✗   SKIP    ",
}

TIER_BET = {
    TIER_ELITE:  "WIN P1 (consider P2 eachway)",
    TIER_STRONG: "WIN P1",
    TIER_GOOD:   "WIN P1 (smaller stake)",
    TIER_STD:    "Info only",
    TIER_WEAK:   "Skip",
    TIER_SKIP:   "Skip",
}

TIER_WIN_PCT = {
    TIER_ELITE:  "~34%",
    TIER_STRONG: "~28%",
    TIER_GOOD:   "~25%",
    TIER_STD:    "~19%",
    TIER_WEAK:   "—",
    TIER_SKIP:   "—",
}

TIER_STAKE_THRESHOLDS = {
    TIER_ELITE:  [(0, 2.0), (30,  4.0), (60,  6.0)],
    TIER_STRONG: [(0, 2.0), (50,  4.0), (100, 6.0)],
    TIER_GOOD:   [(0, 2.0), (75,  4.0), (150, 6.0)],
}

SP_SIGNALS = {"sp_odds_on", "sp_2_to_4", "sp_4_to_6"}

# ── Core scoring helpers ───────────────────────────────────────────────────────

def rpr_coverage(runners: list) -> float:
    if not runners:
        return 0.0
    return sum(
        1 for r in runners
        if str(r.get("rpr") or "").strip() not in ("", "–", "-")
    ) / len(runners)


def ratings_coverage(runners: list) -> float:
    if not runners:
        return 0.0
    rated = sum(
        1 for r in runners
        if (str(r.get("tsr") or "").strip() not in ("", "–", "-"))
        or (str(r.get("rpr") or "").strip() not in ("", "–", "-"))
    )
    return rated / len(runners)


def _sp_free_score(runner: dict) -> float:
    """Legacy compatibility — returns score_runner minus SP signals."""
    sc, signals = score_runner(runner)
    sp_pts = sum(
        SIGNAL_WEIGHTS.get(s, 0) for s in signals if s in SP_SIGNALS
    )
    return sc - sp_pts


def _norm(val, vals, scale=10.0):
    """Normalise val within field. Returns None if val is None."""
    if val is None:
        return None
    valid = [v for v in vals if v is not None]
    if not valid or len(valid) < 2:
        return scale / 2
    lo, hi = min(valid), max(valid)
    return scale / 2 if hi == lo else ((val - lo) / (hi - lo)) * scale


def _v3_score(runner: dict, field_rprs, field_ors, field_tsrs, field_plcs, field_trs) -> float:
    """
    v3 scorer: RPR×2 + OR×2 + TSR×2 + placed_last_4×1 + trainer_ae×1
    All signals normalised within the race field.
    Absent signals contribute 0 (not penalised, not rewarded).
    Jockey signal intentionally excluded — 0% card coverage pre-race.
    SP signals intentionally excluded — circular, contaminates scoring.
    """
    s = 0.0

    n_rpr = _norm(to_float(runner.get("rpr")), field_rprs, 10.0)
    n_or  = _norm(to_float(runner.get("ofr") or runner.get("or")), field_ors, 10.0)
    n_tsr = _norm(to_float(runner.get("ts") or runner.get("tsr")), field_tsrs, 10.0)

    if n_rpr is not None: s += 2.0 * n_rpr
    if n_or  is not None: s += 2.0 * n_or
    if n_tsr is not None: s += 2.0 * n_tsr

    fd   = runner.get("form_detail") or {}
    plc4 = float(fd.get("placed_last_4", 0) or 0) if isinstance(fd, dict) else 0.0
    n_plc = _norm(plc4, field_plcs, 10.0)
    if n_plc is not None: s += 1.0 * n_plc

    t14  = runner.get("trainer_14d") or {}
    t_ae = None
    if isinstance(t14, dict) and (t14.get("runs", 0) or 0) >= 3:
        t_ae = to_float(t14.get("ae") or t14.get("win_pct"))
    n_tr = _norm(t_ae, field_trs, 10.0)
    if n_tr is not None: s += 1.0 * n_tr

    # Distance winner flag (D) — proven winner at this distance
    # Picks with D flag return +£1.00/bet vs +£0.14/bet without (98 vs 209 races)
    flags = runner.get("past_results_flags") or []
    if isinstance(flags, list) and "D" in flags:
        s += 2.0

    return s

    return s


def _score_field(runners: list):
    """
    Score all runners in a race and return sorted list with margin.
    Returns (sorted_list, margin) where sorted_list is
    [(score, runner), ...] descending and margin is score[0]-score[1].
    """
    rprs = [to_float(r.get("rpr")) for r in runners]
    ors  = [to_float(r.get("ofr") or r.get("or")) for r in runners]
    tsrs = [to_float(r.get("ts") or r.get("tsr")) for r in runners]
    plcs = [float((r.get("form_detail") or {}).get("placed_last_4", 0) or 0)
            if isinstance(r.get("form_detail"), dict) else 0.0
            for r in runners]

    def tr_ae(r):
        t = r.get("trainer_14d") or {}
        if not isinstance(t, dict): return None
        if (t.get("runs", 0) or 0) < 3: return None
        return to_float(t.get("ae") or t.get("win_pct"))

    trs = [tr_ae(r) for r in runners]

    scored = [
        (_v3_score(r, rprs, ors, tsrs, plcs, trs), r)
        for r in runners
    ]
    scored.sort(key=lambda x: -x[0])

    margin = round(scored[0][0] - scored[1][0], 2) if len(scored) > 1 else 0.0
    return scored, margin


# ── Race filter ────────────────────────────────────────────────────────────────

def _is_jump(race_type: str) -> bool:
    rt = (race_type or "").lower()
    return any(t in rt for t in ("chase", "hurdle", "nh flat", "national hunt"))


def _race_qualifies(runners: list, race: dict) -> tuple:
    """
    Returns (qualifies: bool, reason: str).
    Evidence-based filter from clean backtest analysis.
    """
    n     = len(runners)
    rtype = str(race.get("type", "") or "")
    cls   = str(race.get("race_class", "") or race.get("class", "") or "").replace("Class", "").strip()
    jump  = _is_jump(rtype)

    if n < 5:
        return False, f"Field too small ({n} runners, min 5)"

    if jump and n > 16:
        return False, f"Jump field too large ({n} runners, max 16)"

    if not jump and n > 20:
        return False, f"Flat field too large ({n} runners, max 20)"

    if cls == "2":
        return False, "Class 2 excluded (consistent -£1.22/bet across all conditions)"

    if cls == "5" and not jump:
        return False, "Class 5 flat excluded (-£0.46/bet, 98 races)"

    return True, ""


# ── Tier assignment ────────────────────────────────────────────────────────────

def _assign_tier(margin: float, is_jump: bool, cls: str) -> int:
    """
    Assign confidence tier based on score margin and race context.

    Evidence base (clean backtest, 545 races):
      margin 2-5:   +£1.13/bet overall; jumps +£1.43, Class 3 +£6.73
      margin 5-10:  +£0.30/bet; jumps +£1.93 hurdles, +£5.65 chase margin 2-5
      margin <2:    -£0.18/bet — skip
      margin 10+:   -£0.18/bet overall; only Class 6 10+ is marginally positive

    Tier rules reflect where genuine edge was found:
      ELITE:  margin 2-5 AND (jump OR Class 1/3)
      STRONG: margin 2-5 other; OR margin 5-10 jump
      GOOD:   margin 5-10 flat; OR margin 10+ with Class 3/4 jump
      STD:    margin <2 (info only); OR margin 10+ flat
      SKIP:   caught by _race_qualifies before we get here
    """
    if margin < 2.0:
        return TIER_STD

    if 2.0 <= margin < 5.0:
        if is_jump or cls in ("1", "3"):
            return TIER_ELITE
        return TIER_STRONG

    if 5.0 <= margin < 10.0:
        if is_jump:
            return TIER_STRONG
        return TIER_GOOD

    # margin >= 10
    if is_jump and cls in ("3", "4"):
        return TIER_GOOD
    if cls == "6":
        # Class 6 flat margin 10+ is marginally positive after outlier exclusion
        # Keep as GOOD but with note — staking system will treat conservatively
        return TIER_GOOD
    return TIER_STD


# ── Main prediction engine ─────────────────────────────────────────────────────

def get_blended_picks(
    runners:  list,
    mw_p1:    float = 0.0,   # ignored — kept for API compatibility
    mw_p2:    float = 0.0,   # ignored — kept for API compatibility
    raw_race: dict  = None,
) -> tuple:
    """
    Returns (tier, p1_runner, p2_runner, reasons).

    v3: pure stats scorer, zero market weight.
    P1 = highest v3 score in field.
    P2 = second highest v3 score (different horse).
    Tier = function of score margin and race context.

    mw_p1 and mw_p2 parameters are accepted but ignored —
    kept for backwards compatibility with callers.
    """
    raw_race = raw_race or {}
    n        = len(runners)
    rtype    = str(raw_race.get("type", "") or "")
    cls      = str(raw_race.get("race_class", "") or raw_race.get("class", "") or "").replace("Class", "").strip()
    jump     = _is_jump(rtype)

    # Race filter
    qualifies, skip_reason = _race_qualifies(runners, raw_race)
    if not qualifies:
        return TIER_SKIP, None, None, [skip_reason]

    if n < 2:
        r0 = runners[0] if runners else None
        return TIER_STD, r0, None, ["Single runner"]

    # Score field
    scored, margin = _score_field(runners)
    p1 = scored[0][1]
    p2 = scored[1][1] if len(scored) > 1 else None

    # Tier
    tier = _assign_tier(margin, jump, cls)

    # Class 6 flat margin <10 — additional skip beyond _race_qualifies
    # (covered by _assign_tier returning TIER_STD for margin<10 Class 6)

    # Build reasons
    p1_sp   = to_float(p1.get("sp_dec"))
    sp_tag  = f" @ {sp_str(p1_sp)}" if p1_sp else ""
    reasons = [
        f"Score margin: {margin:.1f}{sp_tag}",
        f"Race: {rtype or 'Flat'} | Class {cls or '?'} | {n} runners",
    ]

    cov = rpr_coverage(runners)
    if cov < 0.6:
        reasons.append(f"⚠ RPR coverage {cov:.0%} — reduced confidence")

    # Market rank of our pick (informational only — not used in scoring)
    mkt_sorted = sorted(
        [(to_float(r.get("sp_dec")) or 999, r) for r in runners],
        key=lambda x: x[0]
    )
    mkt_rank = next(
        (i + 1 for i, (_, r) in enumerate(mkt_sorted)
         if r.get("horse_id", "") == p1.get("horse_id", "")),
        n
    )
    is_fav = mkt_rank == 1
    reasons.append(
        f"Market rank of pick: {'favourite' if is_fav else str(mkt_rank)+'/'+str(n)}"
        + (" ← non-fav pick (higher value historically)" if not is_fav else "")
    )

    return tier, p1, p2, reasons


# ── Prediction builder (for display) ──────────────────────────────────────────

def conservative_place_terms(n_runners):
    std = place_terms(n_runners)
    return min(std + 1, max(n_runners - 1, 1))


def predict_race(race: dict) -> dict:
    runners    = race.get("runners") or race.get("all_runners") or []
    n_runners  = len(runners)
    places     = place_terms(n_runners)
    cons_places = conservative_place_terms(n_runners)

    # Score all runners for display table using v3 scorer
    scored, margin = _score_field(runners)
    all_scored = []
    for sc, r in scored:
        sp_val = to_float(r.get("sp_dec"), 999)
        _, signals = score_runner(r)   # keep signals dict for display compat
        all_scored.append((sc, sp_val, r, signals))

    tier, p1_runner, p2_runner, reasons = get_blended_picks(
        runners, raw_race=race
    )

    def _runner_to_pick(r):
        if not r:
            return None
        sc = next((s for s, rr in scored if rr.get("horse_id","") == r.get("horse_id","")), 0)
        _, sigs = score_runner(r)
        sp = to_float(r.get("sp_dec"), 999)
        return (sc, sp, r, sigs)

    win_pick    = _runner_to_pick(p1_runner)
    place_pick  = _runner_to_pick(p2_runner)
    place_picks = [place_pick] if place_pick else []

    return {
        "race":          race,
        "n_runners":     n_runners,
        "places":        places,
        "cons_places":   cons_places,
        "win_pick":      win_pick,
        "place_picks":   place_picks,
        "all_scored":    all_scored,
        "tier":          tier,
        "reasons":       reasons,
        "win_score":     all_scored[0][0] if all_scored else 0,
        "tsr_solo":      False,
        "outlier_picks": [],
        "rpr_cov":       rpr_coverage(runners),
    }


# ── Legacy compatibility shim ──────────────────────────────────────────────────

def race_confidence(race: dict, win_score: float) -> tuple:
    """Legacy shim — wraps get_blended_picks() for existing callers."""
    runners = race.get("runners") or race.get("all_runners") or []
    tier, _, _, reasons = get_blended_picks(runners, raw_race=race)
    return tier, reasons


# ── Display helpers (unchanged from v2) ───────────────────────────────────────

YELLOW  = "\033[93m"
CYAN    = "\033[96m"
MAGENTA = "\033[95m"


def tier_color(tier: int) -> str:
    if tier == TIER_ELITE:  return YELLOW
    if tier == TIER_STRONG: return GREEN
    if tier == TIER_GOOD:   return CYAN
    if tier <= TIER_SKIP:   return RED
    return ""


def colored_tier(tier: int) -> str:
    c = tier_color(tier)
    return f"{c}{BOLD}{TIER_LABELS.get(tier,'?')}{RESET}"


def result_line(label, horse, sp_dec, score, actual_pos, needed_top,
                show_score, cons_top=None):
    sp_val  = to_float(sp_dec)
    sp_s    = f"SP {sp_str(sp_val)}" if sp_val else "SP —"
    score_s = f"[{score:.1f}]" if show_score else ""
    landed  = finished_in(actual_pos, needed_top)
    pos_s   = pos_display(actual_pos)

    if needed_top == 1:
        outcome = "WIN" if landed else "needed WIN"
    else:
        outcome = f"top {needed_top}" if landed else f"needed top {needed_top}"

    result = f"{TICK} {green(outcome)}" if landed else f"{CROSS} {red(outcome)}"

    cons_s      = ""
    cons_landed = None
    if cons_top is not None and cons_top > needed_top:
        cons_landed = finished_in(actual_pos, cons_top)
        c_outcome   = f"top {cons_top}" if cons_landed else f"needed top {cons_top}"
        c_result    = f"{TICK} {green(c_outcome)}" if cons_landed else f"{CROSS} {red(c_outcome)}"
        cons_s      = f"  {dim('cons:')} {c_result}"

    line = (
        f"  {bold(label):<10} {horse:<30} {sp_s:<12}"
        + (f" {dim(score_s):<8}" if show_score else "")
        + f" → {pos_s:<22} {result}{cons_s}"
    )
    return line, landed, cons_landed


def display_race(pred: dict, seq_num: int, show_scores: bool = False):
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
    cov    = pred.get("rpr_cov", 1.0)

    cls_s     = f" | Class {cls}" if cls and cls not in ("Unknown", "") else ""
    cov_warn  = f"  {dim(f'⚠ RPR {cov:.0%}')}" if cov < 0.6 else ""
    tier_c    = tier_color(tier)
    bet_s     = TIER_BET.get(tier, "—")
    exp_s     = TIER_WIN_PCT.get(tier, "—")
    reasons   = "  |  ".join(pred["reasons"])
    cons_note = (
        f"  {dim(f'(conservative: top {cons_places})')}"
        if cons_places > places else ""
    )

    print()
    print(f"{tier_c}{BOLD}{'─'*70}{RESET}")
    print(f"{tier_c}{BOLD}  {TIER_LABELS.get(tier,'?')}   BET: {bet_s}   (hist. win {exp_s}){RESET}")
    print(
        f"  {bold(f'#{seq_num}  {off}  {course}')}"
        f"   {rtype}  |  {dist}  |  {going}{cls_s}  |  {n} runners{cov_warn}"
    )
    print(f"  {dim(reasons)}")
    print(f"  Places: top {places}{cons_note}")

    if show_scores:
        print(dim(f"\n  {'Horse':<30} {'SP':<9} {'OR':<5} {'RPR':<5} {'TSR':<5} {'v3Sc':>6}"))
        print(dim(f"  {'-'*30} {'-'*9} {'-'*5} {'-'*5} {'-'*5} {'-'*6}"))
        for sc, _, r, _ in pred["all_scored"]:
            sp_v  = to_float(r.get("sp_dec"))
            sp_d  = f"{sp_str(sp_v)}" if sp_v else "—"
            pos   = r.get("position", "?")
            print(dim(
                f"  {r.get('horse','?'):<30} {sp_d:<9} "
                f"{str(r.get('or','—')):<5} {str(r.get('rpr','—')):<5} "
                f"{str(r.get('tsr','—')):<5} {sc:>6.1f}  [{pos}]"
            ))
        print()

    correct = total = cons_correct = cons_total = 0

    if pred["win_pick"]:
        sc, _, r, _ = pred["win_pick"]
        line, landed, _ = result_line(
            "WIN", r.get("horse","?"), r.get("sp_dec"), sc,
            r.get("position", "?"), 1, show_scores
        )
        print(line)
        total   += 1
        correct += 1 if landed else 0

    for i, pick in enumerate(pred["place_picks"], 1):
        if not pick:
            continue
        sc, _, r, _ = pick
        line, landed, cons_landed = result_line(
            f"PLACE {i}", r.get("horse","?"), r.get("sp_dec"), sc,
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


# ── Data helpers (unchanged from v2) ─────────────────────────────────────────

def available_dates() -> list:
    raw   = sorted(glob.glob(os.path.join(config.DIR_RAW,   "*.json")))
    cards = sorted(glob.glob(os.path.join(config.DIR_CARDS, "*.json")))
    dates = set(os.path.basename(f).replace(".json", "") for f in raw)
    dates |= set(os.path.basename(f).replace(".json", "") for f in cards)
    return sorted(dates)


def load_day(date_str: str) -> dict | None:
    from datetime import date as _date
    today_str = _date.today().strftime("%Y-%m-%d")

    if date_str == today_str:
        card_path = os.path.join(config.DIR_CARDS, f"{date_str}.json")
        if os.path.exists(card_path):
            with open(card_path) as f:
                data = json.load(f)
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


def pick_date_interactive(dates: list) -> str:
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


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Race day predictor v3 — evidence-based scorer")
    parser.add_argument("--date",     help="Date to analyse (YYYY-MM-DD)")
    parser.add_argument("--scores",   action="store_true",
                        help="Show all runner scores for each race")
    parser.add_argument("--bet-only", action="store_true",
                        help="Show only bet races (GOOD and above)")
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

    races  = day.get("results") or day.get("races") or []
    source = day.get("_source", "results")

    all_runners_flat = [r for race in races for r in (race.get("runners") or [])]
    day_cov  = ratings_coverage(all_runners_flat) if all_runners_flat else 0.0
    day_rprc = rpr_coverage(all_runners_flat)     if all_runners_flat else 0.0

    preds = [predict_race(race) for race in races]

    def sort_key(p):
        off   = p["race"].get("off", "99:99")
        parts = off.split(":")
        h     = int(parts[0]) if parts else 99
        m     = int(parts[1]) if len(parts) > 1 else 99
        return (-p["tier"], h, m)

    preds_sorted = sorted(preds, key=sort_key)

    tier_counts = defaultdict(int)
    for p in preds:
        tier_counts[p["tier"]] += 1

    print()
    print("=" * 70)
    print(bold(f"  PREDICTIONS v3 — {date_str}  ({len(races)} races)  [evidence-based scorer]"))
    src_label = "today's racecard (pre-race)" if source == "card" else "historical results"
    cov_color = GREEN if day_rprc >= 0.7 else (YELLOW if day_rprc >= 0.4 else RED)
    print(f"  Source: {src_label}   |   "
          f"RPR coverage: {cov_color}{BOLD}{day_rprc:.0%}{RESET}")
    if day_rprc < 0.6:
        print(f"  {RED}{BOLD}⚠ RPR coverage below 60% — tier confidence reduced.{RESET}")
    print("=" * 70)
    print()

    bet_tiers = (TIER_ELITE, TIER_STRONG, TIER_GOOD)
    for tier in (TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP):
        n = tier_counts.get(tier, 0)
        if n:
            c = tier_color(tier)
            print(
                f"  {c}{BOLD}{TIER_LABELS.get(tier,'?')}{RESET}"
                f"  {n} race{'s' if n != 1 else ''}"
                f"  —  {TIER_BET.get(tier,'')}"
                f"  (hist. win {TIER_WIN_PCT.get(tier,'—')})"
            )
    print()

    tier_stats = {
        t: {"win":0,"win_n":0,"place":0,"place_n":0,
            "cons_place":0,"cons_place_n":0,"all3":0,"cons_all3":0,"races":0}
        for t in (TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_WEAK, TIER_SKIP)
    }

    for seq, pred in enumerate(preds_sorted, 1):
        tier        = pred["tier"]
        places      = pred["places"]
        cons_places = pred["cons_places"]

        if args.bet_only and tier < TIER_GOOD:
            continue

        correct, total, cons_correct, cons_total = display_race(
            pred, seq, show_scores=args.scores
        )

        ts = tier_stats[tier]
        ts["races"] += 1
        win_landed = False
        if pred["win_pick"]:
            r = pred["win_pick"][2]
            win_landed     = finished_in(r.get("position", ""), 1)
            ts["win"]     += 1 if win_landed else 0
            ts["win_n"]   += 1
        for pick in pred["place_picks"]:
            if not pick:
                continue
            r       = pick[2]
            pl      = finished_in(r.get("position", ""), places)
            cons_pl = finished_in(r.get("position", ""), cons_places)
            ts["place"]         += 1 if pl else 0
            ts["place_n"]       += 1
            ts["cons_place"]    += 1 if cons_pl else 0
            ts["cons_place_n"]  += 1
        all3 = win_landed and all(
            finished_in(p[2].get("position",""), places)
            for p in pred["place_picks"] if p
        )
        cons_all3 = win_landed and all(
            finished_in(p[2].get("position",""), cons_places)
            for p in pred["place_picks"] if p
        )
        ts["all3"]      += 1 if all3 else 0
        ts["cons_all3"] += 1 if cons_all3 else 0

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

    for tier in (TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP):
        ts = tier_stats[tier]
        if ts["races"] == 0:
            continue
        c   = tier_color(tier)
        lbl = TIER_LABELS.get(tier, "?")
        print(f"  {c}{BOLD}{lbl}{RESET}")
        print(f"    Races: {ts['races']}   WIN: {ts['win']}/{ts['win_n']} ({pct(ts['win'],ts['win_n'])})")
        if ts["place_n"]:
            print(f"    Place std:  {ts['place']}/{ts['place_n']} ({pct(ts['place'],ts['place_n'])})   "
                  f"All 3 std: {ts['all3']}/{ts['races']} ({pct(ts['all3'],ts['races'])})")
            print(f"    Place cons: {ts['cons_place']}/{ts['cons_place_n']} ({pct(ts['cons_place'],ts['cons_place_n'])})   "
                  f"All 3 cons: {ts['cons_all3']}/{ts['races']} ({pct(ts['cons_all3'],ts['races'])})")
        print()

        total_win          += ts["win"]
        total_win_n        += ts["win_n"]
        total_place        += ts["place"]
        total_place_n      += ts["place_n"]
        total_cons_place   += ts["cons_place"]
        total_cons_place_n += ts["cons_place_n"]
        total_all3         += ts["all3"]
        total_cons_all3    += ts["cons_all3"]
        total_races        += ts["races"]

    print(f"  {bold('OVERALL')}")
    print(f"    Races: {total_races}   WIN: {total_win}/{total_win_n} ({pct(total_win,total_win_n)})")
    if total_place_n:
        print(f"    Place std:  {total_place}/{total_place_n} ({pct(total_place,total_place_n)})   "
              f"All 3 std: {total_all3}/{total_races} ({pct(total_all3,total_races)})")
    print()

    active_bet_tiers = [t for t in bet_tiers if tier_stats[t]["races"] > 0]
    if active_bet_tiers:
        bw  = sum(tier_stats[t]["win"]    for t in active_bet_tiers)
        bn  = sum(tier_stats[t]["win_n"]  for t in active_bet_tiers)
        br  = sum(tier_stats[t]["races"]  for t in active_bet_tiers)
        ba  = sum(tier_stats[t]["all3"]   for t in active_bet_tiers)
        print(f"  {YELLOW}{BOLD}💎🔥✓  BET RACES ONLY (GOOD and above){RESET}")
        print(f"    Races: {br}   WIN: {bw}/{bn} ({pct(bw,bn)})")
        print(f"    All 3 std: {ba}/{br} ({pct(ba,br)})")
        print()


if __name__ == "__main__":
    main()
