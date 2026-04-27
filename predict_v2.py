"""
predict_v2.py  —  Race Day Predictor (System C — market-relative tiers)

Replaces the score-based tier system with System C: market-relative ranking
that blends stats scores with market SP ranking using mw=0.60 for P1 and
mw=0.40 for P2. Tier assigned by agreement between stats rank and market rank.

CONFIDENCE TIERS (System C — validated on 17-day history):
  💎  ELITE    — stats P1 = market P1, strong SP-free score  (66% P1 win)
  🔥  STRONG   — stats/market agree or near-agree            (49% P1 win)
  ✓   GOOD     — moderate agreement                          (36% P1 win)
  ·   STANDARD — limited agreement (info only, no bet)       (30% P1 win)
  ✗   SKIP     — Class 1/2, field >12, or hard skip          (no bet)

BET RECOMMENDATIONS:
  💎  ELITE:   WIN P1+P2 + PLACE both (8+ runners)
  🔥  STRONG:  WIN P1+P2 + PLACE both (8+ runners)
  ✓   GOOD:    WIN P1+P2 only (no place bets)
  ·   STANDARD: Information only
  ✗   SKIP:    Skip / avoid

STAKE CASCADE (per-tier, independent profit tracking):
  ELITE:  £2 → £4 at £30 profit,  £4 → £6 at £60 profit
  STRONG: £2 → £4 at £50 profit,  £4 → £6 at £100 profit
  GOOD:   £2 → £4 at £75 profit,  £4 → £6 at £150 profit

Usage:
    python predict_v2.py
    python predict_v2.py --date 2026-03-20
    python predict_v2.py --date 2026-03-20 --scores
    python predict_v2.py --date 2026-03-20 --bet-only
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

# ── Tier constants ─────────────────────────────────────────────────────────────

TIER_ELITE    =  4   # 💎
TIER_STRONG   =  3   # 🔥
TIER_GOOD     =  2   # ✓
TIER_STD      =  1   # ·
TIER_WEAK     =  0   # (never fires in practice)
TIER_SKIP     = -1   # ✗

# Legacy aliases — keep so betfair_main.py imports don't break immediately
TIER_SUPREME  = TIER_ELITE
# TIER_STRONG already the same value as new TIER_STRONG (3→3 after renumber)
# Old TIER_STRONG was 2, new is 3. Old TIER_GOOD was 1, new is 2.
# betfair_main.py will be updated to use new constants directly.

TIER_LABELS = {
    TIER_ELITE:  "💎  ELITE   ",
    TIER_STRONG: "🔥  STRONG  ",
    TIER_GOOD:   "✓   GOOD    ",
    TIER_STD:    "·   STANDARD",
    TIER_WEAK:   "~   WEAK    ",
    TIER_SKIP:   "✗   SKIP    ",
}

TIER_BET = {
    TIER_ELITE:  "WIN P1+P2 + PLACE",
    TIER_STRONG: "WIN P1+P2 + PLACE",
    TIER_GOOD:   "WIN P1+P2",
    TIER_STD:    "Info only",
    TIER_WEAK:   "Skip",
    TIER_SKIP:   "Skip",
}

TIER_WIN_PCT = {
    TIER_ELITE:  "~67%",
    TIER_STRONG: "~49%",
    TIER_GOOD:   "~36%",
    TIER_STD:    "~30%",
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
    """Fraction of runners with a valid RPR value."""
    if not runners:
        return 0.0
    return sum(
        1 for r in runners
        if str(r.get("rpr") or "").strip() not in ("", "–", "-")
    ) / len(runners)


def ratings_coverage(runners: list) -> float:
    """Fraction of runners with a valid TSR or RPR value (for display)."""
    if not runners:
        return 0.0
    rated = sum(
        1 for r in runners
        if (str(r.get("tsr") or "").strip() not in ("", "–", "-"))
        or (str(r.get("rpr") or "").strip() not in ("", "–", "-"))
    )
    return rated / len(runners)


def _sp_free_score(runner: dict) -> float:
    """Score runner excluding SP-based signals (sp_odds_on, sp_2_to_4, sp_4_to_6)."""
    sc, signals = score_runner(runner)
    sp_pts = sum(
        SIGNAL_WEIGHTS.get(s, 0) for s in signals if s in SP_SIGNALS
    )
    return sc - sp_pts


def _norm(val, vals, scale=10.0) -> float:
    valid = [v for v in vals if v is not None]
    if not valid or len(valid) < 2:
        return scale / 2
    lo, hi = min(valid), max(valid)
    return scale / 2 if hi == lo else ((val - lo) / (hi - lo)) * scale


def _stats_score(runner: dict, field_rprs, field_ors, field_tsrs) -> float:
    """Pure stats score from ratings + form + trainer/jockey AE."""
    s = 0.0
    rpr = to_float(runner.get("rpr"))
    or_ = to_float(runner.get("ofr") or runner.get("or"))
    tsr = to_float(runner.get("ts") or runner.get("tsr"))

    if rpr: s += _norm(rpr, field_rprs, 10.0)
    if or_: s += _norm(or_, field_ors,  10.0)
    if tsr: s += _norm(tsr, field_tsrs,  5.0)
    if rpr and or_ and rpr > or_: s += 2.0

    fd = runner.get("form_detail") or {}
    if isinstance(fd, dict):
        plc4 = fd.get("placed_last_4", 0) or 0
        bad  = fd.get("bad_recent",    0) or 0
        if plc4 >= 3:   s += 2.0
        elif plc4 >= 2: s += 1.0
        if bad == 0 and runner.get("form", ""): s += 1.0

    for f14 in [runner.get("trainer_14d"), runner.get("jockey_14d")]:
        if not isinstance(f14, dict):
            continue
        ae   = f14.get("ae",   0) or 0
        runs = f14.get("runs", 0) or 0
        if runs >= 3:
            if   ae >= 2.0 and runs >= 5: s += 3
            elif ae >= 1.5 and runs >= 5: s += 2
            elif ae >= 1.0 and runs >= 5: s += 1

    return s


# ── System C tier engine ───────────────────────────────────────────────────────

def get_blended_picks(
    runners:  list,
    mw_p1:    float = 0.60,
    mw_p2:    float = 0.40,
    raw_race: dict  = None,
) -> tuple:
    """
    Returns (tier, p1_runner, p2_runner, reasons) using System C logic.

    P1 is chosen from the mw_p1-blended ranking (60% market, 40% stats).
    P2 is the highest-ranked runner by the mw_p2 blend (40% market, 60% stats)
    that is not P1 — gives value alternative the market hasn't fully priced.

    Tier is assigned by the relationship between P1's stats rank and market rank:
      ELITE:    both agree (rank 1/1) + strong SP-free score (≥3)
      STRONG:   both agree (rank 1/1) OR near-agree (diff ≤1) + score ≥3
      GOOD:     near-agree (diff ≤1) OR moderate agree (diff ≤2) + score ≥3
      STANDARD: moderate agreement (diff ≤3) + score ≥2
      WEAK:     large disagreement
      SKIP:     Class 1/2 or >12 runners
    """
    raw_race = raw_race or {}
    n        = len(runners)
    cls      = str(raw_race.get("class", "") or "").replace("Class ", "").strip()

    # ── Hard skips ─────────────────────────────────────────────────────────────
    if cls in ("1", "2"):
        return TIER_SKIP, None, None, ["Class 1/2 — skip"]
    if n > 12:
        return TIER_SKIP, None, None, [f"Field of {n} — skip (>12 runners)"]
    if n < 2:
        r0 = runners[0] if runners else None
        return TIER_STD, r0, None, ["Single runner"]

    # ── Build stats and market ranks ────────────────────────────────────────────
    field_rprs = [to_float(r.get("rpr"))               for r in runners]
    field_ors  = [to_float(r.get("ofr") or r.get("or")) for r in runners]
    field_tsrs = [to_float(r.get("ts") or r.get("tsr")) for r in runners]

    stats_scored = sorted(
        [(_stats_score(r, field_rprs, field_ors, field_tsrs), i, r)
         for i, r in enumerate(runners)],
        key=lambda x: -x[0]
    )
    stats_rank = {
        r.get("horse_id", f"_{i}"): rank + 1
        for rank, (_, i, r) in enumerate(stats_scored)
    }

    mkt_scored = sorted(
        [(to_float(r.get("sp_dec")) or 999, i, r)
         for i, r in enumerate(runners)],
        key=lambda x: x[0]
    )
    mkt_rank = {
        r.get("horse_id", f"_{i}"): rank + 1
        for rank, (_, i, r) in enumerate(mkt_scored)
    }

    def _blend(mw: float) -> list:
        b = []
        for ss, _i, r in stats_scored:
            hid = r.get("horse_id", "")
            sr  = stats_rank.get(hid, n)
            mr  = mkt_rank.get(hid, n)
            cs  = (1 - mw) * (sr - 1) / max(n - 1, 1) + mw * (mr - 1) / max(n - 1, 1)
            b.append((cs, sr, mr, ss, r))
        b.sort(key=lambda x: x[0])
        return b

    # ── P1 from mw_p1 blend ─────────────────────────────────────────────────────
    b1            = _blend(mw_p1)
    _, sr1, mr1, ss1, p1 = b1[0]

    # ── P2 from mw_p2 blend, excluding P1 ──────────────────────────────────────
    p1_hid = p1.get("horse_id", "")
    b2     = _blend(mw_p2)
    p2     = next(
        (r for _, _, _, _, r in b2 if r.get("horse_id", "") != p1_hid),
        None
    )

    # ── Tier from P1 rank agreement + SP-free score ─────────────────────────────
    sc1f      = _sp_free_score(p1)
    rank_diff = abs(sr1 - mr1)
    both_agree = sr1 == 1 and mr1 == 1

    if   both_agree and sc1f >= 3:         tc = TIER_ELITE
    elif both_agree:                       tc = TIER_STRONG
    elif rank_diff <= 1 and sc1f >= 3:     tc = TIER_STRONG
    elif rank_diff <= 1:                   tc = TIER_GOOD
    elif rank_diff <= 2 and sc1f >= 3:     tc = TIER_GOOD
    elif rank_diff <= 3 and sc1f >= 2:     tc = TIER_STD
    elif sc1f >= 1:                        tc = TIER_STD
    else:                                  tc = TIER_WEAK

    # ── Build human-readable reasons ────────────────────────────────────────────
    p1_sp  = to_float(p1.get("sp_dec"))
    sp_tag = f" @ {sp_str(p1_sp)}" if p1_sp else ""
    reasons = []

    if both_agree:
        reasons.append(f"Stats P1 = Market P1 (rank 1/1){sp_tag}")
    else:
        reasons.append(
            f"Stats rank {sr1}, Market rank {mr1} "
            f"(diff {rank_diff}){sp_tag}"
        )
    reasons.append(f"SP-free score: {sc1f:.0f}")
    if rpr_coverage(runners) < 0.6:
        cov = rpr_coverage(runners)
        reasons.append(f"⚠ RPR coverage {cov:.0%} — reduced confidence")

    return tc, p1, p2, reasons


# ── Prediction builder (for display) ──────────────────────────────────────────

def conservative_place_terms(n_runners):
    """Standard place terms + 1 position, capped at field size - 1."""
    std = place_terms(n_runners)
    return min(std + 1, max(n_runners - 1, 1))


def predict_race(race: dict) -> dict:
    """
    Build a prediction dict for display. Uses get_blended_picks() for
    tier and P1/P2 selection. Falls back to score_runner ordering for
    the full scored list used in display.
    """
    runners    = race.get("runners") or race.get("all_runners") or []
    n_runners  = len(runners)
    places     = place_terms(n_runners)
    cons_places = conservative_place_terms(n_runners)

    # Score all runners for display table
    all_scored = []
    for r in runners:
        if "going" not in r and race.get("going"):
            r = {**r, "race_going": race.get("going")}
        sc, signals = score_runner(r)
        sp = to_float(r.get("sp_dec"), 999)
        all_scored.append((sc, sp, r, signals))
    all_scored.sort(key=lambda x: (-x[0], x[1]))

    # System C picks and tier
    tier, p1_runner, p2_runner, reasons = get_blended_picks(
        runners, mw_p1=0.60, mw_p2=0.40, raw_race=race
    )

    # Build win_pick and place_picks in the format display expects
    # (score, sp, runner, signals)
    def _runner_to_pick(r):
        if not r:
            return None
        sc, sigs = score_runner(r)
        sp = to_float(r.get("sp_dec"), 999)
        return (sc, sp, r, sigs)

    win_pick   = _runner_to_pick(p1_runner)
    place_pick = _runner_to_pick(p2_runner)
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
        "tsr_solo":      False,      # TSR solo trigger removed in System C
        "outlier_picks": [],         # Outlier logic removed — STANDARD not bet
        "rpr_cov":       rpr_coverage(runners),
    }


# ── Legacy compatibility shim ──────────────────────────────────────────────────
# race_confidence() is called from scheduler/race_jobs.py and betfair_main.py.
# This shim bridges until those files are updated. Returns (tier, reasons).

def race_confidence(race: dict, win_score: float) -> tuple:
    """
    Legacy shim — wraps get_blended_picks() so existing callers don't break.
    win_score is ignored; System C uses market-relative ranking.
    Callers should migrate to get_blended_picks() directly.
    """
    runners = race.get("runners") or race.get("all_runners") or []
    tier, _, _, reasons = get_blended_picks(runners, raw_race=race)
    return tier, reasons


# ── Display helpers ────────────────────────────────────────────────────────────

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
    score_s = f"[{score}]" if show_score else ""
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
        + (f" {dim(score_s):<6}" if show_score else "")
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

    cls_s     = f" | {cls}" if cls and cls not in ("Unknown", "") else ""
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
        print(dim(f"\n  {'Horse':<30} {'SP':<9} {'OR':<5} {'RPR':<5} {'TSR':<5} Sc  Signals"))
        print(dim(f"  {'-'*30} {'-'*9} {'-'*5} {'-'*5} {'-'*5} --  -------"))
        for sc, _, r, signals in pred["all_scored"]:
            sp_v  = to_float(r.get("sp_dec"))
            sp_d  = f"{sp_str(sp_v)}" if sp_v else "—"
            sigs  = "+".join(signals.keys())
            pos   = r.get("position", "?")
            print(dim(
                f"  {r.get('horse','?'):<30} {sp_d:<9} "
                f"{str(r.get('or','—')):<5} {str(r.get('rpr','—')):<5} "
                f"{str(r.get('tsr','—')):<5} {sc:<3} {sigs}  [{pos}]"
            ))
        print()

    correct      = 0
    total        = 0
    cons_correct = 0
    cons_total   = 0

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


# ── Data helpers ───────────────────────────────────────────────────────────────

def available_dates() -> list:
    raw   = sorted(glob.glob(os.path.join(config.DIR_RAW,   "*.json")))
    cards = sorted(glob.glob(os.path.join(config.DIR_CARDS, "*.json")))
    dates = set(os.path.basename(f).replace(".json", "") for f in raw)
    dates |= set(os.path.basename(f).replace(".json", "") for f in cards)
    return sorted(dates)


def load_day(date_str: str) -> dict | None:
    """
    Load race data for a date. Today prefers cards/ (has RPR pre-race).
    Historical dates use raw/ (post-race results).
    """
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
    parser = argparse.ArgumentParser(description="Race day predictor v2 — System C")
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
    day_rprc = rpr_coverage(all_runners_flat) if all_runners_flat else 0.0

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
    print(bold(f"  PREDICTIONS v2 — {date_str}  ({len(races)} races)  [System C]"))
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
            finished_in(p[2].get("position", ""), places)
            for p in pred["place_picks"] if p
        )
        cons_all3 = win_landed and all(
            finished_in(p[2].get("position", ""), cons_places)
            for p in pred["place_picks"] if p
        )
        ts["all3"]      += 1 if all3 else 0
        ts["cons_all3"] += 1 if cons_all3 else 0

    # ── Summary ────────────────────────────────────────────────────────────────
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
    print(f"    Place std:  {total_place}/{total_place_n} ({pct(total_place,total_place_n)})   "
          f"All 3 std: {total_all3}/{total_races} ({pct(total_all3,total_races)})")
    print(f"    Place cons: {total_cons_place}/{total_cons_place_n} ({pct(total_cons_place,total_cons_place_n)})   "
          f"All 3 cons: {total_cons_all3}/{total_races} ({pct(total_cons_all3,total_races)})")
    print()

    active_bet_tiers = [t for t in bet_tiers if tier_stats[t]["races"] > 0]
    if active_bet_tiers:
        bw   = sum(tier_stats[t]["win"]    for t in active_bet_tiers)
        bn   = sum(tier_stats[t]["win_n"]  for t in active_bet_tiers)
        br   = sum(tier_stats[t]["races"]  for t in active_bet_tiers)
        ba   = sum(tier_stats[t]["all3"]   for t in active_bet_tiers)
        bca  = sum(tier_stats[t]["cons_all3"] for t in active_bet_tiers)
        bp   = sum(tier_stats[t]["place"]  for t in active_bet_tiers)
        bpn  = sum(tier_stats[t]["place_n"] for t in active_bet_tiers)
        bcp  = sum(tier_stats[t]["cons_place"] for t in active_bet_tiers)
        bcpn = sum(tier_stats[t]["cons_place_n"] for t in active_bet_tiers)
        print(f"  {YELLOW}{BOLD}💎🔥✓  BET RACES ONLY (GOOD and above){RESET}")
        print(f"    Races: {br}   WIN: {bw}/{bn} ({pct(bw,bn)})")
        print(f"    Place std:  {bp}/{bpn} ({pct(bp,bpn)})   All 3 std: {ba}/{br} ({pct(ba,br)})")
        print(f"    Place cons: {bcp}/{bcpn} ({pct(bcp,bcpn)})   All 3 cons: {bca}/{br} ({pct(bca,br)})")
        print()


if __name__ == "__main__":
    main()
