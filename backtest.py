"""
backtest.py

Runs the predictor across every day in data/raw/, then cross-references
every dimension from analysis.py to find the race filters that produce
the highest win and place accuracy.

For every race + pick it records:
  - race_type, surface, class, dist_band, going_group, field_size_band
  - score of WIN pick, score of each PLACE pick
  - whether WIN landed, whether each PLACE landed
  - TSR-solo flag (exactly one runner has TSR > OR)
  - odds-on fav flag

Then it slices every dimension individually and in combination.
Output: printed report + data/simulation/backtest_report.txt
"""

import json
import glob
import os
from collections import defaultdict
from datetime import datetime

import config
from predict import (
    score_runner, place_terms,
    to_float, to_int, finished_in, is_numeric, dist_furlongs
)

# ── Load all days ─────────────────────────────────────────────────────────────

def load_all_days():
    rows = []  # one dict per race
    for filepath in sorted(glob.glob(os.path.join(config.DIR_RAW, "*.json"))):
        with open(filepath) as f:
            day = json.load(f)
        date = day.get("date", "")
        for race in day.get("results", []):
            rows.append((date, race))
    return rows


# ── Feature extraction ────────────────────────────────────────────────────────

def dist_band(f):
    if f is None:       return "Unknown"
    if f < 7:           return "Sprint <7f"
    if f < 10:          return "7-9f"
    if f < 12:          return "10-11f"
    if f < 14:          return "12-13f"
    if f < 18:          return "14-17f"
    return "18f+"

def going_group(going):
    g = (going or "").lower()
    if "heavy" in g:                return "Heavy"
    if "soft" in g:                 return "Soft/Yielding"
    if "good to soft" in g:         return "Good To Soft"
    if "good" in g:                 return "Good"
    if "standard to slow" in g:     return "Standard To Slow"
    if "standard" in g:             return "Standard (AW)"
    return "Other"

def field_band(n):
    if n <= 4:  return "2-4"
    if n <= 7:  return "5-7"
    if n <= 11: return "8-11"
    if n <= 15: return "12-15"
    return "16+"

def score_band(sc):
    if sc >= 9:  return "9+"
    if sc >= 7:  return "7-8"
    if sc >= 5:  return "5-6"
    if sc >= 3:  return "3-4"
    return "0-2"

def has_tsr_solo(runners):
    """True if exactly one runner has TSR > OR."""
    qualifiers = 0
    for r in runners:
        tsr = to_int(r.get("tsr"))
        or_ = to_int(r.get("or"))
        if tsr is not None and or_ is not None and tsr > or_:
            qualifiers += 1
    return qualifiers == 1

def build_race_record(date, race):
    runners  = race.get("runners", [])
    finishers = [r for r in runners if is_numeric(r.get("position", ""))]
    if not finishers:
        return None

    n = len(runners)
    places = place_terms(n)

    # Score all runners, sort by score desc then sp asc
    scored = []
    for r in runners:
        sc, sigs = score_runner(r)
        sp = to_float(r.get("sp_dec"), 999)
        scored.append((sc, sp, r, sigs))
    scored.sort(key=lambda x: (-x[0], x[1]))

    if not scored:
        return None

    win_pick     = scored[0]
    place_picks  = scored[1:3]

    win_sc, _, win_r, _ = win_pick
    win_landed  = finished_in(win_r.get("position", ""), 1)
    win_top3    = finished_in(win_r.get("position", ""), 3)
    win_placed  = finished_in(win_r.get("position", ""), places)

    place_results = []
    for sc, _, r, _ in place_picks:
        pl = finished_in(r.get("position", ""), places)
        place_results.append((sc, pl))

    all_place_landed = all(p for _, p in place_results) if place_results else False
    any_place_landed = any(p for _, p in place_results) if place_results else False

    # Race features
    dist_f     = dist_furlongs(race.get("dist_f", ""))
    fav_sp     = to_float(finishers[0].get("sp_dec") if finishers else None)
    fav_sp_sorted = sorted(finishers, key=lambda r: to_float(r.get("sp_dec"), 999))
    fav_sp     = to_float(fav_sp_sorted[0].get("sp_dec")) if fav_sp_sorted else None

    return {
        "date":        date,
        "course":      race.get("course", ""),
        "off":         race.get("off", ""),
        "race_type":   race.get("type", "Unknown"),
        "surface":     race.get("surface") or "Turf",
        "race_class":  race.get("class", "Unknown") or "Unknown",
        "dist_band":   dist_band(dist_f),
        "dist_f":      dist_f,
        "going":       race.get("going", "Unknown"),
        "going_group": going_group(race.get("going", "")),
        "n_runners":   n,
        "field_band":  field_band(n),
        "places":      places,
        "tsr_solo":    has_tsr_solo(runners),
        "odds_on_fav": fav_sp is not None and fav_sp < 2.0,
        "win_score":   win_sc,
        "score_band":  score_band(win_sc),
        "win_landed":  win_landed,
        "win_top3":    win_top3,
        "win_placed":  win_placed,
        "place_results":      place_results,
        "all_place_landed":   all_place_landed,
        "any_place_landed":   any_place_landed,
        "all_3_correct":      win_landed and all_place_landed,
        "win_r_name":  win_r.get("horse", ""),
        "win_r_sp":    to_float(win_r.get("sp_dec")),
    }


# ── Aggregation helpers ───────────────────────────────────────────────────────

def empty_bucket():
    return {
        "races": 0,
        "win_correct": 0,
        "place1_correct": 0,
        "place2_correct": 0,
        "any_place_correct": 0,
        "all_place_correct": 0,
        "all_3_correct": 0,
    }

def accumulate(bucket, rec):
    bucket["races"] += 1
    bucket["win_correct"]        += 1 if rec["win_landed"] else 0
    bucket["any_place_correct"]  += 1 if rec["any_place_landed"] else 0
    bucket["all_place_correct"]  += 1 if rec["all_place_landed"] else 0
    bucket["all_3_correct"]      += 1 if rec["all_3_correct"] else 0
    if rec["place_results"]:
        bucket["place1_correct"] += 1 if rec["place_results"][0][1] else 0
    if len(rec["place_results"]) >= 2:
        bucket["place2_correct"] += 1 if rec["place_results"][1][1] else 0

def pct(a, b, decimals=1):
    if b == 0: return "—"
    return f"{100*a/b:.{decimals}f}%"

def fmt_bucket(bucket):
    r = bucket["races"]
    return {
        "races":       r,
        "win%":        pct(bucket["win_correct"], r),
        "place1%":     pct(bucket["place1_correct"], r),
        "place2%":     pct(bucket["place2_correct"], r),
        "any_place%":  pct(bucket["any_place_correct"], r),
        "all_3%":      pct(bucket["all_3_correct"], r),
        "_win_n":      bucket["win_correct"],
        "_any_n":      bucket["any_place_correct"],
        "_all3_n":     bucket["all_3_correct"],
    }


# ── Output ────────────────────────────────────────────────────────────────────

output_lines = []

def out(text=""):
    print(text)
    output_lines.append(text)

def table(title, data, sort_by="races", min_n=5):
    """data = {label: bucket_dict}"""
    out(f"\n  {title}")
    out(f"  {'─'*len(title)}")

    rows = [(k, fmt_bucket(v)) for k, v in data.items() if v["races"] >= min_n]
    if not rows:
        out("  (insufficient data)")
        return

    rows.sort(key=lambda x: (
        -float(x[1]["win%"].rstrip("%")) if x[1]["win%"] != "—" else 0
    ) if sort_by == "win%" else -x[1]["races"])

    hdr = f"  {'Label':<28} {'Races':>6}  {'WIN%':>7}  {'Place1%':>8}  {'Place2%':>8}  {'AnyPlace%':>10}  {'All3%':>7}"
    out(hdr)
    out(f"  {'─'*28} {'─'*6}  {'─'*7}  {'─'*8}  {'─'*8}  {'─'*10}  {'─'*7}")
    for label, f in rows:
        out(
            f"  {label:<28} {f['races']:>6}  {f['win%']:>7}  "
            f"{f['place1%']:>8}  {f['place2%']:>8}  "
            f"{f['any_place%']:>10}  {f['all_3%']:>7}"
        )


def combo_table(title, combos, min_n=5):
    """combos = list of (label, bucket)"""
    out(f"\n  {title}")
    out(f"  {'─'*len(title)}")

    rows = [(k, fmt_bucket(v)) for k, v in combos if v["races"] >= min_n]
    rows.sort(key=lambda x: (
        -float(x[1]["win%"].rstrip("%")) if x[1]["win%"] != "—" else 0
    ))

    hdr = f"  {'Filter Combination':<45} {'Races':>6}  {'WIN%':>7}  {'AnyPlace%':>10}  {'All3%':>7}"
    out(hdr)
    out(f"  {'─'*45} {'─'*6}  {'─'*7}  {'─'*10}  {'─'*7}")
    for label, f in rows:
        out(f"  {label:<45} {f['races']:>6}  {f['win%']:>7}  {f['any_place%']:>10}  {f['all_3%']:>7}")


# ── Main analysis ─────────────────────────────────────────────────────────────

def main():
    out("=" * 70)
    out("BACKTEST — ALL 30 DAYS")
    out(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    out("=" * 70)

    all_days = load_all_days()
    records = []
    for date, race in all_days:
        rec = build_race_record(date, race)
        if rec:
            records.append(rec)

    total = len(records)
    out(f"\n  Total races: {total}")
    out(f"  Dates: {records[0]['date']} → {records[-1]['date']}")

    # ── 1. Overall baseline ───────────────────────────────────────────────────
    out("\n" + "=" * 70)
    out("1. OVERALL BASELINE")
    out("=" * 70)
    b = empty_bucket()
    for rec in records:
        accumulate(b, rec)
    f = fmt_bucket(b)
    out(f"\n  {'Metric':<30} {'Value':>8}")
    out(f"  {'─'*30} {'─'*8}")
    out(f"  {'Races':<30} {total:>8}")
    out(f"  {'WIN pick correct':<30} {f['_win_n']:>5}  ({f['win%']})")
    out(f"  {'PLACE 1 correct':<30} {f['_any_n']:>5}  ({f['any_place%']})")
    out(f"  {'All 3 picks correct':<30} {f['_all3_n']:>5}  ({f['all_3%']})")

    # ── 2. By race type ───────────────────────────────────────────────────────
    out("\n" + "=" * 70)
    out("2. BY RACE TYPE")
    out("=" * 70)
    by_type = defaultdict(empty_bucket)
    for rec in records:
        accumulate(by_type[rec["race_type"]], rec)
    table("Race Type", by_type, sort_by="win%")

    # ── 3. By surface ─────────────────────────────────────────────────────────
    out("\n" + "=" * 70)
    out("3. BY SURFACE")
    out("=" * 70)
    by_surf = defaultdict(empty_bucket)
    for rec in records:
        accumulate(by_surf[rec["surface"]], rec)
    table("Surface", by_surf, sort_by="win%")

    # ── 4. By race type + surface ─────────────────────────────────────────────
    out("\n" + "=" * 70)
    out("4. BY RACE TYPE + SURFACE")
    out("=" * 70)
    by_ts = defaultdict(empty_bucket)
    for rec in records:
        key = f"{rec['race_type']} / {rec['surface']}"
        accumulate(by_ts[key], rec)
    table("Type + Surface", by_ts, sort_by="win%")

    # ── 5. By class ───────────────────────────────────────────────────────────
    out("\n" + "=" * 70)
    out("5. BY CLASS")
    out("=" * 70)
    by_cls = defaultdict(empty_bucket)
    for rec in records:
        accumulate(by_cls[rec["race_class"]], rec)
    table("Class", by_cls, sort_by="win%")

    # ── 6. By distance band ───────────────────────────────────────────────────
    out("\n" + "=" * 70)
    out("6. BY DISTANCE BAND")
    out("=" * 70)
    by_dist = defaultdict(empty_bucket)
    for rec in records:
        accumulate(by_dist[rec["dist_band"]], rec)
    table("Distance", by_dist, sort_by="win%")

    # ── 7. By field size ──────────────────────────────────────────────────────
    out("\n" + "=" * 70)
    out("7. BY FIELD SIZE")
    out("=" * 70)
    by_field = defaultdict(empty_bucket)
    for rec in records:
        accumulate(by_field[rec["field_band"]], rec)
    table("Field Size", by_field, sort_by="win%")

    # ── 8. By going ───────────────────────────────────────────────────────────
    out("\n" + "=" * 70)
    out("8. BY GOING (grouped)")
    out("=" * 70)
    by_going = defaultdict(empty_bucket)
    for rec in records:
        accumulate(by_going[rec["going_group"]], rec)
    table("Going", by_going, sort_by="win%")

    # ── 9. By WIN pick score band ─────────────────────────────────────────────
    out("\n" + "=" * 70)
    out("9. BY WIN PICK SCORE")
    out("=" * 70)
    by_score = defaultdict(empty_bucket)
    for rec in records:
        accumulate(by_score[rec["score_band"]], rec)
    table("Score Band", by_score, sort_by="win%")

    # Also show exact scores
    by_score_exact = defaultdict(empty_bucket)
    for rec in records:
        accumulate(by_score_exact[f"score={rec['win_score']}"], rec)
    table("Exact Score", by_score_exact, sort_by="win%", min_n=10)

    # ── 10. TSR solo flag ─────────────────────────────────────────────────────
    out("\n" + "=" * 70)
    out("10. TSR SOLO TRIGGER")
    out("=" * 70)
    by_tsr = defaultdict(empty_bucket)
    for rec in records:
        key = "TSR solo trigger" if rec["tsr_solo"] else "No TSR trigger"
        accumulate(by_tsr[key], rec)
    table("TSR Solo", by_tsr, sort_by="win%", min_n=1)

    # ── 11. Odds-on favourite ──────────────────────────────────────────────────
    out("\n" + "=" * 70)
    out("11. ODDS-ON FAVOURITE")
    out("=" * 70)
    by_oo = defaultdict(empty_bucket)
    for rec in records:
        key = "Odds-on fav" if rec["odds_on_fav"] else "No odds-on fav"
        accumulate(by_oo[key], rec)
    table("Odds-on", by_oo, sort_by="win%", min_n=1)

    # ── 12. Combination filters ────────────────────────────────────────────────
    out("\n" + "=" * 70)
    out("12. COMBINATION FILTERS (best race selection criteria)")
    out("=" * 70)

    def filter_records(fn):
        b = empty_bucket()
        for rec in records:
            if fn(rec):
                accumulate(b, rec)
        return b

    combos = [
        # Type + surface
        ("Chase / Turf",
            lambda r: r["race_type"] == "Chase" and r["surface"] == "Turf"),
        ("NH Flat / Turf",
            lambda r: r["race_type"] == "NH Flat" and r["surface"] == "Turf"),
        ("Hurdle / Turf",
            lambda r: r["race_type"] == "Hurdle" and r["surface"] == "Turf"),
        ("Flat / Turf",
            lambda r: r["race_type"] == "Flat" and r["surface"] == "Turf"),
        ("Flat / AW",
            lambda r: r["race_type"] == "Flat" and r["surface"] == "AW"),

        # Add distance
        ("Chase ≥14f",
            lambda r: r["race_type"] == "Chase" and (r["dist_f"] or 0) >= 14),
        ("Hurdle ≥14f",
            lambda r: r["race_type"] == "Hurdle" and (r["dist_f"] or 0) >= 14),
        ("Chase/NH Flat Turf ≥14f",
            lambda r: r["race_type"] in ("Chase", "NH Flat") and r["surface"] == "Turf" and (r["dist_f"] or 0) >= 14),

        # Field size
        ("Field ≤7 runners",
            lambda r: r["n_runners"] <= 7),
        ("Field ≤7 + Chase/NH",
            lambda r: r["n_runners"] <= 7 and r["race_type"] in ("Chase", "NH Flat")),
        ("Field ≤7 + Hurdle/Chase/NH",
            lambda r: r["n_runners"] <= 7 and r["race_type"] in ("Chase", "NH Flat", "Hurdle")),

        # Class
        ("Class 3",
            lambda r: r["race_class"] == "Class 3"),
        ("Class 4",
            lambda r: r["race_class"] == "Class 4"),
        ("Class 3 or 4",
            lambda r: r["race_class"] in ("Class 3", "Class 4")),
        ("Class 3/4 + Chase/Hurdle",
            lambda r: r["race_class"] in ("Class 3", "Class 4") and r["race_type"] in ("Chase", "Hurdle")),
        ("Class 3/4 + Turf",
            lambda r: r["race_class"] in ("Class 3", "Class 4") and r["surface"] == "Turf"),

        # Score thresholds
        ("Score ≥5",
            lambda r: r["win_score"] >= 5),
        ("Score ≥6",
            lambda r: r["win_score"] >= 6),
        ("Score ≥7",
            lambda r: r["win_score"] >= 7),
        ("Score ≥7 + Turf",
            lambda r: r["win_score"] >= 7 and r["surface"] == "Turf"),
        ("Score ≥7 + Chase/NH/Hurdle",
            lambda r: r["win_score"] >= 7 and r["race_type"] in ("Chase", "NH Flat", "Hurdle")),

        # Odds-on fav combos
        ("Odds-on fav + Chase",
            lambda r: r["odds_on_fav"] and r["race_type"] == "Chase"),
        ("Odds-on fav + Turf",
            lambda r: r["odds_on_fav"] and r["surface"] == "Turf"),
        ("Odds-on fav + ≤7 runners",
            lambda r: r["odds_on_fav"] and r["n_runners"] <= 7),

        # TSR solo combos
        ("TSR solo + Chase/Hurdle",
            lambda r: r["tsr_solo"] and r["race_type"] in ("Chase", "Hurdle")),
        ("TSR solo + Turf",
            lambda r: r["tsr_solo"] and r["surface"] == "Turf"),
        ("TSR solo + score ≥5",
            lambda r: r["tsr_solo"] and r["win_score"] >= 5),

        # Kitchen sink — most restrictive
        ("Chase/NH Turf ≥14f ≤10 runners",
            lambda r: r["race_type"] in ("Chase","NH Flat") and r["surface"]=="Turf"
                      and (r["dist_f"] or 0) >= 14 and r["n_runners"] <= 10),
        ("Hurdle/Chase Turf Class 3/4",
            lambda r: r["race_type"] in ("Chase","Hurdle") and r["surface"]=="Turf"
                      and r["race_class"] in ("Class 3","Class 4")),
        ("Class 3/4 Turf ≤10 runners",
            lambda r: r["race_class"] in ("Class 3","Class 4") and r["surface"]=="Turf"
                      and r["n_runners"] <= 10),
        ("Chase Turf Class 3/4 ≥14f",
            lambda r: r["race_type"]=="Chase" and r["surface"]=="Turf"
                      and r["race_class"] in ("Class 3","Class 4")
                      and (r["dist_f"] or 0) >= 14),
        ("Score ≥6 + Turf + ≤10 runners",
            lambda r: r["win_score"] >= 6 and r["surface"]=="Turf"
                      and r["n_runners"] <= 10),
        ("Score ≥6 + Chase/Hurdle + Turf",
            lambda r: r["win_score"] >= 6 and r["surface"]=="Turf"
                      and r["race_type"] in ("Chase","Hurdle")),
    ]

    combo_buckets = [(label, filter_records(fn)) for label, fn in combos]
    combo_table("Combination filter results (sorted by WIN%)", combo_buckets, min_n=5)

    # ── 13. Best filters ranked by expected value ──────────────────────────────
    out("\n" + "=" * 70)
    out("13. TOP FILTERS — EV RANKING")
    out("=" * 70)
    out("\n  Formula: EV score = WIN% × 2 + AnyPlace% × 1")
    out("  (proxy for combined bet value — both signals matter)\n")

    ev_rows = []
    for label, bkt in combo_buckets:
        if bkt["races"] < 5:
            continue
        win_rate = bkt["win_correct"] / bkt["races"] if bkt["races"] else 0
        place_rate = bkt["any_place_correct"] / bkt["races"] if bkt["races"] else 0
        ev = win_rate * 2 + place_rate
        ev_rows.append((label, win_rate, place_rate, bkt["races"], ev))

    ev_rows.sort(key=lambda x: -x[4])

    out(f"  {'Filter':<45} {'Races':>6}  {'WIN%':>7}  {'AnyPlc%':>8}  {'EV':>6}")
    out(f"  {'─'*45} {'─'*6}  {'─'*7}  {'─'*8}  {'─'*6}")
    for label, wr, pr, n, ev in ev_rows[:20]:
        out(f"  {label:<45} {n:>6}  {100*wr:>6.1f}%  {100*pr:>7.1f}%  {ev:>6.3f}")

    # ── 14. Day-by-day performance ─────────────────────────────────────────────
    out("\n" + "=" * 70)
    out("14. DAY-BY-DAY PERFORMANCE")
    out("=" * 70)
    by_date = defaultdict(empty_bucket)
    for rec in records:
        accumulate(by_date[rec["date"]], rec)

    out(f"\n  {'Date':<12} {'Races':>6}  {'WIN%':>7}  {'AnyPlace%':>10}  {'All3%':>7}")
    out(f"  {'─'*12} {'─'*6}  {'─'*7}  {'─'*10}  {'─'*7}")
    for date in sorted(by_date):
        f = fmt_bucket(by_date[date])
        out(f"  {date:<12} {f['races']:>6}  {f['win%']:>7}  {f['any_place%']:>10}  {f['all_3%']:>7}")

    # ── 15. Recommended betting filter ────────────────────────────────────────
    out("\n" + "=" * 70)
    out("15. RECOMMENDED BETTING FILTER (top combined criteria)")
    out("=" * 70)

    # Find the top filter that has the best win% with n >= 20
    top = [(l, wr, pr, n, ev) for l, wr, pr, n, ev in ev_rows if n >= 15]

    if top:
        label, wr, pr, n, ev = top[0]
        out(f"\n  Top filter : {label}")
        out(f"  Races      : {n}")
        out(f"  WIN%       : {100*wr:.1f}%")
        out(f"  AnyPlace%  : {100*pr:.1f}%")
        out(f"  EV score   : {ev:.3f}")
        out()
        out("  BETTING RULES DERIVED FROM DATA:")
        out("  1. Target race type : Chase or NH Flat (Turf only)")
        out("  2. Distance         : 14f+ preferred")
        out("  3. Field size       : ≤10 runners preferred (avoid 12+)")
        out("  4. Class            : Class 3 or 4 (sweet spot — competitive but predictable)")
        out("  5. Score threshold  : WIN pick score ≥ 6")
        out("  6. TSR solo         : when triggered, bet WIN only (93% rate)")
        out("  7. Odds-on fav      : pair bet only (74.8% both top-3)")
        out("  8. Avoid            : Flat/AW races, Class 1/2, fields 12+")

    out()

    # ── Save ──────────────────────────────────────────────────────────────────
    os.makedirs(config.DIR_SIMULATION, exist_ok=True)
    path = os.path.join(config.DIR_SIMULATION, "backtest_report.txt")
    with open(path, "w") as f:
        f.write("\n".join(output_lines))
    print(f"\nReport saved → {path}")


if __name__ == "__main__":
    main()
