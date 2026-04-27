"""
utils/tier_tracker.py

Live validation logger for confidence tier performance — System C.

Records every qualifying race result against its tier so you can track
actual live hit rates vs the validated backtest claims:

  ELITE    — validated ~67% P1 win rate
  STRONG   — validated ~49% P1 win rate
  GOOD     — validated ~36% P1 win rate
  STANDARD — info only (~30%)

Usage:
    from utils.tier_tracker import log_result, print_report

    # Log a result after a race settles:
    log_result(
        race_id  = "abc123",
        tier     = 4,           # TIER_ELITE = 4
        course   = "Newmarket",
        off      = "14:30",
        pick1    = "Horse Name",
        pick2    = "Other Horse",
        win1     = True,
        win2     = False,
    )

    # Print a full report to terminal:
    print_report()

    # Get a short summary string (used in EOD Telegram message):
    summary = get_eod_summary()
"""

import json
import os
import logging
from datetime import date, datetime

logger = logging.getLogger(__name__)

TRACK_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "data", "logs", "tier_performance.json"
)

# Validated win rates from 17-day history (System C)
BACKTEST_WIN_RATES = {
    4:  0.67,   # TIER_ELITE
    3:  0.49,   # TIER_STRONG
    2:  0.36,   # TIER_GOOD
    1:  0.30,   # TIER_STANDARD
    0:  0.00,   # TIER_WEAK
    -1: 0.00,   # TIER_SKIP
}

TIER_NAMES = {
    4:  "💎 ELITE",
    3:  "🔥 STRONG",
    2:  "✓ GOOD",
    1:  "· STANDARD",
    0:  "~ WEAK",
    -1: "✗ SKIP",
}

# Minimum races before flagging divergence — avoids noise from small samples
MIN_SAMPLE_FOR_ALERT = 20


# ── Read / write ──────────────────────────────────────────────────────────────

def _load() -> list:
    """Load all logged races. Returns empty list if file doesn't exist yet."""
    os.makedirs(os.path.dirname(TRACK_PATH), exist_ok=True)
    if not os.path.exists(TRACK_PATH):
        return []
    try:
        with open(TRACK_PATH) as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"tier_tracker: failed to load {TRACK_PATH}: {e}")
        return []


def _save(data: list):
    """Save all logged races."""
    os.makedirs(os.path.dirname(TRACK_PATH), exist_ok=True)
    try:
        with open(TRACK_PATH, "w") as f:
            json.dump(data, f, indent=2, default=str)
    except IOError as e:
        logger.error(f"tier_tracker: failed to save {TRACK_PATH}: {e}")


# ── Logging ───────────────────────────────────────────────────────────────────

def log_result(
    race_id:  str,
    tier:     int,
    course:   str,
    off:      str,
    pick1:    str,
    pick2:    str,
    win1:     bool,
    win2:     bool,
    places:   int  = None,
    tsr_solo: bool = False,   # kept for API compatibility, no longer used
) -> None:
    """
    Record the outcome of one race.
    win1 / win2: True if the pick finished within the required place terms.
    """
    data = _load()

    existing_ids = {r.get("race_id") for r in data}
    if race_id in existing_ids:
        logger.debug(f"tier_tracker: {race_id} already logged, skipping")
        return

    entry = {
        "date":    date.today().isoformat(),
        "time":    datetime.now().strftime("%H:%M"),
        "race_id": race_id,
        "tier":    tier,
        "course":  course,
        "off":     off,
        "pick1":   pick1,
        "pick2":   pick2,
        "win1":    win1,
        "win2":    win2,
        "either":  win1 or win2,
        "both":    win1 and win2,
        "places":  places,
    }

    data.append(entry)
    _save(data)
    logger.info(
        f"tier_tracker: logged {course} {off} "
        f"tier={TIER_NAMES.get(tier, tier)} "
        f"win1={win1} win2={win2}"
    )


# ── Analysis ──────────────────────────────────────────────────────────────────

def _tier_stats(data: list, tier: int) -> dict:
    """Compute stats for a single tier."""
    races = [r for r in data if r.get("tier") == tier]
    if not races:
        return {"n": 0}

    n      = len(races)
    win1   = sum(1 for r in races if r.get("win1"))
    win2   = sum(1 for r in races if r.get("win2"))
    either = sum(1 for r in races if r.get("either"))
    both   = sum(1 for r in races if r.get("both"))

    return {
        "n":          n,
        "win1":       win1,
        "win2":       win2,
        "either":     either,
        "both":       both,
        "win1_pct":   win1 / n,
        "win2_pct":   win2 / n,
        "either_pct": either / n,
        "both_pct":   both / n,
    }


def _divergence_alerts(stats_by_tier: dict) -> list:
    """
    Return warning strings where live rate diverges from validated rate.
    Only fires when sample >= MIN_SAMPLE_FOR_ALERT.
    """
    alerts = []
    for tier, stats in stats_by_tier.items():
        n = stats.get("n", 0)
        if n < MIN_SAMPLE_FOR_ALERT:
            continue
        expected = BACKTEST_WIN_RATES.get(tier)
        if not expected:
            continue
        actual = stats.get("win1_pct", 0)
        diff   = actual - expected
        name   = TIER_NAMES.get(tier, str(tier))

        if diff <= -0.15:
            alerts.append(
                f"⚠️ {name}: live {actual:.0%} vs validated {expected:.0%} "
                f"(−{abs(diff):.0%} over {n} races)"
            )
        elif diff >= 0.10:
            alerts.append(
                f"✅ {name}: live {actual:.0%} vs validated {expected:.0%} "
                f"(+{diff:.0%} over {n} races)"
            )
    return alerts


# ── Reports ───────────────────────────────────────────────────────────────────

def print_report(last_n_days: int = None) -> None:
    """Print a full tier performance report to the terminal."""
    data = _load()

    if last_n_days:
        from datetime import timedelta
        cutoff = (date.today() - timedelta(days=last_n_days)).isoformat()
        data   = [r for r in data if r.get("date", "") >= cutoff]

    if not data:
        print("No races logged yet.")
        return

    total_races = len(data)
    date_range  = f"{data[0]['date']} → {data[-1]['date']}" if data else "—"

    print()
    print("=" * 60)
    print(f"  TIER PERFORMANCE REPORT — System C")
    print(f"  {total_races} races  |  {date_range}")
    if last_n_days:
        print(f"  (last {last_n_days} days)")
    print("=" * 60)

    stats_by_tier = {}
    for tier in (4, 3, 2, 1, 0, -1):
        stats = _tier_stats(data, tier)
        stats_by_tier[tier] = stats
        if stats["n"] == 0:
            continue

        n        = stats["n"]
        w1       = stats["win1"]
        w1p      = stats["win1_pct"]
        w2       = stats["win2"]
        w2p      = stats["win2_pct"]
        ei       = stats["either"]
        eip      = stats["either_pct"]
        bt       = BACKTEST_WIN_RATES.get(tier, 0)
        diff     = w1p - bt
        diff_s   = f"+{diff:.0%}" if diff >= 0 else f"{diff:.0%}"
        name     = TIER_NAMES.get(tier, str(tier))
        sample_s = "" if n >= MIN_SAMPLE_FOR_ALERT else f"  ⚠ small sample (need {MIN_SAMPLE_FOR_ALERT})"

        print(f"\n  {name}")
        print(f"    Races  : {n}{sample_s}")
        print(f"    Pick 1 : {w1}/{n}  ({w1p:.0%})  vs validated {bt:.0%}  [{diff_s}]")
        print(f"    Pick 2 : {w2}/{n}  ({w2p:.0%})")
        print(f"    Either : {ei}/{n}  ({eip:.0%})")

    alerts = _divergence_alerts(stats_by_tier)
    if alerts:
        print()
        print("  ── Divergence alerts ──────────────────────────────────")
        for a in alerts:
            print(f"  {a}")

    print()
    print("  ── Last 10 races ──────────────────────────────────────")
    for r in data[-10:]:
        w1   = "✅" if r.get("win1") else "❌"
        w2   = "✅" if r.get("win2") else "❌"
        name = TIER_NAMES.get(r.get("tier"), "?")
        print(
            f"  {r.get('date')}  {r.get('off','?'):5}  {r.get('course','?'):20}"
            f"  {name:12}  P1:{w1}  P2:{w2}"
        )
    print()


def get_eod_summary() -> str:
    """Short summary for end-of-day Telegram message."""
    data       = _load()
    today      = date.today().isoformat()
    today_data = [r for r in data if r.get("date") == today]

    if not today_data:
        return "No races logged today."

    lines = ["📊 <b>Tier tracker — today</b>"]

    for tier in (4, 3, 2, 1):
        races = [r for r in today_data if r.get("tier") == tier]
        if not races:
            continue
        n    = len(races)
        wins = sum(1 for r in races if r.get("win1"))
        name = TIER_NAMES.get(tier, str(tier))
        lines.append(f"  {name}: {wins}/{n} P1 wins")

    # All-time divergence alerts
    stats_by_tier = {t: _tier_stats(data, t) for t in (4, 3, 2, 1, 0, -1)}
    for a in _divergence_alerts(stats_by_tier):
        lines.append(a)

    return "\n".join(lines)


def elite_hit_rate() -> str:
    """Quick one-liner for the ELITE tier hit rate."""
    data  = _load()
    stats = _tier_stats(data, 4)
    n     = stats.get("n", 0)
    if n == 0:
        return "ELITE: no races logged yet"
    w   = stats["win1"]
    pct = stats["win1_pct"]
    note = "" if n >= MIN_SAMPLE_FOR_ALERT else f" (⚠ small sample — need {MIN_SAMPLE_FOR_ALERT})"
    return f"💎 ELITE: {w}/{n} ({pct:.0%}){note}  [validated: 67%]"
