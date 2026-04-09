"""
utils/tier_tracker.py

Live validation logger for confidence tier performance.

Records every qualifying race result against its tier so you can track
actual live hit rates vs the backtest claims:

  SUPREME  — claimed 93% win rate
  STRONG   — claimed 65%
  GOOD     — claimed 56%
  STANDARD — claimed 43%

Usage:
    from utils.tier_tracker import log_result, print_report

    # Log a result after a race settles:
    log_result(
        race_id  = "abc123",
        tier     = 3,           # TIER_SUPREME = 3
        course   = "Newmarket",
        off      = "14:30",
        pick1    = "Horse Name",
        pick2    = "Other Horse",
        win1     = True,        # did pick1 finish in the required places?
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

# Expected win rates from backtest — used to flag when live rate diverges
BACKTEST_WIN_RATES = {
    3:  0.93,   # TIER_SUPREME
    2:  0.65,   # TIER_STRONG
    1:  0.56,   # TIER_GOOD
    0:  0.43,   # TIER_STANDARD
    -1: 0.35,   # TIER_SKIP
}

TIER_NAMES = {
    3:  "🔥🔥🔥 SUPREME",
    2:  "🔥🔥 STRONG",
    1:  "🔥 GOOD",
    0:  "· STANDARD",
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
    places:   int = None,
    tsr_solo: bool = False,
) -> None:
    """
    Record the outcome of one race.

    win1 / win2: True if the pick finished within the required place terms.
    tsr_solo: True if the SUPREME tier was triggered by a TSR solo signal —
              tracked separately so we can validate the 93% claim specifically
              for TSR solo vs other SUPREME triggers.
    """
    data = _load()

    # Avoid duplicate entries for the same race
    existing_ids = {r.get("race_id") for r in data}
    if race_id in existing_ids:
        logger.debug(f"tier_tracker: {race_id} already logged, skipping")
        return

    entry = {
        "date":     date.today().isoformat(),
        "time":     datetime.now().strftime("%H:%M"),
        "race_id":  race_id,
        "tier":     tier,
        "course":   course,
        "off":      off,
        "pick1":    pick1,
        "pick2":    pick2,
        "win1":     win1,
        "win2":     win2,
        "either":   win1 or win2,
        "both":     win1 and win2,
        "places":   places,
        "tsr_solo": tsr_solo,
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

    n       = len(races)
    win1    = sum(1 for r in races if r.get("win1"))
    win2    = sum(1 for r in races if r.get("win2"))
    either  = sum(1 for r in races if r.get("either"))
    both    = sum(1 for r in races if r.get("both"))

    # TSR solo subset (SUPREME only)
    tsr_races = [r for r in races if r.get("tsr_solo")]
    tsr_wins  = sum(1 for r in tsr_races if r.get("win1"))

    return {
        "n":          n,
        "win1":       win1,
        "win2":       win2,
        "either":     either,
        "both":       both,
        "win1_pct":   win1 / n,
        "either_pct": either / n,
        "both_pct":   both / n,
        "tsr_n":      len(tsr_races),
        "tsr_wins":   tsr_wins,
        "tsr_pct":    tsr_wins / len(tsr_races) if tsr_races else None,
    }


def _divergence_alerts(stats_by_tier: dict) -> list[str]:
    """
    Return a list of warning strings where live rate diverges significantly
    from the backtest claim. Only fires when sample >= MIN_SAMPLE_FOR_ALERT.
    """
    alerts = []
    for tier, stats in stats_by_tier.items():
        n = stats.get("n", 0)
        if n < MIN_SAMPLE_FOR_ALERT:
            continue
        expected = BACKTEST_WIN_RATES.get(tier)
        if expected is None:
            continue
        actual = stats.get("win1_pct", 0)
        diff   = actual - expected
        name   = TIER_NAMES.get(tier, str(tier))

        if diff <= -0.15:
            alerts.append(
                f"⚠️ {name}: live win rate {actual:.0%} vs backtest {expected:.0%} "
                f"(−{abs(diff):.0%} over {n} races) — model may be overfitted"
            )
        elif diff >= 0.10:
            alerts.append(
                f"✅ {name}: live win rate {actual:.0%} vs backtest {expected:.0%} "
                f"(+{diff:.0%} over {n} races) — outperforming"
            )
    return alerts


# ── Reports ───────────────────────────────────────────────────────────────────

def print_report(last_n_days: int = None) -> None:
    """
    Print a full tier performance report to the terminal.

    last_n_days: if set, only include races from the last N days.
    """
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
    print(f"  TIER PERFORMANCE REPORT")
    print(f"  {total_races} races  |  {date_range}")
    if last_n_days:
        print(f"  (last {last_n_days} days)")
    print("=" * 60)

    stats_by_tier = {}
    for tier in (3, 2, 1, 0, -1):
        stats = _tier_stats(data, tier)
        stats_by_tier[tier] = stats
        if stats["n"] == 0:
            continue

        n         = stats["n"]
        w1        = stats["win1"]
        w1p       = stats["win1_pct"]
        ei        = stats["either"]
        eip       = stats["either_pct"]
        bt        = BACKTEST_WIN_RATES.get(tier, 0)
        diff      = w1p - bt
        diff_s    = f"+{diff:.0%}" if diff >= 0 else f"{diff:.0%}"
        name      = TIER_NAMES.get(tier, str(tier))
        sample_s  = "" if n >= MIN_SAMPLE_FOR_ALERT else f"  ⚠ small sample (need {MIN_SAMPLE_FOR_ALERT})"

        print(f"\n  {name}")
        print(f"    Races  : {n}{sample_s}")
        print(f"    Pick 1 : {w1}/{n}  ({w1p:.0%})  vs backtest {bt:.0%}  [{diff_s}]")
        print(f"    Either : {ei}/{n}  ({eip:.0%})")

        # TSR solo breakdown (SUPREME only)
        if tier == 3 and stats["tsr_n"] > 0:
            tp  = stats["tsr_pct"]
            tn  = stats["tsr_n"]
            tw  = stats["tsr_wins"]
            print(f"    TSR solo subset: {tw}/{tn}  ({tp:.0%})  [claimed 93%]")

    # Divergence alerts
    alerts = _divergence_alerts(stats_by_tier)
    if alerts:
        print()
        print("  ── Divergence alerts ──────────────────────────────────")
        for a in alerts:
            print(f"  {a}")

    # Recent form — last 10 races regardless of tier
    print()
    print("  ── Last 10 races ──────────────────────────────────────")
    for r in data[-10:]:
        w1 = "✅" if r.get("win1") else "❌"
        w2 = "✅" if r.get("win2") else "❌"
        name = TIER_NAMES.get(r.get("tier"), "?")
        print(
            f"  {r.get('date')}  {r.get('off','?'):5}  {r.get('course','?'):20}"
            f"  {name:18}  P1:{w1}  P2:{w2}"
        )
    print()


def get_eod_summary() -> str:
    """
    Short summary string for the end-of-day Telegram message.
    Shows today's tier breakdown and any divergence alerts.
    """
    data  = _load()
    today = date.today().isoformat()
    today_data = [r for r in data if r.get("date") == today]

    if not today_data:
        return "No races logged today."

    lines = ["📊 <b>Tier tracker — today</b>"]

    for tier in (3, 2, 1, 0):
        races = [r for r in today_data if r.get("tier") == tier]
        if not races:
            continue
        n    = len(races)
        wins = sum(1 for r in races if r.get("win1"))
        name = TIER_NAMES.get(tier, str(tier))
        lines.append(f"  {name}: {wins}/{n}")

    # All-time divergence alerts (if sample large enough)
    stats_by_tier = {t: _tier_stats(data, t) for t in (3, 2, 1, 0, -1)}
    alerts = _divergence_alerts(stats_by_tier)
    for a in alerts:
        lines.append(a)

    return "\n".join(lines)


def supreme_hit_rate() -> str:
    """
    Quick one-liner for the SUPREME tier hit rate.
    Useful for a quick sanity check from the terminal.

    Usage:
        python -c "from utils.tier_tracker import supreme_hit_rate; print(supreme_hit_rate())"
    """
    data   = _load()
    stats  = _tier_stats(data, 3)
    n      = stats.get("n", 0)
    if n == 0:
        return "SUPREME: no races logged yet"
    w      = stats["win1"]
    pct    = stats["win1_pct"]
    tsr_n  = stats["tsr_n"]
    tsr_w  = stats["tsr_wins"]
    tsr_p  = stats["tsr_pct"]
    note   = "" if n >= MIN_SAMPLE_FOR_ALERT else f" (⚠ small sample — need {MIN_SAMPLE_FOR_ALERT})"

    lines = [f"SUPREME: {w}/{n} ({pct:.0%}){note}  [backtest: 93%]"]
    if tsr_n:
        lines.append(f"  TSR solo: {tsr_w}/{tsr_n} ({tsr_p:.0%})")
    return "\n".join(lines)
