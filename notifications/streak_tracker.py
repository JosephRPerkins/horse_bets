"""
notifications/streak_tracker.py

Tracks two theoretical compounding bets across the day:
  - Standard:     wins if both picks finish in top-`places` positions
  - Conservative: wins if both picks finish in top-`cons_places` positions

Both start at £10. Winnings compound into the next stake. On a loss, reset to £10.
State persists to disk so bot restarts don't wipe the day's progress.

Odds calculation uses a logit-shift model to derive top-N finish odds from win SP:
  A table encodes how much easier it is to finish top-N vs win outright.
  Calibrated empirically; A[N] increases with N (larger target = shorter odds).
"""

import json
import math
import logging
import os
from datetime import date

logger = logging.getLogger(__name__)

INITIAL_STAKE = 10.0
STATE_DIR     = "data/streaks"
STATE_PATH    = os.path.join(STATE_DIR, "today.json")

# Logit-shift constants (from user's calibration).
# A[N] = how much more likely a horse is to finish top-N vs win.
# Table valid for N=2..7; for larger N we use A[7].
A = {2: 0.9, 3: 1.3, 4: 1.6, 5: 2.0, 6: 2.3, 7: 2.8}

# Module-level state — loaded on import, updated after each result
_state: dict = {}


def _fresh_state() -> dict:
    return {
        "date": date.today().isoformat(),
        "std":  {"balance": INITIAL_STAKE, "streak": 0, "best_streak": 0, "peak": INITIAL_STAKE},
        "cons": {"balance": INITIAL_STAKE, "streak": 0, "best_streak": 0, "peak": INITIAL_STAKE},
    }


def load_state() -> None:
    global _state
    os.makedirs(STATE_DIR, exist_ok=True)
    today = date.today().isoformat()
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH) as f:
                data = json.load(f)
            if data.get("date") == today:
                _state = data
                logger.info("streak_tracker: state loaded from disk")
                return
        except Exception as e:
            logger.warning(f"streak_tracker: could not load state: {e}")
    _state = _fresh_state()
    logger.info("streak_tracker: fresh state initialised")


def save_state() -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    try:
        with open(STATE_PATH, "w") as f:
            json.dump(_state, f, indent=2)
    except Exception as e:
        logger.warning(f"streak_tracker: save failed: {e}")


def reset_streaks() -> None:
    """Called at midnight to start a fresh day."""
    global _state
    _state = _fresh_state()
    save_state()
    logger.info("streak_tracker: reset for new day")


# ── Odds calculation ──────────────────────────────────────────────────────────

def top_n_dec_from_sp_dec(sp_dec: float, N: int) -> float:
    """
    Return decimal odds for a horse finishing in the top-N positions,
    derived from its win SP (decimal).

    Returns 1.01 as a floor (near-certainty) if maths would go weird.
    """
    if sp_dec <= 1.0:
        return 1.01
    N_capped = min(max(N, 2), 7)
    a        = A[N_capped]
    try:
        p_win = 1.0 / sp_dec
        lt    = math.log(p_win / (1.0 - p_win)) + a
        p_top = 1.0 / (1.0 + math.exp(-lt))
        return max(1.0 / p_top, 1.01)
    except (ValueError, ZeroDivisionError):
        return 1.01


def combined_top_n_dec(sp_a: float, sp_b: float, N: int) -> float:
    """
    Combined Bet Builder decimal odds — both picks finish top-N.
    Treated as independent (standard Bet Builder pricing assumption).
    """
    return top_n_dec_from_sp_dec(sp_a, N) * top_n_dec_from_sp_dec(sp_b, N)


# ── Per-result update ─────────────────────────────────────────────────────────

def update(race: dict, outcome: dict,
           horse_a: dict | None = None,
           horse_b: dict | None = None) -> str | None:
    """
    Called after each result. Updates both trackers and returns a formatted
    Telegram message string, or None if there's not enough data to track.

    horse_a / horse_b: result-enriched pick dicts (with actual SP from result).
    Falls back to race["top1"] / race["top2"] if not provided.
    """
    if not _state:
        load_state()

    top1 = horse_a or race.get("top1") or {}
    top2 = horse_b or race.get("top2") or {}
    sp_a = top1.get("sp_dec")
    sp_b = top2.get("sp_dec")

    if sp_a is None or sp_b is None or sp_a <= 1.0 or sp_b <= 1.0:
        logger.warning(
            f"streak_tracker: skipping {race.get('off','?')} {race.get('course','?')} "
            f"— sp_a={sp_a} sp_b={sp_b}"
        )
        return None

    std_N  = race.get("places", 3)
    cons_N = race.get("cons_places", 4)
    std_won  = outcome.get("std_win", False)
    cons_won = outcome.get("cons_win", False)

    std_odds  = combined_top_n_dec(sp_a, sp_b, std_N)
    cons_odds = combined_top_n_dec(sp_a, sp_b, cons_N)

    std_tracker  = _state["std"]
    cons_tracker = _state["cons"]

    std_prev_bal  = std_tracker["balance"]
    cons_prev_bal = cons_tracker["balance"]
    std_prev_streak  = std_tracker["streak"]
    cons_prev_streak = cons_tracker["streak"]

    _apply_result(std_tracker,  std_won,  std_odds)
    _apply_result(cons_tracker, cons_won, cons_odds)

    save_state()

    # Build message
    a_name = top1.get("horse", "?")
    b_name = top2.get("horse", "?")
    race_label = f"{race.get('off','?')} {race.get('course','?')}"

    lines = [
        "📈 <b>STREAK TRACKER</b>",
        f"<i>{race_label} — {a_name} + {b_name}</i>",
        "──────────────────────────────",
    ]

    lines += _format_tracker_block(
        label       = f"📊 Standard (top-{std_N})",
        tracker     = std_tracker,
        prev_bal    = std_prev_bal,
        prev_streak = std_prev_streak,
        won         = std_won,
        odds        = std_odds,
        stake       = std_prev_bal,
        sp_a        = sp_a,
        sp_b        = sp_b,
        N           = std_N,
    )

    lines.append("")

    lines += _format_tracker_block(
        label       = f"🛡️ Conservative (top-{cons_N})",
        tracker     = cons_tracker,
        prev_bal    = cons_prev_bal,
        prev_streak = cons_prev_streak,
        won         = cons_won,
        odds        = cons_odds,
        stake       = cons_prev_bal,
        sp_a        = sp_a,
        sp_b        = sp_b,
        N           = cons_N,
    )

    lines.append("──────────────────────────────")
    return "\n".join(lines)


def _apply_result(tracker: dict, won: bool, odds: float) -> None:
    stake = tracker["balance"]
    if won:
        profit = stake * (odds - 1.0)
        tracker["balance"] = round(tracker["balance"] + profit, 2)
        tracker["streak"]  += 1
        tracker["best_streak"] = max(tracker["best_streak"], tracker["streak"])
        tracker["peak"]    = max(tracker["peak"], tracker["balance"])
    else:
        tracker["balance"] = INITIAL_STAKE
        tracker["streak"]  = 0


def _format_tracker_block(
    label: str,
    tracker: dict,
    prev_bal: float,
    prev_streak: int,
    won: bool,
    odds: float,
    stake: float,
    sp_a: float,
    sp_b: float,
    N: int,
) -> list[str]:
    dec_a = top_n_dec_from_sp_dec(sp_a, N)
    dec_b = top_n_dec_from_sp_dec(sp_b, N)

    lines = [label]
    if won:
        profit = round(stake * (odds - 1.0), 2)
        new_bal = tracker["balance"]
        lines.append(
            f"  ✅ Win streak: {tracker['streak']}  |  "
            f"£{prev_bal:.2f} → £{new_bal:.2f} (+£{profit:.2f})"
        )
        lines.append(
            f"  Odds: {dec_a:.2f} × {dec_b:.2f}  |  Combined: {odds:.2f}"
        )
    else:
        lines.append(
            f"  ❌ Reset (streak was {prev_streak})  |  "
            f"£{prev_bal:.2f} → £{INITIAL_STAKE:.2f}"
        )
        lines.append(f"  Odds would have been: {odds:.2f}")

    lines.append(f"  Best today: {tracker['best_streak']} wins  |  Peak: £{tracker['peak']:.2f}")
    return lines


# ── End-of-day section ────────────────────────────────────────────────────────

def get_eod_summary() -> str:
    """Returns a streak summary block to embed in the end-of-day message."""
    if not _state:
        load_state()

    std  = _state.get("std",  {})
    cons = _state.get("cons", {})

    lines = [
        "──────────────────────────────",
        "📈 <b>Streak Tracker</b>",
        f"📊 Standard      — best: {std.get('best_streak', 0)} wins  |  peak: £{std.get('peak', INITIAL_STAKE):.2f}  |  closing: £{std.get('balance', INITIAL_STAKE):.2f}",
        f"🛡️ Conservative  — best: {cons.get('best_streak', 0)} wins  |  peak: £{cons.get('peak', INITIAL_STAKE):.2f}  |  closing: £{cons.get('balance', INITIAL_STAKE):.2f}",
    ]
    return "\n".join(lines)


# Load on import so state is available immediately
load_state()
