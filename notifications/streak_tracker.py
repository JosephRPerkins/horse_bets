"""
notifications/streak_tracker.py

Tracks two theoretical compounding bets across the day:
  - Standard:     wins if both picks finish in top-`places` positions
  - Conservative: wins if both picks finish in top-`cons_places` positions

Both start at £10. Winnings compound into the next stake. On a loss, reset to £10.
State persists to disk so bot restarts don't wipe the day's progress.

Odds calculation — two modes:

  update_from_betfair() [preferred]:
    Uses actual live Betfair place market prices captured at bet time.
    These are real exchange odds, not estimated from win SP.
    Combined odds = place_price_a * place_price_b (treated as independent legs).

  update() [fallback / legacy]:
    Derives place odds from win SP using a logit-shift model.
    Used as fallback if Betfair place prices are unavailable.
    A[N] encodes how much easier it is to finish top-N vs win outright.
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

# Logit-shift constants — fallback only, used when Betfair place prices unavailable
A = {2: 0.9, 3: 1.3, 4: 1.6, 5: 2.0, 6: 2.3, 7: 2.8}

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
    global _state
    _state = _fresh_state()
    save_state()
    logger.info("streak_tracker: reset for new day")


# ── Odds calculation ──────────────────────────────────────────────────────────

def top_n_dec_from_sp_dec(sp_dec: float, N: int) -> float:
    """
    Fallback: derive place odds from win SP using logit-shift model.
    Used only when Betfair place market prices are unavailable.
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
    """Fallback: combined odds from win SP."""
    return top_n_dec_from_sp_dec(sp_a, N) * top_n_dec_from_sp_dec(sp_b, N)


def combined_from_place_prices(place_price_a: float, place_price_b: float) -> float:
    """
    Combined Bet Builder odds using actual Betfair place market prices.
    Both legs treated as independent — standard Bet Builder assumption.
    place_price_a and place_price_b are decimal odds from the place market.
    """
    return round(place_price_a * place_price_b, 2)


# ── Main update functions ─────────────────────────────────────────────────────

def update_from_betfair(
    race:          dict,
    outcome:       dict,
    horse_a_name:  str,
    horse_b_name:  str,
    place_price_a: float,
    place_price_b: float,
    std_places:    int,
    cons_places:   int,
    initial_stake: float = 10.0,
) -> str | None:
    """
    Update streak tracker using actual Betfair place market prices.

    Called from betfair_main._paper_settle() after each race settles.
    Uses real exchange place prices captured at bet time — no SP estimation.

    place_price_a: Betfair place market back price for Pick 1 at bet time
    place_price_b: Betfair place market back price for Pick 2 at bet time
    std_places:    standard place terms for this race
    cons_places:   conservative place terms (standard + 1)
    outcome:       {std_win: bool, cons_win: bool}
    """
    if not _state:
        load_state()

    if not place_price_a or not place_price_b or place_price_a < 1.01 or place_price_b < 1.01:
        logger.warning(
            f"streak_tracker: skipping {race.get('off','?')} {race.get('course','?')} "
            f"— invalid place prices a={place_price_a} b={place_price_b}"
        )
        return None

    std_won  = outcome.get("std_win", False)
    cons_won = outcome.get("cons_win", False)

    # Combined odds = place_price_a * place_price_b (independent legs)
    # Same odds used for both std and cons since prices are from the place market
    # which already reflects the race's actual place terms
    std_odds  = combined_from_place_prices(place_price_a, place_price_b)
    cons_odds = std_odds  # place market prices already account for place terms

    return _run_update(
        race         = race,
        horse_a_name = horse_a_name,
        horse_b_name = horse_b_name,
        std_won      = std_won,
        cons_won     = cons_won,
        std_odds     = std_odds,
        cons_odds    = cons_odds,
        std_N        = std_places,
        cons_N       = cons_places,
        price_source = "Betfair",
        price_a      = place_price_a,
        price_b      = place_price_b,
        initial_stake = initial_stake,
    )


def update(race: dict, outcome: dict,
           horse_a: dict | None = None,
           horse_b: dict | None = None) -> str | None:
    """
    Legacy fallback: update using win SP with logit-shift odds estimation.
    Used when Betfair place prices are not available.
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

    std_N    = race.get("places", 3)
    cons_N   = race.get("cons_places", 4)
    std_won  = outcome.get("std_win", False)
    cons_won = outcome.get("cons_win", False)
    std_odds  = combined_top_n_dec(sp_a, sp_b, std_N)
    cons_odds = combined_top_n_dec(sp_a, sp_b, cons_N)

    return _run_update(
        race         = race,
        horse_a_name = top1.get("horse", "?"),
        horse_b_name = top2.get("horse", "?"),
        std_won      = std_won,
        cons_won     = cons_won,
        std_odds     = std_odds,
        cons_odds    = cons_odds,
        std_N        = std_N,
        cons_N       = cons_N,
        price_source = "SP estimate",
        price_a      = sp_a,
        price_b      = sp_b,
    )


def _run_update(
    race:         dict,
    horse_a_name: str,
    horse_b_name: str,
    std_won:      bool,
    cons_won:     bool,
    std_odds:     float,
    cons_odds:    float,
    std_N:        int,
    cons_N:       int,
    price_source: str,
    price_a:      float,
    price_b:      float,    
    initial_stake: float = 10.0,
) -> str:
    """Internal: apply results to both trackers and build Telegram message."""
    std_tracker  = _state["std"]
    cons_tracker = _state["cons"]

    std_prev_bal     = std_tracker["balance"]
    cons_prev_bal    = cons_tracker["balance"]
    std_prev_streak  = std_tracker["streak"]
    cons_prev_streak = cons_tracker["streak"]

    _apply_result(std_tracker,  std_won,  std_odds,  initial_stake)
    _apply_result(cons_tracker, cons_won, cons_odds, initial_stake)

    save_state()

    race_label = f"{race.get('off','?')} {race.get('course','?')}"
    lines = [
        "📈 <b>STREAK TRACKER</b>",
        f"<i>{race_label} — {horse_a_name} + {horse_b_name}</i>",
        f"<i>Prices: {price_a:.2f} x {price_b:.2f} ({price_source})</i>",
        "------------------------------",
    ]

    lines += _format_tracker_block(
        label       = f"📊 Standard (top-{std_N})",
        tracker     = std_tracker,
        prev_bal    = std_prev_bal,
        prev_streak = std_prev_streak,
        won         = std_won,
        odds        = std_odds,
    )

    lines.append("")

    lines += _format_tracker_block(
        label       = f"🛡️ Conservative (top-{cons_N})",
        tracker     = cons_tracker,
        prev_bal    = cons_prev_bal,
        prev_streak = cons_prev_streak,
        won         = cons_won,
        odds        = cons_odds,
    )

    lines.append("------------------------------")
    return "\n".join(lines)


def _apply_result(tracker: dict, won: bool, odds: float,
                  initial_stake: float = 10.0) -> None:
    stake = tracker["balance"]
    if won:
        profit = stake * (odds - 1.0)
        # Only reinvest half the profit — bank the other half
        tracker["balance"]     = round(tracker["balance"] + profit * 0.5, 2)
        tracker["streak"]      += 1
        tracker["best_streak"] = max(tracker["best_streak"], tracker["streak"])
        tracker["peak"]        = max(tracker["peak"], tracker["balance"])
    else:
        tracker["balance"] = initial_stake
        tracker["streak"]  = 0


def _format_tracker_block(
    label:       str,
    tracker:     dict,
    prev_bal:    float,
    prev_streak: int,
    won:         bool,
    odds:        float,
) -> list:
    lines = [label]
    if won:
        profit    = round(prev_bal * (odds - 1.0), 2)
        reinvest  = round(profit * 0.5, 2)
        banked    = round(profit * 0.5, 2)
        new_bal   = tracker["balance"]
        lines.append(
            f"  ✅ Win streak: {tracker['streak']}  |  "
            f"£{prev_bal:.2f} → £{new_bal:.2f} (+£{reinvest:.2f} reinvested, £{banked:.2f} banked)"
        )
        lines.append(f"  Combined odds: {odds:.2f}")
    else:
        lines.append(
            f"  ❌ Reset (streak was {prev_streak})  |  "
            f"£{prev_bal:.2f} → £{INITIAL_STAKE:.2f}"
        )
        lines.append(f"  Would have been: {odds:.2f}")
    lines.append(
        f"  Best today: {tracker['best_streak']} wins  |  Peak: £{tracker['peak']:.2f}"
    )
    return lines


# ── End-of-day summary ────────────────────────────────────────────────────────

def get_eod_summary() -> str:
    if not _state:
        load_state()

    std  = _state.get("std",  {})
    cons = _state.get("cons", {})

    lines = [
        "------------------------------",
        "📈 <b>Streak Tracker</b>",
        f"📊 Standard      — best: {std.get('best_streak', 0)} wins  |  peak: £{std.get('peak', INITIAL_STAKE):.2f}  |  closing: £{std.get('balance', INITIAL_STAKE):.2f}",
        f"🛡️ Conservative  — best: {cons.get('best_streak', 0)} wins  |  peak: £{cons.get('peak', INITIAL_STAKE):.2f}  |  closing: £{cons.get('balance', INITIAL_STAKE):.2f}",
    ]
    return "\n".join(lines)


# Load on import
load_state()
