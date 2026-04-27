"""
betfair/state.py
Persistent bot state — mode (paper/live), daily P&L, notifications mute, pause flag.

Balance-driven staking (live mode):
  At midnight, cumulative_profit is set from the real Betfair account balance.
  Everything above the nearest £50 floor (leaving £50-100 as betting pot) is
  ring-fenced in banked_profit and never rebetted.
  Stakes scale from the real betting pot, not paper P&L figures.

  Example: balance £125 -> bank £50, betting pot = £75 -> £4/horse
  Example: balance £200 -> bank £100, betting pot = £100 -> £4/horse
  Example: balance £395 -> bank £300, betting pot = £95 -> £2/horse

Circuit breaker:
  Percentage-based — triggers if profit drops below 50% of day's starting pot
  within a rolling 10-race window.
"""

import json
import logging
import os
from datetime import date

logger = logging.getLogger("betfair.state")

STATE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "data", "betfair_state.json"
)

PROFIT_MILESTONE_INTERVAL = 50.0


def _empty() -> dict:
    return {
        "last_date":           "",
        "mode":                "paper",
        "muted":               False,
        "betting_paused":      False,
        "daily_pnl":           0.0,
        "daily_bets":          [],
        "paper_daily_pnl":     0.0,
        "paper_daily_bets":    [],
        "cumulative_profit":   0.0,
        "profit_milestone":    0.0,
        "paper_place_pnl":     0.0,
        "banked_profit":       0.0,
        "day_start_pot":       0.0,
        "profit_history":      [],
        "circuit_paused":      False,
        "streak_active":       False,
        "streak_stake":        2.0,
        "streak_daily_pnl":    0.0,
        "streak_daily_bets":   [],
        "streak_wins":         0,
        "streak_best":         0,
        "streak_peak_stake":   2.0,
    }


def load() -> dict:
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH) as f:
                s = json.load(f)
            for k, v in _empty().items():
                s.setdefault(k, v)
            return s
        except Exception:
            pass
    return _empty()


def save(state: dict):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2, default=str)


def reset_daily(state: dict) -> dict:
    """
    Reset daily counters and recalculate betting pot from real Betfair balance.

    In live mode:
      - Reads real account balance
      - Ring-fences everything above nearest £50, leaving remainder as betting pot
      - Sets cumulative_profit to that real betting pot
      - Accumulates ring-fenced amount in banked_profit

    In paper mode:
      - Falls back to cumulative_profit banking logic
    """
    state["last_date"]        = date.today().strftime("%Y-%m-%d")
    state["daily_pnl"]        = 0.0
    state["daily_bets"]       = []
    state["paper_daily_pnl"]  = 0.0
    state["paper_daily_bets"] = []
    state["paper_place_pnl"]  = 0.0
    state["profit_history"]   = []
    state["circuit_paused"]   = False

    mode = state.get("mode", "paper")

    if mode == "live":
        # ── Live mode: bank from real Betfair balance ─────────────────────────
        try:
            from betfair.api import get_balance
            real_balance = get_balance()
            logger.info(f"Daily reset: real balance = £{real_balance:.2f}")

            # Ring-fence to nearest £50, leaving remainder as betting pot
            banked = (real_balance // 50) * 50
            pot    = round(real_balance - banked, 2)

            # If pot is less than £10, reduce banking by one tier
            # so we don't start with almost nothing to bet
            if pot < 10 and banked >= 50:
                banked -= 50
                pot     = round(real_balance - banked, 2)

            state["banked_profit"]     = round(state.get("banked_profit", 0.0) + banked, 2)
            state["cumulative_profit"] = pot
            state["day_start_pot"]     = pot
            logger.info(
                f"Daily banking (live): balance=£{real_balance:.2f} "
                f"banked=£{banked:.0f} pot=£{pot:.2f}"
            )

        except Exception as e:
            logger.error(f"Daily reset balance fetch failed: {e} — keeping existing profit")
            state["day_start_pot"] = state.get("cumulative_profit", 0.0)

    else:
        # ── Paper mode: bank from cumulative_profit as before ─────────────────
        profit = state.get("cumulative_profit", 0.0)
        if profit > 0:
            banked = (profit // 50) * 50
            carry  = round(profit - banked, 2)
            if carry < 10 and banked >= 50:
                banked -= 50
                carry   = round(profit - banked, 2)
            state["banked_profit"]     = round(state.get("banked_profit", 0.0) + banked, 2)
            state["cumulative_profit"] = carry
            state["day_start_pot"]     = carry
            logger.info(f"Daily banking (paper): banked £{banked:.0f}, carrying £{carry:.2f}")
        else:
            state["day_start_pot"] = 0.0

    # Reset streak daily counters — stake resets to current tier
    if state.get("streak_active", False):
        from betfair.strategy import get_place_stake
        new_stake = get_place_stake(state.get("cumulative_profit", 0.0))
        state["streak_stake"]      = new_stake
        state["streak_peak_stake"] = new_stake
    state["streak_daily_pnl"]  = 0.0
    state["streak_daily_bets"] = []
    state["streak_wins"]       = 0
    state["streak_best"]       = 0

    save(state)
    return state


def update_cumulative_profit(state: dict, pnl: float) -> list:
    """
    Add pnl to cumulative_profit and check for milestone notifications.
    Returns list of Telegram notification strings (empty if no milestone).
    Called after each race settles.
    """
    prev    = state.get("cumulative_profit", 0.0)
    updated = round(prev + pnl, 2)
    state["cumulative_profit"] = updated
    save(state)

    alerts    = []
    milestone = state.get("profit_milestone", 0.0)
    interval  = PROFIT_MILESTONE_INTERVAL

    if updated > 0 and updated >= milestone + interval:
        new_milestone = (updated // interval) * interval
        state["profit_milestone"] = new_milestone
        save(state)
        alerts.append(
            f"🏆 <b>Profit milestone: +£{new_milestone:.0f}</b>\n"
            f"Cumulative profit: £{updated:.2f}\n"
            f"Next milestone: £{new_milestone + interval:.0f}"
        )
    elif updated < 0 and milestone > 0:
        new_milestone = max(0.0, (updated // interval) * interval)
        if new_milestone < milestone:
            state["profit_milestone"] = new_milestone
            save(state)
            alerts.append(
                f"📉 <b>Profit dropped below £{milestone:.0f}</b>\n"
                f"Cumulative profit: £{updated:.2f}"
            )

    return alerts


def check_circuit_breaker(state: dict) -> str | None:
    """
    Called after every race settlement.
    Percentage-based — triggers if cumulative_profit drops below 50% of
    the day's starting pot within a rolling 10-race window.

    Example: day starts with £80 pot -> triggers if profit drops below £40.
    Example: day starts with £50 pot -> triggers if profit drops below £25.

    Returns alert string if triggered, None otherwise.
    """
    profit    = state.get("cumulative_profit", 0.0)
    start_pot = state.get("day_start_pot", 0.0)
    history   = state.get("profit_history", [])

    history.append(round(profit, 2))
    if len(history) > 10:
        history = history[-10:]
    state["profit_history"] = history

    if state.get("circuit_paused", False):
        return None

    # Need a meaningful starting pot to protect
    if start_pot < 20:
        return None

    threshold = round(start_pot * 0.5, 2)

    if profit <= threshold:
        state["circuit_paused"] = True
        state["betting_paused"] = True
        save(state)
        return (
            f"🛑 <b>Circuit breaker triggered</b>\n"
            f"Day started with: £{start_pot:.2f}\n"
            f"Current profit:   £{profit:.2f}\n"
            f"Dropped below 50% of starting pot (£{threshold:.2f}).\n"
            f"All betting paused to protect capital.\n"
            f"------------------------------\n"
            f"Send /breaker to override and continue.\n"
            f"Resets automatically at midnight."
        )

    return None
