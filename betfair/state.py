"""
betfair/state.py
Persistent bot state — mode (paper/live), daily P&L, notifications mute, pause flag.

Winnings-driven staking:
  cumulative_profit tracks net profit since bot started (or was reset).
  Stakes are determined by cumulative_profit, not total Betfair balance.
  This means initial deposit never compounds — only actual winnings scale stakes.
  profit_milestone tracks the last 50 threshold crossed for notifications.
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
        "profit_history":     [],
        "circuit_paused":     False,
        "streak_active":      False,
        "streak_stake":       2.0,
        "streak_daily_pnl":   0.0,
        "streak_daily_bets":  [],
        "streak_wins":        0,
        "streak_best":        0,
        "streak_peak_stake":  2.0,
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
    """Reset daily counters. Cumulative profit and milestone persist across days."""
    state["last_date"]        = date.today().strftime("%Y-%m-%d")
    state["daily_pnl"]        = 0.0
    state["daily_bets"]       = []
    state["paper_daily_pnl"]  = 0.0
    state["paper_daily_bets"] = []
    state["paper_place_pnl"]  = 0.0
    state["profit_history"] = []
    state["circuit_paused"] = False
    
    # ── Daily banking ─────────────────────────────────────────────────────────
    # Bank profit to nearest £50 floor, carry forward remainder.
    # If remainder is within £10 of the next £50 tier, scale back
    # to avoid immediately stepping up stakes on fragile profit.
    profit = state.get("cumulative_profit", 0.0)
    if profit > 0:
        banked  = (profit // 50) * 50
        carry   = round(profit - banked, 2)
        # Safety buffer: if profit is within £10 above a £50 boundary,
        # bank one tier less so carry forward isn't dangerously fragile
        if carry < 10 and banked >= 50:
            banked -= 50
            carry   = round(profit - banked, 2)
        state["banked_profit"]     = round(state.get("banked_profit", 0.0) + banked, 2)
        state["cumulative_profit"] = carry
        logger.info(f"Daily banking: banked £{banked:.0f}, carrying forward £{carry:.2f}")
        state["banked_profit"]     = round(state.get("banked_profit", 0.0) + banked, 2)
        state["cumulative_profit"] = carry
        logger.info(f"Daily banking: banked £{banked:.0f}, carrying forward £{carry:.2f}")

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
    Checks if cumulative profit has fallen significantly from a recent peak,
    crossing below a £100 checkpoint by at least £75.

    Conditions to trigger:
    1. Rolling window of last 10 races contains a peak
    2. Nearest £100 checkpoint below that peak has a gap of >= £75
    3. Current profit has fallen to or below that checkpoint

    Returns alert string if triggered, None otherwise.
    """
    profit  = state.get("cumulative_profit", 0.0)
    history = state.get("profit_history", [])

    history.append(round(profit, 2))
    if len(history) > 10:
        history = history[-10:]
    state["profit_history"] = history

    if len(history) < 2 or state.get("circuit_paused", False):
        return None

    peak = max(history)

    # Find the highest £100 checkpoint below the peak
    # where the gap from peak to checkpoint is >= £75
    checkpoint = (peak // 100) * 100
    while checkpoint >= 0:
        gap = peak - checkpoint
        if gap >= 75:
            # This is a checkpoint worth protecting
            if profit <= checkpoint:
                # Profit has fallen to or below this checkpoint — trigger
                state["circuit_paused"] = True
                state["betting_paused"] = True
                save(state)
                return (
                    f"🛑 <b>Circuit breaker triggered</b>\n"
                    f"Peak profit (last 10 races): £{peak:.2f}\n"
                    f"Current profit: £{profit:.2f}\n"
                    f"Dropped £{peak - profit:.2f} from peak, "
                    f"crossing £{checkpoint:.0f} checkpoint.\n"
                    f"All betting paused to protect capital.\n"
                    f"------------------------------\n"
                    f"Send /breaker to override and continue.\n"
                    f"Resets automatically at midnight."
                )
            break
        # Gap too small at this checkpoint — try next one down
        checkpoint -= 100

    return None
