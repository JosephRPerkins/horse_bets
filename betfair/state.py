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
