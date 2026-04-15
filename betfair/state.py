"""
betfair/state.py
Persistent bot state — mode (paper/live), daily P&L, notifications mute, pause flag.

Winnings-driven staking:
  cumulative_profit tracks net profit since bot started (or was reset).
  Stakes are determined by cumulative_profit, not total Betfair balance.
  This means initial deposit never compounds — only actual winnings scale stakes.
  profit_milestone tracks the last £50 threshold crossed for notifications.
"""

import json
import logging
import os
from datetime import date

logger = logging.getLogger("betfair.state")

STATE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "data", "betfair_state.json"
)

# Milestone notification every £50 of cumulative profit
PROFIT_MILESTONE_INTERVAL = 50.0


def _empty() -> dict:
    return {
        "last_date":           "",
        # Betting mode: "paper" (default — safe) or "live"
        "mode":                "paper",
        # Notifications mute
        "muted":               False,
        # Betting pause (affects both modes)
        "betting_paused":      False,
        # Live bet tracking
        "daily_pnl":           0.0,
        "daily_bets":          [],
        # Paper bet tracking (separate so we can compare)
        "paper_daily_pnl":     0.0,
        "paper_daily_bets":    [],
        # Winnings-driven staking — persists across days
        # Stakes grow only from net profit, not from initial deposit
        "cumulative_profit":   0.0,
        "profit_milestone":    0.0,   # last £50 threshold notified
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
    """Reset daily counters. Cumulative profit and milestone persist."""
    state["last_date"]        = date.today().strftime("%Y-%m-%d")
    state["daily_pnl"]        = 0.0
    state["daily_bets"]       = []
    state["paper_daily_pnl"]  = 0.0
    state["paper_daily_bets"] = []
    # Note: cumulative_profit and profit_milestone are NOT reset daily
    save(state)
    return state


def update_cumulative_profit(state: dict, pnl: float) -> list[str]:
    """
    Add pnl to cumulative_profit and check for milestone notifications.

    Returns list of Telegram notification strings to send (empty if no milestone).
    Called after each race settles.
    """
    prev     = state.get("cumulative_profit", 0.0)
    updated  = round(prev + pnl, 2)
    state["cumulative_profit"] = updated
    save(state)

    alerts = []

    # Check if we've crossed a new £50 milestone
    milestone = state.get("profit_milestone", 0.0)
    interval  = PROFIT_MILESTONE_INTERVAL

    if updated > 0 and updated >= milestone + interval:
        # Calculate which milestone we've just crossed
        new_milestone = (updated // interval) * interval
        state["profit_milestone"] = new_milestone
        save(state)
        alerts.append(
            f"🏆 <b>Profit milestone: +£{new_milestone:.0f}</b>\n"
            f"Cumulative profit: £{updated:.2f}\n"
            f"Next milestone: £{new_milestone + interval:.0f}"
        )
    elif updated < 0 and milestone > 0:
        # Profit has dropped back below a milestone — notify once
        new_milestone = max(0.0, (updated // interval) * interval)
        if new_milestone < milestone:
            state["profit_milestone"] = new_milestone
            save(state)
            alerts.append(
                f"📉 <b>Profit dropped below £{milestone:.0f}</b>\n"
                f"Cumulative profit: £{updated:.2f}"
            )

    return alerts
