"""
betfair/state.py
Persistent bot state — mode (paper/live), daily P&L, notifications mute, pause flag.
"""

import json
import logging
import os
from datetime import date

logger = logging.getLogger("betfair.state")

STATE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "data", "betfair_state.json"
)


def _empty() -> dict:
    return {
        "last_date":         "",
        # Betting mode: "paper" (default — safe) or "live"
        "mode":              "paper",
        # Notifications mute
        "muted":             False,
        # Betting pause (affects both modes)
        "betting_paused":    False,
        # Live bet tracking
        "daily_pnl":         0.0,
        "daily_bets":        [],
        # Paper bet tracking (separate so we can compare)
        "paper_daily_pnl":   0.0,
        "paper_daily_bets":  [],
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
    state["last_date"]        = date.today().strftime("%Y-%m-%d")
    state["daily_pnl"]        = 0.0
    state["daily_bets"]       = []
    state["paper_daily_pnl"]  = 0.0
    state["paper_daily_bets"] = []
    save(state)
    return state
