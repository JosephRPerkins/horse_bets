"""
betfair/balance_log.py

Logs account balance every 15 seconds and tracks bet placement events.
Settlement infers race outcomes by comparing balance movements against
known potential win credits for each placed bet.

Entry types:
  balance    — periodic snapshot
  bet_placed — full bet context for inference
  settled    — credit claimed by a settlement thread (prevents double-counting)
"""

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone, timedelta

logger   = logging.getLogger("betfair.balance_log")
LOG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "data", "betfair_balance_log.json"
)
MAX_AGE_HOURS = 12
_lock         = threading.Lock()


def _load() -> list:
    if not os.path.exists(LOG_PATH):
        return []
    try:
        with open(LOG_PATH) as f:
            return json.load(f)
    except Exception:
        return []


def _save(entries: list):
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "w") as f:
        json.dump(entries, f, indent=2, default=str)


def _prune(entries: list) -> list:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=MAX_AGE_HOURS)).isoformat()
    return [e for e in entries if e.get("ts", "") >= cutoff]


def _append(entry: dict):
    with _lock:
        entries = _load()
        entries = _prune(entries)
        entry["ts"] = datetime.now(timezone.utc).isoformat()
        entries.append(entry)
        _save(entries)


# ── Public API ────────────────────────────────────────────────────────────────

def log_balance(balance: float):
    _append({"type": "balance", "balance": balance})


def log_bet_placed(race: dict, bets: list,
                   balance_before: float, balance_after: float) -> str:
    """
    Log bet placement. Returns the placement timestamp (used by settlement).
    bets: list of bet dicts with horse_name, type, price, size/size_matched.
    """
    details = []
    for bet in bets:
        matched    = bet.get("size_matched") or bet.get("size", 0)
        win_credit = round(matched * (bet["price"] - 1) * (1 - 0.05), 2)
        details.append({
            "bet_id":               str(bet.get("bet_id", "")),
            "type":                 bet["type"],
            "horse":                bet.get("horse_name", "?"),
            "price":                bet["price"],
            "stake":                matched,
            "potential_win_credit": win_credit,
        })

    ts = datetime.now(timezone.utc).isoformat()
    with _lock:
        entries = _load()
        entries = _prune(entries)
        entries.append({
            "ts":                      ts,
            "type":                    "bet_placed",
            "race_id":                 race.get("race_id", ""),
            "race":                    f"{race.get('off','?')} {race.get('course','?')}",
            "race_off_ts":             str(race.get("off_dt", "")),
            "balance_before":          balance_before,
            "balance_after_placement": balance_after,
            "bets":                    details,
        })
        _save(entries)

    logger.info(
        f"Logged bet: {race.get('off')} {race.get('course')} | "
        f"before £{balance_before:.2f} after £{balance_after:.2f}"
    )
    return ts


def log_settled(race_id: str, race_label: str, credit_claimed: float):
    _append({
        "type":           "settled",
        "race_id":        race_id,
        "race":           race_label,
        "credit_claimed": credit_claimed,
    })


def get_entries_after(ts_iso: str) -> list:
    with _lock:
        entries = _load()
    return [e for e in entries if e.get("ts", "") > ts_iso]


def get_all_bet_placed_entries() -> list:
    with _lock:
        entries = _load()
    return [e for e in entries if e.get("type") == "bet_placed"]


def get_claimed_credits_after(ts_iso: str, exclude_race_id: str) -> float:
    with _lock:
        entries = _load()
    total = 0.0
    for e in entries:
        if e.get("type") != "settled":           continue
        if e.get("ts", "") <= ts_iso:            continue
        if e.get("race_id") == exclude_race_id:  continue
        total += e.get("credit_claimed", 0.0)
    return round(total, 2)


# ── Background balance logger ─────────────────────────────────────────────────

def start_balance_logger(get_balance_fn, interval_s: int = 15):
    def _loop():
        while True:
            try:
                log_balance(get_balance_fn())
            except Exception as e:
                logger.error(f"Balance logger: {e}")
            time.sleep(interval_s)

    t = threading.Thread(target=_loop, daemon=True, name="BalanceLogger")
    t.start()
    logger.info(f"Balance logger started (every {interval_s}s)")
