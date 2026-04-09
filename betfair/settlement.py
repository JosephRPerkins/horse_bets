"""
betfair/settlement.py

Settlement via Racing API result lookup.

After each race: wait until race_off + 15 minutes, then poll the Racing API
for the result. Match each bet's horse name against the result runners to
determine finishing position. No balance-log inference — win/loss is read
directly from the result.

Each race runs in its own daemon thread — no race blocks another.

Tier tracker is called after each settlement to log the outcome against the
confidence tier. This feeds the live validation report and EOD summary.
"""

import logging
import time
import threading
import requests
from datetime import datetime, timezone, timedelta

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config

from .api     import get_balance, _norm_horse, COMMISSION
from .state   import save
from .notify  import send

logger         = logging.getLogger("betfair.settlement")
SETTLE_WAIT_M  = 15     # minutes after race off before first result poll
POLL_INTERVAL  = 120    # seconds between result polls
MAX_POLLS      = 10     # 10 × 2 min = 20 minutes of polling


def _fetch_result(race_id: str) -> dict | None:
    """Fetch race result from the Racing API. Returns None if not available."""
    url  = f"{config.RACING_API_BASE_URL}/results/{race_id}"
    auth = (config.RACING_API_USERNAME, config.RACING_API_PASSWORD)
    try:
        r = requests.get(url, auth=auth, timeout=15)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (404, 422):
            return None
        logger.warning(f"_fetch_result HTTP {r.status_code} for {race_id}")
    except Exception as e:
        logger.error(f"_fetch_result error: {e}")
    return None


def _get_finish_pos(result: dict, horse_name: str) -> int | None:
    """Return finishing position (int) for horse_name in a result dict, or None."""
    norm = _norm_horse(horse_name)
    for r in result.get("runners", []):
        bf = _norm_horse(r.get("horse", ""))
        if norm == bf or norm in bf or bf in norm:
            try:
                pos = r.get("position", "")
                return int(pos) if str(pos).strip().isdigit() else None
            except (TypeError, ValueError):
                return None
    return None


def _log_to_tier_tracker(
    race_id:    str,
    race_label: str,
    race:       dict,
    bets:       list,
    results:    list,
    places:     int,
):
    """
    Log this race outcome to the tier tracker.

    Called after settlement completes with real results. Silently skips if
    tier tracker is unavailable or the race dict is missing tier information
    — we never want a tracker failure to affect settlement.

    race:    the race metadata dict passed through from the scheduler/betfair_main
             (must contain 'tier', 'course', 'off', and optionally 'tsr_solo')
    bets:    the original bet list [{horse, price, stake, ...}]
    results: the settled results list [(bet, won, pnl, detail), ...]
    places:  place terms for this race (used to determine what counted as a win)
    """
    try:
        from utils.tier_tracker import log_result

        tier     = race.get("tier")
        course   = race.get("course", "?")
        off      = race.get("off", "?")
        tsr_solo = race.get("tsr_solo", False)

        if tier is None:
            logger.debug("tier_tracker: no tier on race dict — skipping log")
            return

        # Match bets to their settled outcomes
        # bets[0] = pick1, bets[1] = pick2 (order preserved from placement)
        pick1_name = bets[0].get("horse", "?") if len(bets) > 0 else "?"
        pick2_name = bets[1].get("horse", "?") if len(bets) > 1 else "?"

        # win1/win2: True if the pick won (finished 1st — exchange is back bet)
        # We use the settled result directly rather than re-checking position
        win1 = results[0][1] if len(results) > 0 else False
        win2 = results[1][1] if len(results) > 1 else False

        log_result(
            race_id  = race_id,
            tier     = tier,
            course   = course,
            off      = off,
            pick1    = pick1_name,
            pick2    = pick2_name,
            win1     = win1,
            win2     = win2,
            places   = places,
            tsr_solo = tsr_solo,
        )

    except Exception as e:
        # Never let tracker errors bubble up into settlement
        logger.error(f"tier_tracker log failed for {race_label}: {e}")


def settle_race(placement_ts: str, race_id: str, race_label: str,
                race_off_iso: str, balance_before: float,
                balance_after_placement: float,
                bets: list, state: dict,
                race: dict = None, places: int = 1):
    """
    Daemon thread per live race. Waits until race_off + SETTLE_WAIT_M, then
    polls the Racing API for the result and calculates P&L from finishing
    positions.

    bets:   list of {horse, price, stake, potential_win_credit, bet_id}
    race:   full race metadata dict (needed for tier tracker logging).
            Optional for backwards compatibility — tracker is skipped if None.
    places: place terms for this race (needed for tier tracker context).
    """
    logger.info(f"Settlement thread started: {race_label}")

    try:
        race_off = datetime.fromisoformat(race_off_iso)
        if race_off.tzinfo is None:
            race_off = race_off.replace(tzinfo=timezone.utc)
    except Exception:
        race_off = datetime.now(timezone.utc)

    settle_after = race_off + timedelta(minutes=SETTLE_WAIT_M)
    now          = datetime.now(timezone.utc)
    if settle_after > now:
        wait_s = (settle_after - now).total_seconds()
        logger.info(f"{race_label}: waiting {wait_s:.0f}s to settle")
        time.sleep(wait_s)

    # Poll Racing API until results are available
    result = None
    for attempt in range(MAX_POLLS):
        result = _fetch_result(race_id)
        if result and any(
            str(r.get("position", "")).strip().isdigit()
            for r in result.get("runners", [])
        ):
            logger.info(f"{race_label}: result available after {attempt+1} poll(s)")
            break
        if attempt < MAX_POLLS - 1:
            logger.debug(f"{race_label}: result not ready, retrying in {POLL_INTERVAL}s")
            time.sleep(POLL_INTERVAL)

    if not result:
        logger.warning(f"{race_label}: no result found after polling — falling back to balance check")
        _settle_fallback(race_label, bets, state)
        return

    # ── Determine outcome for each bet from result ────────────────────────────
    total_pnl = 0.0
    results   = []

    for bet in bets:
        horse  = bet.get("horse", "?")
        price  = bet.get("price", 1.0)
        stake  = bet.get("stake", 0.0)
        pos    = _get_finish_pos(result, horse)

        if pos == 1:
            profit = round(stake * (price - 1) * (1 - COMMISSION), 2)
            total_pnl += profit
            results.append((bet, True, profit, f"1st (+£{profit:.2f})"))
        elif pos is not None:
            total_pnl -= stake
            ordinal = (
                "2nd" if pos == 2 else "3rd" if pos == 3 else f"{pos}th"
            )
            results.append((bet, False, -stake, f"{ordinal} (-£{stake:.2f})"))
        else:
            # Not found in result or NR
            total_pnl -= stake
            results.append((bet, False, -stake, f"NR/unplaced (-£{stake:.2f})"))

    total_pnl = round(total_pnl, 2)

    # ── Log to tier tracker ───────────────────────────────────────────────────
    # Done immediately after results are known, before state update or
    # notification — so the data is recorded even if something fails downstream.
    if race is not None:
        _log_to_tier_tracker(
            race_id    = race_id,
            race_label = race_label,
            race       = race,
            bets       = bets,
            results    = results,
            places     = places,
        )

    # ── Update daily state ────────────────────────────────────────────────────
    state["daily_pnl"] = round(state.get("daily_pnl", 0.0) + total_pnl, 2)
    state["daily_bets"].append({
        "race":      race_label,
        "total_pnl": total_pnl,
    })
    save(state)

    # ── Notification ──────────────────────────────────────────────────────────
    icon  = "✅" if total_pnl > 0 else ("➖" if total_pnl == 0 else "❌")
    lines = [
        f"{icon} <b>SETTLED — {race_label}</b>",
        "──────────────────────────────",
    ]
    for bet, won, pnl, detail in results:
        b_icon = "✅" if won else "❌"
        lines.append(
            f"{b_icon} {bet['horse']} @ {bet['price']} — "
            f"{'WON' if won else 'LOST'} {detail}"
        )

    current_balance = get_balance()
    full_diff       = round(current_balance - balance_before, 2)
    sign_full       = "+" if full_diff >= 0 else ""
    sign_day        = "+" if state["daily_pnl"] >= 0 else ""
    lines += [
        "──────────────────────────────",
        f"Balance: £{balance_before:.2f} → £{current_balance:.2f} "
        f"({sign_full}£{full_diff:.2f})",
        f"Race P&L: {'+' if total_pnl >= 0 else ''}£{total_pnl:.2f} | "
        f"Day P&L: {sign_day}£{state['daily_pnl']:.2f}",
    ]

    send("\n".join(lines))
    logger.info(f"Settled {race_label}: P&L {'+' if total_pnl>=0 else ''}£{total_pnl:.2f}")


def _settle_fallback(race_label: str, bets: list, state: dict):
    """
    Called when Racing API result is unavailable after polling.
    Marks all bets as unknown and notifies — does NOT attempt balance inference.
    Note: tier tracker is NOT updated on fallback since we have no confirmed result.
    """
    lines = [
        f"⚠️ <b>SETTLE FAILED — {race_label}</b>",
        "──────────────────────────────",
        "Racing API result not available. Outcome unknown.",
    ]
    for bet in bets:
        lines.append(f"  • {bet['horse']} @ {bet['price']} — £{bet['stake']:.2f} staked")
    lines.append("──────────────────────────────")
    lines.append("Check Betfair account manually and update P&L if needed.")
    send("\n".join(lines))
    logger.error(f"Settlement fallback for {race_label} — {len(bets)} bet(s) unresolved")
