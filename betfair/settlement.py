"""
betfair/settlement.py

Settlement via Racing API result lookup.

After each race: wait until race_off + 15 minutes, then poll the Racing API
for the result. Match each bet's horse name against the result runners to
determine finishing position.

Handles both win and place bets:
  - Win bets: pays out if horse finishes 1st
  - Place bets: pays out if horse finishes within cons_places

Cumulative profit is updated with combined win + place P&L so tier scaling
reflects total money made. Streak tracker is called after each settlement.

Each race runs in its own daemon thread — no race blocks another.
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
from .state   import save, update_cumulative_profit
from .notify  import send

logger         = logging.getLogger("betfair.settlement")
SETTLE_WAIT_M  = 15
POLL_INTERVAL  = 120
MAX_POLLS      = 10


def _fetch_result(race_id: str) -> dict | None:
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


def _log_to_tier_tracker(race_id, race_label, race, bets, results, places):
    try:
        from utils.tier_tracker import log_result
        tier     = race.get("tier")
        course   = race.get("course", "?")
        off      = race.get("off", "?")
        tsr_solo = race.get("tsr_solo", False)
        if tier is None:
            return
        pick1_name = bets[0].get("horse", "?") if len(bets) > 0 else "?"
        pick2_name = bets[1].get("horse", "?") if len(bets) > 1 else "?"
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
        logger.error(f"tier_tracker log failed for {race_label}: {e}")


def _next_tier_threshold(profit: float) -> float:
    try:
        from betfair.strategy import STAKE_TIERS
        for min_profit, _, _ in STAKE_TIERS:
            if profit < min_profit:
                return float(min_profit)
        return float(STAKE_TIERS[-1][0])
    except Exception:
        return 0.0


def settle_race(placement_ts: str, race_id: str, race_label: str,
                race_off_iso: str, balance_before: float,
                balance_after_placement: float,
                bets: list, state: dict,
                race: dict = None, places: int = 1,
                place_bets: list = None,
                cons_places: int = None):
    """
    Daemon thread per live race.

    bets:        win market bets [{horse, price, stake, ...}]
    place_bets:  place market bets [{horse, price, stake, cons_places}] or None
    cons_places: conservative place terms (standard + 1) — used if not in bet dict
    race:        full race metadata dict for tier tracker
    places:      standard place terms for tier tracker context
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
        logger.warning(f"{race_label}: no result found after polling")
        _settle_fallback(race_label, bets, place_bets, state)
        return

    # ── Win bet settlement ────────────────────────────────────────────────────
    win_pnl = 0.0
    results = []

    for bet in bets:
        horse = bet.get("horse", "?")
        price = bet.get("price", 1.0)
        stake = bet.get("stake", 0.0)
        pos   = _get_finish_pos(result, horse)

        if pos == 1:
            profit = round(stake * (price - 1) * (1 - COMMISSION), 2)
            win_pnl += profit
            results.append((bet, True, profit, f"1st (+£{profit:.2f})"))
        elif pos is not None:
            win_pnl -= stake
            ordinal = "2nd" if pos==2 else "3rd" if pos==3 else f"{pos}th"
            results.append((bet, False, -stake, f"{ordinal} (-£{stake:.2f})"))
        else:
            win_pnl -= stake
            results.append((bet, False, -stake, f"NR/unplaced (-£{stake:.2f})"))

    win_pnl = round(win_pnl, 2)

    # ── Place bet settlement ──────────────────────────────────────────────────
    place_pnl    = 0.0
    place_results = []
    std_win      = False
    cons_win     = False

    if place_bets:
        std_places_n  = places
        picks_std     = []
        picks_cons    = []

        for bet in place_bets:
            horse    = bet.get("horse", "?")
            price    = bet.get("price", 1.0)
            stake    = bet.get("stake", 0.0)
            c_places = bet.get("cons_places") or cons_places or (places + 1)
            pos      = _get_finish_pos(result, horse)

            if pos is not None and pos <= c_places:
                profit = round(stake * (price - 1) * (1 - COMMISSION), 2)
                place_pnl += profit
                picks_cons.append(True)
                place_results.append((bet, True, profit, f"PLACED top {c_places} (+£{profit:.2f})"))
            else:
                place_pnl -= stake
                picks_cons.append(False)
                pos_s = f"{pos}th" if pos else "NR/inc"
                place_results.append((bet, False, -stake, f"UNPLACED {pos_s} (-£{stake:.2f})"))

            picks_std.append(pos is not None and pos <= std_places_n)

        std_win  = len(picks_std)  >= 2 and all(picks_std[:2])
        cons_win = len(picks_cons) >= 2 and all(picks_cons[:2])
        place_pnl = round(place_pnl, 2)

    # ── Combined P&L ─────────────────────────────────────────────────────────
    combined_pnl = round(win_pnl + place_pnl, 2)

    # ── Tier tracker ─────────────────────────────────────────────────────────
    if race is not None:
        _log_to_tier_tracker(race_id, race_label, race, bets, results, places)

    # ── Update state ──────────────────────────────────────────────────────────
    state["daily_pnl"] = round(state.get("daily_pnl", 0.0) + win_pnl, 2)
    state["daily_bets"].append({
        "race":      race_label,
        "total_pnl": win_pnl,
    })
    if place_bets:
        state["paper_place_pnl"] = round(
            state.get("paper_place_pnl", 0.0) + place_pnl, 2
        )

    # Update cumulative profit with combined win + place
    milestone_alerts = update_cumulative_profit(state, combined_pnl)
    for alert in milestone_alerts:
        send(alert)
    save(state)

    # ── Circuit breaker check ─────────────────────────────────────────────────
    from betfair.state import check_circuit_breaker
    circuit_alert = check_circuit_breaker(state)
    if circuit_alert:
        send(circuit_alert)
                  
    cum_profit = state.get("cumulative_profit", 0.0)

    # ── Streak tracker ────────────────────────────────────────────────────────
    try:
        from notifications.streak_tracker import update_from_betfair, update as streak_sp
        from betfair.strategy import get_place_stake
        outcome    = {"std_win": std_win, "cons_win": cons_win}
        streak_msg = None
        i_stake    = get_place_stake(cum_profit)

        if place_bets and len(place_bets) >= 2:
            streak_msg = update_from_betfair(
                race          = race or {},
                outcome       = outcome,
                horse_a_name  = place_bets[0]["horse"],
                horse_b_name  = place_bets[1]["horse"],
                place_price_a = place_bets[0]["price"],
                place_price_b = place_bets[1]["price"],
                std_places    = places,
                cons_places   = cons_places or (places + 1),
                initial_stake = i_stake,
            )
        elif len(bets) >= 2:
            horse_a = {"horse": bets[0]["horse"], "sp_dec": bets[0]["price"]}
            horse_b = {"horse": bets[1]["horse"], "sp_dec": bets[1]["price"]}
            race_wp = {**(race or {}), "places": places, "cons_places": cons_places or (places + 1)}
            streak_msg = streak_sp(race_wp, outcome, horse_a=horse_a, horse_b=horse_b)
        if streak_msg:
            send(streak_msg)
    except Exception as e:
        logger.error(f"streak_tracker failed for {race_label}: {e}")

    # ── Build notification ────────────────────────────────────────────────────
    icon  = "✅" if combined_pnl > 0 else ("➖" if combined_pnl == 0 else "❌")
    lines = [
        f"{icon} <b>💰 SETTLED — {race_label}</b>",
        "------------------------------",
    ]

    # Win bets
    for bet, won, pnl, detail in results:
        b_icon = "✅" if won else "❌"
        label  = bet.get("label", "")
        lines.append(f"{b_icon} {label} {bet['horse']} @ {bet['price']} — {detail}")

    # Place bets
    if place_results:
        lines.append("------------------------------")
        lines.append("📍 <b>Place bets</b>")
        for bet, won, pnl, detail in place_results:
            b_icon = "✅" if won else "❌"
            lines.append(f"{b_icon} 📍 {bet['horse']} @ {bet['price']:.2f} — {detail}")

    # P&L summary
    current_balance = get_balance()
    bal_diff        = round(current_balance - balance_before, 2)
    bal_sign        = "+" if bal_diff >= 0 else ""
    win_sign        = "+" if win_pnl >= 0 else ""
    place_sign      = "+" if place_pnl >= 0 else ""
    comb_sign       = "+" if combined_pnl >= 0 else ""
    day_sign        = "+" if state["daily_pnl"] >= 0 else ""
    cum_sign        = "+" if cum_profit >= 0 else ""

    lines += ["------------------------------"]
    lines.append(f"Win P&L:         {win_sign}£{win_pnl:.2f}")
    if place_bets:
        lines.append(f"Place P&L:       {place_sign}£{place_pnl:.2f}")
        lines.append(f"Race Combined:   {comb_sign}£{combined_pnl:.2f}")
    lines.append(f"Balance:         £{balance_before:.2f} → £{current_balance:.2f} ({bal_sign}£{bal_diff:.2f})")
    lines.append(f"Day Win P&L:     {day_sign}£{state['daily_pnl']:.2f}")
    lines.append(f"Cumulative P&L:  {cum_sign}£{cum_profit:.2f}")
    lines.append(f"Next tier at:    £{_next_tier_threshold(cum_profit):.0f} profit")

    send("\n".join(lines))
    logger.info(
        f"Settled {race_label}: win {win_sign}£{win_pnl:.2f} "
        f"place {place_sign}£{place_pnl:.2f} combined {comb_sign}£{combined_pnl:.2f} "
        f"| cumulative £{cum_profit:.2f}"
    )


def _settle_fallback(race_label: str, bets: list,
                     place_bets: list, state: dict):
    lines = [
        f"⚠️ <b>💰 SETTLE FAILED — {race_label}</b>",
        "------------------------------",
        "Racing API result not available. Outcome unknown.",
    ]
    for bet in bets:
        lines.append(f"  • {bet['horse']} @ {bet['price']} — £{bet['stake']:.2f} win stake")
    if place_bets:
        for bet in place_bets:
            lines.append(f"  • {bet['horse']} @ {bet['price']:.2f} — £{bet['stake']:.2f} place stake")
    lines.append("------------------------------")
    lines.append("Check Betfair account manually and update P&L if needed.")
    send("\n".join(lines))
    logger.error(f"Settlement fallback for {race_label} — {len(bets)} win + {len(place_bets or [])} place bets unresolved")
