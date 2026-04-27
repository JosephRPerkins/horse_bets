"""
betfair/settlement.py

Settlement via Racing API result lookup.

After each race: wait until race_off + 15 minutes, then poll the Racing API
for the result. Match each bet's horse name against the result runners to
determine finishing position.

Handles both win and place bets:
  - Win bets: pays out if horse finishes 1st
  - Place bets: pays out if horse finishes within place terms derived from
    the ACTUAL number of runners in the result (not the pre-race card size)

BSP bets:
  - Bets placed as MARKET_ON_CLOSE have price=0 at placement time.
  - After the race, get_bsp_matched_price() polls listCurrentOrders to
    retrieve the actual matched BSP price before calculating returns.
  - Falls back to Racing API sp_dec if Betfair order lookup fails.

Cumulative profit is updated with combined win + place P&L so tier scaling
reflects total money made. Streak tracker is called after each settlement.

Each race runs in its own daemon thread — no race blocks another.

Key fix: place terms are calculated from result runner count, not card field
size. Non-runners reduce the field and therefore the place terms on Betfair.
"""

import logging
import time
import requests
from datetime import datetime, timezone, timedelta

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config

from .api     import get_balance, get_bsp_matched_price, _norm_horse, COMMISSION
from .state   import save, update_cumulative_profit, update_tier_profit, tier_profit_summary
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


def _get_sp_from_result(result: dict, horse_name: str) -> float | None:
    """Extract Racing API sp_dec for a horse — used as BSP fallback."""
    norm = _norm_horse(horse_name)
    for r in result.get("runners", []):
        bf = _norm_horse(r.get("horse", ""))
        if norm == bf or norm in bf or bf in norm:
            try:
                return float(r.get("sp_dec") or 0) or None
            except (TypeError, ValueError):
                return None
    return None


def _result_place_terms(result: dict) -> tuple[int, int]:
    """
    Calculate standard and conservative place terms from the ACTUAL
    number of runners in the result (after non-runners removed).
    This matches what Betfair uses for place market settlement.

    Returns (std_places, cons_places)
    """
    from predict import place_terms
    runners = result.get("runners") or []
    n = len([r for r in runners if r.get("horse")])
    if n == 0:
        n = len(runners)
    std  = place_terms(n) if n > 0 else 1
    cons = min(std + 1, max(n - 1, 1))
    return std, cons


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


def _next_tier_threshold(profit: float, tier: int = 0) -> float:
    try:
        from betfair.strategy import next_tier_threshold
        return next_tier_threshold(profit, tier)
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

    bets:        win market bets [{horse, price, stake, bet_id, bsp, ...}]
    place_bets:  place market bets [{horse, price, stake, cons_places}] or None
    cons_places: conservative place terms fallback (if result unavailable)
    race:        full race metadata dict for tier tracker
    places:      standard place terms fallback (if result unavailable)

    BSP bets have price=0 at placement. After result is fetched, we poll
    get_bsp_matched_price() for the actual matched price. Falls back to
    Racing API sp_dec if the Betfair order lookup fails.
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

    # ── Place terms from ACTUAL result runners ────────────────────────────────
    result_std_places, result_cons_places = _result_place_terms(result)
    logger.info(
        f"{race_label}: result runners={len(result.get('runners', []))} "
        f"std_places={result_std_places} cons_places={result_cons_places}"
    )

    # ── Win bet settlement ────────────────────────────────────────────────────
    win_pnl = 0.0
    results = []

    for bet in bets:
        horse   = bet.get("horse", "?")
        stake   = bet.get("stake", 0.0)
        is_bsp  = bet.get("bsp", False)
        bet_id  = bet.get("bet_id", "")
        pos     = _get_finish_pos(result, horse)

        # ── Resolve BSP price ─────────────────────────────────────────────────
        # BSP bets have price=0 at placement. Poll Betfair for matched price.
        # Falls back to Racing API sp_dec if lookup fails or returns None.
        if is_bsp:
            price = None
            if bet_id:
                price = get_bsp_matched_price(bet_id)
            if not price:
                price = _get_sp_from_result(result, horse)
            if not price:
                price = 2.0  # last resort fallback
                logger.warning(f"{race_label}: BSP price unavailable for {horse}, using 2.0")
            else:
                logger.info(f"{race_label}: BSP {horse} settled @ {price:.2f}")
        else:
            price = bet.get("price", 1.0)

        if pos == 1:
            profit = round(stake * (price - 1) * (1 - COMMISSION), 2)
            win_pnl += profit
            results.append((bet, True, profit, price, f"1st (+£{profit:.2f})"))
        elif pos is not None:
            win_pnl -= stake
            ordinal = "2nd" if pos==2 else "3rd" if pos==3 else f"{pos}th"
            results.append((bet, False, -stake, price, f"{ordinal} (-£{stake:.2f})"))
        else:
            win_pnl -= stake
            results.append((bet, False, -stake, price, f"NR/unplaced (-£{stake:.2f})"))

    win_pnl = round(win_pnl, 2)

    # ── Place bet settlement ──────────────────────────────────────────────────
    place_pnl     = 0.0
    place_results = []
    std_win       = False
    cons_win      = False

    if place_bets:
        picks_std  = []
        picks_cons = []

        for bet in place_bets:
            horse    = bet.get("horse", "?")
            price    = bet.get("price", 1.0)
            stake    = bet.get("stake", 0.0)
            pos      = _get_finish_pos(result, horse)
            if pos is not None and pos <= result_std_places:
                profit = round(stake * (price - 1) * (1 - COMMISSION), 2)
                place_pnl += profit
                picks_cons.append(True)
                place_results.append((bet, True, profit,
                                      f"PLACED top {result_std_places} (+£{profit:.2f})"))
            else:
                place_pnl -= stake
                picks_cons.append(False)
                pos_s = f"{pos}th" if pos else "NR/inc"
                place_results.append((bet, False, -stake,
                                      f"UNPLACED {pos_s} (-£{stake:.2f})"))
            picks_std.append(pos is not None and pos <= result_std_places)

        std_win   = len(picks_std)  >= 2 and all(picks_std[:2])
        cons_win  = len(picks_cons) >= 2 and all(picks_cons[:2])
        place_pnl = round(place_pnl, 2)

    # ── Combined P&L ─────────────────────────────────────────────────────────
    combined_pnl = round(win_pnl + place_pnl, 2)

    # ── Tier tracker ─────────────────────────────────────────────────────────
    if race is not None:
        _log_to_tier_tracker(race_id, race_label, race, bets, results,
                             result_std_places)

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

    milestone_alerts = update_cumulative_profit(state, combined_pnl)
    for alert in milestone_alerts:
        send(alert)

    # ── Per-tier profit update ────────────────────────────────────────────────
    race_tier = (race or {}).get("tier")
    if race_tier is not None:
        tier_alerts = update_tier_profit(state, race_tier, combined_pnl)
        for alert in tier_alerts:
            send(alert)

    save(state)

    # ── Circuit breaker — read REAL Betfair balance ───────────────────────────
    try:
        real_balance = get_balance()
        history = state.get("profit_history", [])
        history.append(round(real_balance, 2))
        if len(history) > 10:
            history = history[-10:]
        state["profit_history"] = history
        save(state)

        from betfair.state import check_circuit_breaker
        saved_profit = state.get("cumulative_profit", 0.0)
        state["cumulative_profit"] = real_balance
        circuit_alert = check_circuit_breaker(state)
        state["cumulative_profit"] = saved_profit
        if circuit_alert:
            send(circuit_alert)
    except Exception as e:
        logger.error(f"Circuit breaker check failed: {e}")

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
                std_places    = result_std_places,
                cons_places   = result_cons_places,
                initial_stake = i_stake,
            )
        elif len(bets) >= 2:
            # Use resolved price (index 3) from results tuples
            price_a = results[0][3] if len(results) > 0 else bets[0].get("price", 2.0)
            price_b = results[1][3] if len(results) > 1 else bets[1].get("price", 2.0)
            horse_a = {"horse": bets[0]["horse"], "sp_dec": price_a}
            horse_b = {"horse": bets[1]["horse"], "sp_dec": price_b}
            race_wp = {**(race or {}),
                       "places":      result_std_places,
                       "cons_places": result_cons_places}
            streak_msg = streak_sp(race_wp, outcome,
                                   horse_a=horse_a, horse_b=horse_b)
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

    for bet, won, pnl, price, detail in results:
        b_icon = "✅" if won else "❌"
        label  = bet.get("label", "")
        is_bsp = bet.get("bsp", False)
        bsp_tag = " [BSP]" if is_bsp else ""
        lines.append(
            f"{b_icon} {label} {bet['horse']} @ {price:.2f}{bsp_tag} — {detail}"
        )

    if place_results:
        lines.append("------------------------------")
        lines.append(
            f"📍 <b>Place bets</b> (top {result_std_places} std / "
            f"top {result_cons_places} cons)"
        )
        for bet, won, pnl, detail in place_results:
            b_icon = "✅" if won else "❌"
            lines.append(f"{b_icon} 📍 {bet['horse']} @ {bet['price']:.2f} — {detail}")

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
    lines.append(
        f"Balance:         £{balance_before:.2f} → £{current_balance:.2f} "
        f"({bal_sign}£{bal_diff:.2f})"
    )
    lines.append(f"Day Win P&L:     {day_sign}£{state['daily_pnl']:.2f}")
    lines.append(f"Cumulative P&L:  {cum_sign}£{cum_profit:.2f}")
    race_tier = (race or {}).get("tier")
    if race_tier is not None:
        tier_profit = state.get("tier_profit", {}).get(str(race_tier), 0.0)
        tier_next   = _next_tier_threshold(tier_profit, race_tier)
        from betfair.strategy import get_stake
        tier_stake  = get_stake(tier_profit, race_tier)
        tier_names  = {4: "ELITE", 3: "STRONG", 2: "GOOD"}
        tname       = tier_names.get(race_tier, f"Tier {race_tier}")
        t_sign      = "+" if tier_profit >= 0 else ""
        lines.append(
            f"{tname} pot:       {t_sign}£{tier_profit:.2f}  "
            f"(stake £{tier_stake:.0f}, next @ £{tier_next:.0f})"
        )

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
        price   = bet.get("price") or 0
        is_bsp  = bet.get("bsp", False)
        bsp_tag = " [BSP]" if is_bsp else ""
        price_s = f"@ {price:.2f}" if price else "@ BSP"
        lines.append(
            f"  • {bet['horse']} {price_s}{bsp_tag} — £{bet['stake']:.2f} win stake"
        )
    if place_bets:
        for bet in place_bets:
            lines.append(
                f"  • {bet['horse']} @ {bet['price']:.2f} — £{bet['stake']:.2f} place stake"
            )
    lines.append("------------------------------")
    lines.append("Check Betfair account manually and update P&L if needed.")
    send("\n".join(lines))
    logger.error(
        f"Settlement fallback for {race_label} — "
        f"{len(bets)} win + {len(place_bets or [])} place bets unresolved"
    )
