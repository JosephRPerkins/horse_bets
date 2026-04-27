"""
betfair_main.py - horse_bets_v3 Betfair Exchange Bot

Runs independently of main.py. Reads today.json written by the main bot,
qualifies races from all tiers, and places (or simulates) bets on the
Betfair Exchange T-5 minutes before each race.

Modes (toggle via Telegram):
  /paper - simulated bets (default, safe). Finds real market + price,
           logs what would have been placed, settles from Racing API result.
           Paper ALWAYS runs in the background even in live mode.
  /live  - real bets placed on Betfair Exchange. Full balance-log settlement.

Staking: WINNINGS-DRIVEN (not balance-driven)
  Stakes grow only from cumulative net profit (win + place combined).
  At zero profit stakes stay at £2/horse regardless of account balance.
  Milestone notifications fire every £50 of cumulative profit.

Place bets (paper only):
  A place bet is simulated alongside every win bet using the live
  Betfair place market price. Stake scales with tier (same as win).
  Settled on conservative place terms (standard + 1).
  Tracked separately in Telegram output.

BSP fallback:
  When exchange liquidity is below the dynamic threshold for a horse's price,
  the bot falls back to a MARKET_ON_CLOSE (BSP) order rather than skipping.
  In paper mode, the Racing API SP is used as the BSP proxy at settlement.
  This ensures long-odds winners at thin markets are not missed.

Streak tracker:
  Uses actual Betfair place market prices captured at bet time.
  Tracks both standard and conservative compounding streaks.
  Fires a Telegram update after every race settlement via send().

Bet timing: T-5 minutes before race off.

Usage:
    python betfair_main.py
Background (managed by systemd):
    sudo systemctl restart horse-betfair
"""

import os
import sys
import json
import logging
import time
import threading
import requests
from datetime import datetime, timezone, timedelta, date

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date         import DateTrigger
from apscheduler.triggers.cron         import CronTrigger

import config
from betfair.api         import (
    get_client, get_balance, find_win_market, find_place_market,
    get_market_odds, find_selection_id, place_back, place_bsp,
    get_bsp_matched_price, _to_utc, _to_local_naive, COMMISSION,
)
from betfair.strategy    import (
    qualifies, get_stake, get_place_stake, pick_stakes,
    MIN_BACK_PRICE, MIN_LIQUIDITY, MIN_PICK1_PRICE, MIN_PICK2_PRICE,
    should_back_pick1, should_back_pick2, min_liquidity_for_price,
    next_tier_threshold, BET_TIERS,
)
from predict_v2 import TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP, TIER_LABELS
from betfair.state       import (
    load, save, reset_daily, update_cumulative_profit,
    get_tier_profit, tier_profit_summary,
)
from betfair.balance_log import log_bet_placed, start_balance_logger
from betfair.settlement  import settle_race
from betfair.notify      import send, send_chunks, set_muted
from betfair.commands    import start_command_listener
from predict             import place_terms

# ── Logging ───────────────────────────────────────────────────────────────────

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level    = logging.INFO,
    format   = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers = [logging.FileHandler("logs/betfair.log")],
)
logging.getLogger("apscheduler.scheduler").setLevel(logging.WARNING)
logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)

logger    = logging.getLogger("betfair_main")
CARD_PATH = os.path.join(config.DIR_CARDS, "today.json")

BET_BEFORE_MINUTES = 5


# ── Card loading ──────────────────────────────────────────────────────────────

def _load_today() -> list:
    if not os.path.exists(CARD_PATH):
        return []
    try:
        with open(CARD_PATH) as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Could not read today.json: {e}")
        return []
    today_str = date.today().strftime("%Y-%m-%d")
    if data.get("date") != today_str:
        logger.info(f"today.json is from {data.get('date')}, not today")
        return []
    return data.get("races", [])


def _parse_off_dt(race: dict):
    off_dt_str = race.get("off_dt", "")
    if not off_dt_str:
        return None
    try:
        utc = _to_utc(off_dt_str)
        return _to_local_naive(utc) if utc else None
    except Exception:
        return None


def _race_places(race: dict) -> int:
    n = len(race.get("runners", [])) or race.get("field_size", 0) or 0
    return place_terms(n) if n else 1


def _race_cons_places(race: dict) -> int:
    """Conservative place terms = standard + 1."""
    n = len(race.get("runners", [])) or race.get("field_size", 0) or 1
    return min(_race_places(race) + 1, max(n - 1, 1))

# _pick2_score and _score_gap removed — System C no longer uses score gap redirects

def _find_fallback_pick(race: dict, exclude_names: list, odds: dict, bf_runners: list):
    """
    Find the next best runner from all_runners when Pick 2 is a non-runner.
    Skips any horse in exclude_names or with REMOVED status on Betfair.
    Returns (name, price, sel_id) or (None, None, None) if no fallback found.
    """
    from betfair.api import _norm_horse
    all_runners  = race.get("all_runners", [])
    exclude_norm = [_norm_horse(n) for n in exclude_names]

    for runner in all_runners:
        name = runner.get("horse", "")
        if _norm_horse(name) in exclude_norm:
            continue
        sel_id = find_selection_id(name, bf_runners)
        if sel_id:
            info = odds.get(sel_id, {})
            if info.get("status") == "REMOVED":
                continue
            live_price = info.get("back")
            if live_price and live_price >= MIN_PICK2_PRICE:
                return name, live_price, sel_id
        sp_dec = runner.get("sp_dec")
        if sp_dec and sp_dec >= MIN_PICK2_PRICE:
            return name, sp_dec, None
    return None, None, None


def _next_tier_threshold(profit: float) -> float:
    for min_profit, _, _ in STAKE_TIERS:
        if profit < min_profit:
            return float(min_profit)
    return float(STAKE_TIERS[-1][0])


# ── Racing API result fetcher ─────────────────────────────────────────────────

def _fetch_result(race_id: str):
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


def _get_finish_pos(result: dict, horse_name: str):
    from betfair.api import _norm_horse
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


def _get_sp_from_result(result: dict, horse_name: str):
    """Extract the SP (sp_dec) for a horse from the Racing API result."""
    from betfair.api import _norm_horse
    norm = _norm_horse(horse_name)
    for r in result.get("runners", []):
        bf = _norm_horse(r.get("horse", ""))
        if norm == bf or norm in bf or bf in norm:
            try:
                return float(r.get("sp_dec") or 0) or None
            except (TypeError, ValueError):
                return None
    return None


# ── Paper settlement ──────────────────────────────────────────────────────────

def _paper_settle(race: dict, paper_bets: list, state: dict,
                  place_bets: list = None, silent: bool = False):
    """
    Daemon thread — waits T+15 mins, polls Racing API for result,
    calculates win and place P&L separately, updates cumulative profit
    from BOTH win and place combined, fires streak tracker.

    paper_bets:  win market bets  [{horse, price, stake, label, bsp}]
    place_bets:  place market bets [{horse, price, stake, cons_places}] or None
    silent:      if True, suppresses all Telegram output (used in live mode)

    BSP bets: if bet has bsp=True, price is None at placement time.
    After result is fetched, the Racing API SP is used as the settlement price.
    """
    race_label  = f"{race.get('off','?')} {race.get('course','?')}"
    race_id     = race.get("race_id", "")
    std_places  = _race_places(race)
    cons_places = _race_cons_places(race)

    off_dt_str = race.get("off_dt", "")
    try:
        off_dt = _to_utc(off_dt_str)
    except Exception:
        off_dt = None

    wait_until = (
        off_dt + timedelta(minutes=15) if off_dt
        else datetime.now(timezone.utc) + timedelta(minutes=20)
    )
    now = datetime.now(timezone.utc)
    if wait_until > now:
        wait_s = (wait_until - now).total_seconds()
        logger.info(f"Paper settle {race_label}: waiting {wait_s:.0f}s")
        time.sleep(wait_s)

    result = None
    for attempt in range(10):
        result = _fetch_result(race_id)
        if result and any(
            str(r.get("position", "")).strip().isdigit()
            for r in result.get("runners", [])
        ):
            break
        if attempt < 9:
            logger.debug(f"Paper settle {race_label}: result not ready, retrying in 2m")
            time.sleep(120)

    if not result:
        logger.warning(f"Paper settle {race_label}: no result found after polling")
        if not silent:
            send(f"⚠️ <b>PAPER SETTLE</b> - {race_label}\nResult not available after polling.")
        return

    from predict import place_terms as _place_terms
    result_runners = [r for r in result.get("runners", []) if r.get("horse")]
    n_result = len(result_runners) or len(result.get("runners", []))
    if n_result > 0:
        std_places  = _place_terms(n_result)
        cons_places = min(std_places + 1, max(n_result - 1, 1))

    # ── Win bet settlement ────────────────────────────────────────────────────
    total_pnl   = 0.0
    icon        = "✅"
    bet_results = []
    lines       = [
        f"📝 <b>PAPER SETTLED - {race_label}</b>",
        "------------------------------",
    ]

    for bet in paper_bets:
        horse   = bet["horse"]
        stake   = bet["stake"]
        label   = bet.get("label", "")
        is_bsp  = bet.get("bsp", False)
        pos     = _get_finish_pos(result, horse)

        # For BSP bets use Racing API SP as settlement price
        if is_bsp:
            price = _get_sp_from_result(result, horse)
            if price:
                lines.append(f"🔄 {label} {horse} — BSP settled @ {price:.2f}")
            else:
                price = bet.get("price") or 2.0
                lines.append(f"⚠️ {label} {horse} — BSP price unavailable, using {price:.2f}")
        else:
            price = bet["price"]

        if pos == 1:
            profit = round(stake * (price - 1) * (1 - COMMISSION), 2)
            total_pnl += profit
            won = True
            lines.append(f"✅ {label} {horse} @ {price:.2f} - WON 1st (+£{profit:.2f})")
        elif pos is not None:
            total_pnl -= stake
            won = False
            ord_s = "nd" if pos==2 else "rd" if pos==3 else "th"
            lines.append(f"❌ {label} {horse} @ {price:.2f} - LOST {pos}{ord_s} (-£{stake:.2f})")
        else:
            total_pnl -= stake
            won = False
            lines.append(f"❌ {label} {horse} @ {price:.2f} - LOST (NR/inc) (-£{stake:.2f})")
        bet_results.append((bet, won))

    # ── Place bet settlement ──────────────────────────────────────────────────
    place_pnl     = 0.0
    std_win       = False
    cons_win      = False

    if place_bets:
        win_note = " — place only (no win bets)" if not paper_bets else ""
        lines.append("------------------------------")
        lines.append(f"📍 <b>Place bets (£{place_bets[0]['stake']:.0f} each, top {cons_places}){win_note}</b>")

        picks_placed_std  = []
        picks_placed_cons = []

        for bet in place_bets:
            horse    = bet["horse"]
            price    = bet["price"]
            stake    = bet["stake"]
            pos      = _get_finish_pos(result, horse)

            if pos is not None and pos <= std_places:
                profit = round(stake * (price - 1) * (1 - COMMISSION), 2)
                place_pnl += profit
                picks_placed_cons.append(True)
                lines.append(f"✅ 📍 {horse} @ {price:.2f} - PLACED top {std_places} (+£{profit:.2f})")
            else:
                place_pnl -= stake
                picks_placed_cons.append(False)
                pos_s = f"{pos}th" if pos else "NR/inc"
                lines.append(f"❌ 📍 {horse} @ {price:.2f} - UNPLACED {pos_s} (-£{stake:.2f})")

            picks_placed_std.append(pos is not None and pos <= std_places)

        std_win  = len(picks_placed_std)  >= 2 and all(picks_placed_std[:2])
        cons_win = len(picks_placed_cons) >= 2 and all(picks_placed_cons[:2])

        state["paper_place_pnl"] = round(
            state.get("paper_place_pnl", 0.0) + place_pnl, 2
        )

    # ── Combined P&L ─────────────────────────────────────────────────────────
    combined_pnl = total_pnl + place_pnl

    # ── Streak tracker ────────────────────────────────────────────────────────
    if not silent:
        try:
            from notifications.streak_tracker import (
                update_from_betfair, update as streak_update_sp
            )
            outcome    = {"std_win": std_win, "cons_win": cons_win}
            streak_msg = None

            if place_bets and len(place_bets) >= 2:
                from betfair.strategy import get_place_stake
                streak_msg = update_from_betfair(
                    race          = race,
                    outcome       = outcome,
                    horse_a_name  = place_bets[0]["horse"],
                    horse_b_name  = place_bets[1]["horse"],
                    place_price_a = place_bets[0]["price"],
                    place_price_b = place_bets[1]["price"],
                    std_places    = std_places,
                    cons_places   = cons_places,
                    initial_stake = get_place_stake(state.get("cumulative_profit", 0.0)),
                )
            elif len(paper_bets) >= 2:
                horse_a = {"horse": paper_bets[0]["horse"], "sp_dec": paper_bets[0].get("price")}
                horse_b = {"horse": paper_bets[1]["horse"], "sp_dec": paper_bets[1].get("price")}
                race_wp = {**race, "places": std_places, "cons_places": cons_places}
                streak_msg = streak_update_sp(race_wp, outcome,
                                              horse_a=horse_a, horse_b=horse_b)
            if streak_msg:
                send(streak_msg)

        except Exception as e:
            logger.error(f"streak_tracker failed for {race_label}: {e}")

    # ── Tier tracker — paper mode only ───────────────────────────────────────
    if not silent:
        try:
            from utils.tier_tracker import log_result
            tier     = race.get("tier")
            tsr_solo = race.get("tsr_solo", False)
            if tier is not None and len(bet_results) >= 1:
                win1 = bet_results[0][1] if len(bet_results) > 0 else False
                win2 = bet_results[1][1] if len(bet_results) > 1 else False
                log_result(
                    race_id  = f"paper_{race_id}",
                    tier     = tier,
                    course   = race.get("course", "?"),
                    off      = race.get("off", "?"),
                    pick1    = paper_bets[0]["horse"] if paper_bets else "?",
                    pick2    = paper_bets[1]["horse"] if len(paper_bets) > 1 else "?",
                    win1     = win1,
                    win2     = win2,
                    places   = std_places,
                    tsr_solo = tsr_solo,
                )
        except Exception as e:
            logger.error(f"tier_tracker paper log failed for {race_label}: {e}")

    # ── Update state ──────────────────────────────────────────────────────────
    if not silent:
        milestone_alerts = update_cumulative_profit(state, combined_pnl)
        for alert in milestone_alerts:
            send(alert)

    if total_pnl + place_pnl < 0:
        icon = "❌"
    elif total_pnl + place_pnl == 0:
        icon = "➖"

    state["paper_daily_pnl"] = round(state.get("paper_daily_pnl", 0.0) + total_pnl, 2)
    state["paper_daily_bets"].append({
        "race":      race_label,
        "total_pnl": round(total_pnl, 2),
    })
    save(state)

    # ── Circuit breaker check — paper mode only ───────────────────────────────
    if not silent:
        from betfair.state import check_circuit_breaker
        circuit_alert = check_circuit_breaker(state)
        if circuit_alert:
            send(circuit_alert)

    if silent:
        logger.info(
            f"Paper settle (silent) {race_label}: "
            f"win {'+' if total_pnl>=0 else ''}£{total_pnl:.2f} "
            f"place {'+' if place_pnl>=0 else ''}£{place_pnl:.2f}"
        )
        return

    # ── Build and send Telegram notification ─────────────────────────────────
    cum_profit     = state.get("cumulative_profit", 0.0)
    day_place_pnl  = state.get("paper_place_pnl", 0.0)
    sign           = "+" if total_pnl >= 0 else ""
    place_sign     = "+" if place_pnl >= 0 else ""
    day_sign       = "+" if state["paper_daily_pnl"] >= 0 else ""
    day_place_sign = "+" if day_place_pnl >= 0 else ""
    profit_sign    = "+" if cum_profit >= 0 else ""
    comb_sign      = "+" if combined_pnl >= 0 else ""

    lines += ["------------------------------"]
    lines.append(f"Win P&L:         {sign}£{total_pnl:.2f}")
    if place_bets:
        lines.append(f"Place P&L:       {place_sign}£{place_pnl:.2f}")
        lines.append(f"Race Combined:   {comb_sign}£{combined_pnl:.2f}")
    lines.append(f"Day Win P&L:     {day_sign}£{state['paper_daily_pnl']:.2f}")
    if place_bets:
        lines.append(f"Day Place P&L:   {day_place_sign}£{day_place_pnl:.2f}")
    lines += [
        f"Cumulative P&L:  {profit_sign}£{cum_profit:.2f}",
        f"Next tier at:    £{_next_tier_threshold(cum_profit):.0f} profit",
    ]

    send(f"{icon} " + "\n".join(lines)[2:])

    _clear_pending_settlement(state, race.get("race_id",""))
    logger.info(
        f"Paper settled {race_label}: win {sign}£{total_pnl:.2f} "
        f"place {place_sign}£{place_pnl:.2f} combined "
        f"{'+' if combined_pnl>=0 else ''}£{combined_pnl:.2f} "
        f"| cumulative £{cum_profit:.2f}"
    )


# ── Shared market fetch ───────────────────────────────────────────────────────

def _get_market(race: dict):
    mkt, _ = find_win_market(race)
    if mkt is None:
        return None, None, None
    odds = get_market_odds(mkt.market_id)
    if not odds:
        return None, None, None
    return mkt, odds, mkt.runners or []

def _save_pending_settlement(state: dict, race_id: str, payload: dict):
    pending = state.get("pending_settlements", {})
    pending[race_id] = payload
    state["pending_settlements"] = pending
    save(state)

def _clear_pending_settlement(state: dict, race_id: str):
    pending = state.get("pending_settlements", {})
    pending.pop(race_id, None)
    state["pending_settlements"] = pending
    save(state)

# ── Live bet job ──────────────────────────────────────────────────────────────

def _live_bet_job(race: dict, state: dict):
    """Place real bets on Betfair Exchange."""
    off_str    = race.get("off", "?")
    course     = race.get("course", "?")
    race_label = f"{off_str} {course}"
    tier_label = race.get("tier_label", "")
    tier       = race.get("tier", 0)
    balance    = get_balance()
    profit     = get_tier_profit(state, tier)
    lines      = []

    top1   = race.get("top1") or {}
    top2   = race.get("top2") or {}
    a_name = top1.get("horse", "?")
    b_name = top2.get("horse", "?")

    mkt, odds, bf_runners = _get_market(race)
    if mkt is None:
        send(f"⚠️ 💰 {race_label} - no Betfair market/odds found")
        return

    market_id = mkt.market_id
    a_sel_id  = find_selection_id(a_name, bf_runners)
    b_sel_id  = find_selection_id(b_name, bf_runners)

    a_info = odds.get(a_sel_id, {}) if a_sel_id else {}
    b_info = odds.get(b_sel_id, {}) if b_sel_id else {}

    if a_info.get("status") == "REMOVED":
        send(f"⏭️ 💰 <b>SKIP - {race_label}</b>\n⭐ Pick 1 {a_name} - REMOVED (non-runner)")
        return

    if b_info.get("status") == "REMOVED":
        fallback_name, fallback_price, fallback_sel = _find_fallback_pick(
            race, [a_name, b_name], odds, bf_runners
        )
        if fallback_name:
            send(f"⚠️ 💰 {race_label}\n🔵 Pick 2 {b_name} - NR, substituting {fallback_name} @ {fallback_price:.2f}")
            b_name   = fallback_name
            b_sel_id = fallback_sel
            b_info   = odds.get(fallback_sel, {}) if fallback_sel else {}
        else:
            send(f"⏭️ 💰 <b>SKIP - {race_label}</b>\n🔵 Pick 2 {b_name} - NR, no viable substitute")
            return

    a_live = a_info.get("back")
    b_live = b_info.get("back")
    liq_a      = a_info.get("back_size", 0.0)
    liq_b      = b_info.get("back_size", 0.0)
    lay_liq_a  = a_info.get("lay_size", 0.0)
    lay_liq_b  = b_info.get("lay_size", 0.0)

    n_runners_live = len(race.get("all_runners") or [])
    stake_a, stake_b, stake_place = pick_stakes(
        profit, tier, a_live, b_live, n_runners=n_runners_live
    )
    place_only = False
    if place_only:
        send(f"🐴 💰 {race_label} — two-horse race, place market only (paper tracking)")
        return
    if stake_a == 0 and stake_b == 0:
        if a_live and a_live < MIN_PICK1_PRICE:
            reason = (
                f"Pick 1 {a_name} @ {a_live} odds-on — "
                f"no viable redirect (P2 score {p2_sc} or price insufficient)"
            )
        elif b_live and b_live < MIN_PICK2_PRICE:
            reason = f"Pick 2 {b_name} @ {b_live} below min {MIN_PICK2_PRICE}"
        else:
            reason = f"Pick 1 {a_name} @ {a_live} below min {MIN_PICK1_PRICE} — no viable redirect"
        lines.append(f"⏭️ Win bets skipped — {reason}")
        # Don't return — fall through to place bets

    redirect = stake_a == 0
    actual_a, actual_b, skipped, liq_reason = apply_liquidity(
        stake_a, stake_b, liq_a if not redirect else 0.0, liq_b, redirect
    )

    # ── BSP fallback — insufficient liquidity ─────────────────────────────────
    # When liquidity is below the dynamic threshold for a horse's price,
    # submit a MARKET_ON_CLOSE order instead of skipping.
    # Only falls back when liq > 0 (market exists but is thin).
    # If liq = 0, skip entirely — no market interest at all.
    use_bsp_a = False
    use_bsp_b = False
    if skipped and (liq_a > 0 or liq_b > 0):
        min_liq_a = min_liquidity_for_price(a_live or 0, stake_a) if stake_a > 0 else 0
        min_liq_b = min_liquidity_for_price(b_live or 0, stake_b) if stake_b > 0 else 0
        if not redirect:
            use_bsp_a = stake_a > 0 and liq_a > 0 and liq_a < min_liq_a
            use_bsp_b = stake_b > 0 and liq_b > 0 and liq_b < min_liq_b
            actual_a  = stake_a if use_bsp_a else actual_a
            actual_b  = stake_b if use_bsp_b else actual_b
            skipped   = False
        else:
            use_bsp_b = stake_b > 0 and liq_b > 0 and liq_b < min_liq_b
            actual_b  = stake_b if use_bsp_b else actual_b
            skipped   = not use_bsp_b

    if skipped:
        send(f"⏭️ 💰 <b>SKIP - {race_label}</b>\n⚠️ {liq_reason}")
        return

    tsr_tag = " 🔥 TSR" if tsr else ""
    lines = [
        f"💰 <b>LIVE BET - {race_label}</b>",
        f"{tier_label}{tsr_tag}",
        f"Balance: £{balance:.2f} | Profit: £{profit:.2f} | Tier: £{get_stake(profit):.0f}/horse",
        "------------------------------",
    ]

    if redirect:
        if a_live and a_live < MIN_PICK1_PRICE:
            lines.append(f"⏭️ Pick 1 odds-on ({a_live}) — £{actual_b:.2f} on Pick 2 only")
        else:
            lines.append(f"⏭️ Weak gap + P2 shorter ({a_live} vs {b_live}) — £{actual_b:.2f} on Pick 2 only (gap={gap})")
    elif stake_b == 0 and stake_a > 0:
        lines.append(f"ℹ️ Pick 2 below min price — backing Pick 1 solo")
    elif stake_a == 0 and stake_b > 0:
        lines.append(f"⏭️ Weak gap + P2 shorter — £{actual_b:.2f} on Pick 2 only (gap={gap})")
    elif actual_b < stake_b and not use_bsp_b:
        lines.append(
            f"⚠️ Stake reduced £{stake_b:.0f}→£{actual_b:.0f} "
            f"(P1 liq: £{liq_a:.0f}, P2 liq: £{liq_b:.0f})"
        )

    bets_placed    = []
    balance_before = balance

    def _try_back(sel_id, horse, stake, label, live_price, liq, lay_liq=0.0, use_bsp=False):
        if stake == 0 or sel_id is None:
            return None
        if use_bsp:
            bet = place_bsp(market_id, sel_id, stake)
            if bet:
                bet["horse_name"] = horse
                lines.append(
                    f"🔄 {label}: {horse} — BSP £{stake:.2f} "
                    f"(guaranteed fill @ market price | back liq: £{liq:.0f} | lay: £{lay_liq:.0f})"
                )
                return bet
            lines.append(f"❌ {label}: {horse} - BSP order rejected")
            return None
        min_price = 1.2 if tsr else MIN_BACK_PRICE
        if not live_price or live_price < min_price:
            lines.append(f"⚠️ {label}: {horse} - no viable price ({live_price})")
            return None
        bet = place_back(market_id, sel_id, live_price, stake)
        if bet:
            bet["horse_name"] = horse
            matched = bet.get("size_matched") or stake
            tag = "⏳" if bet.get("pending") else "✅"
            required = round(matched * (live_price - 1), 2)
            lay_ok   = "✅" if lay_liq >= required else "⚠️"
            lines.append(
                f"{tag} {label}: {horse} @ {live_price} £{matched:.2f} "
                f"(back liq: £{liq:.0f} | payout: £{required:.0f} {lay_ok} lay: £{lay_liq:.0f})"
            )
            return bet
        lines.append(f"❌ {label}: {horse} - rejected by Betfair")
        return None

    a_label = "⭐ Pick 1 (TSR)" if tsr else "⭐ Pick 1"
    bet_a = _try_back(a_sel_id, a_name, actual_a, a_label, a_live, liq_a, lay_liq=lay_liq_a, use_bsp=True)
    bet_b = _try_back(b_sel_id, b_name, actual_b, "🔵 Pick 2", b_live, liq_b, lay_liq=lay_liq_b, use_bsp=True)

    if bet_a: bets_placed.append(bet_a)
    if bet_b: bets_placed.append(bet_b)

    if not bets_placed:
        lines.append("\nℹ️ No win bets placed — checking place market")
        send("\n".join(lines))
        # Don't return — fall through to place bets
        balance_after = get_balance()
        placement_ts  = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    if bets_placed:
        send("\n".join(lines))
        time.sleep(2)
        balance_after = get_balance()
        placement_ts  = log_bet_placed(race, bets_placed, balance_before, balance_after)

    settle_bets = []
    for b in bets_placed:
        matched    = b.get("size_matched") or b.get("size", 0)
        price      = b.get("price") or 0
        win_credit = round(matched * (price - 1) * 0.95, 2) if price > 1 else 0
        settle_bets.append({
            "bet_id":               str(b.get("bet_id", "")),
            "type":                 "BACK",
            "horse":                b.get("horse_name", "?"),
            "price":                price,
            "stake":                matched,
            "potential_win_credit": win_credit,
            "bsp":                  b.get("bsp", False),
            "market_id":            market_id,  # add this
        })

    # ── Save market IDs for BSP fetch job ────────────────────────────────────
    try:
        import json as _json
        from datetime import date as _date
        bsp_log_path = os.path.join(
            os.path.dirname(__file__), "data", "results",
            f"{_date.today().strftime('%Y-%m-%d')}_markets.json"
        )
        existing = []
        if os.path.exists(bsp_log_path):
            with open(bsp_log_path) as f:
                existing = _json.load(f)
        existing.append({
            "race_label": race_label,
            "race_id":    race.get("race_id",""),
            "market_id":  market_id,
            "ts":         datetime.now().isoformat(),
            "bets":       settle_bets,
        })
        with open(bsp_log_path, "w") as f:
            _json.dump(existing, f, indent=2)
    except Exception as e:
        logger.error(f"BSP log save failed: {e}")
      
    # ── Live place bets ───────────────────────────────────────────────────────
    live_place_bets = []
    p_stake         = get_place_stake(profit, tier)
    cons_places     = _race_cons_places(race)
  
    try:
        place_mkt, _ = find_place_market(race)
        logger.info(f"{race_label}: place_mkt={'found' if place_mkt else 'None'}")
        place_odds_map = {}
        place_runners  = []
        if place_mkt is not None:
            place_odds_map = get_market_odds(place_mkt.market_id)
            place_runners  = place_mkt.runners or []
            if not place_odds_map:
                # Check we have time to retry before race off
                off_utc = _to_utc(race.get("off_dt",""))
                now_utc = datetime.now(timezone.utc)
                mins_to_off = (off_utc - now_utc).total_seconds() / 60 if off_utc else 0
                if mins_to_off > 3:
                    logger.info(f"{race_label}: place market empty, retrying in 90s ({mins_to_off:.1f} mins to off)")
                    time.sleep(90)
                    place_odds_map = get_market_odds(place_mkt.market_id)
                if not place_odds_map:
                    logger.info(f"{race_label}: place market still empty after retry")
            win_note = " — place only" if not bets_placed else ""
            place_lines    = ["------------------------------", f"📍 <b>Place bets{win_note}</b>"]

            n_runners = len(race.get("all_runners") or [])
            # No place bets on <=4 runner races (Betfair only pays winner)
            if n_runners <= 4:
                place_lines.append("📍 ≤4 runners — place bets skipped (win only)")
                horses_to_place = []
            else:
                horses_to_place = [
                    h for h in [a_name, b_name]
                    if h and h != "?"
                ]
            for horse in horses_to_place:
                sel_id = find_selection_id(horse, place_runners)
                if sel_id is None:
                    place_lines.append(f"⚠️ 📍 {horse} — not found in place market")
                    continue
                p_info    = place_odds_map.get(sel_id, {})
                p_price   = p_info.get("back")
                p_liq     = p_info.get("back_size", 0.0)
                p_lay_liq = p_info.get("lay_size", 0.0)

                if not p_price or p_price < 1.1:
                    place_lines.append(f"⚠️ 📍 {horse} — no viable place price")
                    continue

                min_stake         = 2.0
                required_for_min  = round(min_stake * (p_price - 1), 2)
                if p_lay_liq == 0 or p_lay_liq < required_for_min:
                    place_lines.append(
                        f"⏭️ 📍 {horse} @ {p_price:.2f} — lay liq £{p_lay_liq:.0f} "
                        f"insufficient (needs £{required_for_min:.0f} for min stake)"
                    )
                    continue

                max_stake_from_liq = p_lay_liq / (p_price - 1)
                actual_p_stake     = min(p_stake, max_stake_from_liq)
                actual_p_stake     = max(min_stake, round(actual_p_stake / 2) * 2)

                place_bet = place_bsp(place_mkt.market_id, sel_id, actual_p_stake)
                if place_bet:
                    place_bet["horse_name"] = horse
                    matched_p = place_bet.get("size_matched") or actual_p_stake
                    place_lines.append(
                        f"✅ 📍 {horse} — BSP £{matched_p:.2f} "
                        f"(guaranteed fill | lay liq: £{p_lay_liq:.0f})"
                    )
                    live_place_bets.append({
                        "horse":       horse,
                        "price":       None,
                        "stake":       matched_p or actual_p_stake,
                        "cons_places": cons_places,
                        "lay_liq":     p_lay_liq,
                        "bsp":         True,
                        "bet_id":      str(place_bet.get("bet_id", "")),
                    })
                else:
                    place_lines.append(f"❌ 📍 {horse} — BSP place order rejected")

            if len(place_lines) > 2:
                send("\n".join(place_lines))
        else:
            send(f"📍 No place market found for {race_label}")
    except Exception as e:
        logger.error(f"Live place bet failed for {race_label}: {e}")
        send(f"⚠️ 📍 Place bet error for {race_label}: {e}")

    t = threading.Thread(
        target = settle_race,
        args   = (
            placement_ts, race.get("race_id", ""), race_label,
            str(race.get("off_dt", "")), balance_before, balance_after,
            settle_bets, state,
        ),
        kwargs = {
            "race":        race,
            "places":      _race_places(race),
            "place_bets":  live_place_bets if live_place_bets else None,
            "cons_places": cons_places,
        },
        daemon = True,
        name   = f"Settle_{race.get('race_id', '')}",
    )
    t.start()


# ── Paper bet job ─────────────────────────────────────────────────────────────

def _paper_bet_job(race: dict, state: dict, silent: bool = False):
    """
    Simulate win and place bets using live Betfair prices.
    Place bets scale with win stake tier. Place prices stored for streak tracker.
    Liquidity checks applied to both win and place bets so paper results
    reflect realistic exchange fill conditions.

    BSP fallback: when liquidity is below the dynamic threshold, the bet is
    logged as a BSP bet with price=None. At settlement, the Racing API SP
    is used as the price — the most accurate paper proxy for BSP.
    """
    off_str    = race.get("off", "?")
    course     = race.get("course", "?")
    race_label = f"{off_str} {course}"
    tier_label = race.get("tier_label", "")
    tier       = race.get("tier", 0)
    balance    = get_balance()
    profit     = get_tier_profit(state, tier)
    lines      = []

    top1   = race.get("top1") or {}
    top2   = race.get("top2") or {}
    a_name = top1.get("horse", "?")
    b_name = top2.get("horse", "?")

    mkt, odds, bf_runners = _get_market(race)
    mkt_ok   = mkt is not None
    a_sel_id = find_selection_id(a_name, bf_runners) if mkt_ok else None
    b_sel_id = find_selection_id(b_name, bf_runners) if mkt_ok else None

    a_info = odds.get(a_sel_id, {}) if (mkt_ok and a_sel_id) else {}
    b_info = odds.get(b_sel_id, {}) if (mkt_ok and b_sel_id) else {}

    a_live = a_info.get("back") or top1.get("sp_dec")
    b_live = b_info.get("back") or top2.get("sp_dec")

    if a_live is None or b_live is None:
        logger.error(
            f"Missing prices: a_live={a_live}, b_live={b_live} — skipping bet"
        )
        return
      
    liq_a      = a_info.get("back_size", 0.0)
    liq_b      = b_info.get("back_size", 0.0)
    lay_liq_a  = a_info.get("lay_size", 0.0)
    lay_liq_b  = b_info.get("lay_size", 0.0)

    # ── Non-runner checks ─────────────────────────────────────────────────────
    if mkt_ok and a_info.get("status") == "REMOVED":
        if not silent:
            send(f"⏭️ 📝 <b>PAPER SKIP - {race_label}</b>\n⭐ Pick 1 {a_name} - REMOVED (non-runner)")
        return

    if mkt_ok and b_info.get("status") == "REMOVED":
        fallback_name, fallback_price, fallback_sel = _find_fallback_pick(
            race, [a_name, b_name], odds, bf_runners
        )
        if fallback_name:
            logger.info(f"Pick 2 {b_name} NR - substituting {fallback_name}")
            if not silent:
                send(f"⚠️ 📝 {race_label}\n🔵 Pick 2 {b_name} - NR, using {fallback_name} @ {fallback_price:.2f}")
            b_name   = fallback_name
            b_live   = fallback_price
            b_sel_id = fallback_sel
            b_info   = odds.get(fallback_sel, {}) if fallback_sel else {}
            liq_b    = b_info.get("back_size", 0.0)
        else:
            if not silent:
                send(f"⏭️ 📝 <b>PAPER SKIP - {race_label}</b>\n🔵 Pick 2 {b_name} - NR, no viable substitute")
            return

    # ── Stake calculation ─────────────────────────────────────────────────────
    n_runners_live = len(race.get("all_runners") or [])
    stake_a, stake_b, stake_place = pick_stakes(
        profit, tier, a_live, b_live, n_runners=n_runners_live
    )
    place_only = False
    if not place_only and stake_a == 0 and stake_b == 0:
        if not silent:
            if a_live and a_live < MIN_PICK1_PRICE:
                reason = (
                    f"Pick 1 {a_name} @ {a_live} odds-on — "
                    f"no viable redirect (P2 score {p2_sc} or price insufficient)"
                )
            elif b_live and b_live < MIN_PICK2_PRICE:
                reason = f"Pick 2 {b_name} @ {b_live} below min {MIN_PICK2_PRICE}"
            else:
                reason = f"Pick 1 {a_name} @ {a_live} below min {MIN_PICK1_PRICE} — no viable redirect"
            lines.append(f"⏭️ Win bets skipped — {reason}")
        # Don't return — fall through to place bets

    redirect = stake_a == 0 if not place_only else False

    actual_a, actual_b, skipped, _ = apply_liquidity(
        stake_a if not place_only else 0.0,
        stake_b if not place_only else 0.0,
        liq_a, liq_b, redirect
    )

    # ── Paper BSP fallback ────────────────────────────────────────────────────
    # When liquidity is below dynamic threshold, flag as BSP.
    # Price logged as None — settlement uses Racing API SP instead.
    use_bsp_a = False
    use_bsp_b = False
    if not place_only and mkt_ok:
        if skipped and (liq_a > 0 or liq_b > 0):
            min_liq_a = min_liquidity_for_price(a_live or 0, stake_a) if stake_a > 0 else 0
            min_liq_b = min_liquidity_for_price(b_live or 0, stake_b) if stake_b > 0 else 0
            if not redirect:
                use_bsp_a = stake_a > 0 and liq_a > 0 and liq_a < min_liq_a
                use_bsp_b = stake_b > 0 and liq_b > 0 and liq_b < min_liq_b
                actual_a  = stake_a if use_bsp_a else actual_a
                actual_b  = stake_b if use_bsp_b else actual_b
                skipped   = False
            else:
                use_bsp_b = stake_b > 0 and liq_b > 0 and liq_b < min_liq_b
                actual_b  = stake_b if use_bsp_b else actual_b
                skipped   = not use_bsp_b

    if place_only:
        actual_a, actual_b = 0.0, 0.0
    elif not mkt_ok:
        actual_a, actual_b = stake_a, stake_b

    # ── Build bet notification ────────────────────────────────────────────────
    paper_bets = []
    tsr_tag    = " 🔥 TSR" if tsr else ""
    p_stake    = get_place_stake(profit, tier)

    lines = [
        f"📝 <b>PAPER BET - {race_label}</b>",
        f"{tier_label}{tsr_tag}",
        f"Balance: £{balance:.2f} | Profit: £{profit:.2f} | "
        f"Win: £{get_stake(profit):.0f}/horse | Place: £{p_stake:.0f}/horse",
        "------------------------------",
    ]
    if not mkt_ok:
        lines.append("⚠️ No Betfair market - using RA odds")

    if redirect:
        if a_live and a_live < MIN_PICK1_PRICE:
            lines.append(f"⏭️ Pick 1 odds-on ({a_live}) — £{actual_b:.2f} on Pick 2 only")
        else:
            lines.append(f"⏭️ Weak gap + P2 shorter ({a_live} vs {b_live}) — £{actual_b:.2f} on Pick 2 only (gap={gap})")
    elif stake_a == 0 and stake_b > 0 and not redirect:
        lines.append(f"⏭️ Weak gap + P2 shorter — £{actual_b:.2f} on Pick 2 only (gap={gap})")
    elif stake_b == 0 and stake_a > 0:
        lines.append(f"ℹ️ Pick 2 below min price — backing Pick 1 solo")
    elif actual_b < stake_b and mkt_ok and not use_bsp_b:
        lines.append(
            f"⚠️ Stake reduced £{stake_b:.0f}→£{actual_b:.0f} "
            f"(P1 liq: £{liq_a:.0f}, P2 liq: £{liq_b:.0f})"
        )

    def _log_win(horse, stake, price, liq, label, use_bsp=False):
        if stake == 0:
            return
        if use_bsp:
            lines.append(
                f"🔄 {label}: {horse} — BSP £{stake:.2f} "
                f"(liq too low: £{liq:.0f}, will settle at SP)"
            )
            paper_bets.append({
                "horse": horse, "price": None, "stake": stake,
                "label": label, "bsp": True,
            })
        elif price and price > (1.2 if tsr else 1.0):
            required = round(stake * (price - 1), 2)
            lay_liq  = lay_liq_a if horse == a_name else lay_liq_b
            lay_ok   = "✅" if lay_liq >= required else "⚠️"
            lines.append(
                f"📝 {label}: {horse} @ {price:.2f} - paper £{stake:.2f}"
                + (f" (back: £{liq:.0f} | payout: £{required:.0f} {lay_ok} lay: £{lay_liq:.0f})" if mkt_ok and liq else "")
            )
            paper_bets.append({
                "horse": horse, "price": price, "stake": stake,
                "label": label, "bsp": False,
            })
        else:
            lines.append(f"⚠️ {label}: {horse} - no usable price")

    a_label = "⭐ Pick 1 (TSR)" if tsr else "⭐ Pick 1"
    _log_win(a_name, actual_a, a_live, liq_a, a_label, use_bsp=use_bsp_a)
    _log_win(b_name, actual_b, b_live, liq_b, "🔵 Pick 2", use_bsp=use_bsp_b)

    if place_only and not paper_bets:
        lines.append("🐴 <b>Two-horse race — place bets only</b>")

    if not paper_bets and not place_only:
        if not silent:
            lines.append("\nℹ️ No win bets — checking place market only")
        # Don't return — fall through to place bet section

    # ── Place market bets ─────────────────────────────────────────────────────
    # Place stake scales with tier via get_place_stake().
    # Capped at £2 for GOOD/SKIP tiers.
    # Liquidity checked — skips horses where exchange cannot fill the bet.
    place_bets  = []
    cons_places = _race_cons_places(race)

    if not silent:
        try:
            place_mkt, _ = find_place_market(race)
            place_odds_map = {}
            place_runners  = []
            if place_mkt is not None:
                place_odds_map = get_market_odds(place_mkt.market_id)
                place_runners  = place_mkt.runners or []
                if not place_odds_map:
                    # Check we have time to retry before race off
                    off_utc = _to_utc(race.get("off_dt",""))
                    now_utc = datetime.now(timezone.utc)
                    mins_to_off = (off_utc - now_utc).total_seconds() / 60 if off_utc else 0
                    if mins_to_off > 3:
                        logger.info(f"{race_label}: place market empty, retrying in 90s ({mins_to_off:.1f} mins to off)")
                        time.sleep(90)
                        place_odds_map = get_market_odds(place_mkt.market_id)
                    if not place_odds_map:
                        logger.info(f"{race_label}: place market still empty after retry")

                n_runners = len(race.get("all_runners") or [])
                # No place bets on <=4 runner races (Betfair only pays winner)
                if n_runners <= 4:
                    place_lines.append("📍 ≤4 runners — place bets skipped (win only)")
                    horses_to_place = []
                else:
                    horses_to_place = [
                        h for h in [a_name, b_name]
                        if h and h != "?"
                    ]            
                for horse in horses_to_place:
                    sel_id = find_selection_id(horse, place_runners)
                    if sel_id is None:
                        lines.append(f"⚠️ 📍 {horse} — not found in place market")
                        logger.warning(f"Place bet {race_label}: {horse} not found in place runners")
                        continue
                    p_info    = place_odds_map.get(sel_id, {})
                    p_price   = p_info.get("back")
                    p_liq     = p_info.get("back_size", 0.0)
                    p_lay_liq = p_info.get("lay_size", 0.0)

                    if not p_price or p_price < 1.1:
                        lines.append(f"⚠️ 📍 {horse} — no viable place price (back={p_price})")
                        logger.warning(f"Place bet {race_label}: {horse} no price back={p_price} sel={sel_id} keys={list(place_odds_map.keys())[:5]}")
                        continue

                    min_stake        = 2.0
                    required_for_min = round(min_stake * (p_price - 1), 2)
                    if p_lay_liq == 0 or p_lay_liq < required_for_min:
                        lines.append(
                            f"⏭️ 📍 {horse} @ {p_price:.2f} — lay liq £{p_lay_liq:.0f} "
                            f"insufficient (needs £{required_for_min:.0f} for min stake)"
                        )
                        continue

                    max_stake_from_liq = p_lay_liq / (p_price - 1)
                    actual_p_stake     = min(p_stake, max_stake_from_liq)
                    actual_p_stake     = max(min_stake, round(actual_p_stake / 2) * 2)

                    place_bets.append({
                        "horse":       horse,
                        "price":       p_price,
                        "stake":       actual_p_stake,
                        "cons_places": cons_places,
                        "lay_liq":     p_lay_liq,
                    })

                if place_bets:
                    lines.append("------------------------------")
                    lines.append(f"📍 <b>Place bets (£{p_stake:.0f} each, top {cons_places})</b>")
                    for pb in place_bets:
                        pb_required = round(p_stake * (pb['price'] - 1), 2)
                        pb_lay_ok   = "✅" if pb.get('lay_liq', 0) >= pb_required else "⚠️"
                        lines.append(f"  📍 {pb['horse']} @ {pb['price']:.2f} (payout: £{pb_required:.0f} {pb_lay_ok} lay: £{pb.get('lay_liq',0):.0f})")
                else:
                    lines.append("📍 Place market: no prices available")
            else:
                lines.append("📍 No place market found")
        except Exception as e:
            logger.warning(f"Place market lookup failed for {race_label}: {e}")
            lines.append("📍 Place market lookup failed")

    if not silent:
        send("\n".join(lines))

    if not silent:
        _save_pending_settlement(state, race.get("race_id",""), {
            "race_label":   race_label,
            "race_off_iso": str(race.get("off_dt","")),
            "paper_bets":   paper_bets,
            "place_bets":   place_bets,
            "race":         race,
            "ts":           datetime.now().isoformat(),
        })
  
    t = threading.Thread(
        target = _paper_settle,
        args   = (race, paper_bets, state),
        kwargs = {"place_bets": place_bets if not silent else None,
                  "silent": silent},
        daemon = True,
        name   = f"PaperSettle_{race.get('race_id', '')}",
    )
    t.start()


# ── Unified bet job ───────────────────────────────────────────────────────────

def bet_job(race: dict, state: dict):
    from betfair.commands import is_betting_allowed
    if not is_betting_allowed(state, tier):
        logger.info(f"Paused - skipping {race.get('off')} {race.get('course')}")
        return

    now_utc = datetime.now(timezone.utc)
    off_utc = _to_utc(race.get("off_dt", ""))
    if off_utc and now_utc >= off_utc:
        send(
            f"⏭️ <b>MISSED</b> - {race.get('off')} {race.get('course')}\n"
            f"Race already started at job fire time."
        )
        return

    mode = state.get("mode", "paper")
    if mode == "live":
        _live_bet_job(race, state)
        _paper_bet_job(race, state, silent=True)
    else:
        _paper_bet_job(race, state, silent=False)


# ── End of day ────────────────────────────────────────────────────────────────

def end_of_day_job(state: dict):
    logger.info("end_of_day_job")
    bal         = get_balance()
    profit      = state.get("cumulative_profit", 0.0)
    today       = date.today().strftime("%A %-d %B %Y")
    mode        = state.get("mode", "paper").upper()

    live_bets   = state.get("daily_bets", [])
    live_pnl    = state.get("daily_pnl", 0.0)
    paper_bets  = state.get("paper_daily_bets", [])
    paper_pnl   = state.get("paper_daily_pnl", 0.0)
    paper_place = state.get("paper_place_pnl", 0.0)
    live_wins   = sum(1 for b in live_bets  if b.get("total_pnl", 0) > 0)
    paper_wins  = sum(1 for b in paper_bets if b.get("total_pnl", 0) > 0)

    profit_sign = "+" if profit >= 0 else ""
    lines = [
        f"📋 <b>BETFAIR DAILY SUMMARY - {today}</b>",
        f"Mode: {mode}",
        "==============================",
        f"Balance:          £{bal:.2f}",
        f"Cumulative P&L:   {profit_sign}£{profit:.2f}  (win + place)",
        f"Banked profit:    £{state.get('banked_profit', 0.0):.2f}",
        f"Tier pots:\n{tier_profit_summary(state)}",
    ]

    if paper_bets:
        sign       = "+" if paper_pnl >= 0 else ""
        place_sign = "+" if paper_place >= 0 else ""
        combined   = round(paper_pnl + paper_place, 2)
        comb_sign  = "+" if combined >= 0 else ""
        lines += [
            "-- 📝 Paper ------------------",
            f"Races: {len(paper_bets)} | Wins: {paper_wins} | Losses: {len(paper_bets)-paper_wins}",
            f"Win P&L:   {sign}£{paper_pnl:.2f}",
            f"Place P&L: {place_sign}£{paper_place:.2f}",
            f"Combined:  {comb_sign}£{combined:.2f}",
        ]
        for b in paper_bets:
            icon   = "✅" if b.get("total_pnl", 0) > 0 else "❌"
            b_sign = "+" if b.get("total_pnl", 0) >= 0 else ""
            lines.append(f"  {icon} {b['race']} - {b_sign}£{b['total_pnl']:.2f}")

    if live_bets:
        sign = "+" if live_pnl >= 0 else ""
        lines += [
            "-- 💰 Live -------------------",
            f"Races: {len(live_bets)} | Wins: {live_wins} | Losses: {len(live_bets)-live_wins}",
            f"P&L: {sign}£{live_pnl:.2f}",
        ]
        for b in live_bets:
            icon   = "✅" if b.get("total_pnl", 0) > 0 else "❌"
            b_sign = "+" if b.get("total_pnl", 0) >= 0 else ""
            lines.append(f"  {icon} {b['race']} - {b_sign}£{b['total_pnl']:.2f}")

    try:
        from utils.tier_tracker import get_eod_summary
        tracker_summary = get_eod_summary()
        if tracker_summary:
            lines += ["==============================", tracker_summary]
    except Exception as e:
        logger.error(f"tier_tracker EOD summary failed: {e}")

    try:
        from notifications.streak_tracker import get_eod_summary as streak_eod
        streak_summary = streak_eod()
        if streak_summary:
            lines += ["==============================", streak_summary]
    except Exception as e:
        logger.error(f"streak_tracker EOD summary failed: {e}")

    lines += [
        "------------------------------",
        f"Tomorrow's stake: {stake_display(profit)}",
        "==============================",
    ]
    send_chunks("\n".join(lines))


# ── Startup ───────────────────────────────────────────────────────────────────

def startup(scheduler: BackgroundScheduler, state: dict, send_briefing: bool = True):
    races      = _load_today()
    now        = datetime.now()
    bal        = get_balance()
    profit     = state.get("cumulative_profit", 0.0)
    mode       = state.get("mode", "paper").upper()
    qualifying = [r for r in races if qualifies(r)]
    scheduled  = 0
    last_off   = None

    for race in qualifying:
        off_dt = _parse_off_dt(race)
        if off_dt is None:
            continue
        bet_time = off_dt - timedelta(minutes=BET_BEFORE_MINUTES)
        if bet_time <= now:
            continue

        def _make_job(r, s):
            return lambda: bet_job(r, s)

        scheduler.add_job(
            _make_job(race, state),
            DateTrigger(run_date=bet_time),
            id               = f"bet_{race.get('race_id', '')}",
            replace_existing = True,
        )
        scheduled += 1
        if last_off is None or off_dt > last_off:
            last_off = off_dt

    if last_off:
        eod = last_off + timedelta(minutes=90)
        if eod > now:
            scheduler.add_job(
                lambda: end_of_day_job(state),
                DateTrigger(run_date=eod),
                id="end_of_day", replace_existing=True,
            )

    if send_briefing:
        paused    = state.get("betting_paused", False)
        muted     = state.get("muted", False)
        mode_icon = "💰" if mode == "LIVE" else "📝"
        live_pnl  = state.get("daily_pnl", 0.0)
        paper_pnl = state.get("paper_daily_pnl", 0.0)

        tier_counts = {}
        for r in qualifying:
            label = (r.get("tier_label") or "·").split()[0]
            tier_counts[label] = tier_counts.get(label, 0) + 1
        tier_summary = " | ".join(f"{v}x{k}" for k, v in tier_counts.items())

        profit_sign = "+" if profit >= 0 else ""
        p_stake     = get_place_stake(profit)
        lines = [
            f"🤖 <b>BETFAIR BOT v3</b> {mode_icon} {mode}",
            "==============================",
            f"Balance:          £{bal:.2f}",
            f"Cumulative P&L:   {profit_sign}£{profit:.2f}  (win + place)",
            f"Banked profit:    £{state.get('banked_profit', 0.0):.2f}",
            f"Win tier:         £{get_stake(profit):.0f}/horse",
            f"Place tier:       £{p_stake:.0f}/horse",
            f"Next tier at:     £{_next_tier_threshold(profit):.0f} profit",
            f"Betting:          {'⏸️ PAUSED' if paused else '▶️ ACTIVE'}",
            f"Notifications:    {'🔕 MUTED' if muted else '🔔 ON'}",
            f"Live P&L:         {'+' if live_pnl >= 0 else ''}£{live_pnl:.2f}",
            f"Paper P&L:        {'+' if paper_pnl >= 0 else ''}£{paper_pnl:.2f}",
            f"Bet timing:       T-{BET_BEFORE_MINUTES} mins",
            "------------------------------",
            f"Qualifying: {len(qualifying)} | Scheduled: {scheduled}",
            f"Tiers: {tier_summary or 'none'}",
            f"Filters: Turf | Not Heavy | Not Irish staying chase | Good/Skip capped",
            "------------------------------",
        ]
        for r in sorted(qualifying, key=lambda x: x.get("off", "99:99")):
            top1    = r.get("top1") or {}
            top2    = r.get("top2") or {}
            tier    = r.get("tier", 0)
            badge   = (r.get("tier_label") or "·").split()[0]
            a_price = top1.get("sp_dec")
            b_price = top2.get("sp_dec")
            r_tier  = r.get("tier", 0)
            r_profit = get_tier_profit(state, r_tier)
            n_r     = len(r.get("all_runners") or [])
            if a_price and b_price:
                s_a, s_b, _ = pick_stakes(r_profit, r_tier, a_price, b_price, n_runners=n_r)
            else:
                s_a = get_stake(r_profit, r_tier)
                s_b = s_a
            off_dt  = _parse_off_dt(r)
            bet_at  = (off_dt - timedelta(minutes=BET_BEFORE_MINUTES) + timedelta(hours=1)).strftime("%H:%M") if off_dt else "?"
            p1_note    = " (odds-on→skip)" if (a_price and a_price < MIN_PICK1_PRICE) else ""
            p2_note    = " (solo P1)" if (b_price and b_price is not None and b_price < MIN_PICK2_PRICE) else ""
            place_note = " 📍" if (s_a == 0 and s_b == 0) else ""
            lines.append(
                f"{badge} <b>{r.get('off','?')} {r.get('course','?')}</b>{tsr_m}"
                f" [bet@{bet_at}]{place_note}\n"
                f"  ⭐ {top1.get('horse','?')} ({top1.get('sp','?')}){p1_note} £{s_a:.2f} | "
                f"🔵 {top2.get('horse','?')} ({top2.get('sp','?')}){p2_note} £{s_b:.2f}"
            )
        if not qualifying:
            lines.append("No qualifying races today.")
        lines.append("==============================")
        send_chunks("\n".join(lines))
    # ── Re-queue any pending settlements from before restart ──────────────────
    pending = state.get("pending_settlements", {})
    if pending:
        logger.info(f"Re-queuing {len(pending)} pending settlements from before restart")
        send(f"⚠️ Re-queuing {len(pending)} pending settlements from previous session")
        for race_id, payload in list(pending.items()):
            t = threading.Thread(
                target = _paper_settle,
                args   = (payload["race"], payload["paper_bets"], state),
                kwargs = {"place_bets": payload.get("place_bets"),
                          "silent": False},
                daemon = True,
                name   = f"PaperSettle_{race_id}",
            )
            t.start()
            logger.info(f"Re-queued settlement for {payload['race_label']}")
          
    logger.info(
        f"startup: {scheduled} scheduled, mode={mode}, "
        f"balance=£{bal:.2f}, profit=£{profit:.2f}"
    )


def _midnight_job(scheduler: BackgroundScheduler, state: dict):
    logger.info("midnight_job")
    state = reset_daily(state)
    try:
        from notifications.streak_tracker import reset_streaks
        reset_streaks()
    except Exception as e:
        logger.error(f"streak_tracker reset failed: {e}")
    startup(scheduler, state, send_briefing=False)


def _midday_refresh(scheduler: BackgroundScheduler, state: dict):
    logger.info("midday_refresh")
    races      = _load_today()
    now        = datetime.now()
    bal        = get_balance()
    profit     = state.get("cumulative_profit", 0.0)
    qualifying = [r for r in races if qualifies(r)]
    scheduled  = 0
    for race in qualifying:
        off_dt   = _parse_off_dt(race)
        if off_dt is None:
            continue
        bet_time = off_dt - timedelta(minutes=BET_BEFORE_MINUTES)
        if bet_time <= now:
            continue
        def _make_job(r, s):
            return lambda: bet_job(r, s)
        scheduler.add_job(
            _make_job(race, state),
            DateTrigger(run_date=bet_time),
            id=f"bet_{race.get('race_id', '')}",
            replace_existing=True,
        )
        scheduled += 1
    mode = state.get("mode", "paper").upper()
    send(
        f"🔄 <b>Midday refresh</b> - {scheduled} races scheduled\n"
        f"Mode: {mode} | Balance: £{bal:.2f} | Profit: £{profit:.2f} | {stake_display(profit)}"
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    logger.info("Betfair bot v3 starting")
    os.makedirs("data", exist_ok=True)

    try:
        get_client()
        logger.info("Betfair login OK")
    except Exception as e:
        logger.error(f"Betfair login failed: {e}")
        send(f"❌ Betfair login failed: {e}")
        sys.exit(1)

    state = load()
    today = date.today().strftime("%Y-%m-%d")
    if state.get("last_date") != today:
        state = reset_daily(state)

    set_muted(state.get("muted", False))

    scheduler = BackgroundScheduler(timezone="Europe/London")

    scheduler.add_job(
        lambda: _midnight_job(scheduler, state),
        CronTrigger(hour=4, minute=45, timezone="Europe/London"),
        id="midnight",
    )
    scheduler.add_job(
        lambda: _midday_refresh(scheduler, state),
        CronTrigger(hour=10, minute=0, timezone="Europe/London"),
        id="midday_refresh",
    )

    scheduler.start()
    logger.info("Scheduler started")

    start_balance_logger(get_balance, interval_s=15)
    start_command_listener(state)
    startup(scheduler, state, send_briefing=True)

    try:
        while True:
            time.sleep(30)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Betfair bot stopping")
        scheduler.shutdown()
        send("🤖 Betfair bot v3 offline")


if __name__ == "__main__":
    main()
