"""
betfair_main.py - horse_bets_v3 Betfair Exchange Bot

Runs independently of main.py. Reads today.json written by the main bot,
qualifies races from all tiers, and places (or simulates) bets on the
Betfair Exchange 10 minutes before each race.

Modes (toggle via Telegram):
  /paper - simulated bets (default, safe). Finds real market + price,
           logs what would have been placed, settles from Racing API result.
           Paper ALWAYS runs in the background even in live mode.
  /live  - real bets placed on Betfair Exchange. Full balance-log settlement.

Staking: WINNINGS-DRIVEN (not balance-driven)
  Stakes grow only from cumulative net profit, not from initial deposit.
  At zero profit stakes stay at 2/horse regardless of account balance.
  Milestone notifications fire every 50 of cumulative profit.

Usage:
    python betfair_main.py
Background:
    nohup python betfair_main.py >> logs/betfair.log 2>&1 &
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
    get_client, get_balance, find_win_market,
    get_market_odds, find_selection_id, place_back,
    _to_utc, _to_local_naive, COMMISSION,
)
from betfair.strategy    import (
    qualifies, is_tsr_trigger, get_stake, pick_stakes, apply_liquidity,
    check_topup_alerts, stake_display, MIN_BACK_PRICE, MIN_LIQUIDITY,
    MIN_PICK1_PRICE, MIN_PICK2_PRICE, should_back_pick1, should_back_pick2,
    STAKE_TIERS,
)
from betfair.state       import load, save, reset_daily, update_cumulative_profit
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
    handlers = [
        logging.FileHandler("logs/betfair.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logging.getLogger("apscheduler.scheduler").setLevel(logging.WARNING)
logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)

logger    = logging.getLogger("betfair_main")
CARD_PATH = os.path.join(config.DIR_CARDS, "today.json")


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


def _pick2_score(race: dict) -> int:
    """Extract the model score for Pick 2 from the race dict."""
    top2 = race.get("top2") or {}
    return int(top2.get("score", 0) or 0)


def _next_tier_threshold(profit: float) -> float:
    """Return the next profit threshold that would increase the stake tier."""
    for min_profit, _, _ in STAKE_TIERS:
        if profit < min_profit:
            return float(min_profit)
    return float(STAKE_TIERS[-1][0])


# ── Racing API result fetcher ──────────────────────────────────────────────────

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


# ── Paper settlement ──────────────────────────────────────────────────────────

def _paper_settle(race: dict, paper_bets: list, state: dict):
    """
    Daemon thread - waits until race_off + 15 minutes, polls Racing API
    for result, calculates P&L, updates cumulative profit, fires milestones.
    """
    race_label = f"{race.get('off','?')} {race.get('course','?')}"
    race_id    = race.get("race_id", "")

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
        send(f"⚠️ <b>PAPER SETTLE</b> - {race_label}\nResult not available after polling.")
        return

    total_pnl   = 0.0
    icon        = "✅"
    bet_results = []
    lines       = [
        f"📝 <b>PAPER SETTLED - {race_label}</b>",
        "------------------------------",
    ]

    for bet in paper_bets:
        horse = bet["horse"]
        price = bet["price"]
        stake = bet["stake"]
        label = bet.get("label", "")
        pos   = _get_finish_pos(result, horse)

        if pos == 1:
            profit = round(stake * (price - 1) * (1 - COMMISSION), 2)
            total_pnl += profit
            won = True
            lines.append(f"✅ {label} {horse} @ {price} - WON 1st (+£{profit:.2f})")
        elif pos is not None:
            total_pnl -= stake
            won = False
            ord_s = "st" if pos==1 else "nd" if pos==2 else "rd" if pos==3 else "th"
            lines.append(f"❌ {label} {horse} @ {price} - LOST {pos}{ord_s} (-£{stake:.2f})")
        else:
            total_pnl -= stake
            won = False
            lines.append(f"❌ {label} {horse} @ {price} - LOST (NR/inc) (-£{stake:.2f})")
        bet_results.append((bet, won))

    # Log to tier tracker
    try:
        from utils.tier_tracker import log_result
        tier     = race.get("tier")
        tsr_solo = race.get("tsr_solo", False)
        places   = _race_places(race)
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
                places   = places,
                tsr_solo = tsr_solo,
            )
    except Exception as e:
        logger.error(f"tier_tracker paper log failed for {race_label}: {e}")

    # Update cumulative profit and fire any milestone notifications
    milestone_alerts = update_cumulative_profit(state, total_pnl)
    for alert in milestone_alerts:
        send(alert)

    if total_pnl < 0:
        icon = "❌"
    elif total_pnl == 0:
        icon = "➖"

    state["paper_daily_pnl"] = round(state.get("paper_daily_pnl", 0.0) + total_pnl, 2)
    state["paper_daily_bets"].append({
        "race":      race_label,
        "total_pnl": round(total_pnl, 2),
    })
    save(state)

    cum_profit  = state.get("cumulative_profit", 0.0)
    sign        = "+" if total_pnl >= 0 else ""
    day_sign    = "+" if state["paper_daily_pnl"] >= 0 else ""
    profit_sign = "+" if cum_profit >= 0 else ""
    lines += [
        "------------------------------",
        f"Race P&L:        {sign}£{total_pnl:.2f}",
        f"Day P&L:         {day_sign}£{state['paper_daily_pnl']:.2f}",
        f"Cumulative P&L:  {profit_sign}£{cum_profit:.2f}",
        f"Next tier at:    £{_next_tier_threshold(cum_profit):.0f} profit",
    ]
    send(f"{icon} " + "\n".join(lines)[2:])
    logger.info(
        f"Paper settled {race_label}: P&L {sign}£{total_pnl:.2f} "
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


# ── Live bet job ──────────────────────────────────────────────────────────────

def _live_bet_job(race: dict, state: dict):
    """Place real bets on Betfair Exchange."""
    off_str    = race.get("off", "?")
    course     = race.get("course", "?")
    race_label = f"{off_str} {course}"
    tier_label = race.get("tier_label", "")
    tier       = race.get("tier", 0)
    tsr        = is_tsr_trigger(race)
    balance    = get_balance()
    profit     = state.get("cumulative_profit", 0.0)
    p2_sc      = _pick2_score(race)

    top1   = race.get("top1") or {}
    top2   = race.get("top2") or {}
    a_name = top1.get("horse", "?")
    b_name = top2.get("horse", "?")

    prev_stake = state.get("_prev_tier_stake")
    for alert in check_topup_alerts(balance, profit, prev_stake):
        send(alert)
    state["_prev_tier_stake"] = get_stake(profit)

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
        send(f"⏭️ 💰 <b>SKIP - {race_label}</b>\n🔵 Pick 2 {b_name} - REMOVED (non-runner)")
        return

    a_live = a_info.get("back")
    b_live = b_info.get("back")
    liq_a  = a_info.get("back_size", 0.0)
    liq_b  = b_info.get("back_size", 0.0)

    stake_a, stake_b = pick_stakes(profit, tsr, a_live, b_live, tier=tier, pick2_score=p2_sc)
    if stake_b == 0:
        reason = (f"Pick 2 @ {b_live} below min {MIN_PICK2_PRICE}"
                  if b_live else "Pick 2 price unavailable")
        send(f"⏭️ 💰 <b>SKIP - {race_label}</b>\n{reason}")
        return

    redirect = stake_a == 0
    actual_a, actual_b, skipped, liq_reason = apply_liquidity(
        stake_a, stake_b, liq_a if not redirect else 0.0, liq_b, redirect
    )
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
        lines.append(
            f"⏭️ Pick 1 odds-on ({a_live}) - £{actual_b:.2f} on Pick 2 only "
            f"(P2 score {p2_sc} qualifies)"
        )
    elif actual_b < stake_b:
        lines.append(
            f"⚠️ Stake reduced from £{stake_b:.0f} to £{actual_b:.0f} "
            f"- matched to lower liquidity (P1: £{liq_a:.0f}, P2: £{liq_b:.0f})"
        )

    bets_placed    = []
    balance_before = balance

    def _try_back(sel_id, horse, stake, label, live_price, liq):
        if stake == 0 or sel_id is None:
            return None
        price = live_price
        if not price or price < MIN_BACK_PRICE:
            lines.append(f"⚠️ {label}: {horse} - no viable price ({price})")
            return None
        bet = place_back(market_id, sel_id, price, stake)
        if bet:
            bet["horse_name"] = horse
            matched = bet.get("size_matched", stake)
            if bet.get("pending"):
                lines.append(
                    f"⏳ {label}: {horse} @ {price} - order live £{stake:.2f} "
                    f"(liq: £{liq:.0f})"
                )
            else:
                lines.append(
                    f"✅ {label}: {horse} @ {price} - matched £{matched:.2f} "
                    f"(liq: £{liq:.0f})"
                )
            return bet
        lines.append(f"❌ {label}: {horse} - rejected by Betfair")
        return None

    a_label = "⭐ Pick 1 (TSR)" if tsr else "⭐ Pick 1"
    bet_a = _try_back(a_sel_id, a_name, actual_a, a_label, a_live, liq_a)
    bet_b = _try_back(b_sel_id, b_name, actual_b, "🔵 Pick 2", b_live, liq_b)

    if bet_a: bets_placed.append(bet_a)
    if bet_b: bets_placed.append(bet_b)

    if not bets_placed:
        lines.append("\nℹ️ No bets placed")
        send("\n".join(lines))
        return

    send("\n".join(lines))

    time.sleep(2)
    balance_after = get_balance()
    placement_ts  = log_bet_placed(race, bets_placed, balance_before, balance_after)

    settle_bets = []
    for b in bets_placed:
        matched    = b.get("size_matched") or b.get("size", 0)
        win_credit = round(matched * (b["price"] - 1) * 0.95, 2)
        settle_bets.append({
            "bet_id":               str(b.get("bet_id", "")),
            "type":                 "BACK",
            "horse":                b.get("horse_name", "?"),
            "price":                b["price"],
            "stake":                matched,
            "potential_win_credit": win_credit,
        })

    race_off_iso = str(race.get("off_dt", ""))
    places       = _race_places(race)

    t = threading.Thread(
        target = settle_race,
        args   = (
            placement_ts, race.get("race_id", ""), race_label,
            race_off_iso, balance_before, balance_after,
            settle_bets, state,
        ),
        kwargs = {"race": race, "places": places},
        daemon = True,
        name   = f"Settle_{race.get('race_id', '')}",
    )
    t.start()


# ── Paper bet job ─────────────────────────────────────────────────────────────

def _paper_bet_job(race: dict, state: dict, silent: bool = False):
    """
    Simulate bets using cumulative profit for stake tier, not Betfair balance.
    """
    off_str    = race.get("off", "?")
    course     = race.get("course", "?")
    race_label = f"{off_str} {course}"
    tier_label = race.get("tier_label", "")
    tier       = race.get("tier", 0)
    tsr        = is_tsr_trigger(race)
    balance    = get_balance()
    profit     = state.get("cumulative_profit", 0.0)
    p2_sc      = _pick2_score(race)

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
    liq_a  = a_info.get("back_size", 0.0)
    liq_b  = b_info.get("back_size", 0.0)

    stake_a, stake_b = pick_stakes(profit, tsr, a_live, b_live, tier=tier, pick2_score=p2_sc)
    if stake_b == 0:
        if not silent:
            reason = (f"Pick 2 {b_name} @ {b_live} below min {MIN_PICK2_PRICE}"
                      if b_live else f"Pick 2 {b_name} - no price")
            send(f"⏭️ 📝 <b>PAPER SKIP - {race_label}</b>\n{reason}")
        return

    redirect = stake_a == 0

    actual_a, actual_b, _, _ = apply_liquidity(
        stake_a, stake_b, liq_a, liq_b, redirect
    )
    if not mkt_ok:
        actual_a, actual_b = stake_a, stake_b

    paper_bets = []
    tsr_tag    = " 🔥 TSR" if tsr else ""
    lines      = [
        f"📝 <b>PAPER BET - {race_label}</b>",
        f"{tier_label}{tsr_tag}",
        f"Balance: £{balance:.2f} | Profit: £{profit:.2f} | Tier: £{get_stake(profit):.0f}/horse",
        "------------------------------",
    ]
    if not mkt_ok:
        lines.append("⚠️ No Betfair market - using RA odds")

    if redirect:
        lines.append(
            f"⏭️ Pick 1 odds-on ({a_live}) - £{actual_b:.2f} on Pick 2 only "
            f"(P2 score {p2_sc} qualifies)"
        )
    elif actual_b < stake_b and mkt_ok:
        lines.append(
            f"⚠️ Stake reduced from £{stake_b:.0f} to £{actual_b:.0f} "
            f"(P1 liq: £{liq_a:.0f}, P2 liq: £{liq_b:.0f})"
        )

    def _log(horse, stake, price, liq, label):
        if stake == 0:
            return
        if price and price > 1:
            lines.append(
                f"📝 {label}: {horse} @ {price:.2f} - paper £{stake:.2f}"
                + (f" (liq: £{liq:.0f})" if mkt_ok and liq else "")
            )
            paper_bets.append({"horse": horse, "price": price, "stake": stake, "label": label})
        else:
            lines.append(f"⚠️ {label}: {horse} - no usable price")

    a_label = "⭐ Pick 1 (TSR)" if tsr else "⭐ Pick 1"
    _log(a_name, actual_a, a_live, liq_a, a_label)
    _log(b_name, actual_b, b_live, liq_b, "🔵 Pick 2")

    if not paper_bets:
        if not silent:
            lines.append("\nℹ️ No paper bets logged")
            send("\n".join(lines))
        return

    if not silent:
        send("\n".join(lines))

    t = threading.Thread(
        target = _paper_settle,
        args   = (race, paper_bets, state),
        daemon = True,
        name   = f"PaperSettle_{race.get('race_id', '')}",
    )
    t.start()


# ── Unified bet job ───────────────────────────────────────────────────────────

def bet_job(race: dict, state: dict):
    if state.get("betting_paused", False):
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
    bal        = get_balance()
    profit     = state.get("cumulative_profit", 0.0)
    today      = date.today().strftime("%A %-d %B %Y")
    mode       = state.get("mode", "paper").upper()

    live_bets  = state.get("daily_bets", [])
    live_pnl   = state.get("daily_pnl", 0.0)
    paper_bets = state.get("paper_daily_bets", [])
    paper_pnl  = state.get("paper_daily_pnl", 0.0)
    live_wins  = sum(1 for b in live_bets  if b.get("total_pnl", 0) > 0)
    paper_wins = sum(1 for b in paper_bets if b.get("total_pnl", 0) > 0)

    profit_sign = "+" if profit >= 0 else ""
    lines = [
        f"📋 <b>BETFAIR DAILY SUMMARY - {today}</b>",
        f"Mode: {mode}",
        "==============================",
        f"Balance:          £{bal:.2f}",
        f"Cumulative P&L:   {profit_sign}£{profit:.2f}",
        f"Current tier:     £{get_stake(profit):.0f}/horse",
        f"Next tier at:     £{_next_tier_threshold(profit):.0f} profit",
    ]

    if paper_bets:
        sign = "+" if paper_pnl >= 0 else ""
        lines += [
            "-- 📝 Paper ------------------",
            f"Races: {len(paper_bets)} | Wins: {paper_wins} | Losses: {len(paper_bets)-paper_wins}",
            f"P&L: {sign}£{paper_pnl:.2f}",
        ]
        for b in paper_bets:
            icon = "✅" if b.get("total_pnl", 0) > 0 else "❌"
            sign = "+" if b.get("total_pnl", 0) >= 0 else ""
            lines.append(f"  {icon} {b['race']} - {sign}£{b['total_pnl']:.2f}")

    if live_bets:
        sign = "+" if live_pnl >= 0 else ""
        lines += [
            "-- 💰 Live -------------------",
            f"Races: {len(live_bets)} | Wins: {live_wins} | Losses: {len(live_bets)-live_wins}",
            f"P&L: {sign}£{live_pnl:.2f}",
        ]
        for b in live_bets:
            icon = "✅" if b.get("total_pnl", 0) > 0 else "❌"
            sign = "+" if b.get("total_pnl", 0) >= 0 else ""
            lines.append(f"  {icon} {b['race']} - {sign}£{b['total_pnl']:.2f}")

    try:
        from utils.tier_tracker import get_eod_summary
        tracker_summary = get_eod_summary()
        if tracker_summary:
            lines += ["==============================", tracker_summary]
    except Exception as e:
        logger.error(f"tier_tracker EOD summary failed: {e}")

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
        bet_time = off_dt - timedelta(minutes=10)
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

        tsr_count  = sum(1 for r in qualifying if is_tsr_trigger(r))
        tier_counts = {}
        for r in qualifying:
            label = (r.get("tier_label") or "·").split()[0]
            tier_counts[label] = tier_counts.get(label, 0) + 1
        tier_summary = " | ".join(f"{v}x{k}" for k, v in tier_counts.items())

        profit_sign = "+" if profit >= 0 else ""
        lines = [
            f"🤖 <b>BETFAIR BOT v3</b> {mode_icon} {mode}",
            "==============================",
            f"Balance:          £{bal:.2f}",
            f"Cumulative P&L:   {profit_sign}£{profit:.2f}",
            f"Current tier:     £{get_stake(profit):.0f}/horse",
            f"Next tier at:     £{_next_tier_threshold(profit):.0f} profit",
            f"Betting:          {'⏸️ PAUSED' if paused else '▶️ ACTIVE'}",
            f"Notifications:    {'🔕 MUTED' if muted else '🔔 ON'}",
            f"Live P&L:         {'+' if live_pnl >= 0 else ''}£{live_pnl:.2f}",
            f"Paper P&L:        {'+' if paper_pnl >= 0 else ''}£{paper_pnl:.2f}",
            "------------------------------",
            f"Qualifying: {len(qualifying)} | Scheduled: {scheduled}",
            f"Tiers: {tier_summary or 'none'}",
            f"Filters: Turf | Not Heavy | Not Irish staying chase | Good/Skip capped",
            "------------------------------",
        ]
        for r in sorted(qualifying, key=lambda x: x.get("off", "99:99")):
            top1    = r.get("top1") or {}
            top2    = r.get("top2") or {}
            badge   = (r.get("tier_label") or "·").split()[0]
            tsr_m   = " 🔥" if is_tsr_trigger(r) else ""
            a_price = top1.get("sp_dec")
            b_price = top2.get("sp_dec")
            p2_sc   = _pick2_score(r)
            s_a, s_b = pick_stakes(
                profit, is_tsr_trigger(r), a_price, b_price,
                tier=r.get("tier", 0), pick2_score=p2_sc
            )
            off_dt  = _parse_off_dt(r)
            bet_at  = (off_dt - timedelta(minutes=10)).strftime("%H:%M") if off_dt else "?"
            p1_note = " (odds-on→skip)" if (a_price and a_price < MIN_PICK1_PRICE) else ""
            p2_note = " (under 3/1)" if (b_price and b_price < MIN_PICK2_PRICE) else ""
            lines.append(
                f"{badge} <b>{r.get('off','?')} {r.get('course','?')}</b>{tsr_m}"
                f" [bet@{bet_at}]\n"
                f"  ⭐ {top1.get('horse','?')} ({top1.get('sp','?')}){p1_note} £{s_a:.2f} | "
                f"🔵 {top2.get('horse','?')} ({top2.get('sp','?')}){p2_note} £{s_b:.2f}"
            )
        if not qualifying:
            lines.append("No qualifying races today.")
        lines.append("==============================")
        send_chunks("\n".join(lines))

    logger.info(
        f"startup: {scheduled} scheduled, mode={mode}, "
        f"balance=£{bal:.2f}, profit=£{profit:.2f}"
    )


def _midnight_job(scheduler: BackgroundScheduler, state: dict):
    logger.info("midnight_job")
    state = reset_daily(state)
    startup(scheduler, state, send_briefing=True)


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
        if off_dt is None: continue
        bet_time = off_dt - timedelta(minutes=10)
        if bet_time <= now: continue
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
        CronTrigger(hour=0, minute=1),
        id="midnight",
    )
    for h, m, jid in [(11, 50, "refresh_1150"), (12, 30, "refresh_1230")]:
        scheduler.add_job(
            lambda: _midday_refresh(scheduler, state),
            CronTrigger(hour=h, minute=m),
            id=jid,
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
