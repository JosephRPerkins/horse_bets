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

Staking — Strategy H (validated on 348 races, +£148 vs +£92 baseline):
  Chase:   P1 win £4 (128.6% ROI, Kelly-justified)
  Hurdle:  P1 win £2 + P2 win £2 + P1 place £2 + P2 place £2 (8+ runners)
  NH Flat: P1 win £2 + P1 place £2 (8+ runners)
  Flat:    P1 win £2 only

  Chase P2 win removed (-£0.930/bet). Flat place bets removed (-£0.127/bet).
  Hurdle P2 win added (+£0.796/bet, 27% win rate — outperforms P1).

BSP fallback:
  When exchange liquidity is below the dynamic threshold for a horse's price,
  the bot falls back to a MARKET_ON_CLOSE (BSP) order rather than skipping.
  In paper mode, the Racing API SP is used as the BSP proxy at settlement.

Streak tracker:
  Uses actual Betfair place market prices captured at bet time.
  Tracks both standard and conservative compounding streaks.

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
    win_stake_for_pick, place_stake_for_pick,
    MIN_BACK_PRICE, MIN_LIQUIDITY, MIN_PICK1_PRICE, MIN_PICK2_PRICE,
    should_back_pick1, should_back_pick2, min_liquidity_for_price,
    next_tier_threshold, BET_TIERS, apply_liquidity, p2_win_stake_for_pick,
    p2_place_stake,
)
from predict_v2 import TIER_ELITE, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP, TIER_LABELS
from betfair.state       import (
    load, save, reset_daily, update_cumulative_profit,
    get_tier_profit, tier_profit_summary, _state_lock,
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


# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_jump_race(race: dict) -> bool:
    rtype = (race.get("type") or "").lower()
    return any(t in rtype for t in ("chase", "hurdle", "nh flat", "national hunt"))


def _is_hurdle_race(race: dict) -> bool:
    rtype = (race.get("type") or "").lower()
    return "hurdle" in rtype


def _is_chase_race(race: dict) -> bool:
    rtype = (race.get("type") or "").lower()
    return "chase" in rtype


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
    n = len(race.get("runners", [])) or race.get("field_size", 0) or 1
    return min(_race_places(race) + 1, max(n - 1, 1))


def _find_fallback_pick(race: dict, exclude_names: list, odds: dict, bf_runners: list):
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


def _next_tier_threshold(profit: float, tier: int = 0) -> float:
    from betfair.strategy import next_tier_threshold
    return next_tier_threshold(profit, tier)


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
        horse  = bet["horse"]
        stake  = bet["stake"]
        label  = bet.get("label", "")
        is_bsp = bet.get("bsp", False)
        pos    = _get_finish_pos(result, horse)

        if is_bsp:
            sp_price  = _get_sp_from_result(result, horse)
            bsp_price = None
            bet_id    = bet.get("bet_id", "")
            if bet_id:
                from betfair.api import get_cleared_order
                cleared = get_cleared_order(bet_id)
                if cleared and cleared.get("price"):
                    bsp_price = cleared["price"]
            price = bsp_price or sp_price or bet.get("price") or 2.0
            if bsp_price and sp_price:
                diff = round(bsp_price - sp_price, 2)
                sign = "+" if diff >= 0 else ""
                lines.append(
                    f"🔄 {label} {horse} — BSP @ {bsp_price:.2f} "
                    f"(SP {sp_price:.2f}, diff {sign}{diff:.2f})"
                )
            elif bsp_price:
                lines.append(f"🔄 {label} {horse} — BSP @ {bsp_price:.2f}")
            elif sp_price:
                lines.append(f"📝 {label} {horse} — settled @ SP {sp_price:.2f}")
            else:
                lines.append(f"⏭️ {label} {horse} — no price (late NR?) — stake voided")
                bet_results.append((bet, None))
                continue
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
    place_pnl  = 0.0
    std_win    = False
    cons_win   = False

    if place_bets:
        win_note = " — place only (no win bets)" if not paper_bets else ""
        lines.append("------------------------------")
        lines.append(f"📍 <b>Place bets (£{place_bets[0]['stake']:.0f} each, top {cons_places}){win_note}</b>")

        picks_placed_std  = []
        picks_placed_cons = []

        for bet in place_bets:
            horse = bet["horse"]
            price = bet["price"]
            stake = bet["stake"]
            pos   = _get_finish_pos(result, horse)

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

    # ── Tier tracker ──────────────────────────────────────────────────────────
    if not silent:
        try:
            from utils.tier_tracker import log_result
            tier = race.get("tier")
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
                    tsr_solo = False,
                )
        except Exception as e:
            logger.error(f"tier_tracker paper log failed for {race_label}: {e}")

    # ── Update state ──────────────────────────────────────────────────────────
    if not silent:
        with _state_lock:
            milestone_alerts = update_cumulative_profit(state, combined_pnl)
        for alert in milestone_alerts:
            send(alert)
        from betfair.state import update_tier_profit
        race_tier = race.get("tier")
        if race_tier is not None:
            tier_alerts = update_tier_profit(state, race_tier, combined_pnl)
            for alert in tier_alerts:
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

    if silent:
        logger.info(
            f"Paper settle (silent) {race_label}: "
            f"win {'+' if total_pnl>=0 else ''}£{total_pnl:.2f} "
            f"place {'+' if place_pnl>=0 else ''}£{place_pnl:.2f}"
        )
        return

    cum_profit    = state.get("cumulative_profit", 0.0)
    day_place_pnl = state.get("paper_place_pnl", 0.0)
    sign          = "+" if total_pnl >= 0 else ""
    place_sign    = "+" if place_pnl >= 0 else ""
    day_sign      = "+" if state["paper_daily_pnl"] >= 0 else ""
    comb_sign     = "+" if combined_pnl >= 0 else ""
    true_total    = state.get("total_pnl", 0.0)
    true_sign     = "+" if true_total >= 0 else ""

    lines += ["------------------------------"]
    lines.append(f"Win P&L:         {sign}£{total_pnl:.2f}")
    if place_bets:
        lines.append(f"Place P&L:       {place_sign}£{place_pnl:.2f}")
        lines.append(f"Race Combined:   {comb_sign}£{combined_pnl:.2f}")
    lines.append(f"Day Win P&L:     {day_sign}£{state['paper_daily_pnl']:.2f}")
    if place_bets:
        lines.append(f"Day Place P&L:   {'+' if day_place_pnl>=0 else ''}£{day_place_pnl:.2f}")
    lines += [
        f"Cumulative P&L:  {'+' if cum_profit>=0 else ''}£{cum_profit:.2f}",
        f"True total P&L:  {true_sign}£{true_total:.2f}",
    ]

    send(f"{icon} " + "\n".join(lines)[2:])
    _clear_pending_settlement(state, race.get("race_id",""))
    logger.info(
        f"Paper settled {race_label}: win {sign}£{total_pnl:.2f} "
        f"place {place_sign}£{place_pnl:.2f} combined "
        f"{comb_sign}£{combined_pnl:.2f} | cumulative £{cum_profit:.2f}"
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
    off_str    = race.get("off", "?")
    course     = race.get("course", "?")
    race_label = f"{off_str} {course}"
    tier_label = race.get("tier_label", "")
    tier       = race.get("tier", 0)
    balance    = get_balance()

    top1   = race.get("top1") or {}
    top2   = race.get("top2") or {}
    a_name = top1.get("horse", "?")
    b_name = top2.get("horse", "?")

    is_jump   = _is_jump_race(race)
    is_hurdle = _is_hurdle_race(race)
    is_chase  = _is_chase_race(race)
    n_runners = len(race.get("all_runners") or [])

    mkt, odds, bf_runners = _get_market(race)
    if mkt is None:
        send(f"⚠️ 💰 {race_label} - no Betfair market/odds found")
        return

    market_id = mkt.market_id
    a_sel_id  = find_selection_id(a_name, bf_runners)
    b_sel_id  = find_selection_id(b_name, bf_runners)

    a_info = odds.get(a_sel_id, {}) if a_sel_id else {}
    b_info = odds.get(b_sel_id, {}) if b_sel_id else {}

    a_live = a_info.get("back")
    b_live = b_info.get("back")

    if a_info.get("status") == "REMOVED":
        logger.info(f"Live: Pick 1 {a_name} NR — promoting P2")
        if b_live:
            send(f"⚠️ 💰 {race_label}\n⭐ Pick 1 {a_name} NR — promoting {b_name} to P1")
            a_name = b_name; a_live = b_live; a_sel_id = b_sel_id; a_info = b_info
            fallback2, fallback2_price, fallback2_sel = _find_fallback_pick(
                race, [a_name], odds, bf_runners
            )
            if fallback2:
                b_name = fallback2; b_live = fallback2_price
                b_sel_id = fallback2_sel
                b_info = odds.get(fallback2_sel, {}) if fallback2_sel else {}
            else:
                b_name = "?"; b_live = None; b_sel_id = None; b_info = {}
        else:
            send(f"⏭️ 💰 <b>SKIP - {race_label}</b>\n⭐ Pick 1 {a_name} - NR, no viable substitute")
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

    from predict_v2 import _sp_free_score
    a_score = _sp_free_score(top1) if top1 else 0
    b_score = _sp_free_score(top2) if top2 else 0

    # Strategy H stakes
    stake_a     = win_stake_for_pick(a_live, a_score, is_chase=is_chase)
    stake_b     = p2_win_stake_for_pick(b_live, b_score, is_hurdle=is_hurdle)
    stake_place = place_stake_for_pick(b_score, tier, sp=b_live or 0.0,
                                       is_jump=is_jump, n_runners=n_runners,
                                       is_chase=is_chase)

    if stake_a == 0:
        reason = f"Pick 1 {a_name} @ {a_live} below min price" if a_live else "no price"
        send(f"⏭️ 💰 <b>SKIP - {race_label}</b>\n{reason}")
        return

    actual_a, actual_b, skipped, _ = apply_liquidity(stake_a, 0.0, 0.0, 0.0)

    p2_win_note = f" | P2 win: £{stake_b:.0f}" if stake_b > 0 else ""
    lines = [
        f"💰 <b>LIVE BET - {race_label}</b>",
        f"{tier_label}",
        f"Balance: £{balance:.2f} | {tier_profit_summary(state)}",
        f"P1 win: £{stake_a:.0f}{p2_win_note} | Place: £{stake_place:.0f}"
        + (" (jump only)" if stake_place > 0 else ""),
        "------------------------------",
    ]

    bets_placed    = []
    balance_before = balance

    def _try_back(sel_id, horse, stake, label, live_price):
        if stake == 0 or sel_id is None:
            return None
        bet = place_bsp(market_id, sel_id, stake)
        if bet:
            bet["horse_name"] = horse
            lines.append(f"🔄 {label}: {horse} — BSP £{stake:.2f} (guaranteed fill)")
            return bet
        lines.append(f"❌ {label}: {horse} - BSP order rejected")
        return None

    bet_a = _try_back(a_sel_id, a_name, actual_a, "⭐ Pick 1", a_live)
    if bet_a:
        bets_placed.append(bet_a)

    # P2 win bet — hurdle only
    if stake_b > 0 and b_sel_id:
        bet_b = _try_back(b_sel_id, b_name, stake_b, "🔵 Pick 2 win", b_live)
        if bet_b:
            bets_placed.append(bet_b)

    if not bets_placed:
        send("\n".join(lines))
        return

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
            "market_id":            market_id,
        })

    # ── Live place bets ───────────────────────────────────────────────────────
    live_place_bets = []
    cons_places     = _race_cons_places(race)

    if stake_place > 0:
        try:
            place_mkt, _ = find_place_market(race)
            if place_mkt is not None:
                place_odds_map = get_market_odds(place_mkt.market_id)
                place_runners  = place_mkt.runners or []
                place_type     = "hurdle" if is_hurdle else "chase/jump"
                place_lines    = ["------------------------------",
                                  f"📍 <b>Place bets — {place_type}</b>"]

                # Strategy H: P1 place all jump; P2 place hurdle only
                place_horses = [a_name]
                if is_hurdle and b_name and b_name != "?":
                    place_horses.append(b_name)

                for horse in place_horses:
                    if not horse or horse == "?":
                        continue
                    sel_id = find_selection_id(horse, place_runners)
                    if not sel_id:
                        place_lines.append(f"⚠️ 📍 {horse} — not found in place market")
                        continue
                    p_info  = place_odds_map.get(sel_id, {})
                    p_price = p_info.get("back")
                    if not p_price or p_price < 1.1:
                        place_lines.append(f"⏭️ 📍 {horse} — no viable place price")
                        continue
                    place_bet = place_bsp(place_mkt.market_id, sel_id, stake_place)
                    if place_bet:
                        place_bet["horse_name"] = horse
                        place_lines.append(f"✅ 📍 {horse} @ {p_price:.2f} — BSP £{stake_place:.2f}")
                        live_place_bets.append({
                            "horse":       horse,
                            "price":       None,
                            "stake":       stake_place,
                            "cons_places": cons_places,
                            "bsp":         True,
                            "bet_id":      str(place_bet.get("bet_id", "")),
                        })
                    else:
                        place_lines.append(f"❌ 📍 {horse} — BSP place order rejected")
                send("\n".join(place_lines))
        except Exception as e:
            logger.error(f"Live place bet failed for {race_label}: {e}")

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
    off_str    = race.get("off", "?")
    course     = race.get("course", "?")
    race_label = f"{off_str} {course}"
    tier_label = race.get("tier_label", "")
    tier       = race.get("tier", 0)
    balance    = get_balance()
    profit     = get_tier_profit(state, tier)

    top1   = race.get("top1") or {}
    top2   = race.get("top2") or {}
    a_name = top1.get("horse", "?")
    b_name = top2.get("horse", "?")

    is_jump   = _is_jump_race(race)
    is_hurdle = _is_hurdle_race(race)
    is_chase  = _is_chase_race(race)
    n_runners = len(race.get("all_runners") or [])

    mkt, odds, bf_runners = _get_market(race)
    mkt_ok   = mkt is not None
    a_sel_id = find_selection_id(a_name, bf_runners) if mkt_ok else None
    b_sel_id = find_selection_id(b_name, bf_runners) if mkt_ok else None

    a_info = odds.get(a_sel_id, {}) if (mkt_ok and a_sel_id) else {}
    b_info = odds.get(b_sel_id, {}) if (mkt_ok and b_sel_id) else {}

    a_live = a_info.get("back") or top1.get("sp_dec")
    b_live = b_info.get("back") or top2.get("sp_dec")

    if mkt_ok and a_live is None:
        if not silent:
            send(f"⚠️ 📝 {race_label}\n⭐ Pick 1 {a_name} — no price, treating as late NR")
        a_info = {"status": "REMOVED"}
    if mkt_ok and b_live is None:
        b_info = {"status": "REMOVED"}

    if a_live is None and not mkt_ok:
        logger.error(f"No prices and no market for {race_label} — skipping")
        return

    # ── Non-runner checks ─────────────────────────────────────────────────────
    if mkt_ok and a_info.get("status") == "REMOVED":
        logger.info(f"Pick 1 {a_name} NR — attempting P2-only bet")
        fallback_name, fallback_price, fallback_sel = _find_fallback_pick(
            race, [a_name], odds, bf_runners
        )
        if fallback_name and b_live:
            if not silent:
                send(f"⚠️ 📝 {race_label}\n⭐ Pick 1 {a_name} - NR, promoting P2 {b_name} to P1")
            a_name = b_name; a_live = b_live; a_sel_id = b_sel_id; a_info = b_info
            fallback2, fallback2_price, fallback2_sel = _find_fallback_pick(
                race, [a_name], odds, bf_runners
            )
            if fallback2:
                b_name = fallback2; b_live = fallback2_price
                b_sel_id = fallback2_sel
                b_info = odds.get(fallback2_sel, {}) if fallback2_sel else {}
            else:
                b_name = "?"; b_live = None; b_sel_id = None; b_info = {}
        else:
            if not silent:
                send(f"⏭️ 📝 <b>PAPER SKIP - {race_label}</b>\n⭐ Pick 1 {a_name} - NR, no viable substitute")
            return

    if mkt_ok and b_info.get("status") == "REMOVED":
        fallback_name, fallback_price, fallback_sel = _find_fallback_pick(
            race, [a_name, b_name], odds, bf_runners
        )
        if fallback_name:
            logger.info(f"Pick 2 {b_name} NR - substituting {fallback_name}")
            if not silent:
                send(f"⚠️ 📝 {race_label}\n🔵 Pick 2 {b_name} - NR, using {fallback_name} @ {fallback_price:.2f}")
            b_name = fallback_name; b_live = fallback_price
            b_sel_id = fallback_sel
            b_info = odds.get(fallback_sel, {}) if fallback_sel else {}
        else:
            if not silent:
                send(f"⏭️ 📝 <b>PAPER SKIP - {race_label}</b>\n🔵 Pick 2 {b_name} - NR, no viable substitute")
            return

    # ── Stake calculation — Strategy H ───────────────────────────────────────
    from predict_v2 import _sp_free_score
    a_score = _sp_free_score(top1) if top1 else 0
    b_score = _sp_free_score(top2) if top2 else 0

    stake_a     = win_stake_for_pick(a_live, a_score, is_chase=is_chase)
    stake_b     = p2_win_stake_for_pick(b_live or 0.0, b_score, is_hurdle=is_hurdle)
    stake_place = place_stake_for_pick(b_score, tier, sp=b_live or 0.0,
                                       is_jump=is_jump, n_runners=n_runners,
                                       is_chase=is_chase)

    if stake_a == 0:
        if not silent:
            reason = f"Pick 1 {a_name} @ {a_live} below min price" if a_live else "no price"
            send(f"⏭️ 📝 <b>PAPER SKIP - {race_label}</b>\n{reason}")
        return

    actual_a, _, _, _ = apply_liquidity(stake_a, 0.0, 0.0, 0.0)

    # ── Build notification ────────────────────────────────────────────────────
    p2_win_note = f" | P2 win: £{stake_b:.0f}" if stake_b > 0 else ""
    lines = [
        f"📝 <b>PAPER BET - {race_label}</b>",
        f"{tier_label}",
        f"Balance: £{balance:.2f} | Profit: £{profit:.2f} | "
        f"P1 win: £{stake_a:.0f}{p2_win_note} | Place: £{stake_place:.0f}"
        + (" (jump only)" if stake_place > 0 else ""),
        "------------------------------",
    ]
    if not mkt_ok:
        lines.append("⚠️ No Betfair market - using RA odds")

    # ── Paper bets list ───────────────────────────────────────────────────────
    paper_bets = []

    if actual_a > 0:
        sp_display = f"@ {a_live:.2f} " if a_live and a_live >= 1.01 else ""
        lines.append(f"📝 ⭐ Pick 1: {a_name} {sp_display}— £{actual_a:.0f}")
        paper_bets.append({
            "horse": a_name, "price": None, "stake": actual_a,
            "label": "⭐ Pick 1", "bsp": True,
        })

    # P2 win bet — hurdle races only (Strategy H)
    if stake_b > 0 and b_name and b_name != "?" and b_live:
        sp_display_b = f"@ {b_live:.2f} " if b_live >= 1.01 else ""
        lines.append(f"📝 🔵 Pick 2 (win): {b_name} {sp_display_b}— £{stake_b:.0f}")
        paper_bets.append({
            "horse": b_name, "price": None, "stake": stake_b,
            "label": "🔵 Pick 2", "bsp": True,
        })

    # ── Place bets — P1 all jump, P2 hurdle only ──────────────────────────────
    place_bets  = []
    cons_places = _race_cons_places(race)

    if not silent and stake_place > 0:
        try:
            place_mkt, _ = find_place_market(race)
            place_odds_map = {}
            place_runners  = []
            if place_mkt is not None:
                place_odds_map = get_market_odds(place_mkt.market_id)
                place_runners  = place_mkt.runners or []
                if not place_odds_map:
                    off_utc = _to_utc(race.get("off_dt",""))
                    now_utc = datetime.now(timezone.utc)
                    mins_to_off = (off_utc - now_utc).total_seconds() / 60 if off_utc else 0
                    if mins_to_off > 3:
                        logger.info(f"{race_label}: place market empty, retrying in 90s")
                        time.sleep(90)
                        place_odds_map = get_market_odds(place_mkt.market_id)

                # Strategy H: P1 place all jump; P2 place hurdle only
                place_horses = [a_name]
                if is_hurdle and b_name and b_name != "?":
                    place_horses.append(b_name)
                place_type = "hurdle" if is_hurdle else "chase/jump"
                place_lines = ["------------------------------",
                               f"📍 <b>Place bets — {place_type} (£{stake_place:.0f} each)</b>"]

                for horse in place_horses:
                    if not horse or horse == "?":
                        continue
                    sel_id = find_selection_id(horse, place_runners)
                    if sel_id is None:
                        place_lines.append(f"⚠️ 📍 {horse} — not found in place market")
                        continue
                    p_info  = place_odds_map.get(sel_id, {})
                    p_price = p_info.get("back")
                    if not p_price or p_price < 1.1:
                        place_lines.append(f"⏭️ 📍 {horse} — no viable place price")
                        continue
                    place_bets.append({
                        "horse":       horse,
                        "price":       p_price,
                        "stake":       stake_place,
                        "cons_places": cons_places,
                    })
                    place_lines.append(f"  📍 {horse} @ {p_price:.2f}")

                lines += place_lines
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
        kwargs = {"place_bets": place_bets if place_bets else None,
                  "silent": silent},
        daemon = True,
        name   = f"PaperSettle_{race.get('race_id', '')}",
    )
    t.start()


# ── Unified bet job ───────────────────────────────────────────────────────────

def bet_job(race: dict, state: dict):
    from betfair.commands import is_betting_allowed
    from predict_v2 import get_blended_picks, TIER_LABELS
    from utils.helpers import is_non_runner

    # Re-pick from active runners at bet time
    all_runners = race.get("all_runners") or []
    active      = [r for r in all_runners if not is_non_runner(r)]
    if active and len(active) >= 2:
        raw_meta = {
            "class":   race.get("race_class") or race.get("class", ""),
            "surface": race.get("surface", "Turf"),
            "type":    race.get("type", "Unknown"),
        }
        new_tier, new_p1, new_p2, new_reasons = get_blended_picks(
            active, mw_p1=0.0, mw_p2=0.0, raw_race=raw_meta
        )
        if new_p1:
            race = {
                **race,
                "tier":         new_tier,
                "tier_label":   TIER_LABELS.get(new_tier, "·   STANDARD"),
                "tier_reasons": new_reasons,
                "top1":         new_p1,
                "top2":         new_p2,
                "all_runners":  active,
            }

    tier = race.get("tier", 0)
    mode = state.get("mode", "paper")
    live_allowed  = is_betting_allowed(state, tier, live=True)
    paper_allowed = is_betting_allowed(state, tier, live=False)

    if not paper_allowed and not live_allowed:
        logger.info(f"Globally paused - skipping {race.get('off')} {race.get('course')}")
        return

    now_utc = datetime.now(timezone.utc)
    off_utc = _to_utc(race.get("off_dt", ""))
    if off_utc and now_utc >= off_utc:
        send(
            f"⏭️ <b>MISSED</b> - {race.get('off')} {race.get('course')}\n"
            f"Race already started at job fire time."
        )
        return

    if mode == "live":
        if live_allowed:
            _live_bet_job(race, state)
        else:
            logger.info(f"Live bet paused for tier {tier} — running paper only")
            send(f"⏸️ Live bet paused ({TIER_LABELS.get(tier,'?').strip()}) — paper tracking only")
        _paper_bet_job(race, state, silent=False)
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
    paper_place= state.get("paper_place_pnl", 0.0)
    live_wins  = sum(1 for b in live_bets  if b.get("total_pnl", 0) > 0)
    paper_wins = sum(1 for b in paper_bets if b.get("total_pnl", 0) > 0)

    profit_sign = "+" if profit >= 0 else ""
    lines = [
        f"📋 <b>BETFAIR DAILY SUMMARY - {today}</b>",
        f"Mode: {mode}",
        "==============================",
        f"Balance:          £{bal:.2f}",
        f"Cumulative P&L:   {profit_sign}£{profit:.2f}  (win + place)",
        f"True total P&L:   {'+' if state.get('total_pnl',0)>=0 else ''}£{state.get('total_pnl',0):.2f}  (never reset)",
        f"Banked profit:    £{state.get('banked_profit', 0.0):.2f}",
        f"Tier pots:\n{tier_profit_summary(state)}",
    ]

    if paper_bets:
        sign      = "+" if paper_pnl >= 0 else ""
        p_sign    = "+" if paper_place >= 0 else ""
        combined  = round(paper_pnl + paper_place, 2)
        c_sign    = "+" if combined >= 0 else ""
        lines += [
            "-- 📝 Paper ------------------",
            f"Races: {len(paper_bets)} | Wins: {paper_wins} | Losses: {len(paper_bets)-paper_wins}",
            f"Win P&L:   {sign}£{paper_pnl:.2f}",
            f"Place P&L: {p_sign}£{paper_place:.2f}",
            f"Combined:  {c_sign}£{combined:.2f}",
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
        tier_profit_summary(state),
        "==============================",
    ]
    send_chunks("\n".join(lines))

    from betfair.state import eod_loss_check
    combined_day = round(paper_pnl + paper_place, 2)
    loss_alert = eod_loss_check(state, combined_day)
    if loss_alert:
        send(loss_alert)


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
        lines = [
            f"🤖 <b>BETFAIR BOT v3</b> {mode_icon} {mode}",
            "==============================",
            f"Balance:          £{bal:.2f}",
            f"Cumulative P&L:   {profit_sign}£{profit:.2f}  (win + place)",
            f"True total P&L:   {'+' if state.get('total_pnl',0)>=0 else ''}£{state.get('total_pnl',0):.2f}  (never reset)",
            f"Banked profit:    £{state.get('banked_profit', 0.0):.2f}",
            tier_profit_summary(state),
            f"Betting:          {'⏸️ PAUSED' if paused else '▶️ ACTIVE'}",
            f"Notifications:    {'🔕 MUTED' if muted else '🔔 ON'}",
            f"Live P&L:         {'+' if live_pnl >= 0 else ''}£{live_pnl:.2f}",
            f"Paper P&L:        {'+' if paper_pnl >= 0 else ''}£{paper_pnl:.2f}",
            f"Bet timing:       T-{BET_BEFORE_MINUTES} mins",
            "------------------------------",
            f"Total races: {len(races)} | Qualifying: {len(qualifying)} | Bets remaining: {scheduled}",
            f"Tiers: {tier_summary or 'none'}",
            f"Filters: Not Heavy | Not Irish staying chase | Class 2 excluded | Class 5 flat excluded",
            "------------------------------",
        ]

        for r in sorted(qualifying, key=lambda x: x.get("off", "99:99")):
            top1        = r.get("top1") or {}
            top2        = r.get("top2") or {}
            tier        = r.get("tier", 0)
            badge       = (r.get("tier_label") or "·").split()[0]
            a_price     = top1.get("sp_dec")
            b_price     = top2.get("sp_dec")
            r_tier      = r.get("tier", 0)
            n_r         = len(r.get("all_runners") or [])
            is_jump_r   = _is_jump_race(r)
            is_hurdle_r = _is_hurdle_race(r)
            is_chase_r  = _is_chase_race(r)

            from predict_v2 import _sp_free_score
            a_score = _sp_free_score(top1) if top1 else 0
            b_score = _sp_free_score(top2) if top2 else 0

            s_a     = win_stake_for_pick(a_price, a_score, is_chase=is_chase_r) if a_price else 0
            s_b     = p2_win_stake_for_pick(b_price or 0.0, b_score, is_hurdle=is_hurdle_r) if b_price else 0
            s_place = place_stake_for_pick(b_score, r_tier, sp=b_price or 0.0,
                                           is_jump=is_jump_r, n_runners=n_r,
                                           is_chase=is_chase_r)

            off_dt    = _parse_off_dt(r)
            bet_at    = (off_dt - timedelta(minutes=BET_BEFORE_MINUTES) + timedelta(hours=1)).strftime("%H:%M") if off_dt else "?"
            p1_note   = " (below min→skip)" if (a_price and a_price < MIN_PICK1_PRICE) else ""
            place_tag = " 📍" if s_place > 0 else ""
            p2_note   = f" win=£{s_b:.0f}" if s_b > 0 else f" place=£{s_place:.0f}"

            lines.append(
                f"{badge} <b>{r.get('off','?')} {r.get('course','?')}</b>"
                f" [bet@{bet_at}]{place_tag}\n"
                f"  ⭐ {top1.get('horse','?')} ({top1.get('sp','?')}){p1_note} win=£{s_a:.0f} | "
                f"🔵 {top2.get('horse','?')} ({top2.get('sp','?')}){p2_note}"
            )

        if not qualifying:
            lines.append("No qualifying races today.")
        lines.append("==============================")
        send_chunks("\n".join(lines))

    # Re-queue pending settlements from before restart
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
            id=f"bet_{race.get('race_id', '')}",
            replace_existing=True,
        )
        scheduled += 1
    mode = state.get("mode", "paper").upper()
    send(
        f"🔄 <b>Midday refresh</b> - {scheduled} races scheduled\n"
        f"Mode: {mode} | Balance: £{bal:.2f} | Profit: £{profit:.2f}"
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
