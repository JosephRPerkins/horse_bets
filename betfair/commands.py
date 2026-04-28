"""
betfair/commands.py
Telegram command listener for the Betfair bot.

Commands:
  /paper        — switch to paper trading (simulated bets, no real money)
  /live         — switch to live trading (real bets on Betfair Exchange)
  /mute         — silence all Betfair bot Telegram notifications
  /unmute       — resume notifications
  /stop         — pause ALL betting (both modes, all tiers)
  /start        — resume ALL betting
  /stopelite    — pause ELITE tier only
  /startelite   — resume ELITE tier only
  /stopstrong   — pause STRONG tier only
  /startstrong  — resume STRONG tier only
  /stopgood     — pause GOOD tier only
  /startgood    — resume GOOD tier only
  /resetprofit  — reset all P&L and tier pots before going live
  /resetpots    — reset tier profit pots only (keeps daily P&L)
  /breaker      — override circuit breaker and resume betting
  /streakstart  — enable live streak place betting
  /streakstop   — disable streak betting
  /streakstatus — streak stake, P&L, win streak
  /status       — balance, mode, today's P&L, tier pot summary
  /races        — today's qualifying races with stakes
  /help         — command list
"""

import logging
import time
import threading
import requests
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config

from .strategy import (
    get_stake, pick_stakes, MIN_PICK1_PRICE, MIN_PICK2_PRICE,
    next_tier_threshold, BET_TIERS,
)
from .state    import (
    save, get_tier_profit, update_tier_profit,
    reset_tier_profits, tier_profit_summary,
)
from .notify   import send, send_chunks, set_muted, is_muted
from .api      import get_balance

logger = logging.getLogger("betfair.commands")

CARD_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "data", "cards", "today.json"
)

# Tier pause state keys — stored in state dict so they persist across restarts
TIER_PAUSE_KEYS = {
    4: "tier_paused_elite",
    3: "tier_paused_strong",
    2: "tier_paused_good",
}
TIER_NAMES = {4: "ELITE", 3: "STRONG", 2: "GOOD"}
TIER_BADGES = {4: "💎", 3: "🔥", 2: "✓"}

HELP_TEXT = """\
🤖 <b>Betfair Bot — System C</b>
══════════════════════════════
<b>Mode</b>
/paper   — paper trading (simulated, safe — default)
/live    — live trading (real bets on exchange)

<b>Notifications</b>
/mute    — silence all notifications
/unmute  — resume notifications

<b>Betting control — all tiers</b>
/stop    — pause ALL betting
/start   — resume ALL betting

<b>Tier controls</b>
/startelite   — resume ELITE tier
/stopelite    — pause ELITE tier
/startstrong  — resume STRONG tier
/stopstrong   — pause STRONG tier
/startgood    — resume GOOD tier
/stopgood     — pause GOOD tier

<b>Profit management</b>
/resetprofit  — reset all P&L + tier pots (use before going live)
/resetpots    — reset tier pots only (keeps daily P&L)

<b>Streak betting</b>
/streakstart  — enable streak place betting
/streakstop   — disable streak betting
/streakstatus — stake, P&L, win streak

/breaker      — override circuit breaker and resume

/status       — full status: balance, mode, tier pots
/races        — today's qualifying races with stakes
/help         — this list
══════════════════════════════
<b>System C tiers</b>
  💎 ELITE:  WIN+PLACE P1+P2 — ~67% P1 win
  🔥 STRONG: WIN+PLACE P1+P2 — ~49% P1 win
  ✓ GOOD:   WIN only P1+P2  — ~36% P1 win
<b>Stakes</b>
  ELITE:  £2 → £4 @ £30 profit → £6 @ £60
  STRONG: £2 → £4 @ £50 profit → £6 @ £100
  GOOD:   £2 → £4 @ £75 profit → £6 @ £150\
"""


def _is_tier_paused(state: dict, tier: int) -> bool:
    """Return True if this specific tier has been paused."""
    key = TIER_PAUSE_KEYS.get(tier)
    return bool(state.get(key, False)) if key else False


def _set_tier_paused(state: dict, tier: int, paused: bool):
    key = TIER_PAUSE_KEYS.get(tier)
    if key:
        state[key] = paused
        save(state)


def is_betting_allowed(state: dict, tier: int, live: bool = False) -> bool:
    """
    Return True if betting is allowed for this race.

    live=False (paper): only blocked by global /stop. Tier pause flags
    and circuit breaker do NOT affect paper — it always runs as a shadow.

    live=True: blocked by global pause, tier-specific pause, or circuit breaker.
    """
    if state.get("betting_paused", False):
        return False
    if live and _is_tier_paused(state, tier):
        return False
    return True


def _races_status(state: dict = None) -> str:
    if not os.path.exists(CARD_PATH):
        return "⚠️ today.json not found — main bot may not have run yet."
    try:
        with open(CARD_PATH) as f:
            data = json.load(f)
    except Exception as e:
        return f"⚠️ Could not read today.json: {e}"

    from .strategy import qualifies

    races      = data.get("races", [])
    qualifying = [r for r in races if qualifies(r)]
    if not qualifying:
        return "No qualifying races today."

    bal = get_balance()
    lines = [
        f"🏇 <b>Today's qualifying races</b> ({len(qualifying)} of {len(races)})",
        f"Balance: £{bal:.2f}",
        "──────────────────────────────",
    ]

    for r in sorted(qualifying, key=lambda x: x.get("off", "99:99")):
        tier    = r.get("tier", 0)
        badge   = TIER_BADGES.get(tier, "·")
        top1    = r.get("top1") or {}
        top2    = r.get("top2") or {}
        a_price = top1.get("sp_dec")
        b_price = top2.get("sp_dec")
        n_r     = len(r.get("all_runners") or [])
        profit  = get_tier_profit(state or {}, tier)

        s_a, s_b, s_p = pick_stakes(profit, tier, a_price, b_price, n_runners=n_r)

        p1_note    = " ⚠️odds-on" if (a_price and a_price < MIN_PICK1_PRICE) else ""
        p2_note    = " ⚠️under 2/1" if (b_price and b_price is not None and b_price < MIN_PICK2_PRICE) else ""
        place_note = f" 📍£{s_p:.0f}" if s_p > 0 else ""
        paused_note = " ⏸️" if _is_tier_paused(state or {}, tier) else ""

        lines.append(
            f"{badge} <b>{r.get('off','?')} {r.get('course','?')}</b>{paused_note}\n"
            f"  ⭐ {top1.get('horse','?')} ({top1.get('sp','?')}){p1_note} £{s_a:.0f} | "
            f"🔵 {top2.get('horse','?')} ({top2.get('sp','?')}){p2_note} £{s_b:.0f}"
            f"{place_note}"
        )

    return "\n".join(lines)


def handle_command(cmd: str, state: dict) -> None:
    cmd = cmd.strip().lower()
    if "@" in cmd:
        cmd = cmd[:cmd.index("@")]

    # ── Mode ──────────────────────────────────────────────────────────────────

    if cmd == "/paper":
        old_mode = state.get("mode", "paper")
        state["mode"] = "paper"
        save(state)
        msg = (
            f"📝 <b>Switched to PAPER mode</b>\n"
            f"Bets will be simulated — no real money placed.\n"
            f"Paper P&L today: {'+' if state.get('paper_daily_pnl', 0) >= 0 else ''}"
            f"£{state.get('paper_daily_pnl', 0.0):.2f}"
        )
        if old_mode == "live":
            msg += "\n⚠️ Switched from LIVE — no pending orders affected."
        send(msg)
        logger.info("Switched to paper mode")

    elif cmd == "/live":
        bal        = get_balance()
        cum_profit = state.get("cumulative_profit", 0.0)
        banked     = state.get("banked_profit", 0.0)
        state["mode"] = "live"
        save(state)
        warning = ""
        if cum_profit > 0 or banked > 0:
            warning = (
                f"\n⚠️ Paper cumulative profit: £{cum_profit:.2f} | Banked: £{banked:.2f}\n"
                f"Send /resetprofit to clear paper figures before live trading."
            )
        send(
            f"💰 <b>Switched to LIVE mode</b>\n"
            f"Real bets will now be placed on Betfair Exchange.\n"
            f"Balance: £{bal:.2f}{warning}\n"
            f"⚠️ This uses real money. Send /paper to return to simulation."
        )
        logger.info("Switched to live mode")

    # ── Global betting control ─────────────────────────────────────────────────

    elif cmd == "/stop":
        state["betting_paused"] = True
        save(state)
        send("⏸️ <b>All betting paused</b>\nNo new bets will be placed.\nSend /start to resume.")
        logger.info("All betting paused")

    elif cmd == "/start":
        state["betting_paused"] = False
        save(state)
        mode = state.get("mode", "paper").upper()
        bal  = get_balance()
        tier_lines = []
        for tc in (4, 3, 2):
            if _is_tier_paused(state, tc):
                tier_lines.append(f"  {TIER_BADGES[tc]} {TIER_NAMES[tc]}: still paused (use /start{TIER_NAMES[tc].lower()})")
        pause_note = "\n" + "\n".join(tier_lines) if tier_lines else ""
        send(
            f"▶️ <b>All betting resumed</b>\n"
            f"Mode: {mode} | Balance: £{bal:.2f}{pause_note}"
        )
        logger.info("All betting resumed")

    # ── Tier-specific controls ─────────────────────────────────────────────────

    elif cmd in ("/startelite", "/stopelite",
                 "/startstrong", "/stopstrong",
                 "/startgood", "/stopgood"):

        tier_map = {
            "/startelite":  (4, False), "/stopelite":  (4, True),
            "/startstrong": (3, False), "/stopstrong": (3, True),
            "/startgood":   (2, False), "/stopgood":   (2, True),
        }
        tier, pausing = tier_map[cmd]
        _set_tier_paused(state, tier, pausing)
        tname  = TIER_NAMES[tier]
        badge  = TIER_BADGES[tier]
        profit = get_tier_profit(state, tier)
        stake  = get_stake(profit, tier)
        nxt    = next_tier_threshold(profit, tier)
        sign   = "+" if profit >= 0 else ""
        action = "⏸️ paused" if pausing else "▶️ resumed"
        send(
            f"{badge} <b>{tname} betting {action}</b>\n"
            f"Pot: {sign}£{profit:.2f} | Stake: £{stake:.0f} | Next: £{nxt:.0f}"
        )
        logger.info(f"{tname} betting {'paused' if pausing else 'resumed'}")

    # ── Profit management ──────────────────────────────────────────────────────

    elif cmd == "/resetprofit":
        old_cum    = state.get("cumulative_profit", 0.0)
        old_banked = state.get("banked_profit", 0.0)
        old_place  = state.get("paper_place_pnl", 0.0)
        old_pots   = state.get("tier_profit", {})
        state["cumulative_profit"]  = 0.0
        state["banked_profit"]      = 0.0
        state["paper_daily_pnl"]    = 0.0
        state["paper_daily_bets"]   = []
        state["paper_place_pnl"]    = 0.0
        state["profit_milestone"]   = 0.0
        reset_tier_profits(state)
        save(state)
        send(
            f"🔄 <b>Full profit reset</b>\n"
            f"Cumulative P&L: £{old_cum:.2f} → £0.00\n"
            f"Banked profit:  £{old_banked:.2f} → £0.00\n"
            f"Place P&L:      £{old_place:.2f} → £0.00\n"
            f"ELITE pot:  £{old_pots.get('4',0):.2f} → £0.00\n"
            f"STRONG pot: £{old_pots.get('3',0):.2f} → £0.00\n"
            f"GOOD pot:   £{old_pots.get('2',0):.2f} → £0.00\n"
            f"All stakes reset to £2/horse (base tier)."
        )
        logger.info(f"Full profit reset: cum={old_cum:.2f} banked={old_banked:.2f}")

    elif cmd == "/resetpots":
        old_pots = state.get("tier_profit", {})
        reset_tier_profits(state)
        send(
            f"🔄 <b>Tier pots reset</b>\n"
            f"ELITE:  £{old_pots.get('4',0):.2f} → £0.00\n"
            f"STRONG: £{old_pots.get('3',0):.2f} → £0.00\n"
            f"GOOD:   £{old_pots.get('2',0):.2f} → £0.00\n"
            f"Daily P&L unchanged. Stakes reset to £2/horse per tier."
        )
        logger.info("Tier pots reset")

    elif cmd == "/breaker":
        if not state.get("circuit_paused", False):
            send("ℹ️ Circuit breaker is not currently active.")
            return
        peak   = max(state.get("profit_history", [0.0]))
        profit = state.get("cumulative_profit", 0.0)
        state["circuit_paused"] = False
        state["betting_paused"] = False
        state["profit_history"] = []
        save(state)
        send(
            f"⚡ <b>Circuit breaker overridden</b>\n"
            f"Betting resumed.\n"
            f"Peak was £{peak:.2f} | Current: £{profit:.2f}\n"
            f"Loss window reset — monitoring fresh from here.\n"
            f"⚠️ Proceed with caution."
        )
        logger.info(f"Circuit breaker overridden — peak={peak:.2f} profit={profit:.2f}")

    # ── Streak betting ─────────────────────────────────────────────────────────

    elif cmd == "/streakstart":
        from betfair.strategy import get_place_stake
        profit    = state.get("cumulative_profit", 0.0)
        new_stake = get_place_stake(profit)
        state["streak_active"]     = True
        state["streak_stake"]      = new_stake
        state["streak_peak_stake"] = new_stake
        save(state)
        send(
            f"📈 <b>Streak betting ENABLED</b>\n"
            f"Starting stake: £{new_stake:.0f}/horse (place market)\n"
            f"Both picks must place to win.\n"
            f"Win: reinvest 50% | Loss: reset to £{new_stake:.0f}\n"
            f"Resets daily at midnight."
        )
        logger.info(f"Streak betting enabled at £{new_stake:.0f}/horse")

    elif cmd == "/streakstop":
        state["streak_active"] = False
        save(state)
        pnl  = state.get("streak_daily_pnl", 0.0)
        sign = "+" if pnl >= 0 else ""
        send(
            f"📈 <b>Streak betting DISABLED</b>\n"
            f"Today's streak P&L: {sign}£{pnl:.2f}\n"
            f"Best streak: {state.get('streak_best', 0)} wins\n"
            f"Peak stake: £{state.get('streak_peak_stake', 2.0):.2f}"
        )
        logger.info("Streak betting disabled")

    elif cmd == "/streakstatus":
        active = state.get("streak_active", False)
        stake  = state.get("streak_stake", 2.0)
        pnl    = state.get("streak_daily_pnl", 0.0)
        wins   = state.get("streak_wins", 0)
        best   = state.get("streak_best", 0)
        peak   = state.get("streak_peak_stake", 2.0)
        sign   = "+" if pnl >= 0 else ""
        send(
            f"📈 <b>Streak Status</b>\n"
            f"Active:     {'✅ YES' if active else '❌ NO'}\n"
            f"Next stake: £{stake:.2f}/horse\n"
            f"Win streak: {wins} (best: {best})\n"
            f"Peak stake: £{peak:.2f}\n"
            f"Today P&L:  {sign}£{pnl:.2f}"
        )

    # ── Notifications ──────────────────────────────────────────────────────────

    elif cmd == "/mute":
        state["muted"] = True
        save(state)
        set_muted(True)
        send("🔕 <b>Notifications muted</b>\nSend /unmute to resume.")
        logger.info("Notifications muted")

    elif cmd == "/unmute":
        state["muted"] = False
        save(state)
        set_muted(False)
        bal = get_balance()
        send(
            f"🔔 <b>Notifications resumed</b>\n"
            f"Balance: £{bal:.2f} | Mode: {state.get('mode','paper').upper()}"
        )
        logger.info("Notifications unmuted")

    # ── Status ─────────────────────────────────────────────────────────────────

    elif cmd == "/status":
        bal    = get_balance()
        mode   = state.get("mode", "paper").upper()
        paused = state.get("betting_paused", False)
        muted  = is_muted()
        circuit = state.get("circuit_paused", False)

        live_pnl   = state.get("daily_pnl", 0.0)
        paper_pnl  = state.get("paper_daily_pnl", 0.0)
        live_bets  = state.get("daily_bets", [])
        paper_bets = state.get("paper_daily_bets", [])
        live_wins  = sum(1 for b in live_bets  if b.get("total_pnl", 0) > 0)
        paper_wins = sum(1 for b in paper_bets if b.get("total_pnl", 0) > 0)

        mode_icon = "💰" if mode == "LIVE" else "📝"
        lines = [
            "📊 <b>Betfair Bot Status — System C</b>",
            "──────────────────────────────",
            f"Mode:          {mode_icon} {mode}",
            f"Betting:       {'🛑 CIRCUIT BREAKER' if circuit else '⏸️ PAUSED' if paused else '▶️ ACTIVE'}",
            f"Notifications: {'🔕 MUTED' if muted else '🔔 ON'}",
            f"Balance:       £{bal:.2f}",
        ]

        # Tier-specific pause status
        tier_statuses = []
        for tc in (4, 3, 2):
            if _is_tier_paused(state, tc):
                tier_statuses.append(f"{TIER_BADGES[tc]} {TIER_NAMES[tc]}: ⏸️")
        if tier_statuses:
            lines.append("Tier pauses:   " + " | ".join(tier_statuses))

        lines.append("──────────────────────────────")

        # Per-tier pot summary
        lines.append(tier_profit_summary(state))
        lines.append("──────────────────────────────")

        if live_bets or mode == "LIVE":
            sign = "+" if live_pnl >= 0 else ""
            lines.append(
                f"💰 Live P&L:  {sign}£{live_pnl:.2f} "
                f"({live_wins}W / {len(live_bets)-live_wins}L)"
            )
        if paper_bets or mode == "PAPER":
            sign = "+" if paper_pnl >= 0 else ""
            lines.append(
                f"📝 Paper P&L: {sign}£{paper_pnl:.2f} "
                f"({paper_wins}W / {len(paper_bets)-paper_wins}L)"
            )

        # Last 5 settled races
        recent = (live_bets if mode == "LIVE" else paper_bets)[-5:]
        if recent:
            lines.append("──────────────────────────────")
            for b in recent:
                icon = "✅" if b.get("total_pnl", 0) > 0 else "❌"
                sign = "+" if b.get("total_pnl", 0) >= 0 else ""
                lines.append(f"  {icon} {b['race']} → {sign}£{b['total_pnl']:.2f}")

        send("\n".join(lines))

    elif cmd == "/races":
        send_chunks(_races_status(state))

    elif cmd == "/help":
        send(HELP_TEXT)

    else:
        send(f"❓ Unknown command: {cmd}\nSend /help for available commands.")


def start_command_listener(state: dict) -> None:
    """Poll Telegram for commands every 5 seconds in a background thread."""
    offset = [0]

    def _poll():
        while True:
            try:
                url  = (f"https://api.telegram.org/"
                        f"bot{config.BETFAIR_TELEGRAM_BOT_TOKEN}/getUpdates")
                resp = requests.get(
                    url, params={"offset": offset[0], "timeout": 10}, timeout=15
                )
                if resp.status_code == 200:
                    for update in resp.json().get("result", []):
                        offset[0] = update["update_id"] + 1
                        txt = (update.get("message", {}).get("text") or "").strip()
                        if txt.startswith("/"):
                            logger.info(f"Command: {txt}")
                            handle_command(txt, state)
            except Exception as e:
                logger.error(f"Command listener error: {e}")
            time.sleep(5)

    t = threading.Thread(target=_poll, daemon=True, name="BetfairCommands")
    t.start()
    logger.info("Betfair command listener started")
